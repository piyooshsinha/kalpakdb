//! Linux io_uring backend (`--features uring`).
//!
//! Two wins over the portable backend:
//!
//! 1. **Batched submission** on the group-commit path: a `put_many` of N
//!    records costs one `io_uring_enter` carrying the write SQEs plus a
//!    drain-ordered fsync SQE, instead of N+1 syscalls.
//! 2. **`O_DIRECT`**: segment files are opened with `O_DIRECT` where the
//!    filesystem supports it, bypassing the OS page cache — KalpakDB's warm
//!    tier (moka) IS the cache, so the kernel caching the same bytes again
//!    is pure waste and evicts more useful pages (cache thrashing under
//!    context offload, exactly what the project plan called out). Records
//!    are 4 KiB-aligned on disk by design; this module supplies the missing
//!    pieces — pointer-aligned buffers for writes and an aligned read
//!    window for sub-record reads. On filesystems without `O_DIRECT`
//!    support (e.g. tmpfs) the backend transparently falls back to
//!    page-cached I/O.
//!
//! Reads stay on `pread` (a single synchronous read gains nothing from a
//! ring round-trip); writes go through the ring.

use std::alloc::{alloc, dealloc, Layout};
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Result};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::Mutex;

use io_uring::{opcode, squeue, types, IoUring};

use crate::io::{IoBackend, SegmentFile};
use crate::BLOCK_ALIGN;

/// SQEs submitted per ring round; batches larger than this loop.
const RING_DEPTH: u32 = 128;

const ALIGN: usize = BLOCK_ALIGN as usize;

/// Heap buffer aligned to the direct-I/O boundary.
struct AlignedBuf {
    ptr: NonNull<u8>,
    layout: Layout,
}

// SAFETY: AlignedBuf owns its allocation exclusively.
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    fn zeroed(len: usize) -> Self {
        let layout = Layout::from_size_align(len.max(ALIGN), ALIGN).expect("valid layout");
        // SAFETY: layout has non-zero size.
        let raw = unsafe { alloc(layout) };
        let ptr = NonNull::new(raw).expect("aligned allocation failed");
        // Zero so padding bytes written to disk are deterministic.
        unsafe { std::ptr::write_bytes(ptr.as_ptr(), 0, layout.size()) };
        Self { ptr, layout }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.layout.size()) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.layout.size()) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

fn align_down(v: u64) -> u64 {
    v & !(BLOCK_ALIGN - 1)
}

fn align_up(v: u64) -> u64 {
    v.div_ceil(BLOCK_ALIGN) * BLOCK_ALIGN
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UringBackend;

pub struct UringFile {
    file: File,
    /// Whether the file is opened with `O_DIRECT` (false on filesystems
    /// that reject it; everything then behaves like the portable backend,
    /// just with batched submission).
    direct: bool,
    /// One ring per segment file, serialized: writers already hold the
    /// store's append lock, so contention here is nil.
    ring: Mutex<IoUring>,
}

impl IoBackend for UringBackend {
    type SegmentFile = UringFile;

    fn open(&self, path: &Path) -> Result<UringFile> {
        let direct_attempt = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(libc::O_DIRECT)
            .open(path);
        let (file, direct) = match direct_attempt {
            Ok(f) => (f, true),
            // EINVAL/ENOTSUP: filesystem (e.g. tmpfs) refuses O_DIRECT.
            Err(_) => (
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(path)?,
                false,
            ),
        };
        Ok(UringFile {
            file,
            direct,
            ring: Mutex::new(IoUring::new(RING_DEPTH)?),
        })
    }
}

impl UringFile {
    /// Submit a slice of write SQEs (and optionally a trailing fsync) and
    /// wait for them all, surfacing the first failure.
    fn submit_round(&self, writes: &[(u64, &[u8])], fsync: bool) -> Result<()> {
        let mut ring = self.ring.lock().unwrap();
        let fd = types::Fd(self.file.as_raw_fd());
        let mut expected = 0u32;

        {
            let mut sq = ring.submission();
            for (offset, buf) in writes {
                let sqe = opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
                    .offset(*offset)
                    .build()
                    .user_data(expected as u64);
                // SAFETY: the buffers outlive submit_and_wait below — we do
                // not return until every CQE has been reaped.
                unsafe {
                    sq.push(&sqe).map_err(|e| Error::other(e.to_string()))?;
                }
                expected += 1;
            }
            if fsync {
                // IO_DRAIN: the fsync executes only after every prior SQE
                // in the ring completes, preserving write-then-flush order.
                let sqe = opcode::Fsync::new(fd)
                    .build()
                    .flags(squeue::Flags::IO_DRAIN)
                    .user_data(u64::MAX);
                unsafe {
                    sq.push(&sqe).map_err(|e| Error::other(e.to_string()))?;
                }
                expected += 1;
            }
        }

        ring.submit_and_wait(expected as usize)?;

        let mut completed = 0u32;
        for cqe in ring.completion() {
            completed += 1;
            let res = cqe.result();
            if res < 0 {
                return Err(Error::from_raw_os_error(-res));
            }
            // A short write would corrupt the record; records are small
            // enough (<= segment roll size) that the kernel writes them
            // fully or errors. Treat short as an error rather than retry.
            if cqe.user_data() != u64::MAX {
                let idx = cqe.user_data() as usize;
                if (res as usize) != writes[idx].1.len() {
                    return Err(Error::new(
                        ErrorKind::WriteZero,
                        format!("short io_uring write: {} of {}", res, writes[idx].1.len()),
                    ));
                }
            }
        }
        debug_assert_eq!(completed, expected);
        Ok(())
    }

    /// Copy `writes` into pointer-aligned buffers, coalescing contiguous
    /// runs (a group commit's appends are contiguous, so a whole batch
    /// usually becomes ONE aligned write).
    fn align_writes(writes: &[(u64, &[u8])]) -> Result<Vec<(u64, AlignedBuf)>> {
        let mut runs: Vec<(u64, AlignedBuf)> = Vec::new();
        let mut i = 0;
        while i < writes.len() {
            let start = writes[i].0;
            if !start.is_multiple_of(BLOCK_ALIGN) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "O_DIRECT write offset not aligned",
                ));
            }
            // Extend the run while the next write begins exactly where the
            // previous one ended.
            let mut end = start + writes[i].1.len() as u64;
            let mut j = i + 1;
            while j < writes.len() && writes[j].0 == end {
                end += writes[j].1.len() as u64;
                j += 1;
            }
            let run_len = align_up(end - start) as usize;
            let mut buf = AlignedBuf::zeroed(run_len);
            let mut at = 0usize;
            for (_, b) in &writes[i..j] {
                buf.as_mut_slice()[at..at + b.len()].copy_from_slice(b);
                at += b.len();
            }
            runs.push((start, buf));
            i = j;
        }
        Ok(runs)
    }

    fn write_batch_direct(&self, writes: &[(u64, &[u8])], sync_after: bool) -> Result<()> {
        let runs = Self::align_writes(writes)?;
        let borrowed: Vec<(u64, &[u8])> = runs
            .iter()
            .map(|(off, buf)| (*off, buf.as_slice()))
            .collect();
        // Coalesced runs are few; they fit one ring round in practice, but
        // chunk anyway for pathological inputs.
        let chunk = (RING_DEPTH - 1) as usize;
        let mut rounds = borrowed.chunks(chunk).peekable();
        while let Some(round) = rounds.next() {
            let last = rounds.peek().is_none();
            self.submit_round(round, sync_after && last)?;
        }
        Ok(())
    }
}

impl SegmentFile for UringFile {
    fn len(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        if !self.direct {
            return std::os::unix::fs::FileExt::read_exact_at(&self.file, buf, offset);
        }
        // O_DIRECT requires aligned offset, length, and pointer. Payload
        // reads land mid-record (offset+64), so read the covering aligned
        // window and slice out the request. Segment files only ever grow in
        // whole aligned records, so the window never crosses EOF.
        let start = align_down(offset);
        let end = align_up(offset + buf.len() as u64);
        let mut window = AlignedBuf::zeroed((end - start) as usize);
        std::os::unix::fs::FileExt::read_exact_at(&self.file, window.as_mut_slice(), start)?;
        let skip = (offset - start) as usize;
        buf.copy_from_slice(&window.as_slice()[skip..skip + buf.len()]);
        Ok(())
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        if self.direct {
            self.write_batch_direct(&[(offset, buf)], false)
        } else {
            self.submit_round(&[(offset, buf)], false)
        }
    }

    fn sync(&self) -> Result<()> {
        self.file.sync_data()
    }

    fn write_batch(&self, writes: &[(u64, &[u8])], sync_after: bool) -> Result<()> {
        if writes.is_empty() {
            if sync_after {
                self.sync()?;
            }
            return Ok(());
        }
        if self.direct {
            return self.write_batch_direct(writes, sync_after);
        }
        // Leave room for the fsync SQE in the final round.
        let chunk = (RING_DEPTH - 1) as usize;
        let mut rounds = writes.chunks(chunk).peekable();
        while let Some(round) = rounds.next() {
            let last = rounds.peek().is_none();
            self.submit_round(round, sync_after && last)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::BlockStore;
    use kalpak_core::BlockId;

    #[test]
    fn uring_roundtrip_and_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::open_with(UringBackend, dir.path()).unwrap();
        let a = store.put(b"uring block").unwrap();
        assert_eq!(store.get(&a).unwrap(), b"uring block");
        let b = store.put(b"uring block").unwrap();
        assert_eq!(a, b);
        assert_eq!(store.stats().blocks, 1);
    }

    #[test]
    fn uring_group_commit_batch() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::open_with(UringBackend, dir.path()).unwrap();
        // More writes than one ring round (RING_DEPTH=128) to exercise the
        // chunked submission path, with sizes crossing chunk boundaries.
        let payloads: Vec<Vec<u8>> = (0..300u32)
            .map(|i| i.to_le_bytes().repeat(64 * (1 + (i as usize % 5))))
            .collect();
        let ids = store
            .put_many(payloads.iter().map(|p| p.as_slice()))
            .unwrap();
        for (id, payload) in ids.iter().zip(&payloads) {
            assert_eq!(&store.get(id).unwrap(), payload);
        }
    }

    #[test]
    fn uring_survives_reopen_with_std_backend() {
        // Records written by the uring backend are byte-identical to the
        // portable backend's: a store written with one opens with the other.
        let dir = tempfile::tempdir().unwrap();
        let id;
        {
            let store = BlockStore::open_with(UringBackend, dir.path()).unwrap();
            id = store.put(b"cross-backend").unwrap();
        }
        let store = BlockStore::open(dir.path()).unwrap();
        assert_eq!(store.get(&id).unwrap(), b"cross-backend");
        assert_eq!(BlockId::of(b"cross-backend"), id);
    }

    #[test]
    fn direct_io_reports_mode_and_roundtrips() {
        // Whether O_DIRECT engaged depends on the filesystem under $TMPDIR;
        // either way the file must roundtrip sub-record reads correctly.
        let dir = tempfile::tempdir().unwrap();
        let backend = UringBackend;
        let f = backend.open(&dir.path().join("seg-x.klpk")).unwrap();
        eprintln!("O_DIRECT active: {}", f.direct);

        // One aligned record write, then a misaligned interior read — the
        // aligned-window path under O_DIRECT, plain pread otherwise.
        let mut record = vec![0u8; ALIGN * 2];
        for (i, b) in record.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        f.write_batch(&[(0, record.as_slice())], true).unwrap();
        let mut out = vec![0u8; 1000];
        f.read_at(&mut out, 64).unwrap();
        assert_eq!(out, record[64..1064]);
    }

    #[test]
    fn aligned_buf_is_aligned_and_zeroed() {
        let buf = AlignedBuf::zeroed(10);
        assert_eq!(buf.ptr.as_ptr() as usize % ALIGN, 0);
        assert_eq!(buf.layout.size(), ALIGN);
        assert!(buf.as_slice().iter().all(|&b| b == 0));
        let big = AlignedBuf::zeroed(ALIGN * 3 + 1);
        assert_eq!(big.ptr.as_ptr() as usize % ALIGN, 0);
    }

    #[test]
    fn contiguous_batch_coalesces_to_one_run() {
        let a = vec![1u8; ALIGN];
        let b = vec![2u8; ALIGN * 2];
        let c = vec![3u8; ALIGN];
        // a@0, b@4096, c@12288 are contiguous; d@20480 leaves a gap.
        let d = vec![4u8; ALIGN];
        let writes: Vec<(u64, &[u8])> = vec![
            (0, a.as_slice()),
            (ALIGN as u64, b.as_slice()),
            ((ALIGN * 3) as u64, c.as_slice()),
            ((ALIGN * 5) as u64, d.as_slice()),
        ];
        let runs = UringFile::align_writes(&writes).unwrap();
        assert_eq!(runs.len(), 2, "contiguous prefix must coalesce");
        assert_eq!(runs[0].0, 0);
        assert_eq!(runs[0].1.as_slice().len(), ALIGN * 4);
        assert_eq!(runs[0].1.as_slice()[ALIGN], 2);
        assert_eq!(runs[1].0, (ALIGN * 5) as u64);
    }
}
