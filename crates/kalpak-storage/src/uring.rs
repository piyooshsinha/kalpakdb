//! Linux io_uring backend (`--features uring`).
//!
//! The win is batched submission on the group-commit path: a `put_many` of
//! N records costs one `io_uring_enter` carrying N write SQEs plus a
//! drain-ordered fsync SQE, instead of N+1 syscalls. Reads stay on `pread`
//! (a single synchronous read gains nothing from a ring round-trip).
//!
//! `O_DIRECT` is future work: it requires an aligned buffer pool (record
//! *offsets* are already 4 KiB-aligned, but heap buffers are not), so this
//! backend currently goes through the page cache like the portable one.

use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Result};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::Mutex;

use io_uring::{opcode, squeue, types, IoUring};

use crate::io::{IoBackend, SegmentFile};

/// SQEs submitted per ring round; batches larger than this loop.
const RING_DEPTH: u32 = 128;

#[derive(Debug, Default, Clone, Copy)]
pub struct UringBackend;

pub struct UringFile {
    file: File,
    /// One ring per segment file, serialized: writers already hold the
    /// store's append lock, so contention here is nil.
    ring: Mutex<IoUring>,
}

impl IoBackend for UringBackend {
    type SegmentFile = UringFile;

    fn open(&self, path: &Path) -> Result<UringFile> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(UringFile {
            file,
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
}

impl SegmentFile for UringFile {
    fn len(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(&self.file, buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.submit_round(&[(offset, buf)], false)
    }

    fn sync(&self) -> Result<()> {
        self.file.sync_data()
    }

    fn write_batch(&self, writes: &[(u64, &[u8])], sync_after: bool) -> Result<()> {
        // Leave room for the fsync SQE in the final round.
        let chunk = (RING_DEPTH - 1) as usize;
        let mut rounds = writes.chunks(chunk).peekable();
        while let Some(round) = rounds.next() {
            let last = rounds.peek().is_none();
            self.submit_round(round, sync_after && last)?;
        }
        if writes.is_empty() && sync_after {
            self.sync()?;
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
}
