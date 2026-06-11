use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use kalpak_core::{BlockId, Error};

use crate::io::{IoBackend, SegmentFile, StdBackend};
use crate::segment::{self, Location, HEADER_LEN};

/// Segments roll over once they pass this size (256 MiB). Small enough to
/// keep recovery scans and future compaction units manageable, large enough
/// to amortize file overhead.
const SEGMENT_ROLL_BYTES: u64 = 256 * 1024 * 1024;

/// Content-addressed block store over append-only segment files.
///
/// `put` is idempotent: storing bytes that already exist returns the existing
/// id without writing. `get` verifies the payload hash on every read, so
/// corruption is detected at the read site rather than propagated.
///
/// Locking is split so that **reads never wait on disk I/O**: records are
/// immutable once indexed, so readers only briefly lock `files` to clone an
/// `Arc` of the right segment handle, then read with no lock held. Writers
/// serialize on `append` (which owns the tail offset) and hold no shared
/// lock during `write_at`/`sync` — an fsync in `put_many` cannot stall a
/// `get`. The index is only updated after the sync, so readers can never
/// observe an unflushed block.
pub struct BlockStore<B: IoBackend = StdBackend> {
    backend: B,
    dir: PathBuf,
    index: RwLock<HashMap<BlockId, Location>>,
    /// Segment handles for readers. Write-locked only on segment roll.
    files: RwLock<Vec<Arc<B::SegmentFile>>>,
    /// Append position in the active (last) segment. Serializes writers.
    append: Mutex<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreStats {
    pub blocks: u64,
    pub segments: u32,
    pub bytes_on_disk: u64,
}

impl BlockStore<StdBackend> {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, Error> {
        Self::open_with(StdBackend, dir)
    }
}

impl<B: IoBackend> BlockStore<B> {
    /// Open a store in `dir`, rebuilding the index by scanning every segment.
    pub fn open_with(backend: B, dir: impl AsRef<Path>) -> Result<Self, Error> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let mut seg_ids: Vec<u32> = std::fs::read_dir(&dir)?
            .filter_map(|e| {
                let name = e.ok()?.file_name().into_string().ok()?;
                let num = name.strip_prefix("seg-")?.strip_suffix(".klpk")?;
                num.parse().ok()
            })
            .collect();
        seg_ids.sort_unstable();

        let mut index = HashMap::new();
        let mut files = Vec::new();
        let mut tail = 0;
        for seg_id in &seg_ids {
            let file = backend.open(&segment_path(&dir, *seg_id))?;
            // Only the last segment's tail matters; earlier ones are sealed.
            tail = segment::scan(&file, *seg_id, |id, loc| {
                index.insert(id, loc);
            })?;
            files.push(Arc::new(file));
        }
        if files.is_empty() {
            files.push(Arc::new(backend.open(&segment_path(&dir, 0))?));
            tail = 0;
        }

        Ok(Self {
            backend,
            dir,
            index: RwLock::new(index),
            files: RwLock::new(files),
            append: Mutex::new(tail),
        })
    }

    /// Store `payload`, returning its content address. No-op if present.
    pub fn put(&self, payload: &[u8]) -> Result<BlockId, Error> {
        Ok(self.put_many(std::iter::once(payload))?[0])
    }

    /// Store a batch of payloads under a single fsync (group commit).
    ///
    /// Puts are fsync-bound: one block per sync caps throughput at the
    /// device's flush rate. Batching amortizes the flush, so offloading a
    /// multi-chunk context costs one sync instead of one per chunk. Returns
    /// ids in input order; duplicates (within the batch or with existing
    /// blocks) are deduplicated.
    pub fn put_many<'a, I>(&self, payloads: I) -> Result<Vec<BlockId>, Error>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        // Hash outside any lock.
        let prepared: Vec<(BlockId, &[u8])> =
            payloads.into_iter().map(|p| (BlockId::of(p), p)).collect();
        let ids: Vec<BlockId> = prepared.iter().map(|(id, _)| *id).collect();

        // Writers serialize on the tail; readers are untouched by this lock.
        let mut tail = self.append.lock().unwrap();
        let mut active = {
            let files = self.files.read().unwrap();
            (files.len() as u32 - 1, Arc::clone(files.last().unwrap()))
        };
        let mut staged: Vec<(BlockId, Location)> = Vec::new();
        let mut dirty = false;

        for (id, payload) in &prepared {
            // The append lock is held, so the index check is race-free with
            // other writers; `staged` covers duplicates within this batch.
            if self.index.read().unwrap().contains_key(id)
                || staged.iter().any(|(sid, _)| sid == id)
            {
                continue;
            }
            let record = segment::encode_record(id, payload);

            if *tail + record.len() as u64 > SEGMENT_ROLL_BYTES && *tail > 0 {
                // Seal the old segment, then roll. The files write lock is
                // held only for the push, never across disk I/O.
                if dirty {
                    active.1.sync()?;
                }
                let new_file = {
                    let mut files = self.files.write().unwrap();
                    let seg_id = files.len() as u32;
                    files.push(Arc::new(
                        self.backend.open(&segment_path(&self.dir, seg_id))?,
                    ));
                    (seg_id, Arc::clone(files.last().unwrap()))
                };
                active = new_file;
                *tail = 0;
            }

            let offset = *tail;
            active.1.write_at(&record, offset)?;
            dirty = true;
            *tail += record.len() as u64;
            staged.push((
                *id,
                Location {
                    segment: active.0,
                    offset,
                    payload_len: payload.len() as u64,
                },
            ));
        }

        // One sync covers every record staged above. Readers keep reading
        // sealed records throughout: no shared lock is held here.
        if dirty {
            active.1.sync()?;
        }

        // Publish only after the data is durable.
        if !staged.is_empty() {
            let mut index = self.index.write().unwrap();
            for (id, loc) in staged {
                index.insert(id, loc);
            }
        }
        Ok(ids)
    }

    /// Fetch a block, verifying its hash before returning.
    pub fn get(&self, id: &BlockId) -> Result<Vec<u8>, Error> {
        let loc = *self
            .index
            .read()
            .unwrap()
            .get(id)
            .ok_or(Error::BlockNotFound(*id))?;

        // Clone the segment handle under a brief lock, then read with no
        // lock held: indexed records are immutable, and an in-flight fsync
        // on the active segment cannot block this path.
        let file = Arc::clone(&self.files.read().unwrap()[loc.segment as usize]);
        let mut payload = vec![0u8; loc.payload_len as usize];
        file.read_at(&mut payload, loc.offset + HEADER_LEN as u64)?;

        if !id.verify(&payload) {
            return Err(Error::Corrupt { id: *id });
        }
        Ok(payload)
    }

    pub fn contains(&self, id: &BlockId) -> bool {
        self.index.read().unwrap().contains_key(id)
    }

    pub fn stats(&self) -> StoreStats {
        let blocks = self.index.read().unwrap().len() as u64;
        let tail = *self.append.lock().unwrap();
        let files = self.files.read().unwrap();
        let sealed: u64 = files[..files.len() - 1]
            .iter()
            .map(|f| f.len().unwrap_or(0))
            .sum();
        StoreStats {
            blocks,
            segments: files.len() as u32,
            bytes_on_disk: sealed + tail,
        }
    }
}

fn segment_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("seg-{id:08}.klpk"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::StdBackend;

    #[test]
    fn put_get_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::open(dir.path()).unwrap();
        let id = store.put(b"hello kalpak").unwrap();
        assert_eq!(store.get(&id).unwrap(), b"hello kalpak");
    }

    #[test]
    fn put_is_idempotent_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::open(dir.path()).unwrap();
        let a = store.put(b"same bytes").unwrap();
        let before = store.stats().bytes_on_disk;
        let b = store.put(b"same bytes").unwrap();
        assert_eq!(a, b);
        assert_eq!(store.stats().bytes_on_disk, before);
        assert_eq!(store.stats().blocks, 1);
    }

    #[test]
    fn missing_block_errors() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::open(dir.path()).unwrap();
        let id = BlockId::of(b"never stored");
        assert!(matches!(store.get(&id), Err(Error::BlockNotFound(_))));
    }

    #[test]
    fn index_rebuilds_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b);
        {
            let store = BlockStore::open(dir.path()).unwrap();
            a = store.put(b"first").unwrap();
            b = store.put(vec![0xAB; 10_000].as_slice()).unwrap();
        }
        let store = BlockStore::open(dir.path()).unwrap();
        assert_eq!(store.get(&a).unwrap(), b"first");
        assert_eq!(store.get(&b).unwrap(), vec![0xAB; 10_000]);
        assert_eq!(store.stats().blocks, 2);
    }

    #[test]
    fn torn_tail_write_is_ignored_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let id;
        {
            let store = BlockStore::open(dir.path()).unwrap();
            id = store.put(b"durable").unwrap();
        }
        // Simulate a crash mid-append: a header with no payload behind it.
        let seg = dir.path().join("seg-00000000.klpk");
        let mut bytes = std::fs::read(&seg).unwrap();
        let fake = segment::encode_record(&BlockId::of(b"lost"), &[1u8; 8000]);
        bytes.extend_from_slice(&fake[..crate::segment::HEADER_LEN + 10]);
        std::fs::write(&seg, &bytes).unwrap();

        let store = BlockStore::open(dir.path()).unwrap();
        assert_eq!(store.get(&id).unwrap(), b"durable");
        assert_eq!(store.stats().blocks, 1);
        // And the store can keep appending over the torn region.
        let id2 = store.put(b"after recovery").unwrap();
        assert_eq!(store.get(&id2).unwrap(), b"after recovery");
    }

    #[test]
    fn corruption_detected_on_read() {
        let dir = tempfile::tempdir().unwrap();
        let id;
        {
            let store = BlockStore::open(dir.path()).unwrap();
            id = store.put(b"pristine payload").unwrap();
        }
        let seg = dir.path().join("seg-00000000.klpk");
        let mut bytes = std::fs::read(&seg).unwrap();
        bytes[crate::segment::HEADER_LEN] ^= 0xFF; // flip a payload bit
        std::fs::write(&seg, &bytes).unwrap();

        let store = BlockStore::open(dir.path()).unwrap();
        assert!(matches!(store.get(&id), Err(Error::Corrupt { .. })));
    }

    #[test]
    fn many_blocks_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::open(dir.path()).unwrap();
        let ids: Vec<_> = (0..200u32)
            .map(|i| {
                let payload = vec![i as u8; (i as usize % 9000) + 1];
                (store.put(&payload).unwrap(), payload)
            })
            .collect();
        for (id, payload) in ids {
            assert_eq!(store.get(&id).unwrap(), payload);
        }
    }

    #[test]
    fn works_with_explicit_backend() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::open_with(StdBackend, dir.path()).unwrap();
        let id = store.put(b"backend generic").unwrap();
        assert_eq!(store.get(&id).unwrap(), b"backend generic");
    }

    #[test]
    fn put_many_single_sync_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::open(dir.path()).unwrap();
        let payloads: Vec<Vec<u8>> = (0..50u8).map(|i| vec![i; 3000]).collect();
        let refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
        let ids = store.put_many(refs).unwrap();
        assert_eq!(ids.len(), 50);
        for (id, payload) in ids.iter().zip(&payloads) {
            assert_eq!(&store.get(id).unwrap(), payload);
        }
        assert_eq!(store.stats().blocks, 50);
    }

    #[test]
    fn put_many_dedups_within_batch_and_against_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::open(dir.path()).unwrap();
        store.put(b"already there").unwrap();
        let before = store.stats().bytes_on_disk;
        let ids = store
            .put_many([
                b"already there".as_slice(),
                b"new".as_slice(),
                b"new".as_slice(),
            ])
            .unwrap();
        assert_eq!(ids[1], ids[2]);
        assert_eq!(store.stats().blocks, 2);
        // Only "new" was written once.
        assert_eq!(store.stats().bytes_on_disk, before + 4096);
    }

    #[test]
    fn put_many_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let ids;
        {
            let store = BlockStore::open(dir.path()).unwrap();
            ids = store
                .put_many([b"alpha".as_slice(), b"beta".as_slice()])
                .unwrap();
        }
        let store = BlockStore::open(dir.path()).unwrap();
        assert_eq!(store.get(&ids[0]).unwrap(), b"alpha");
        assert_eq!(store.get(&ids[1]).unwrap(), b"beta");
    }
}
