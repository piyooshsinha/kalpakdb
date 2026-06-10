use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use kalpak_core::{BlockId, Error};

use crate::io::{IoBackend, SegmentFile, StdBackend};
use crate::segment::{self, Location, HEADER_LEN};

/// Segments roll over once they pass this size (256 MiB). Small enough to
/// keep recovery scans and future compaction units manageable, large enough
/// to amortize file overhead.
const SEGMENT_ROLL_BYTES: u64 = 256 * 1024 * 1024;

struct Segments<F> {
    files: Vec<F>,
    /// Append position in the active (last) segment.
    tail: u64,
}

/// Content-addressed block store over append-only segment files.
///
/// `put` is idempotent: storing bytes that already exist returns the existing
/// id without writing. `get` verifies the payload hash on every read, so
/// corruption is detected at the read site rather than propagated.
pub struct BlockStore<B: IoBackend = StdBackend> {
    backend: B,
    dir: PathBuf,
    index: RwLock<HashMap<BlockId, Location>>,
    segments: RwLock<Segments<B::SegmentFile>>,
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
        for (i, seg_id) in seg_ids.iter().enumerate() {
            let file = backend.open(&segment_path(&dir, *seg_id))?;
            tail = segment::scan(&file, *seg_id, |id, loc| {
                index.insert(id, loc);
            })?;
            // Only the last segment's tail matters; earlier ones are sealed.
            let _ = i;
            files.push(file);
        }
        if files.is_empty() {
            files.push(backend.open(&segment_path(&dir, 0))?);
            tail = 0;
        }

        Ok(Self {
            backend,
            dir,
            index: RwLock::new(index),
            segments: RwLock::new(Segments { files, tail }),
        })
    }

    /// Store `payload`, returning its content address. No-op if present.
    pub fn put(&self, payload: &[u8]) -> Result<BlockId, Error> {
        let id = BlockId::of(payload);
        if self.index.read().unwrap().contains_key(&id) {
            return Ok(id);
        }

        let record = segment::encode_record(&id, payload);
        let mut segs = self.segments.write().unwrap();

        // Re-check under the write lock: a racing put of the same bytes may
        // have landed while we were encoding.
        if self.index.read().unwrap().contains_key(&id) {
            return Ok(id);
        }

        if segs.tail + record.len() as u64 > SEGMENT_ROLL_BYTES && segs.tail > 0 {
            let seg_id = segs.files.len() as u32;
            segs.files.push(self.backend.open(&segment_path(&self.dir, seg_id))?);
            segs.tail = 0;
        }

        let seg_id = (segs.files.len() - 1) as u32;
        let offset = segs.tail;
        let file = segs.files.last().unwrap();
        file.write_at(&record, offset)?;
        file.sync()?;
        segs.tail += record.len() as u64;

        self.index.write().unwrap().insert(
            id,
            Location {
                segment: seg_id,
                offset,
                payload_len: payload.len() as u64,
            },
        );
        Ok(id)
    }

    /// Fetch a block, verifying its hash before returning.
    pub fn get(&self, id: &BlockId) -> Result<Vec<u8>, Error> {
        let loc = *self
            .index
            .read()
            .unwrap()
            .get(id)
            .ok_or(Error::BlockNotFound(*id))?;

        let mut payload = vec![0u8; loc.payload_len as usize];
        {
            let segs = self.segments.read().unwrap();
            let file = &segs.files[loc.segment as usize];
            file.read_at(&mut payload, loc.offset + HEADER_LEN as u64)?;
        }

        if !id.verify(&payload) {
            return Err(Error::Corrupt { id: *id });
        }
        Ok(payload)
    }

    pub fn contains(&self, id: &BlockId) -> bool {
        self.index.read().unwrap().contains_key(id)
    }

    pub fn stats(&self) -> StoreStats {
        let index = self.index.read().unwrap();
        let segs = self.segments.read().unwrap();
        let sealed: u64 = segs.files[..segs.files.len() - 1]
            .iter()
            .map(|f| f.len().unwrap_or(0))
            .sum();
        StoreStats {
            blocks: index.len() as u64,
            segments: segs.files.len() as u32,
            bytes_on_disk: sealed + segs.tail,
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
}
