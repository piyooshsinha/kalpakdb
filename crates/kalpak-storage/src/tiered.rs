//! Two-tier store: a byte-budgeted in-memory warm buffer over the durable
//! cold block store.
//!
//! This is the single-node shape of Kalpak's tiering story (NUC RAM = warm,
//! Mac Mini SSD = cold, in the dev topology). `put` writes through to disk —
//! the warm tier is never the only copy of anything — and `get` promotes
//! cold hits, evicting least-recently-used blocks once the byte budget is
//! exceeded. Because blocks are immutable and content-addressed there is no
//! invalidation problem: a cached block can never be stale, only evicted.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use kalpak_core::{BlockId, Error};

use crate::io::{IoBackend, StdBackend};
use crate::store::BlockStore;

/// LRU warm buffer keyed by content address, capped in bytes.
struct WarmTier {
    /// Insertion/touch order: front = coldest, back = hottest. Entries are
    /// (id, payload); `map` points at payloads shared with readers.
    order: Vec<BlockId>,
    map: HashMap<BlockId, Arc<Vec<u8>>>,
    bytes: u64,
    budget: u64,
}

impl WarmTier {
    fn touch(&mut self, id: &BlockId) {
        if let Some(pos) = self.order.iter().position(|x| x == id) {
            let id = self.order.remove(pos);
            self.order.push(id);
        }
    }

    fn insert(&mut self, id: BlockId, payload: Arc<Vec<u8>>) {
        let len = payload.len() as u64;
        // A block larger than the whole budget skips the warm tier entirely.
        if len > self.budget {
            return;
        }
        if self.map.insert(id, payload).is_none() {
            self.order.push(id);
            self.bytes += len;
        } else {
            self.touch(&id);
        }
        while self.bytes > self.budget {
            let coldest = self.order.remove(0);
            if let Some(evicted) = self.map.remove(&coldest) {
                self.bytes -= evicted.len() as u64;
            }
        }
    }
}

pub struct TieredStore<B: IoBackend = StdBackend> {
    cold: BlockStore<B>,
    warm: Mutex<WarmTier>,
    hits: AtomicU64,
    misses: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TierStats {
    pub warm_blocks: u64,
    pub warm_bytes: u64,
    pub warm_budget: u64,
    pub hits: u64,
    pub misses: u64,
}

impl TieredStore<StdBackend> {
    pub fn open(dir: impl AsRef<Path>, warm_budget_bytes: u64) -> Result<Self, Error> {
        Ok(Self::new(BlockStore::open(dir)?, warm_budget_bytes))
    }
}

impl<B: IoBackend> TieredStore<B> {
    pub fn new(cold: BlockStore<B>, warm_budget_bytes: u64) -> Self {
        Self {
            cold,
            warm: Mutex::new(WarmTier {
                order: Vec::new(),
                map: HashMap::new(),
                bytes: 0,
                budget: warm_budget_bytes,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Write through to the cold store, then warm the block.
    pub fn put(&self, payload: &[u8]) -> Result<BlockId, Error> {
        let id = self.cold.put(payload)?;
        self.warm
            .lock()
            .unwrap()
            .insert(id, Arc::new(payload.to_vec()));
        Ok(id)
    }

    /// Serve from RAM when possible; on a cold hit, verify and promote.
    pub fn get(&self, id: &BlockId) -> Result<Arc<Vec<u8>>, Error> {
        {
            let mut warm = self.warm.lock().unwrap();
            if let Some(payload) = warm.map.get(id).cloned() {
                warm.touch(id);
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(payload);
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let payload = Arc::new(self.cold.get(id)?);
        self.warm.lock().unwrap().insert(*id, Arc::clone(&payload));
        Ok(payload)
    }

    pub fn contains(&self, id: &BlockId) -> bool {
        self.cold.contains(id)
    }

    pub fn cold(&self) -> &BlockStore<B> {
        &self.cold
    }

    pub fn tier_stats(&self) -> TierStats {
        let warm = self.warm.lock().unwrap();
        TierStats {
            warm_blocks: warm.map.len() as u64,
            warm_bytes: warm.bytes,
            warm_budget: warm.budget,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_through_then_serve_from_warm() {
        let dir = tempfile::tempdir().unwrap();
        let store = TieredStore::open(dir.path(), 1 << 20).unwrap();
        let id = store.put(b"hot block").unwrap();

        // Nuke the cold store's segment behind the tier's back: a warm hit
        // must not touch disk at all.
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let p = entry.unwrap().path();
            if p.extension().is_some_and(|e| e == "klpk") {
                std::fs::write(&p, b"garbage").unwrap();
            }
        }
        assert_eq!(store.get(&id).unwrap().as_slice(), b"hot block");
        let s = store.tier_stats();
        assert_eq!((s.hits, s.misses), (1, 0));
    }

    #[test]
    fn cold_hit_promotes_to_warm() {
        let dir = tempfile::tempdir().unwrap();
        let id;
        {
            let plain = BlockStore::open(dir.path()).unwrap();
            id = plain.put(b"written cold").unwrap();
        }
        let store = TieredStore::open(dir.path(), 1 << 20).unwrap();
        assert_eq!(store.get(&id).unwrap().as_slice(), b"written cold");
        assert_eq!(store.tier_stats().misses, 1);
        assert_eq!(store.get(&id).unwrap().as_slice(), b"written cold");
        assert_eq!(store.tier_stats().hits, 1);
    }

    #[test]
    fn eviction_respects_byte_budget_lru_order() {
        let dir = tempfile::tempdir().unwrap();
        // Budget fits two 1000-byte blocks, not three.
        let store = TieredStore::open(dir.path(), 2500).unwrap();
        let a = store.put(&[1u8; 1000]).unwrap();
        let b = store.put(&[2u8; 1000]).unwrap();
        // Touch `a` so `b` is the LRU candidate.
        store.get(&a).unwrap();
        let c = store.put(&[3u8; 1000]).unwrap();

        let s = store.tier_stats();
        assert_eq!(s.warm_blocks, 2);
        assert!(s.warm_bytes <= s.warm_budget);

        let warm_has = |id: &kalpak_core::BlockId| {
            let before = store.tier_stats().hits;
            store.get(id).unwrap();
            store.tier_stats().hits > before
        };
        assert!(warm_has(&c));
        assert!(warm_has(&a) || !warm_has(&b)); // `b` was evicted, not `a`
    }

    #[test]
    fn oversized_block_bypasses_warm_but_is_durable() {
        let dir = tempfile::tempdir().unwrap();
        let store = TieredStore::open(dir.path(), 100).unwrap();
        let id = store.put(&[7u8; 5000]).unwrap();
        assert_eq!(store.tier_stats().warm_blocks, 0);
        assert_eq!(store.get(&id).unwrap().len(), 5000);
    }
}
