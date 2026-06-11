//! Two-tier store: a byte-budgeted in-memory warm buffer over the durable
//! cold block store.
//!
//! This is the single-node shape of Kalpak's tiering story (NUC RAM = warm,
//! Mac Mini SSD = cold, in the dev topology). `put` writes through to disk —
//! the warm tier is never the only copy of anything — and `get` promotes
//! cold hits. Because blocks are immutable and content-addressed there is no
//! invalidation problem: a cached block can never be stale, only evicted.
//!
//! The warm tier is a `moka` concurrent cache: lock-free reads under the
//! highly parallel access pattern of inference workloads (a hand-rolled
//! `Mutex<Vec>` LRU costs an O(N) scan-and-shift per touch and serializes
//! every reader — measurably catastrophic past ~10^5 blocks). Eviction is
//! size-aware (each entry weighs its payload length) under moka's TinyLFU
//! policy, which approximates LRU but also resists scan pollution.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use kalpak_core::{BlockId, Error};

use crate::io::{IoBackend, StdBackend};
use crate::store::BlockStore;

pub struct TieredStore<B: IoBackend = StdBackend> {
    cold: BlockStore<B>,
    warm: moka::sync::Cache<BlockId, Arc<Vec<u8>>>,
    warm_budget: u64,
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
        let warm = moka::sync::Cache::builder()
            .max_capacity(warm_budget_bytes)
            .weigher(|_id: &BlockId, payload: &Arc<Vec<u8>>| {
                payload.len().try_into().unwrap_or(u32::MAX)
            })
            .build();
        Self {
            cold,
            warm,
            warm_budget: warm_budget_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    fn warm_insert(&self, id: BlockId, payload: Arc<Vec<u8>>) {
        // A block larger than the whole budget skips the warm tier entirely.
        if (payload.len() as u64) <= self.warm_budget {
            self.warm.insert(id, payload);
        }
    }

    /// Write through to the cold store, then warm the block.
    pub fn put(&self, payload: &[u8]) -> Result<BlockId, Error> {
        let id = self.cold.put(payload)?;
        self.warm_insert(id, Arc::new(payload.to_vec()));
        Ok(id)
    }

    /// Group-commit a batch (one fsync), then warm every block.
    pub fn put_many<'a, I>(&self, payloads: I) -> Result<Vec<BlockId>, Error>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let payloads: Vec<&[u8]> = payloads.into_iter().collect();
        let ids = self.cold.put_many(payloads.iter().copied())?;
        for (id, payload) in ids.iter().zip(&payloads) {
            self.warm_insert(*id, Arc::new(payload.to_vec()));
        }
        Ok(ids)
    }

    /// Serve from RAM when possible; on a cold hit, verify and promote.
    pub fn get(&self, id: &BlockId) -> Result<Arc<Vec<u8>>, Error> {
        if let Some(payload) = self.warm.get(id) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(payload);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let payload = Arc::new(self.cold.get(id)?);
        self.warm_insert(*id, Arc::clone(&payload));
        Ok(payload)
    }

    pub fn contains(&self, id: &BlockId) -> bool {
        self.cold.contains(id)
    }

    pub fn cold(&self) -> &BlockStore<B> {
        &self.cold
    }

    /// Compact sealed segments, dropping blocks for which `live` is false,
    /// and evict the dropped blocks from the warm tier.
    pub fn compact(
        &self,
        live: impl Fn(&BlockId) -> bool,
    ) -> Result<crate::store::CompactStats, Error> {
        let stats = self.cold.compact(&live)?;
        if stats.blocks_dropped > 0 {
            // Dead ids are no longer in the cold index; sweep warm entries
            // that the cold store no longer backs.
            let dead: Vec<BlockId> = self
                .warm
                .iter()
                .filter(|(id, _)| !self.cold.contains(id))
                .map(|(id, _)| *id)
                .collect();
            for id in dead {
                self.warm.invalidate(&id);
            }
        }
        Ok(stats)
    }

    pub fn tier_stats(&self) -> TierStats {
        // Flush moka's pending maintenance so counts reflect evictions.
        self.warm.run_pending_tasks();
        TierStats {
            warm_blocks: self.warm.entry_count(),
            warm_bytes: self.warm.weighted_size(),
            warm_budget: self.warm_budget,
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
    fn eviction_respects_byte_budget() {
        let dir = tempfile::tempdir().unwrap();
        // Budget fits two 1000-byte blocks, not three.
        let store = TieredStore::open(dir.path(), 2500).unwrap();
        let ids: Vec<_> = (1u8..=3).map(|i| store.put(&[i; 1000]).unwrap()).collect();

        let s = store.tier_stats();
        assert!(
            s.warm_bytes <= s.warm_budget,
            "warm tier exceeded its budget: {s:?}"
        );
        assert!(s.warm_blocks < 3, "all three blocks cannot fit the budget");

        // Every block stays readable regardless of which was evicted.
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(store.get(id).unwrap().as_slice(), &[i as u8 + 1; 1000]);
        }
    }

    #[test]
    fn oversized_block_bypasses_warm_but_is_durable() {
        let dir = tempfile::tempdir().unwrap();
        let store = TieredStore::open(dir.path(), 100).unwrap();
        let id = store.put(&[7u8; 5000]).unwrap();
        assert_eq!(store.tier_stats().warm_blocks, 0);
        assert_eq!(store.get(&id).unwrap().len(), 5000);
    }

    #[test]
    fn concurrent_reads_and_writes() {
        let dir = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(TieredStore::open(dir.path(), 1 << 20).unwrap());
        let seed: Vec<_> = (0..50u8).map(|i| store.put(&[i; 2000]).unwrap()).collect();

        // Readers hammer the warm tier while writers group-commit batches:
        // the regression this guards is reads stalling behind fsyncs.
        let mut handles = Vec::new();
        for t in 0..4 {
            let store = store.clone();
            let seed = seed.clone();
            handles.push(std::thread::spawn(move || {
                for round in 0..200 {
                    let id = &seed[(t * 7 + round) % seed.len()];
                    assert!(store.get(id).is_ok());
                }
            }));
        }
        for t in 0..2u64 {
            let store = store.clone();
            handles.push(std::thread::spawn(move || {
                for round in 0..20u64 {
                    let payloads: Vec<Vec<u8>> = (0..8u64)
                        .map(|i| (t * 100_000 + round * 100 + i).to_le_bytes().repeat(300))
                        .collect();
                    store
                        .put_many(payloads.iter().map(|p| p.as_slice()))
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(store.tier_stats().warm_bytes <= store.tier_stats().warm_budget);
    }
}
