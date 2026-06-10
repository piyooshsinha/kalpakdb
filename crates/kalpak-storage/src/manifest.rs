//! Prefix manifest: maps a [`CacheKey`] (model fingerprint + chained prefix
//! hash) to the ordered list of block ids holding that prefix's KV data.
//!
//! This is the lookup layer that turns the block store into a cache server:
//! an inference client chunks its token stream, chains `CacheKey`s, and
//! probes the manifest for the longest already-materialized prefix.
//!
//! Persistence is an append-only JSON-lines log (`manifest.jsonl`), replayed
//! on open with last-binding-wins semantics. A torn tail line from a crash is
//! skipped on replay and overwritten by the next append. Like the segment
//! files, the log is the only source of truth; there is no separate index to
//! corrupt. In the clustered phase this mapping moves into the Raft state
//! machine — it is exactly the "metadata, never data" split the control
//! plane is for.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

use kalpak_core::{BlockId, CacheKey, Error};
use serde::{Deserialize, Serialize};

const LOG_NAME: &str = "manifest.jsonl";

#[derive(Serialize, Deserialize)]
struct Binding {
    key: CacheKey,
    blocks: Vec<BlockId>,
}

/// Durable `CacheKey -> [BlockId]` mapping.
pub struct PrefixManifest {
    map: Mutex<HashMap<CacheKey, Vec<BlockId>>>,
    log: Mutex<File>,
}

impl PrefixManifest {
    /// Open (creating if absent) the manifest log in `dir` and replay it.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, Error> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let path = dir.join(LOG_NAME);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let mut map = HashMap::new();
        let mut valid_end: u64 = 0;
        let mut reader = BufReader::new(&mut file);
        let mut line = String::new();
        loop {
            line.clear();
            let n = reader.read_line(&mut line)?;
            if n == 0 {
                break;
            }
            match serde_json::from_str::<Binding>(line.trim_end()) {
                Ok(b) => {
                    map.insert(b.key, b.blocks);
                    valid_end += n as u64;
                }
                // Torn tail write: stop here, append position resets to the
                // end of the last intact line.
                Err(_) => break,
            }
        }
        file.seek(SeekFrom::Start(valid_end))?;
        file.set_len(valid_end)?;

        Ok(Self {
            map: Mutex::new(map),
            log: Mutex::new(file),
        })
    }

    /// Durably bind `key` to an ordered block list. Rebinding overwrites.
    pub fn bind(&self, key: CacheKey, blocks: Vec<BlockId>) -> Result<(), Error> {
        let line = serde_json::to_string(&Binding {
            key: key.clone(),
            blocks: blocks.clone(),
        })
        .expect("manifest binding serializes");

        {
            let mut log = self.log.lock().unwrap();
            log.write_all(line.as_bytes())?;
            log.write_all(b"\n")?;
            log.sync_data()?;
        }
        self.map.lock().unwrap().insert(key, blocks);
        Ok(())
    }

    pub fn lookup(&self, key: &CacheKey) -> Option<Vec<BlockId>> {
        self.map.lock().unwrap().get(key).cloned()
    }

    /// Probe a chain of keys (root-first) and return the deepest bound one
    /// with its blocks — the longest reusable prefix for this context.
    pub fn longest_prefix<'a>(
        &self,
        chain: impl IntoIterator<Item = &'a CacheKey>,
    ) -> Option<(&'a CacheKey, Vec<BlockId>)> {
        let map = self.map.lock().unwrap();
        let mut best = None;
        for key in chain {
            match map.get(key) {
                Some(blocks) => best = Some((key, blocks.clone())),
                None => break,
            }
        }
        best
    }

    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.lock().unwrap().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalpak_core::ModelFingerprint;

    fn fp() -> ModelFingerprint {
        ModelFingerprint::new("test/model", "tok-abc", "fp16/paged-16")
    }

    #[test]
    fn bind_lookup_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let m = PrefixManifest::open(dir.path()).unwrap();
        let key = CacheKey::root(fp(), &[1, 2, 3]);
        let blocks = vec![BlockId::of(b"kv-chunk-0")];
        m.bind(key.clone(), blocks.clone()).unwrap();
        assert_eq!(m.lookup(&key), Some(blocks));
    }

    #[test]
    fn replays_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let key = CacheKey::root(fp(), &[1, 2, 3]);
        let blocks = vec![BlockId::of(b"a"), BlockId::of(b"b")];
        {
            let m = PrefixManifest::open(dir.path()).unwrap();
            m.bind(key.clone(), blocks.clone()).unwrap();
        }
        let m = PrefixManifest::open(dir.path()).unwrap();
        assert_eq!(m.lookup(&key), Some(blocks));
    }

    #[test]
    fn rebind_last_wins_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let key = CacheKey::root(fp(), &[9]);
        {
            let m = PrefixManifest::open(dir.path()).unwrap();
            m.bind(key.clone(), vec![BlockId::of(b"old")]).unwrap();
            m.bind(key.clone(), vec![BlockId::of(b"new")]).unwrap();
        }
        let m = PrefixManifest::open(dir.path()).unwrap();
        assert_eq!(m.lookup(&key), Some(vec![BlockId::of(b"new")]));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn torn_tail_line_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let key = CacheKey::root(fp(), &[1]);
        {
            let m = PrefixManifest::open(dir.path()).unwrap();
            m.bind(key.clone(), vec![BlockId::of(b"safe")]).unwrap();
        }
        // Simulate a crash mid-append: half a JSON line at the tail.
        let path = dir.path().join(LOG_NAME);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(b"{\"key\":{\"finger");
        std::fs::write(&path, &bytes).unwrap();

        let m = PrefixManifest::open(dir.path()).unwrap();
        assert_eq!(m.lookup(&key), Some(vec![BlockId::of(b"safe")]));
        assert_eq!(m.len(), 1);
        // Appending after recovery produces a clean log again.
        let key2 = CacheKey::root(fp(), &[2]);
        m.bind(key2.clone(), vec![BlockId::of(b"post")]).unwrap();
        let m2 = PrefixManifest::open(dir.path()).unwrap();
        assert_eq!(m2.len(), 2);
        assert_eq!(m2.lookup(&key2), Some(vec![BlockId::of(b"post")]));
    }

    #[test]
    fn longest_prefix_walks_the_chain() {
        let dir = tempfile::tempdir().unwrap();
        let m = PrefixManifest::open(dir.path()).unwrap();

        let k0 = CacheKey::root(fp(), &[1, 2]);
        let k1 = k0.extend(&[3, 4]);
        let k2 = k1.extend(&[5, 6]);
        m.bind(k0.clone(), vec![BlockId::of(b"c0")]).unwrap();
        m.bind(k1.clone(), vec![BlockId::of(b"c0"), BlockId::of(b"c1")])
            .unwrap();
        // k2 not materialized yet.

        let chain = [k0.clone(), k1.clone(), k2.clone()];
        let (hit, blocks) = m.longest_prefix(chain.iter()).unwrap();
        assert_eq!(hit, &k1);
        assert_eq!(blocks.len(), 2);

        // A chain whose root is unknown has no reusable prefix.
        let other = [CacheKey::root(fp(), &[7, 7])];
        assert!(m.longest_prefix(other.iter()).is_none());
    }
}
