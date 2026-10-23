//! Local retention/intents plus leader-owned repair of Raft requirements.
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use kalpak_core::BlockId;
use serde::{Deserialize, Serialize};

const MAX_RECORDS: usize = 16_384;

pub(crate) fn valid_generation(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Requirements {
    version: u32,
    blocks: BTreeMap<BlockId, usize>,
    #[serde(default)]
    locations: BTreeMap<BlockId, BTreeMap<u64, String>>,
}

pub(crate) struct RepairLog {
    pub(crate) generation: String,
    path: PathBuf,
    requirements: Mutex<Requirements>,
}

impl RepairLog {
    pub(crate) fn open(dir: &Path) -> io::Result<Self> {
        let generation_path = dir.join("storage-generation");
        let generation = match std::fs::read_to_string(&generation_path) {
            Ok(value) => value,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut temporary = tempfile::NamedTempFile::new_in(dir)?;
                let seed = format!(
                    "{:?}-{:?}-{}",
                    temporary.path(),
                    std::time::SystemTime::now(),
                    std::process::id()
                );
                let value = BlockId::of(seed.as_bytes()).to_string();
                temporary.write_all(value.as_bytes())?;
                temporary.as_file().sync_all()?;
                temporary
                    .persist(&generation_path)
                    .map_err(|error| error.error)?;
                File::open(dir)?.sync_all()?;
                value
            }
            Err(error) => return Err(error),
        };
        if !valid_generation(&generation) {
            return Err(io::Error::other("invalid storage generation"));
        }
        let path = dir.join("replica-requirements.json");
        let requirements = match std::fs::read(&path) {
            Ok(bytes) => {
                serde_json::from_slice::<Requirements>(&bytes).map_err(io::Error::other)?
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Requirements {
                version: 1,
                ..Default::default()
            },
            Err(error) => return Err(error),
        };
        if requirements.version != 1
            || requirements.blocks.len() > MAX_RECORDS
            || requirements.blocks.values().any(|n| !(1..=16).contains(n))
            || requirements.locations.iter().any(|(id, nodes)| {
                !requirements.blocks.contains_key(id)
                    || nodes.len() > 16
                    || nodes
                        .values()
                        .any(|generation| !valid_generation(generation))
            })
        {
            return Err(io::Error::other("invalid replica requirements"));
        }
        Ok(Self {
            generation,
            path,
            requirements: Mutex::new(requirements),
        })
    }

    pub(crate) fn record(&self, ids: &[BlockId], replicas: usize) -> io::Result<()> {
        if !(1..=16).contains(&replicas) {
            return Err(io::Error::other("invalid replica count"));
        }
        let mut current = self.requirements.lock().unwrap();
        let mut next = current.clone();
        for id in ids {
            let target = next.blocks.entry(*id).or_insert(replicas);
            *target = (*target).max(replicas); // retries cannot downgrade intent
        }
        if next.blocks == current.blocks {
            return Ok(());
        }
        if next.blocks.len() > MAX_RECORDS {
            return Err(io::Error::other("replica requirement capacity exhausted"));
        }
        self.publish(&next)?;
        *current = next;
        Ok(())
    }

    fn publish(&self, next: &Requirements) -> io::Result<()> {
        let bytes = serde_json::to_vec(next).map_err(io::Error::other)?;
        let pending = self.path.with_extension("pending");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&pending)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&pending, &self.path)?;
        File::open(self.path.parent().unwrap())?.sync_all()?;
        Ok(())
    }

    pub(crate) fn retained(&self, id: &BlockId) -> bool {
        self.requirements.lock().unwrap().blocks.contains_key(id)
    }

    pub(crate) fn known_nodes(&self, id: &BlockId) -> Vec<u64> {
        self.requirements
            .lock()
            .unwrap()
            .locations
            .get(id)
            .map(|nodes| nodes.keys().copied().collect())
            .unwrap_or_default()
    }

    pub(crate) fn observe(
        &self,
        ids: &[BlockId],
        node: u64,
        generation: Option<&str>,
    ) -> io::Result<()> {
        if generation.is_some_and(|generation| !valid_generation(generation)) {
            return Err(io::Error::other("invalid peer storage generation"));
        }
        let mut current = self.requirements.lock().unwrap();
        let mut next = current.clone();
        // A changed incarnation invalidates every old observation for this
        // node, not just the block currently being checked.
        if let Some(generation) = generation {
            for nodes in next.locations.values_mut() {
                if nodes.get(&node).is_some_and(|old| old != generation) {
                    nodes.remove(&node);
                }
            }
        }
        for id in ids {
            if !next.blocks.contains_key(id) {
                continue;
            }
            let nodes = next.locations.entry(*id).or_default();
            if let Some(generation) = generation {
                if !nodes.contains_key(&node) && nodes.len() >= 16 {
                    nodes.pop_first();
                }
                nodes.insert(node, generation.to_string());
            } else {
                nodes.remove(&node);
            }
        }
        if next.locations != current.locations {
            self.publish(&next)?;
            *current = next;
        }
        Ok(())
    }

    fn batch(&self, after: Option<BlockId>) -> Vec<(BlockId, usize)> {
        let guard = self.requirements.lock().unwrap();
        guard
            .blocks
            .iter()
            .filter(|(id, count)| **count > 1 && after.is_none_or(|after| **id > after))
            .take(16)
            .map(|(id, count)| (*id, *count))
            .collect()
    }
}

pub(crate) fn start(state: Arc<crate::server::AppState>) {
    tokio::spawn(async move {
        let mut cursor = None;
        let mut migration_cursor = None;
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            // Failed uploads and older versions may have source-local
            // intents that were never committed. Any source can promote
            // these through the current leader once quorum returns.
            let known = state.control.replica_requirements();
            let local_intents = state.repairs.batch(migration_cursor);
            if local_intents.is_empty() {
                migration_cursor = None;
            }
            for (id, replicas) in local_intents {
                migration_cursor = Some(id);
                if !known.get(&id).is_some_and(|n| *n >= replicas) {
                    let _ = crate::server::commit_requirements(&state, vec![id], replicas).await;
                }
            }
            let metrics = state.control.metrics();
            if metrics.current_leader != Some(metrics.id) {
                continue;
            }
            let mut candidates: BTreeMap<_, _> = state.repairs.batch(cursor).into_iter().collect();
            for (id, count) in state
                .control
                .replica_requirements()
                .into_iter()
                .filter(|(id, _)| cursor.is_none_or(|after| *id > after))
                .take(16)
            {
                let target = candidates.entry(id).or_insert(count);
                *target = (*target).max(count);
            }
            let batch: Vec<_> = candidates.into_iter().take(16).collect();
            if batch.is_empty() {
                cursor = None;
                continue;
            }
            for (id, replicas) in batch {
                cursor = Some(id);
                let Ok(lease) = state.uploads.try_acquire() else {
                    break;
                };
                let disk_state = state.clone();
                let disk_lease = lease.clone();
                let payload = tokio::task::spawn_blocking(move || {
                    let _lease = disk_lease;
                    disk_state
                        .repairs
                        .record(&[id], replicas)
                        .map_err(kalpak_core::Error::from)?;
                    Ok::<_, kalpak_core::Error>(
                        disk_state
                            .store
                            .verify_cold(&id)
                            .and_then(|_| disk_state.store.get(&id)),
                    )
                })
                .await;
                let Ok(Ok(payload)) = payload else {
                    continue;
                };
                // Persisting the pin must succeed first. A subsequent cold
                // read failure can safely trigger retrieval of a replacement.
                let mut payload = payload
                    .ok()
                    .map(|payload| bytes::Bytes::copy_from_slice(&payload));
                if payload.is_none() {
                    // A newly elected leader need not have the tensor yet.
                    // Fetch from a retained copy, never through cascading reads.
                    let known = state.repairs.known_nodes(&id);
                    let mut peers = state.control.peer_nodes();
                    peers.sort_by_key(|(node, _)| (!known.contains(node), *node));
                    for (_, peer) in peers {
                        let response = state
                            .http
                            .get(format!("{}://{peer}/v1/blocks/{id}", state.peer_scheme))
                            .header("x-kalpak-internal", "1")
                            .timeout(std::time::Duration::from_secs(5))
                            .send()
                            .await;
                        if let Ok(mut response) = response {
                            if !response.status().is_success() {
                                continue;
                            }
                            let mut bytes = Vec::new();
                            while let Ok(Some(chunk)) = response.chunk().await {
                                if chunk.len() > state.max_block_bytes.saturating_sub(bytes.len()) {
                                    bytes.clear();
                                    break;
                                }
                                bytes.extend_from_slice(&chunk);
                            }
                            if BlockId::of(&bytes) == id {
                                let disk_state = state.clone();
                                let disk_lease = lease.clone();
                                let saved = tokio::task::spawn_blocking(move || {
                                    let _lease = disk_lease;
                                    let _write_guard = disk_state
                                        .retention
                                        .protect(&[id])
                                        .map_err(kalpak_core::Error::from)?;
                                    disk_state
                                        .store
                                        .repair(&id, &bytes)
                                        .map(|_| bytes::Bytes::from(bytes))
                                })
                                .await;
                                if let Ok(Ok(bytes)) = saved {
                                    payload = Some(bytes);
                                    break;
                                }
                            }
                        }
                    }
                }
                if let Some(payload) = payload {
                    // Existing API returns an Arc<Vec>; account for this
                    // additional copy in future resident-memory controls.
                    let result = crate::server::acknowledge_replicas(
                        &state,
                        "/v1/blocks",
                        payload,
                        serde_json::json!({"id": id.to_string()}),
                        replicas,
                        lease.clone(),
                    )
                    .await;
                    if let Err(error) = result {
                        eprintln!("kalpakdb: repair {id}: {error}");
                    }
                } else {
                    eprintln!("kalpakdb: repair source unavailable for {id}");
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observations_survive_restart_and_incarnation_changes_invalidate_old_locations() {
        let dir = tempfile::tempdir().unwrap();
        let log = RepairLog::open(dir.path()).unwrap();
        let local_generation = log.generation.clone();
        let a = BlockId::of(b"a");
        let b = BlockId::of(b"b");
        let old = "a".repeat(64);
        let new = "b".repeat(64);
        log.record(&[a, b], 2).unwrap();
        log.observe(&[a, b], 7, Some(&old)).unwrap();
        drop(log);
        let log = RepairLog::open(dir.path()).unwrap();
        assert_eq!(log.generation, local_generation);
        assert_eq!(log.requirements.lock().unwrap().locations[&b][&7], old);
        log.observe(&[a], 7, Some(&new)).unwrap();
        assert_eq!(log.requirements.lock().unwrap().locations[&a][&7], new);
        assert!(!log.requirements.lock().unwrap().locations[&b].contains_key(&7));
        log.observe(&[a], 7, None).unwrap();
        assert!(!log.requirements.lock().unwrap().locations[&a].contains_key(&7));
        assert!(log.observe(&[a], 7, Some("bad")).is_err());
        let fresh = tempfile::tempdir().unwrap();
        assert_ne!(
            RepairLog::open(fresh.path()).unwrap().generation,
            local_generation
        );
    }

    #[test]
    fn requirements_survive_reopen_and_never_downgrade() {
        let dir = tempfile::tempdir().unwrap();
        let id = BlockId::of(b"repair");
        let log = RepairLog::open(dir.path()).unwrap();
        log.record(&[id], 3).unwrap();
        log.record(&[id], 1).unwrap();
        assert!(log.retained(&id));
        drop(log);
        std::fs::write(
            dir.path().join("replica-requirements.pending"),
            b"unfinished",
        )
        .unwrap();
        let log = RepairLog::open(dir.path()).unwrap();
        assert_eq!(log.batch(None), vec![(id, 3)]);
        assert!(log.batch(Some(id)).is_empty());
        std::fs::write(dir.path().join("replica-requirements.json"), b"corrupt").unwrap();
        assert!(RepairLog::open(dir.path()).is_err());
    }

    #[test]
    fn capacity_failure_does_not_partially_publish_intent() {
        let dir = tempfile::tempdir().unwrap();
        let log = RepairLog::open(dir.path()).unwrap();
        let ids: Vec<_> = (0..=MAX_RECORDS)
            .map(|n| BlockId::of(&n.to_le_bytes()))
            .collect();
        assert!(log.record(&ids, 2).is_err());
        assert!(log.batch(None).is_empty());
        assert!(ids.iter().all(|id| !log.retained(id)));
        assert!(!dir.path().join("replica-requirements.json").exists());
    }
}
