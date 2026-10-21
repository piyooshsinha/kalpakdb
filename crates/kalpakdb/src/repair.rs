//! Local retention/intents plus leader-owned repair of Raft requirements.
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use kalpak_core::BlockId;
use serde::{Deserialize, Serialize};

const MAX_RECORDS: usize = 16_384;

#[derive(Clone, Default, Serialize, Deserialize)]
struct Requirements {
    version: u32,
    blocks: BTreeMap<BlockId, usize>,
}

pub(crate) struct RepairLog {
    path: PathBuf,
    requirements: Mutex<Requirements>,
}

impl RepairLog {
    pub(crate) fn open(dir: &Path) -> io::Result<Self> {
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
        {
            return Err(io::Error::other("invalid replica requirements"));
        }
        Ok(Self {
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
        let bytes = serde_json::to_vec(&next).map_err(io::Error::other)?;
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
        *current = next;
        Ok(())
    }

    /// Hold the intent lock across compaction, so a newly recorded upload
    /// cannot race the empty check. Retention reclamation is deliberately
    /// unavailable until a distributed GC/release protocol exists.
    pub(crate) fn compact<T>(
        &self,
        operation: impl FnOnce() -> Result<T, kalpak_core::Error>,
    ) -> Result<T, kalpak_core::Error> {
        let guard = self.requirements.lock().unwrap();
        if !guard.blocks.is_empty() {
            return Err(io::Error::other(
                "compaction disabled while replica retention requirements exist",
            )
            .into());
        }
        operation()
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
                    disk_state.store.get(&id)
                })
                .await;
                if payload.is_err() {
                    continue;
                }
                if let Ok(Err(ref error)) = payload {
                    if !matches!(error, kalpak_core::Error::BlockNotFound(_)) {
                        eprintln!("kalpakdb: repair {id} cannot prepare local source: {error}");
                        continue;
                    }
                }
                let mut payload = payload
                    .ok()
                    .and_then(Result::ok)
                    .map(|payload| bytes::Bytes::copy_from_slice(&payload));
                if payload.is_none() {
                    // A newly elected leader need not have the tensor yet.
                    // Fetch from a retained copy, never through cascading reads.
                    for (_, peer) in state.control.peer_nodes() {
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
                                    disk_state
                                        .store
                                        .put(&bytes)
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
    fn requirements_survive_reopen_and_never_downgrade() {
        let dir = tempfile::tempdir().unwrap();
        let id = BlockId::of(b"repair");
        let log = RepairLog::open(dir.path()).unwrap();
        log.record(&[id], 3).unwrap();
        log.record(&[id], 1).unwrap();
        assert!(log.compact(|| Ok(())).is_err());
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
        assert!(log.compact(|| Ok(())).is_ok());
        assert!(!dir.path().join("replica-requirements.json").exists());
    }
}
