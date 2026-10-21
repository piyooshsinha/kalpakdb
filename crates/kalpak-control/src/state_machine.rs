//! The replicated metadata state machine: agent registry + prefix bindings.
//!
//! State is shared behind an `Arc<RwLock<..>>` so the control plane can serve
//! linearizable-enough local reads (after `Raft::ensure_linearizable` at the
//! call site, when needed) without a round trip through the log.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use kalpak_core::{AgentId, BlockId, CacheKey};
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    EntryPayload, RaftSnapshotBuilder, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};

use crate::types::{ChainBinding, Entry, LogId, NodeId, Request, Response, TypeConfig};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentRecord {
    pub display_name: String,
    /// Log index of the registration, for lineage ordering.
    pub registered_at: u64,
}

#[derive(Default, Serialize, Deserialize)]
pub struct MetadataState {
    pub agents: HashMap<AgentId, AgentRecord>,
    /// CacheKey -> ordered blocks; HashMap with serde needs string keys, so
    /// bindings are stored under the serialized key.
    pub bindings: HashMap<String, BindingRecord>,
    /// Prefix tree edges: parent key -> child keys. Fed by `BindPrefix`
    /// requests that declare their parent; drives lookahead prefetch.
    #[serde(default)]
    pub children: HashMap<String, Vec<String>>,
    /// How many bindings reference each block: the structural importance
    /// signal (a block shared by many prefixes — e.g. a common system
    /// prompt — matters more than a single-context block).
    #[serde(default)]
    pub block_refs: HashMap<BlockId, u32>,
    pub last_applied: Option<LogId>,
    pub membership: StoredMembership<NodeId, openraft::BasicNode>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BindingRecord {
    pub agent: AgentId,
    pub blocks: Vec<BlockId>,
}

pub fn binding_key(key: &CacheKey) -> String {
    serde_json::to_string(key).expect("cache key serializes")
}

#[derive(Default, Clone)]
pub struct StateMachineStore {
    pub state: Arc<RwLock<MetadataState>>,
    snapshot: Arc<RwLock<Option<StoredSnapshotShared>>>,
    snapshot_idx: Arc<RwLock<u64>>,
    directory: Option<PathBuf>,
    publication: Arc<Mutex<()>>,
}

type StoredSnapshotShared = (SnapshotMeta<NodeId, openraft::BasicNode>, Vec<u8>);

#[derive(Serialize, Deserialize)]
struct DurableSnapshot {
    version: u32,
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    data: Vec<u8>,
}

impl StateMachineStore {
    /// Recover the snapshot before OpenRaft inspects applied state or logs.
    pub fn open(directory: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&directory)?;
        let store = Self {
            directory: Some(directory.clone()),
            ..Self::default()
        };
        let bytes = match std::fs::read(directory.join("raft-snapshot.json")) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(store),
            Err(e) => return Err(e),
        };
        let saved: DurableSnapshot =
            serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
        if saved.version != 1 {
            return Err(std::io::Error::other("unsupported snapshot version"));
        }
        let state = Self::decode_snapshot(&saved.meta, &saved.data)?;
        *store.state.write().unwrap() = state;
        *store.snapshot.write().unwrap() = Some((saved.meta, saved.data));
        Ok(store)
    }

    fn decode_snapshot(
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        data: &[u8],
    ) -> std::io::Result<MetadataState> {
        let (state, consumed): (MetadataState, usize) =
            bincode::serde::decode_from_slice(data, bincode::config::standard())
                .map_err(std::io::Error::other)?;
        if consumed != data.len()
            || state.last_applied != meta.last_log_id
            || state.membership != meta.last_membership
        {
            return Err(std::io::Error::other("snapshot metadata/payload mismatch"));
        }
        Ok(state)
    }

    fn persist_snapshot(
        &self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        data: &[u8],
    ) -> std::io::Result<()> {
        if let Some(dir) = &self.directory {
            let saved = DurableSnapshot {
                version: 1,
                meta: meta.clone(),
                data: data.to_vec(),
            };
            let bytes = serde_json::to_vec(&saved).map_err(std::io::Error::other)?;
            crate::durability::replace(&dir.join("raft-snapshot.json"), &bytes)?;
        }
        Ok(())
    }

    fn apply_one(state: &mut MetadataState, req: &Request) -> Response {
        match req {
            Request::RegisterAgent {
                agent,
                display_name,
            } => {
                let at = state.last_applied.map(|l| l.index).unwrap_or(0);
                state.agents.insert(
                    *agent,
                    AgentRecord {
                        display_name: display_name.clone(),
                        registered_at: at,
                    },
                );
                Response::Registered
            }
            Request::BindChain { agent, bindings } => {
                for ChainBinding {
                    key,
                    blocks,
                    parent,
                } in bindings
                {
                    Self::bind_one(state, agent, key, blocks, parent.as_ref());
                }
                Response::Bound
            }
            Request::BindPrefix {
                agent,
                key,
                blocks,
                parent,
            } => {
                Self::bind_one(state, agent, key, blocks, parent.as_deref());
                Response::Bound
            }
        }
    }

    fn bind_one(
        state: &mut MetadataState,
        agent: &AgentId,
        key: &CacheKey,
        blocks: &[BlockId],
        parent: Option<&CacheKey>,
    ) {
        let key_s = binding_key(key);
        // Refcounts track binding membership: rebinding a key first releases
        // its old blocks, so counts stay exact rather than ever-growing.
        if let Some(old) = state.bindings.get(&key_s) {
            for b in &old.blocks {
                if let Some(n) = state.block_refs.get_mut(b) {
                    *n = n.saturating_sub(1);
                    if *n == 0 {
                        state.block_refs.remove(b);
                    }
                }
            }
        }
        for b in blocks {
            *state.block_refs.entry(*b).or_insert(0) += 1;
        }
        state.bindings.insert(
            key_s.clone(),
            BindingRecord {
                agent: *agent,
                blocks: blocks.to_vec(),
            },
        );
        if let Some(parent) = parent {
            let kids = state.children.entry(binding_key(parent)).or_default();
            if !kids.contains(&key_s) {
                kids.push(key_s);
            }
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let _publication = self.publication.lock().unwrap();
        let (data, last_applied, membership) = {
            let state = self.state.read().unwrap();
            // bincode, not JSON: snapshotting holds the state read lock, so
            // serialization speed bounds how long writes can stall once the
            // binding map grows large.
            let data = bincode::serde::encode_to_vec(&*state, bincode::config::standard())
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            (data, state.last_applied, state.membership.clone())
        };

        let idx = {
            let mut i = self.snapshot_idx.write().unwrap();
            *i += 1;
            *i
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id: format!(
                "snapshot-{}-{idx}",
                last_applied.map(|l| l.index).unwrap_or(0)
            ),
        };
        // A snapshot must be durable BEFORE it is returned: OpenRaft may
        // now discard the only log entries containing this metadata.
        self.persist_snapshot(&meta, &data)
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
        *self.snapshot.write().unwrap() = Some((meta.clone(), data.clone()));

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId>, StoredMembership<NodeId, openraft::BasicNode>), StorageError<NodeId>>
    {
        let state = self.state.read().unwrap();
        Ok((state.last_applied, state.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry> + Send,
    {
        let mut state = self.state.write().unwrap();
        let mut replies = Vec::new();
        for entry in entries {
            state.last_applied = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => replies.push(Response::Registered),
                EntryPayload::Normal(ref req) => {
                    let r = Self::apply_one(&mut state, req);
                    replies.push(r);
                }
                EntryPayload::Membership(ref mem) => {
                    state.membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    replies.push(Response::Registered);
                }
            }
        }
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let _publication = self.publication.lock().unwrap();
        let new_state = Self::decode_snapshot(meta, &data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        self.persist_snapshot(meta, &data)
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
        *self.state.write().unwrap() = new_state;
        *self.snapshot.write().unwrap() = Some((meta.clone(), data));
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(self
            .snapshot
            .read()
            .unwrap()
            .clone()
            .map(|(meta, data)| Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(data)),
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn installed_snapshot_is_durable_and_rejects_mismatched_metadata() {
        let source_dir = tempfile::tempdir().unwrap();
        let mut source = StateMachineStore::open(source_dir.path().to_owned()).unwrap();
        let a = AgentId::from_verifying_key(
            &ed25519_dalek::SigningKey::from_bytes(&[17; 32]).verifying_key(),
        );
        StateMachineStore::apply_one(
            &mut source.state.write().unwrap(),
            &Request::RegisterAgent {
                agent: a,
                display_name: "installed".to_string(),
            },
        );
        let snap = source.build_snapshot().await.unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let mut target = StateMachineStore::open(target_dir.path().to_owned()).unwrap();
        target
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();
        drop(target);
        let mut target = StateMachineStore::open(target_dir.path().to_owned()).unwrap();
        assert_eq!(
            target.state.read().unwrap().agents[&a].display_name,
            "installed"
        );
        let good = target.get_current_snapshot().await.unwrap().unwrap();
        let mut bad_meta = good.meta.clone();
        bad_meta.last_log_id = Some(openraft::LogId::new(
            openraft::CommittedLeaderId::new(1, 1),
            90,
        ));
        assert!(target
            .install_snapshot(&bad_meta, good.snapshot)
            .await
            .is_err());
        drop(target);
        assert!(StateMachineStore::open(target_dir.path().to_owned())
            .unwrap()
            .state
            .read()
            .unwrap()
            .agents
            .contains_key(&a));
    }

    #[tokio::test]
    async fn unfinished_snapshot_replacement_does_not_replace_published_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = StateMachineStore::open(dir.path().to_owned()).unwrap();
        let snap = store.build_snapshot().await.unwrap();
        drop(store);
        std::fs::write(
            dir.path().join("raft-snapshot.pending"),
            b"interrupted replacement",
        )
        .unwrap();
        let mut recovered = StateMachineStore::open(dir.path().to_owned()).unwrap();
        assert_eq!(
            recovered
                .get_current_snapshot()
                .await
                .unwrap()
                .unwrap()
                .meta,
            snap.meta
        );
        std::fs::write(
            dir.path().join("raft-snapshot.json"),
            b"corrupt published snapshot",
        )
        .unwrap();
        assert!(StateMachineStore::open(dir.path().to_owned()).is_err());
    }
}
