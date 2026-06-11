//! The replicated metadata state machine: agent registry + prefix bindings.
//!
//! State is shared behind an `Arc<RwLock<..>>` so the control plane can serve
//! linearizable-enough local reads (after `Raft::ensure_linearizable` at the
//! call site, when needed) without a round trip through the log.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, RwLock};

use kalpak_core::{AgentId, BlockId, CacheKey};
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    EntryPayload, RaftSnapshotBuilder, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};

use crate::types::{Entry, LogId, NodeId, Request, Response, TypeConfig};

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
}

type StoredSnapshotShared = (SnapshotMeta<NodeId, openraft::BasicNode>, Vec<u8>);

impl StateMachineStore {
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
            Request::BindPrefix { agent, key, blocks } => {
                state.bindings.insert(
                    binding_key(key),
                    BindingRecord {
                        agent: *agent,
                        blocks: blocks.clone(),
                    },
                );
                Response::Bound
            }
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
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
            snapshot_id: format!("snapshot-{idx}"),
        };
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
        let (new_state, _): (MetadataState, _) =
            bincode::serde::decode_from_slice(&data, bincode::config::standard())
                .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
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
