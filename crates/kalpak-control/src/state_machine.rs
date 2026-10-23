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
    pub leases: crate::LeaseState,
    pub replica_requirements: std::collections::BTreeMap<BlockId, usize>,
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

// Exact pre-replica-state bincode layout. serde defaults cannot make an
// appended bincode struct field backward compatible.
#[derive(Serialize, Deserialize)]
struct LegacyMetadataState {
    agents: HashMap<AgentId, AgentRecord>,
    bindings: HashMap<String, BindingRecord>,
    children: HashMap<String, Vec<String>>,
    block_refs: HashMap<BlockId, u32>,
    last_applied: Option<LogId>,
    membership: StoredMembership<NodeId, openraft::BasicNode>,
}
const SNAPSHOT_V2: &[u8] = b"KLPKSM02";
const SNAPSHOT_V3: &[u8] = b"KLPKSM03";
const SNAPSHOT_V4: &[u8] = b"KLPKSM04";

// Exact version-three layout: bincode cannot default an appended field.
#[derive(Serialize, Deserialize)]
struct V3LeaseState {
    initialized: bool,
    records: std::collections::BTreeMap<BlockId, crate::UploadLease>,
    revisions: HashMap<String, u64>,
}

#[derive(Serialize, Deserialize)]
struct V3MetadataState {
    leases: V3LeaseState,
    replica_requirements: std::collections::BTreeMap<BlockId, usize>,
    agents: HashMap<AgentId, AgentRecord>,
    bindings: HashMap<String, BindingRecord>,
    children: HashMap<String, Vec<String>>,
    block_refs: HashMap<BlockId, u32>,
    last_applied: Option<LogId>,
    membership: StoredMembership<NodeId, openraft::BasicNode>,
}

#[derive(Serialize, Deserialize)]
struct V2MetadataState {
    replica_requirements: std::collections::BTreeMap<BlockId, usize>,
    agents: HashMap<AgentId, AgentRecord>,
    bindings: HashMap<String, BindingRecord>,
    children: HashMap<String, Vec<String>>,
    block_refs: HashMap<BlockId, u32>,
    last_applied: Option<LogId>,
    membership: StoredMembership<NodeId, openraft::BasicNode>,
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
            if let Some(payload) = data.strip_prefix(SNAPSHOT_V4) {
                let (state, consumed) =
                    bincode::serde::decode_from_slice(payload, bincode::config::standard())
                        .map_err(std::io::Error::other)?;
                (state, consumed + SNAPSHOT_V4.len())
            } else if let Some(payload) = data.strip_prefix(SNAPSHOT_V3) {
                let (old, consumed): (V3MetadataState, usize) =
                    bincode::serde::decode_from_slice(payload, bincode::config::standard())
                        .map_err(std::io::Error::other)?;
                (
                    MetadataState {
                        leases: crate::LeaseState {
                            initialized: old.leases.initialized,
                            records: old.leases.records,
                            revisions: old.leases.revisions,
                            cancelled: Default::default(),
                        },
                        replica_requirements: old.replica_requirements,
                        agents: old.agents,
                        bindings: old.bindings,
                        children: old.children,
                        block_refs: old.block_refs,
                        last_applied: old.last_applied,
                        membership: old.membership,
                    },
                    consumed + SNAPSHOT_V3.len(),
                )
            } else if let Some(payload) = data.strip_prefix(SNAPSHOT_V2) {
                let (old, consumed): (V2MetadataState, usize) =
                    bincode::serde::decode_from_slice(payload, bincode::config::standard())
                        .map_err(std::io::Error::other)?;
                (
                    MetadataState {
                        leases: Default::default(),
                        replica_requirements: old.replica_requirements,
                        agents: old.agents,
                        bindings: old.bindings,
                        children: old.children,
                        block_refs: old.block_refs,
                        last_applied: old.last_applied,
                        membership: old.membership,
                    },
                    consumed + SNAPSHOT_V2.len(),
                )
            } else {
                let (legacy, consumed): (LegacyMetadataState, usize) =
                    bincode::serde::decode_from_slice(data, bincode::config::standard())
                        .map_err(std::io::Error::other)?;
                (
                    MetadataState {
                        leases: Default::default(),
                        agents: legacy.agents,
                        bindings: legacy.bindings,
                        children: legacy.children,
                        block_refs: legacy.block_refs,
                        last_applied: legacy.last_applied,
                        membership: legacy.membership,
                        replica_requirements: Default::default(),
                    },
                    consumed,
                )
            };
        if consumed != data.len()
            || state.last_applied != meta.last_log_id
            || state.membership != meta.last_membership
        {
            return Err(std::io::Error::other("snapshot metadata/payload mismatch"));
        }
        if state.replica_requirements.len() > 16_384
            || state
                .replica_requirements
                .values()
                .any(|n| !(2..=16).contains(n))
        {
            return Err(std::io::Error::other(
                "invalid snapshot replica requirements",
            ));
        }
        state.leases.validate()?;
        if state.leases.cancelled.iter().any(|(id, at)| {
            *at < state.leases.records[id].created_at
                || *at > state.last_applied.map(|id| id.index).unwrap_or(0)
        }) {
            return Err(std::io::Error::other("invalid lease cancellation position"));
        }
        if state
            .leases
            .revisions
            .keys()
            .any(|key| !state.bindings.contains_key(key))
            || state.leases.records.values().any(|lease| {
                !state.agents.contains_key(&lease.agent)
                    || lease.committed.as_ref().is_some_and(|commit| {
                        state
                            .leases
                            .revisions
                            .get(&binding_key(&commit.binding.key))
                            .is_none_or(|revision| *revision <= commit.expected_revision)
                    })
            })
        {
            return Err(std::io::Error::other("inconsistent upload lease snapshot"));
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
            Request::CancelUploadLease { lease, agent } => {
                crate::leases::cancel(state, *lease, *agent)
            }
            Request::BeginUploadLease {
                lease,
                agent,
                blocks,
            } => crate::leases::reserve(state, *lease, *agent, blocks),
            Request::CommitUploadLease {
                lease,
                agent,
                binding,
                expected_revision,
            } => {
                match crate::leases::validate_commit(
                    state,
                    lease,
                    agent,
                    binding,
                    *expected_revision,
                ) {
                    Err(message) => Response::Rejected(message),
                    Ok(false) => Response::Bound,
                    Ok(true) => {
                        Self::bind_one(
                            state,
                            agent,
                            &binding.key,
                            &binding.blocks,
                            binding.parent.as_ref(),
                        );
                        state.leases.records.get_mut(lease).unwrap().committed =
                            Some(crate::leases::LeaseCommit {
                                binding: binding.clone(),
                                expected_revision: *expected_revision,
                            });
                        Response::Bound
                    }
                }
            }
            Request::RequireReplicas { blocks, replicas } => {
                if !(2..=16).contains(replicas) || blocks.len() > 4096 {
                    return Response::Rejected("invalid replica requirements".to_string());
                }
                let mut next = state.replica_requirements.clone();
                for id in blocks {
                    let n = next.entry(*id).or_insert(*replicas);
                    *n = (*n).max(*replicas);
                }
                if next.len() > 16_384 {
                    return Response::Rejected(
                        "replica requirement capacity exhausted".to_string(),
                    );
                }
                state.replica_requirements = next;
                Response::ReplicasRequired
            }
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
        if state.leases.initialized {
            let revision = state.leases.revisions.entry(key_s.clone()).or_insert(0);
            *revision = revision.saturating_add(1);
        }
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
            let encoded = bincode::serde::encode_to_vec(&*state, bincode::config::standard())
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut data = SNAPSHOT_V4.to_vec();
            data.extend_from_slice(&encoded);
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

    fn lease_setup() -> (MetadataState, AgentId, CacheKey, BlockId) {
        let mut state = MetadataState::default();
        let agent = AgentId::from_verifying_key(
            &ed25519_dalek::SigningKey::from_bytes(&[35; 32]).verifying_key(),
        );
        StateMachineStore::apply_one(
            &mut state,
            &Request::RegisterAgent {
                agent,
                display_name: "leases".into(),
            },
        );
        (
            state,
            agent,
            CacheKey::root(kalpak_core::ModelFingerprint::new("m", "t", "l"), &[1]),
            BlockId::of(b"data"),
        )
    }

    #[tokio::test]
    async fn cancellation_is_terminal_idempotent_owner_checked_and_durable() {
        let (mut state, agent, key, block) = lease_setup();
        let lease = BlockId::of(b"cancel-me");
        let reserve = Request::BeginUploadLease {
            lease,
            agent,
            blocks: vec![block],
        };
        StateMachineStore::apply_one(&mut state, &reserve);
        let stranger = AgentId::from_verifying_key(
            &ed25519_dalek::SigningKey::from_bytes(&[38; 32]).verifying_key(),
        );
        for request in [
            Request::CancelUploadLease {
                lease,
                agent: stranger,
            },
            Request::CancelUploadLease {
                lease: BlockId::of(b"unknown"),
                agent,
            },
        ] {
            assert!(matches!(
                StateMachineStore::apply_one(&mut state, &request),
                Response::Rejected(_)
            ));
        }
        assert!(state.leases.cancelled.is_empty());
        let cancel = Request::CancelUploadLease { lease, agent };
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &cancel),
            Response::LeaseCancelled
        ));
        let at = state.leases.cancelled[&lease];
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &cancel),
            Response::LeaseCancelled
        ));
        assert_eq!(state.leases.cancelled[&lease], at);
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &reserve),
            Response::Rejected(_)
        ));
        let commit = Request::CommitUploadLease {
            lease,
            agent,
            binding: ChainBinding {
                key,
                blocks: vec![block],
                parent: None,
            },
            expected_revision: 0,
        };
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &commit),
            Response::Rejected(_)
        ));
        assert!(state.bindings.is_empty());
        assert!(state.block_refs.is_empty());
        assert!(state.leases.revisions.is_empty());
        assert_eq!(state.leases.records[&lease].blocks, vec![block]);
        let dir = tempfile::tempdir().unwrap();
        let mut store = StateMachineStore::open(dir.path().to_owned()).unwrap();
        *store.state.write().unwrap() = state;
        let snapshot = store.build_snapshot().await.unwrap();
        assert!(snapshot.snapshot.get_ref().starts_with(SNAPSHOT_V4));
        drop(store);
        let store = StateMachineStore::open(dir.path().to_owned()).unwrap();
        assert!(store
            .state
            .read()
            .unwrap()
            .leases
            .cancelled
            .contains_key(&lease));
        assert!(matches!(
            StateMachineStore::apply_one(&mut store.state.write().unwrap(), &commit),
            Response::Rejected(_)
        ));
        // Reject a damaged snapshot that claims a future cancellation index.
        store
            .state
            .write()
            .unwrap()
            .leases
            .cancelled
            .insert(lease, 1);
        let mut bad = SNAPSHOT_V4.to_vec();
        bad.extend(
            bincode::serde::encode_to_vec(
                &*store.state.read().unwrap(),
                bincode::config::standard(),
            )
            .unwrap(),
        );
        assert!(StateMachineStore::decode_snapshot(&snapshot.meta, &bad).is_err());
    }

    #[test]
    fn commit_winning_before_cancel_keeps_its_binding_and_retry_contract() {
        let (mut state, agent, key, block) = lease_setup();
        let lease = BlockId::of(b"commit-wins");
        StateMachineStore::apply_one(
            &mut state,
            &Request::BeginUploadLease {
                lease,
                agent,
                blocks: vec![block],
            },
        );
        let commit = Request::CommitUploadLease {
            lease,
            agent,
            binding: ChainBinding {
                key: key.clone(),
                blocks: vec![block],
                parent: None,
            },
            expected_revision: 0,
        };
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &commit),
            Response::Bound
        ));
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &Request::CancelUploadLease { lease, agent }),
            Response::Rejected(_)
        ));
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &commit),
            Response::Bound
        ));
        assert_eq!(state.bindings[&binding_key(&key)].blocks, vec![block]);
        assert_eq!(state.block_refs[&block], 1);
        assert_eq!(state.leases.revisions[&binding_key(&key)], 1);
        assert!(state.leases.cancelled.is_empty());
    }

    #[test]
    fn v3_snapshot_preserves_committed_leases_and_fences_without_cancellations() {
        let (mut state, agent, key, block) = lease_setup();
        let lease = BlockId::of(b"v3-reservation");
        StateMachineStore::apply_one(
            &mut state,
            &Request::BeginUploadLease {
                lease,
                agent,
                blocks: vec![block],
            },
        );
        StateMachineStore::apply_one(
            &mut state,
            &Request::CommitUploadLease {
                lease,
                agent,
                binding: ChainBinding {
                    key: key.clone(),
                    blocks: vec![block],
                    parent: None,
                },
                expected_revision: 0,
            },
        );
        let old = V3MetadataState {
            leases: V3LeaseState {
                initialized: state.leases.initialized,
                records: state.leases.records,
                revisions: state.leases.revisions,
            },
            replica_requirements: state.replica_requirements,
            agents: state.agents,
            bindings: state.bindings,
            children: state.children,
            block_refs: state.block_refs,
            last_applied: state.last_applied,
            membership: state.membership,
        };
        let mut data = SNAPSHOT_V3.to_vec();
        data.extend(bincode::serde::encode_to_vec(&old, bincode::config::standard()).unwrap());
        let meta = SnapshotMeta {
            last_log_id: None,
            last_membership: Default::default(),
            snapshot_id: "v3".into(),
        };
        let state = StateMachineStore::decode_snapshot(&meta, &data).unwrap();
        assert!(state.leases.records[&lease].committed.is_some());
        assert_eq!(state.leases.revisions[&binding_key(&key)], 1);
        assert!(state.leases.cancelled.is_empty());
    }

    #[test]
    fn different_pre_activation_replay_histories_start_at_the_same_fence() {
        let (mut replayed, agent, key, block) = lease_setup();
        let (mut imported, _, _, _) = lease_setup();
        let old = Request::BindPrefix {
            agent,
            key: key.clone(),
            blocks: vec![block],
            parent: None,
        };
        // One node replays old mutations; another imports their final binding
        // from a newer pre-lease snapshot. Historical counters are unknowable.
        for _ in 0..4 {
            StateMachineStore::apply_one(&mut replayed, &old);
        }
        StateMachineStore::apply_one(&mut imported, &old);
        assert!(replayed.leases.revisions.is_empty());
        assert!(imported.leases.revisions.is_empty());
        let lease = BlockId::of(b"activation-barrier");
        for state in [&mut replayed, &mut imported] {
            assert!(matches!(
                StateMachineStore::apply_one(
                    state,
                    &Request::BeginUploadLease {
                        lease,
                        agent,
                        blocks: vec![block]
                    }
                ),
                Response::LeaseReserved
            ));
            assert!(matches!(
                StateMachineStore::apply_one(
                    state,
                    &Request::CommitUploadLease {
                        lease,
                        agent,
                        binding: ChainBinding {
                            key: key.clone(),
                            blocks: vec![block],
                            parent: None
                        },
                        expected_revision: 0
                    }
                ),
                Response::Bound
            ));
            assert_eq!(state.leases.revisions[&binding_key(&key)], 1);
        }
        assert_eq!(
            serde_json::to_value(&replayed.leases).unwrap(),
            serde_json::to_value(&imported.leases).unwrap()
        );
    }

    #[tokio::test]
    async fn lease_commit_is_atomic_revision_fenced_and_durable() {
        let (mut state, agent, key, block) = lease_setup();
        let lease = BlockId::of(b"lease-1");
        let reserve = Request::BeginUploadLease {
            lease,
            agent,
            blocks: vec![block],
        };
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &reserve),
            Response::LeaseReserved
        ));
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &reserve),
            Response::LeaseReserved
        ));
        let commit = Request::CommitUploadLease {
            lease,
            agent,
            binding: ChainBinding {
                key: key.clone(),
                blocks: vec![block],
                parent: None,
            },
            expected_revision: 0,
        };
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &commit),
            Response::Bound
        ));
        assert_eq!(state.leases.revisions[&binding_key(&key)], 1);
        assert_eq!(state.block_refs[&block], 1);
        let newer = BlockId::of(b"newer");
        StateMachineStore::apply_one(
            &mut state,
            &Request::BindPrefix {
                agent,
                key: key.clone(),
                blocks: vec![newer],
                parent: None,
            },
        );
        assert_eq!(state.leases.revisions[&binding_key(&key)], 2);
        // A consumed retry must not reapply its older state after a legacy bind.
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &commit),
            Response::Bound
        ));
        assert_eq!(state.bindings[&binding_key(&key)].blocks, vec![newer]);
        let second = BlockId::of(b"lease-2");
        StateMachineStore::apply_one(
            &mut state,
            &Request::BeginUploadLease {
                lease: second,
                agent,
                blocks: vec![block],
            },
        );
        let mut second_commit = commit.clone();
        if let Request::CommitUploadLease {
            lease,
            expected_revision,
            ..
        } = &mut second_commit
        {
            *lease = second;
            *expected_revision = 1;
        }
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &second_commit),
            Response::Rejected(_)
        ));
        assert!(state.leases.records[&second].committed.is_none());
        assert_eq!(state.bindings[&binding_key(&key)].blocks, vec![newer]);
        if let Request::CommitUploadLease {
            expected_revision, ..
        } = &mut second_commit
        {
            *expected_revision = 2;
        }
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &second_commit),
            Response::Bound
        ));
        assert_eq!(state.leases.revisions[&binding_key(&key)], 3);
        assert_eq!(state.block_refs[&block], 1);
        let dir = tempfile::tempdir().unwrap();
        let mut store = StateMachineStore::open(dir.path().to_owned()).unwrap();
        *store.state.write().unwrap() = state;
        let snapshot = store.build_snapshot().await.unwrap();
        assert!(snapshot.snapshot.get_ref().starts_with(SNAPSHOT_V4));
        drop(store);
        let store = StateMachineStore::open(dir.path().to_owned()).unwrap();
        let state = store.state.read().unwrap();
        assert!(state.leases.records[&second].committed.is_some());
        assert_eq!(state.leases.revisions[&binding_key(&key)], 3);
    }

    #[test]
    fn leases_reject_owner_changes_wrong_payloads_and_capacity_without_mutation() {
        let (mut state, agent, key, block) = lease_setup();
        let lease = BlockId::of(b"lease");
        StateMachineStore::apply_one(
            &mut state,
            &Request::BeginUploadLease {
                lease,
                agent,
                blocks: vec![block],
            },
        );
        let bad = Request::BeginUploadLease {
            lease,
            agent,
            blocks: vec![BlockId::of(b"different")],
        };
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &bad),
            Response::Rejected(_)
        ));
        let wrong = Request::CommitUploadLease {
            lease,
            agent,
            binding: ChainBinding {
                key: key.clone(),
                blocks: vec![BlockId::of(b"wrong")],
                parent: None,
            },
            expected_revision: 0,
        };
        assert!(matches!(
            StateMachineStore::apply_one(&mut state, &wrong),
            Response::Rejected(_)
        ));
        assert!(state.bindings.is_empty());
        assert!(state.leases.records[&lease].committed.is_none());
        let record = state.leases.records[&lease].clone();
        for n in 0..crate::MAX_LEASES - 1 {
            state
                .leases
                .records
                .insert(BlockId::of(&n.to_le_bytes()), record.clone());
        }
        let before = state.leases.records.len();
        assert!(matches!(
            StateMachineStore::apply_one(
                &mut state,
                &Request::BeginUploadLease {
                    lease: BlockId::of(b"beyond-capacity"),
                    agent,
                    blocks: vec![block]
                }
            ),
            Response::Rejected(_)
        ));
        assert_eq!(state.leases.records.len(), before);
    }

    #[test]
    fn v2_snapshot_with_bindings_imports_without_leases() {
        let (mut state, agent, key, block) = lease_setup();
        StateMachineStore::apply_one(
            &mut state,
            &Request::BindPrefix {
                agent,
                key: key.clone(),
                blocks: vec![block],
                parent: None,
            },
        );
        let old = V2MetadataState {
            replica_requirements: state.replica_requirements,
            agents: state.agents,
            bindings: state.bindings,
            children: state.children,
            block_refs: state.block_refs,
            last_applied: state.last_applied,
            membership: state.membership,
        };
        let mut data = SNAPSHOT_V2.to_vec();
        data.extend(bincode::serde::encode_to_vec(&old, bincode::config::standard()).unwrap());
        let meta = SnapshotMeta {
            last_log_id: None,
            last_membership: Default::default(),
            snapshot_id: "v2".into(),
        };
        let state = StateMachineStore::decode_snapshot(&meta, &data).unwrap();
        assert_eq!(state.agents.len(), 1);
        assert_eq!(state.bindings[&binding_key(&key)].blocks, vec![block]);
        assert!(state.leases.revisions.is_empty());
        assert!(state.leases.records.is_empty());
    }

    #[test]
    fn legacy_snapshot_layout_remains_readable() {
        let legacy = LegacyMetadataState {
            agents: Default::default(),
            bindings: Default::default(),
            children: Default::default(),
            block_refs: Default::default(),
            last_applied: None,
            membership: Default::default(),
        };
        let bytes = bincode::serde::encode_to_vec(&legacy, bincode::config::standard()).unwrap();
        let meta = SnapshotMeta {
            last_log_id: None,
            last_membership: Default::default(),
            snapshot_id: "legacy".to_string(),
        };
        let recovered = StateMachineStore::decode_snapshot(&meta, &bytes).unwrap();
        assert!(recovered.replica_requirements.is_empty());
    }

    #[tokio::test]
    async fn replica_requirements_survive_snapshot_and_reject_downgrades() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = StateMachineStore::open(dir.path().to_owned()).unwrap();
        let id = BlockId::of(b"replicated-intent");
        {
            let mut state = store.state.write().unwrap();
            assert!(matches!(
                StateMachineStore::apply_one(
                    &mut state,
                    &Request::RequireReplicas {
                        blocks: vec![id],
                        replicas: 3
                    }
                ),
                Response::ReplicasRequired
            ));
            StateMachineStore::apply_one(
                &mut state,
                &Request::RequireReplicas {
                    blocks: vec![id],
                    replicas: 2,
                },
            );
            assert!(matches!(
                StateMachineStore::apply_one(
                    &mut state,
                    &Request::RequireReplicas {
                        blocks: vec![id],
                        replicas: 17
                    }
                ),
                Response::Rejected(_)
            ));
        }
        store.build_snapshot().await.unwrap();
        drop(store);
        let recovered = StateMachineStore::open(dir.path().to_owned()).unwrap();
        assert_eq!(recovered.state.read().unwrap().replica_requirements[&id], 3);
    }

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
