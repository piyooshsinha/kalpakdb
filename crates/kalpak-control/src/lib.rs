//! Kalpak control plane: a Raft-replicated metadata state machine.
//!
//! Replicates agent registrations and prefix-manifest bindings via
//! `openraft`. Strictly metadata: tensors and blobs stay on the data plane
//! and are referenced here by content address only.
//!
//! Topology is dynamic: boot a node with [`ControlPlane::start_node`],
//! initialize a cluster on the first one, then grow it with
//! [`ControlPlane::add_learner`] + [`ControlPlane::change_membership`].
//! Inter-node RPCs travel as JSON over HTTP ([`network::HttpNetworkFactory`]);
//! the node's API server wires `/raft/*` to the [`ControlPlane::handle_*`]
//! methods. The Raft log and vote are durable when a data directory is given.

mod durability;
mod leases;
mod log_store;
mod network;
mod state_machine;
mod types;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use kalpak_core::{AgentId, BlockId, CacheKey};
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config, Raft};

pub use leases::{
    LeaseDeadline, LeaseState, LeaseStatusCounts, UploadLease, MAX_LEASES, MAX_LEASE_BLOCKS,
    MAX_TTL_TICKS,
};
pub use state_machine::{AgentRecord, BindingRecord, MetadataState};
pub use types::{ChainBinding, NodeId, Request, Response, TypeConfig};

use log_store::LogStore;
use network::HttpNetworkFactory;
pub use network::MeshClientTls;
use state_machine::{binding_key, StateMachineStore};

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("raft init: {0}")]
    Init(String),
    #[error("raft write: {0}")]
    Write(String),
    #[error("metadata request rejected: {0}")]
    Rejected(String),
    #[error("raft membership: {0}")]
    Membership(String),
    /// This node is a follower; the caller should retry against the leader.
    #[error("not the leader; leader is {leader_addr:?}")]
    NotLeader { leader_addr: Option<String> },
}

pub struct ControlPlane {
    raft: Raft<TypeConfig>,
    sm: StateMachineStore,
}

impl ControlPlane {
    /// Boot a Raft node. With `data_dir`, the log and vote are durable and
    /// replayed on restart. The node joins no cluster by itself — call
    /// [`Self::init_cluster`] on the first node, then grow membership.
    pub async fn start_node(
        node_id: NodeId,
        data_dir: Option<&Path>,
    ) -> Result<Self, ControlError> {
        Self::start_node_with_mesh(node_id, data_dir, None).await
    }

    /// Like [`Self::start_node`], with optional mutually-authenticated TLS
    /// for the node-to-node transport.
    pub async fn start_node_with_mesh(
        node_id: NodeId,
        data_dir: Option<&Path>,
        mesh: Option<&MeshClientTls>,
    ) -> Result<Self, ControlError> {
        let config = Config {
            heartbeat_interval: 250,
            election_timeout_min: 500,
            election_timeout_max: 1000,
            ..Default::default()
        };
        let config = Arc::new(
            config
                .validate()
                .map_err(|e| ControlError::Init(e.to_string()))?,
        );

        let mut log_store = match data_dir {
            Some(dir) => LogStore::open(dir.join("control"))
                .map_err(|e| ControlError::Init(e.to_string()))?,
            None => LogStore::in_memory(),
        };
        let sm = match data_dir {
            Some(dir) => StateMachineStore::open(dir.join("control"))
                .map_err(|e| ControlError::Init(format!("snapshot recovery: {e}")))?,
            None => StateMachineStore::default(),
        };
        // An older deployment may already have purged logs while its only
        // snapshot was volatile. Do not start an apparently healthy empty
        // node when the discarded state cannot be reconstructed.
        let log_state = openraft::storage::RaftLogStorage::get_log_state(&mut log_store)
            .await
            .map_err(|e| ControlError::Init(format!("log recovery: {e}")))?;
        if let Some(purged) = log_state.last_purged_log_id {
            let applied = sm.state.read().unwrap().last_applied;
            if !applied.is_some_and(|id| id.index >= purged.index) {
                return Err(ControlError::Init(
                    "purged Raft logs require a durable snapshot; recover from a healthy peer or verified backup".to_string(),
                ));
            }
        }

        let network = match mesh {
            Some(m) => HttpNetworkFactory::with_mesh_tls(m)
                .map_err(|e| ControlError::Init(format!("mesh tls: {e}")))?,
            None => HttpNetworkFactory::default(),
        };
        let raft = Raft::new(node_id, config, network, log_store, sm.clone())
            .await
            .map_err(|e| ControlError::Init(e.to_string()))?;

        Ok(Self { raft, sm })
    }

    /// Single-voter cluster for embedded/dev use: start and self-elect.
    pub async fn start_single_node(node_id: NodeId) -> Result<Self, ControlError> {
        let cp = Self::start_node(node_id, None).await?;
        cp.init_cluster(BTreeMap::from([(node_id, "local".to_string())]))
            .await?;
        Ok(cp)
    }

    /// Form a new cluster from this node with the given `id -> addr` voters.
    pub async fn init_cluster(
        &self,
        members: BTreeMap<NodeId, String>,
    ) -> Result<(), ControlError> {
        let members: BTreeMap<NodeId, BasicNode> = members
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect();
        self.raft
            .initialize(members)
            .await
            .map_err(|e| ControlError::Init(e.to_string()))
    }

    /// Add a node as a learner (it starts receiving the log immediately).
    pub async fn add_learner(&self, id: NodeId, addr: String) -> Result<(), ControlError> {
        self.raft
            .add_learner(id, BasicNode::new(addr), true)
            .await
            .map(|_| ())
            .map_err(|e| ControlError::Membership(e.to_string()))
    }

    /// Promote the given set to voters (must already be members/learners).
    pub async fn change_membership(&self, voters: BTreeSet<NodeId>) -> Result<(), ControlError> {
        self.raft
            .change_membership(voters, false)
            .await
            .map(|_| ())
            .map_err(|e| ControlError::Membership(e.to_string()))
    }

    pub async fn register_agent(
        &self,
        agent: AgentId,
        display_name: impl Into<String>,
    ) -> Result<(), ControlError> {
        self.write(Request::RegisterAgent {
            agent,
            display_name: display_name.into(),
        })
        .await
        .map(|_| ())
    }

    pub async fn begin_upload_lease(
        &self,
        lease: BlockId,
        agent: AgentId,
        blocks: Vec<BlockId>,
    ) -> Result<(), ControlError> {
        match self
            .write(Request::BeginUploadLease {
                lease,
                agent,
                blocks,
            })
            .await?
        {
            Response::LeaseReserved => Ok(()),
            Response::Rejected(message) => Err(ControlError::Rejected(message)),
            _ => Err(ControlError::Write("unexpected lease response".into())),
        }
    }

    pub async fn commit_upload_lease(
        &self,
        lease: BlockId,
        agent: AgentId,
        binding: ChainBinding,
        expected_revision: u64,
    ) -> Result<(), ControlError> {
        match self
            .write(Request::CommitUploadLease {
                lease,
                agent,
                binding,
                expected_revision,
            })
            .await?
        {
            Response::Bound => Ok(()),
            Response::Rejected(message) => Err(ControlError::Rejected(message)),
            _ => Err(ControlError::Write(
                "unexpected lease commit response".into(),
            )),
        }
    }

    /// Close a pending reservation in Raft without releasing any stored bytes.
    pub async fn cancel_upload_lease(
        &self,
        lease: BlockId,
        agent: AgentId,
    ) -> Result<(), ControlError> {
        match self
            .write(Request::CancelUploadLease { lease, agent })
            .await?
        {
            Response::LeaseCancelled => Ok(()),
            Response::Rejected(message) => Err(ControlError::Rejected(message)),
            _ => Err(ControlError::Write(
                "unexpected lease cancellation response".into(),
            )),
        }
    }

    pub async fn renew_upload_lease(
        &self,
        lease: BlockId,
        agent: AgentId,
        expected_generation: u64,
        ttl_ticks: u64,
    ) -> Result<LeaseDeadline, ControlError> {
        match self
            .write(Request::RenewUploadLease {
                lease,
                agent,
                expected_generation,
                ttl_ticks,
            })
            .await?
        {
            Response::LeaseRenewed {
                generation,
                deadline_tick,
            } => Ok(LeaseDeadline {
                generation,
                deadline_tick,
                ttl_ticks,
            }),
            Response::Rejected(message) => Err(ControlError::Rejected(message)),
            _ => Err(ControlError::Write(
                "unexpected lease renewal response".into(),
            )),
        }
    }

    /// Trusted coordinator operation; intentionally not exposed over HTTP.
    pub async fn advance_lease_clock(&self, expected_tick: u64) -> Result<(), ControlError> {
        match self
            .write(Request::AdvanceLeaseClock { expected_tick })
            .await?
        {
            Response::LeaseClockAdvanced => Ok(()),
            Response::Rejected(message) => Err(ControlError::Rejected(message)),
            _ => Err(ControlError::Write(
                "unexpected lease clock response".into(),
            )),
        }
    }

    pub async fn expire_upload_lease(
        &self,
        lease: BlockId,
        generation: u64,
        deadline_tick: u64,
    ) -> Result<(), ControlError> {
        match self
            .write(Request::ExpireUploadLease {
                lease,
                generation,
                deadline_tick,
            })
            .await?
        {
            Response::LeaseExpired => Ok(()),
            Response::Rejected(message) => Err(ControlError::Rejected(message)),
            _ => Err(ControlError::Write(
                "unexpected lease expiry response".into(),
            )),
        }
    }

    /// Local hints only; generation and deadline are checked again in Raft.
    pub fn lease_expiry_work(&self) -> (u64, bool, Vec<(BlockId, LeaseDeadline)>) {
        let state = self.sm.state.read().unwrap();
        let active = state.leases.deadlines.keys().any(|id| {
            state.leases.records[id].committed.is_none()
                && !state.leases.cancelled.contains_key(id)
                && !state.leases.is_expired(id)
        });
        let due = state
            .leases
            .deadlines
            .iter()
            .filter(|(id, _)| state.leases.is_expired(id) && !state.leases.expired.contains_key(id))
            .take(16)
            .map(|(id, deadline)| (*id, deadline.clone()))
            .collect();
        (state.leases.clock, active, due)
    }

    pub fn upload_lease_status(&self) -> LeaseStatusCounts {
        self.sm.state.read().unwrap().leases.status_counts()
    }

    /// Local diagnostics only, not authorization to release retention guards.
    pub fn upload_lease_is_cancelled(&self, id: &BlockId) -> bool {
        self.sm
            .state
            .read()
            .unwrap()
            .leases
            .cancelled
            .contains_key(id)
    }

    /// Legacy three-way summary. Expired records are omitted; use
    /// upload_lease_status for the complete partition of lifecycle states.
    pub fn upload_lease_lifecycle_counts(&self) -> (usize, usize, usize) {
        let counts = self.upload_lease_status();
        (counts.pending, counts.committed, counts.cancelled)
    }

    /// Local diagnostic view; callers needing authority must issue a Raft write.
    pub fn upload_lease(&self, id: &BlockId) -> Option<UploadLease> {
        self.sm
            .state
            .read()
            .unwrap()
            .leases
            .records
            .get(id)
            .cloned()
    }

    pub fn upload_lease_counts(&self) -> (usize, usize) {
        let counts = self.upload_lease_status();
        (counts.total, counts.pending)
    }

    pub fn binding_revision(&self, key: &CacheKey) -> u64 {
        self.sm
            .state
            .read()
            .unwrap()
            .leases
            .revisions
            .get(&binding_key(key))
            .copied()
            .unwrap_or(0)
    }

    pub async fn require_replicas(
        &self,
        blocks: Vec<BlockId>,
        replicas: usize,
    ) -> Result<(), ControlError> {
        if !(2..=16).contains(&replicas) || blocks.len() > 4096 {
            return Err(ControlError::Write(
                "invalid replica requirements".to_string(),
            ));
        }
        if self.metrics().current_leader == Some(self.metrics().id)
            && blocks.iter().all(|id| {
                self.sm
                    .state
                    .read()
                    .unwrap()
                    .replica_requirements
                    .get(id)
                    .is_some_and(|n| *n >= replicas)
            })
        {
            self.raft
                .ensure_linearizable()
                .await
                .map_err(|e| ControlError::Write(e.to_string()))?;
            return Ok(());
        }
        match self
            .write(Request::RequireReplicas { blocks, replicas })
            .await?
        {
            Response::ReplicasRequired => Ok(()),
            Response::Rejected(message) => Err(ControlError::Write(message)),
            _ => Err(ControlError::Write(
                "unexpected replica response".to_string(),
            )),
        }
    }

    pub fn replica_requirements(&self) -> BTreeMap<BlockId, usize> {
        self.sm.state.read().unwrap().replica_requirements.clone()
    }

    pub async fn bind_prefix(
        &self,
        agent: AgentId,
        key: CacheKey,
        blocks: Vec<BlockId>,
        parent: Option<CacheKey>,
    ) -> Result<(), ControlError> {
        self.write(Request::BindPrefix {
            agent,
            key,
            blocks,
            parent: parent.map(Box::new),
        })
        .await
        .map(|_| ())
    }

    /// Bind a whole prefix chain in ONE consensus round (one log fsync).
    pub async fn bind_chain(
        &self,
        agent: AgentId,
        bindings: Vec<ChainBinding>,
    ) -> Result<(), ControlError> {
        self.write(Request::BindChain { agent, bindings })
            .await
            .map(|_| ())
    }

    /// Blocks bound to the children of `key` in the prefix tree: the
    /// one-step-ahead speculative prefetch set.
    pub fn child_blocks(&self, key: &CacheKey) -> Vec<BlockId> {
        let state = self.sm.state.read().unwrap();
        let Some(kids) = state.children.get(&binding_key(key)) else {
            return Vec::new();
        };
        kids.iter()
            .filter_map(|k| state.bindings.get(k))
            .flat_map(|rec| rec.blocks.iter().copied())
            .collect()
    }

    async fn write(&self, req: Request) -> Result<Response, ControlError> {
        let resp = self
            .raft
            .client_write(req)
            .await
            .map_err(|e| match e.forward_to_leader() {
                Some(fwd) => ControlError::NotLeader {
                    leader_addr: fwd.leader_node.as_ref().map(|n| n.addr.clone()),
                },
                None => ControlError::Write(e.to_string()),
            })?;
        Ok(resp.data)
    }

    /// Addresses of all current cluster members except this node.
    pub fn peer_addrs(&self) -> Vec<String> {
        self.peer_nodes()
            .into_iter()
            .map(|(_, addr)| addr)
            .collect()
    }

    /// Stable node identities paired with the current peer endpoints.
    pub fn peer_nodes(&self) -> Vec<(u64, String)> {
        let metrics = self.metrics();
        let self_id = metrics.id;
        metrics
            .membership_config
            .nodes()
            .filter(|(id, _)| **id != self_id)
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect()
    }

    /// Look up a binding from the applied state machine.
    pub fn lookup(&self, key: &CacheKey) -> Option<BindingRecord> {
        self.sm
            .state
            .read()
            .unwrap()
            .bindings
            .get(&binding_key(key))
            .cloned()
    }

    pub fn agent(&self, id: &AgentId) -> Option<AgentRecord> {
        self.sm.state.read().unwrap().agents.get(id).cloned()
    }

    pub fn agent_count(&self) -> usize {
        self.sm.state.read().unwrap().agents.len()
    }

    pub fn binding_count(&self) -> usize {
        self.sm.state.read().unwrap().bindings.len()
    }

    /// All registered agents with their records.
    pub fn agents_list(&self) -> Vec<(AgentId, AgentRecord)> {
        let state = self.sm.state.read().unwrap();
        let mut v: Vec<_> = state
            .agents
            .iter()
            .map(|(id, r)| (*id, r.clone()))
            .collect();
        v.sort_by_key(|(_, r)| r.registered_at);
        v
    }

    /// Child key -> parent key over the whole prefix tree (serialized
    /// form), for lineage rendering.
    pub fn parent_index(&self) -> std::collections::HashMap<String, String> {
        let state = self.sm.state.read().unwrap();
        let mut idx = std::collections::HashMap::new();
        for (parent, kids) in &state.children {
            for kid in kids {
                idx.insert(kid.clone(), parent.clone());
            }
        }
        idx
    }

    /// All bindings owned by `agent`: (serialized cache key, blocks).
    pub fn bindings_of(&self, agent: &AgentId) -> Vec<(String, Vec<BlockId>)> {
        let state = self.sm.state.read().unwrap();
        state
            .bindings
            .iter()
            .filter(|(_, rec)| rec.agent == *agent)
            .map(|(k, rec)| (k.clone(), rec.blocks.clone()))
            .collect()
    }

    /// Capture each binding and its revision under one state lock. A stale
    /// local view is safe to use as a CAS hint, but not as GC authority.
    pub fn bindings_with_revisions(&self, agent: &AgentId) -> Vec<(String, Vec<BlockId>, u64)> {
        let state = self.sm.state.read().unwrap();
        state
            .bindings
            .iter()
            .filter(|(_, binding)| binding.agent == *agent)
            .map(|(key, binding)| {
                (
                    key.clone(),
                    binding.blocks.clone(),
                    state.leases.revisions.get(key).copied().unwrap_or(0),
                )
            })
            .collect()
    }

    /// Per-follower replicated log position (leader only): the lag view.
    pub fn replication_state(&self) -> Option<std::collections::BTreeMap<NodeId, Option<u64>>> {
        self.metrics().replication.map(|m| {
            m.into_iter()
                .map(|(id, log)| (id, log.map(|l| l.index)))
                .collect()
        })
    }

    /// How many bindings reference `id`.
    pub fn block_ref_count(&self, id: &BlockId) -> u32 {
        self.sm
            .state
            .read()
            .unwrap()
            .block_refs
            .get(id)
            .copied()
            .unwrap_or(0)
    }

    /// One local metadata observation, never a linearizable GC certificate.
    pub fn reclamation_evidence(&self, id: &BlockId) -> serde_json::Value {
        let state = self.sm.state.read().unwrap();
        let mut pending = 0usize;
        let mut committed = 0usize;
        let mut cancelled = 0usize;
        let mut expired = 0usize;
        for (lease_id, lease) in &state.leases.records {
            if !lease.blocks.contains(id) {
                continue;
            }
            if lease.committed.is_some() {
                committed += 1;
            } else if state.leases.cancelled.contains_key(lease_id) {
                cancelled += 1;
            } else if state.leases.is_expired(lease_id) {
                expired += 1;
            } else {
                pending += 1;
            }
        }
        serde_json::json!({
            "last_applied": state.last_applied,
            "logical_tick": state.leases.clock,
            "binding_references": state.block_refs.get(id).copied().unwrap_or(0),
            "required_replicas": state.replica_requirements.get(id).copied().unwrap_or(0),
            "leases": {"pending": pending, "committed": committed,
                "cancelled": cancelled, "expired": expired}
        })
    }

    /// Blocks referenced by at least `min_refs` bindings — the structural
    /// importance set for pinned warm placement (IMPRESS-style).
    pub fn shared_blocks(&self, min_refs: u32) -> Vec<BlockId> {
        let state = self.sm.state.read().unwrap();
        state
            .block_refs
            .iter()
            .filter(|(_, n)| **n >= min_refs)
            .map(|(b, _)| *b)
            .collect()
    }

    /// Every block referenced by any binding: the GC live set.
    pub fn bound_blocks(&self) -> std::collections::HashSet<BlockId> {
        self.sm
            .state
            .read()
            .unwrap()
            .bindings
            .values()
            .flat_map(|rec| rec.blocks.iter().copied())
            .collect()
    }

    /// Raft metrics for the observability plane.
    pub fn metrics(&self) -> openraft::RaftMetrics<NodeId, BasicNode> {
        self.raft.metrics().borrow().clone()
    }

    /// Trigger a snapshot of the state machine (log compaction).
    pub async fn snapshot(&self) -> Result<(), ControlError> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| ControlError::Write(e.to_string()))
    }

    // ---- Raft RPC handlers, exposed by the node's API server ----
    //
    // JSON-level so the API server never depends on openraft types; the
    // serialized `Result<_, RaftError>` is exactly what HttpNetwork expects.

    pub async fn handle_append_entries(
        &self,
        req: serde_json::Value,
    ) -> Result<serde_json::Value, serde_json::Error> {
        let req: AppendEntriesRequest<TypeConfig> = serde_json::from_value(req)?;
        let res: Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>> =
            self.raft.append_entries(req).await;
        serde_json::to_value(res)
    }

    pub async fn handle_vote(
        &self,
        req: serde_json::Value,
    ) -> Result<serde_json::Value, serde_json::Error> {
        let req: VoteRequest<NodeId> = serde_json::from_value(req)?;
        let res: Result<VoteResponse<NodeId>, RaftError<NodeId>> = self.raft.vote(req).await;
        serde_json::to_value(res)
    }

    pub async fn handle_install_snapshot(
        &self,
        req: serde_json::Value,
    ) -> Result<serde_json::Value, serde_json::Error> {
        let req: InstallSnapshotRequest<TypeConfig> = serde_json::from_value(req)?;
        let res: Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>> =
            self.raft.install_snapshot(req).await;
        serde_json::to_value(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalpak_core::ModelFingerprint;

    fn agent(seed: u8) -> AgentId {
        use ed25519_dalek::SigningKey;
        AgentId::from_verifying_key(&SigningKey::from_bytes(&[seed; 32]).verifying_key())
    }

    fn key(tokens: &[u32]) -> CacheKey {
        CacheKey::root(
            ModelFingerprint::new("test/model", "tok", "fp16/paged-16"),
            tokens,
        )
    }

    #[tokio::test]
    async fn single_node_elects_and_applies() {
        let cp = ControlPlane::start_single_node(1).await.unwrap();
        let a = agent(1);
        cp.register_agent(a, "researcher").await.unwrap();
        assert_eq!(cp.agent(&a).unwrap().display_name, "researcher");
        assert_eq!(cp.agent_count(), 1);
    }

    #[tokio::test]
    async fn bindings_replicate_through_the_log() {
        let cp = ControlPlane::start_single_node(1).await.unwrap();
        let a = agent(2);
        cp.register_agent(a, "coder").await.unwrap();

        let k = key(&[1, 2, 3]);
        let blocks = vec![
            kalpak_core::BlockId::of(b"kv0"),
            kalpak_core::BlockId::of(b"kv1"),
        ];
        cp.bind_prefix(a, k.clone(), blocks.clone(), None)
            .await
            .unwrap();

        let rec = cp.lookup(&k).unwrap();
        assert_eq!(rec.blocks, blocks);
        assert_eq!(rec.agent, a);
        assert!(cp.lookup(&key(&[9, 9])).is_none());
    }

    #[tokio::test]
    async fn snapshot_compacts_without_losing_state() {
        let cp = ControlPlane::start_single_node(1).await.unwrap();
        let a = agent(3);
        cp.register_agent(a, "planner").await.unwrap();
        for i in 0..20u32 {
            cp.bind_prefix(
                a,
                key(&[i]),
                vec![kalpak_core::BlockId::of(&i.to_le_bytes())],
                None,
            )
            .await
            .unwrap();
        }
        cp.snapshot().await.unwrap();
        assert_eq!(cp.binding_count(), 20);
        assert!(cp.lookup(&key(&[7])).is_some());
    }

    #[tokio::test]
    async fn metrics_expose_leadership() {
        let cp = ControlPlane::start_single_node(42).await.unwrap();
        let m = cp.metrics();
        assert_eq!(m.id, 42);
        assert_eq!(m.current_leader, Some(42));
    }

    #[tokio::test]
    async fn durable_node_restarts_with_its_log() {
        let dir = tempfile::tempdir().unwrap();
        let a = agent(4);
        let k = key(&[5, 6]);
        {
            let cp = ControlPlane::start_node(1, Some(dir.path())).await.unwrap();
            cp.init_cluster(BTreeMap::from([(1, "local".to_string())]))
                .await
                .unwrap();
            cp.register_agent(a, "survivor").await.unwrap();
            cp.bind_prefix(a, k.clone(), vec![kalpak_core::BlockId::of(b"kv")], None)
                .await
                .unwrap();
        }
        // Restart from the same directory: log replays, no re-init needed
        // (membership is in the log), state machine catches up.
        let cp = ControlPlane::start_node(1, Some(dir.path())).await.unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if cp.agent(&a).is_some() && cp.lookup(&k).is_some() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "state machine did not recover from the durable log"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(cp.agent(&a).unwrap().display_name, "survivor");
    }

    #[tokio::test]
    async fn durable_snapshot_survives_log_purge_and_restart() {
        use openraft::storage::{RaftLogStorage, RaftStateMachine};
        let dir = tempfile::tempdir().unwrap();
        let a = agent(9);
        let k = key(&[80, 81]);
        let blocks = vec![BlockId::of(b"durable-snapshot")];
        let cp = ControlPlane::start_node(1, Some(dir.path())).await.unwrap();
        cp.init_cluster(BTreeMap::from([(1, "local".to_string())]))
            .await
            .unwrap();
        cp.register_agent(a, "snapshotted").await.unwrap();
        cp.bind_prefix(a, k.clone(), blocks.clone(), None)
            .await
            .unwrap();
        let index = cp.metrics().last_applied.unwrap().index;
        cp.snapshot().await.unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut sm = cp.sm.clone();
        loop {
            if sm
                .get_current_snapshot()
                .await
                .unwrap()
                .is_some_and(|s| s.meta.last_log_id.is_some_and(|id| id.index >= index))
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "snapshot did not complete"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        cp.raft.trigger().purge_log(index).await.unwrap();
        loop {
            let mut log = LogStore::open(dir.path().join("control")).unwrap();
            if log
                .get_log_state()
                .await
                .unwrap()
                .last_purged_log_id
                .is_some_and(|l| l.index >= index)
            {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "logs did not purge");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        cp.raft.shutdown().await.unwrap();
        drop(cp);
        drop(sm);
        let restarted = ControlPlane::start_node(1, Some(dir.path())).await.unwrap();
        // Immediately available from the durable snapshot, without waiting
        // for election or a peer to reconstruct discarded log entries.
        assert_eq!(restarted.agent(&a).unwrap().display_name, "snapshotted");
        assert_eq!(restarted.lookup(&k).unwrap().blocks, blocks);
        restarted.raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn missing_snapshot_after_purge_fails_startup() {
        use openraft::storage::RaftLogStorage;
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogStore::open(dir.path().join("control")).unwrap();
        log.purge(openraft::LogId::new(
            openraft::CommittedLeaderId::new(1, 1),
            7,
        ))
        .await
        .unwrap();
        drop(log);
        let result = ControlPlane::start_node(1, Some(dir.path())).await;
        assert!(
            matches!(result, Err(ControlError::Init(message)) if message.contains("durable snapshot"))
        );
    }
}
