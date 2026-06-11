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

pub use state_machine::{AgentRecord, BindingRecord, MetadataState};
pub use types::{NodeId, Request, Response, TypeConfig};

use log_store::LogStore;
use network::HttpNetworkFactory;
use state_machine::{binding_key, StateMachineStore};

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("raft init: {0}")]
    Init(String),
    #[error("raft write: {0}")]
    Write(String),
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

        let log_store = match data_dir {
            Some(dir) => LogStore::open(dir.join("control"))
                .map_err(|e| ControlError::Init(e.to_string()))?,
            None => LogStore::in_memory(),
        };
        let sm = StateMachineStore::default();

        let raft = Raft::new(
            node_id,
            config,
            HttpNetworkFactory::default(),
            log_store,
            sm.clone(),
        )
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
        let metrics = self.metrics();
        let self_id = metrics.id;
        metrics
            .membership_config
            .nodes()
            .filter(|(id, _)| **id != self_id)
            .map(|(_, node)| node.addr.clone())
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

    /// Per-follower replicated log position (leader only): the lag view.
    pub fn replication_state(&self) -> Option<std::collections::BTreeMap<NodeId, Option<u64>>> {
        self.metrics().replication.map(|m| {
            m.into_iter()
                .map(|(id, log)| (id, log.map(|l| l.index)))
                .collect()
        })
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
}
