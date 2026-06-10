//! Kalpak control plane: a Raft-replicated metadata state machine.
//!
//! Replicates agent registrations and prefix-manifest bindings via
//! `openraft`. Strictly metadata: tensors and blobs stay on the data plane
//! and are referenced here by content address only.
//!
//! [`ControlPlane::start_single_node`] boots a one-voter cluster for the
//! embedded/dev deployment; the multi-node transport (gRPC) replaces
//! [`network::LocalOnlyNetwork`] without touching storage or the state
//! machine.

mod log_store;
mod network;
mod state_machine;
mod types;

use std::collections::BTreeMap;
use std::sync::Arc;

use kalpak_core::{AgentId, BlockId, CacheKey};
use openraft::{BasicNode, Config, Raft};

pub use state_machine::{AgentRecord, BindingRecord, MetadataState};
pub use types::{NodeId, Request, Response, TypeConfig};

use log_store::LogStore;
use network::LocalOnlyNetwork;
use state_machine::{binding_key, StateMachineStore};

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("raft init: {0}")]
    Init(String),
    #[error("raft write: {0}")]
    Write(String),
}

pub struct ControlPlane {
    raft: Raft<TypeConfig>,
    sm: StateMachineStore,
}

impl ControlPlane {
    /// Boot a single-voter cluster and wait for it to elect itself.
    pub async fn start_single_node(node_id: NodeId) -> Result<Self, ControlError> {
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

        let log_store = LogStore::default();
        let sm = StateMachineStore::default();

        let raft = Raft::new(node_id, config, LocalOnlyNetwork, log_store, sm.clone())
            .await
            .map_err(|e| ControlError::Init(e.to_string()))?;

        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode::new("local"));
        raft.initialize(members)
            .await
            .map_err(|e| ControlError::Init(e.to_string()))?;

        Ok(Self { raft, sm })
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
    ) -> Result<(), ControlError> {
        self.write(Request::BindPrefix { agent, key, blocks })
            .await
            .map(|_| ())
    }

    async fn write(&self, req: Request) -> Result<Response, ControlError> {
        let resp = self
            .raft
            .client_write(req)
            .await
            .map_err(|e| ControlError::Write(e.to_string()))?;
        Ok(resp.data)
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
        cp.bind_prefix(a, k.clone(), blocks.clone()).await.unwrap();

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
}
