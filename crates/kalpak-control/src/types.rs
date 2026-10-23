//! Raft type configuration and the metadata commands Kalpak replicates.
//!
//! The control plane replicates **metadata only**: agent registrations and
//! prefix-manifest bindings. Raw KV tensors never enter the Raft log — they
//! move on the data plane and are referenced here by content address.

use std::io::Cursor;

use kalpak_core::{AgentId, BlockId, CacheKey};
use serde::{Deserialize, Serialize};

pub type NodeId = u64;

/// A metadata mutation submitted to the replicated state machine.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    /// Monotonic desired copy counts; raw payloads never enter consensus.
    RequireReplicas {
        blocks: Vec<BlockId>,
        replicas: usize,
    },
    /// Register (or re-register) an agent identity.
    RegisterAgent {
        agent: AgentId,
        display_name: String,
    },
    /// Bind a prefix cache key to the ordered blocks materializing it.
    /// `parent` links the key into the prefix tree (the key this one
    /// extends), enabling one-step-ahead speculative prefetch on lookup.
    BindPrefix {
        agent: AgentId,
        key: CacheKey,
        blocks: Vec<BlockId>,
        #[serde(default)]
        parent: Option<Box<CacheKey>>,
    },
    /// Bind a whole prefix chain atomically: one consensus round (and one
    /// Raft log fsync) instead of one per depth. The chain is applied in
    /// order, so parents always exist before their children.
    BindChain {
        agent: AgentId,
        bindings: Vec<ChainBinding>,
    },
    BeginUploadLease {
        lease: BlockId,
        agent: AgentId,
        blocks: Vec<BlockId>,
    },
    CommitUploadLease {
        lease: BlockId,
        agent: AgentId,
        binding: ChainBinding,
        expected_revision: u64,
    },
}

/// One link of an atomic chain bind.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChainBinding {
    pub key: CacheKey,
    pub blocks: Vec<BlockId>,
    #[serde(default)]
    pub parent: Option<CacheKey>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Response {
    ReplicasRequired,
    Rejected(String),
    Registered,
    Bound,
    LeaseReserved,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TypeConfig;

impl openraft::RaftTypeConfig for TypeConfig {
    type D = Request;
    type R = Response;
    type NodeId = NodeId;
    type Node = openraft::BasicNode;
    type Entry = openraft::Entry<TypeConfig>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<TypeConfig>;
}

pub type Entry = openraft::Entry<TypeConfig>;
pub type LogId = openraft::LogId<NodeId>;
pub type StorageError = openraft::StorageError<NodeId>;
