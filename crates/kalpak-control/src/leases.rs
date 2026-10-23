//! Replicated reservations and single-use binding commits. No expiration or
//! reclamation authority is implied by a committed reservation.
use crate::{ChainBinding, MetadataState, Response};
use kalpak_core::{AgentId, BlockId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

pub const MAX_LEASES: usize = 4096;
pub const MAX_LEASE_BLOCKS: usize = 256;
pub const MAX_RESERVED_BLOCKS: usize = 65_536;

fn valid_binding(binding: &ChainBinding) -> bool {
    let valid_key = |key: &kalpak_core::CacheKey| {
        [
            &key.fingerprint.model_id,
            &key.fingerprint.tokenizer_hash,
            &key.fingerprint.kv_layout,
        ]
        .iter()
        .all(|value| value.len() <= 1024)
    };
    valid_key(&binding.key)
        && binding
            .parent
            .as_ref()
            .is_none_or(|parent| valid_key(parent) && parent.fingerprint == binding.key.fingerprint)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaseCommit {
    pub binding: ChainBinding,
    pub expected_revision: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadLease {
    pub agent: AgentId,
    pub blocks: Vec<BlockId>,
    pub created_at: u64,
    pub committed: Option<LeaseCommit>,
}

#[derive(Default, Serialize, Deserialize)]
pub struct LeaseState {
    /// The first successful reservation is a common Raft-ordered barrier.
    /// Older snapshots cannot reconstruct historical binding revisions.
    pub initialized: bool,
    pub records: BTreeMap<BlockId, UploadLease>,
    /// After activation, every binding mutation (including legacy APIs)
    /// advances this fence. Imported bindings start at the common zero fence.
    pub revisions: HashMap<String, u64>,
}

impl LeaseState {
    pub(crate) fn validate(&self) -> std::io::Result<()> {
        if self.records.len() > MAX_LEASES
            || (!self.initialized && (!self.records.is_empty() || !self.revisions.is_empty()))
            || self
                .records
                .values()
                .map(|lease| lease.blocks.len())
                .sum::<usize>()
                > MAX_RESERVED_BLOCKS
            || self.records.values().any(|lease| {
                lease.blocks.is_empty()
                    || lease.blocks.len() > MAX_LEASE_BLOCKS
                    || lease.committed.as_ref().is_some_and(|commit| {
                        !valid_binding(&commit.binding)
                            || commit.binding.blocks != lease.blocks
                            || !self
                                .revisions
                                .contains_key(&crate::state_machine::binding_key(
                                    &commit.binding.key,
                                ))
                    })
            })
        {
            return Err(std::io::Error::other("invalid upload lease snapshot"));
        }
        Ok(())
    }
}

pub(crate) fn reserve(
    state: &mut MetadataState,
    id: BlockId,
    agent: AgentId,
    blocks: &[BlockId],
) -> Response {
    if blocks.is_empty() || blocks.len() > MAX_LEASE_BLOCKS || !state.agents.contains_key(&agent) {
        return Response::Rejected(
            "lease requires a registered owner and 1..256 block hashes".into(),
        );
    }
    if !state.leases.initialized {
        state.leases.initialized = true;
        state.leases.revisions.clear();
    }
    if let Some(old) = state.leases.records.get(&id) {
        return if old.agent == agent && old.blocks == blocks {
            Response::LeaseReserved
        } else {
            Response::Rejected("lease ID already belongs to a different reservation".into())
        };
    }
    if state.leases.records.len() >= MAX_LEASES
        || state
            .leases
            .records
            .values()
            .map(|lease| lease.blocks.len())
            .sum::<usize>()
            + blocks.len()
            > MAX_RESERVED_BLOCKS
    {
        return Response::Rejected("replicated upload lease capacity exhausted".into());
    }
    state.leases.records.insert(
        id,
        UploadLease {
            agent,
            blocks: blocks.to_vec(),
            created_at: state.last_applied.map(|id| id.index).unwrap_or(0),
            committed: None,
        },
    );
    Response::LeaseReserved
}

/// Validation is side-effect-free. An exact retry of a consumed lease is an
/// acknowledgement only: it must NOT apply its old binding again.
pub(crate) fn validate_commit(
    state: &MetadataState,
    id: &BlockId,
    agent: &AgentId,
    binding: &ChainBinding,
    expected_revision: u64,
) -> Result<bool, String> {
    if !valid_binding(binding) {
        return Err("invalid lease binding fingerprint or parent".into());
    }
    let lease = state.leases.records.get(id).ok_or("unknown upload lease")?;
    if &lease.agent != agent || lease.blocks != binding.blocks {
        return Err("lease owner or blocks mismatch".into());
    }
    if let Some(old) = &lease.committed {
        return if old.expected_revision == expected_revision
            && old.binding.key == binding.key
            && old.binding.parent == binding.parent
            && old.binding.blocks == binding.blocks
        {
            Ok(false)
        } else {
            Err("upload lease already consumed".into())
        };
    }
    let key = crate::state_machine::binding_key(&binding.key);
    if state
        .bindings
        .get(&key)
        .is_some_and(|old| &old.agent != agent)
    {
        return Err("binding belongs to a different agent".into());
    }
    let current = state.leases.revisions.get(&key).copied().unwrap_or(0);
    if current == u64::MAX || current != expected_revision {
        return Err("binding revision conflict".into());
    }
    Ok(true)
}
