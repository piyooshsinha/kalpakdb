//! Replicated reservations and optional logical-tick expiry. No lifecycle
//! transition is authority to reclaim physical data.
use crate::{ChainBinding, MetadataState, Response};
use kalpak_core::{AgentId, BlockId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

pub const MAX_LEASES: usize = 4096;
pub const MAX_LEASE_BLOCKS: usize = 256;
pub const MAX_RESERVED_BLOCKS: usize = 65_536;
pub const MAX_TTL_TICKS: u64 = 86_400;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaseDeadline {
    pub generation: u64,
    pub deadline_tick: u64,
    pub ttl_ticks: u64,
}

#[derive(Debug, Serialize)]
pub struct LeaseStatusCounts {
    pub total: usize,
    pub pending: usize,
    pub committed: usize,
    pub cancelled: usize,
    pub expired: usize,
    pub current_tick: u64,
    pub expiry_markers: usize,
}

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
    /// First cancellation's Raft log position. IDs and hashes remain retained
    /// as replay tombstones; cancellation is never physical GC authority.
    pub cancelled: BTreeMap<BlockId, u64>,
    pub clock: u64,
    pub deadlines: BTreeMap<BlockId, LeaseDeadline>,
    pub expired: BTreeMap<BlockId, u64>,
}

impl LeaseState {
    pub(crate) fn validate(&self) -> std::io::Result<()> {
        if self.records.len() > MAX_LEASES
            || self.deadlines.iter().any(|(id, deadline)| {
                !self.records.contains_key(id)
                    || deadline.generation == 0
                    || !(1..=MAX_TTL_TICKS).contains(&deadline.ttl_ticks)
                    || deadline
                        .deadline_tick
                        .checked_sub(deadline.ttl_ticks)
                        .is_none_or(|issued| issued > self.clock)
            })
            || self.expired.keys().any(|id| {
                self.records
                    .get(id)
                    .is_none_or(|lease| lease.committed.is_some())
                    || self.cancelled.contains_key(id)
                    || self
                        .deadlines
                        .get(id)
                        .is_none_or(|deadline| deadline.deadline_tick > self.clock)
            })
            || self.cancelled.iter().any(|(id, _)| {
                self.records
                    .get(id)
                    .is_none_or(|lease| lease.committed.is_some())
            })
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
            if state.leases.is_expired(&id) {
                Response::Rejected("upload lease expired".into())
            } else if state.leases.cancelled.contains_key(&id) {
                Response::Rejected("upload lease cancelled".into())
            } else {
                Response::LeaseReserved
            }
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
    if state.leases.cancelled.contains_key(id) {
        return Err("upload lease cancelled".into());
    }
    if state.leases.is_expired(id) {
        return Err("upload lease expired".into());
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

pub(crate) fn cancel(state: &mut MetadataState, id: BlockId, agent: AgentId) -> Response {
    let Some(lease) = state.leases.records.get(&id) else {
        return Response::Rejected("unknown upload lease".into());
    };
    if lease.agent != agent {
        return Response::Rejected("lease owner mismatch".into());
    }
    if lease.committed.is_some() {
        return Response::Rejected("committed upload lease cannot be cancelled".into());
    }
    if state.leases.is_expired(&id) {
        return Response::Rejected("upload lease expired".into());
    }
    state
        .leases
        .cancelled
        .entry(id)
        .or_insert_with(|| state.last_applied.map(|id| id.index).unwrap_or(0));
    Response::LeaseCancelled
}

impl LeaseState {
    pub fn is_expired(&self, id: &BlockId) -> bool {
        self.records
            .get(id)
            .is_some_and(|lease| lease.committed.is_none())
            && !self.cancelled.contains_key(id)
            && (self.expired.contains_key(id)
                || self
                    .deadlines
                    .get(id)
                    .is_some_and(|deadline| deadline.deadline_tick <= self.clock))
    }

    pub fn status_counts(&self) -> LeaseStatusCounts {
        let committed = self
            .records
            .values()
            .filter(|lease| lease.committed.is_some())
            .count();
        let expired = self.records.keys().filter(|id| self.is_expired(id)).count();
        LeaseStatusCounts {
            total: self.records.len(),
            pending: self.records.len() - committed - expired - self.cancelled.len(),
            committed,
            cancelled: self.cancelled.len(),
            expired,
            current_tick: self.clock,
            expiry_markers: self.expired.len(),
        }
    }
}

pub(crate) fn renew(
    state: &mut MetadataState,
    id: BlockId,
    agent: AgentId,
    expected: u64,
    ttl: u64,
) -> Response {
    let Some(lease) = state.leases.records.get(&id) else {
        return Response::Rejected("unknown upload lease".into());
    };
    if lease.agent != agent
        || lease.committed.is_some()
        || state.leases.cancelled.contains_key(&id)
        || state.leases.is_expired(&id)
    {
        return Response::Rejected("lease is not pending for this owner".into());
    }
    if !(1..=MAX_TTL_TICKS).contains(&ttl) || expected == u64::MAX {
        return Response::Rejected("invalid TTL or renewal generation".into());
    }
    if let Some(old) = state.leases.deadlines.get(&id) {
        if old.generation == expected + 1 && old.ttl_ticks == ttl {
            return Response::LeaseRenewed {
                generation: old.generation,
                deadline_tick: old.deadline_tick,
            };
        }
    }
    let current = state
        .leases
        .deadlines
        .get(&id)
        .map(|old| old.generation)
        .unwrap_or(0);
    if expected != current {
        return Response::Rejected("lease renewal generation conflict".into());
    }
    let Some(deadline_tick) = state.leases.clock.checked_add(ttl) else {
        return Response::Rejected("lease clock exhausted".into());
    };
    if state
        .leases
        .deadlines
        .get(&id)
        .is_some_and(|old| old.deadline_tick >= deadline_tick)
    {
        return Response::Rejected("renewal must extend the deadline".into());
    }
    let generation = current + 1;
    state.leases.deadlines.insert(
        id,
        LeaseDeadline {
            generation,
            deadline_tick,
            ttl_ticks: ttl,
        },
    );
    Response::LeaseRenewed {
        generation,
        deadline_tick,
    }
}

pub(crate) fn advance(state: &mut MetadataState, expected: u64) -> Response {
    if expected < state.leases.clock {
        return Response::LeaseClockAdvanced;
    }
    if expected != state.leases.clock {
        return Response::Rejected("lease clock conflict".into());
    }
    let Some(next) = expected.checked_add(1) else {
        return Response::Rejected("lease clock exhausted".into());
    };
    state.leases.clock = next;
    Response::LeaseClockAdvanced
}

pub(crate) fn expire(
    state: &mut MetadataState,
    id: BlockId,
    generation: u64,
    deadline_tick: u64,
) -> Response {
    if !state.leases.is_expired(&id)
        || state.leases.deadlines.get(&id).is_none_or(|deadline| {
            deadline.generation != generation || deadline.deadline_tick != deadline_tick
        })
    {
        return Response::Rejected("stale or premature lease expiry".into());
    }
    let at = state.last_applied.map(|id| id.index).unwrap_or(0);
    state.leases.expired.entry(id).or_insert(at);
    Response::LeaseExpired
}
