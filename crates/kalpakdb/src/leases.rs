//! Signed, opt-in replicated reservations. Committing a binding does not
//! release local upload guards or enable garbage collection.
use crate::server::{ApiError, AppState};
use axum::http::StatusCode;
use axum::{extract::State, Json};
use kalpak_core::{AgentId, BlockId};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub(crate) struct Begin {
    lease: BlockId,
    agent: AgentId,
    blocks: Vec<BlockId>,
    signature: String,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Commit {
    lease: BlockId,
    agent: AgentId,
    binding: kalpak_control::ChainBinding,
    expected_revision: u64,
    signature: String,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Cancel {
    lease: BlockId,
    agent: AgentId,
    signature: String,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Renew {
    lease: BlockId,
    agent: AgentId,
    expected_generation: u64,
    ttl_ticks: u64,
    signature: String,
}

pub(crate) async fn renew(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Renew>,
) -> Result<Json<serde_json::Value>, ApiError> {
    verify(
        &req.agent,
        &kalpak_core::signing::lease_renew_message(
            &req.agent,
            &req.lease,
            req.expected_generation,
            req.ttl_ticks,
        ),
        &req.signature,
    )?;
    match state
        .control
        .renew_upload_lease(req.lease, req.agent, req.expected_generation, req.ttl_ticks)
        .await
    {
        Ok(deadline) => Ok(Json(
            json!({"generation": deadline.generation, "deadline_tick": deadline.deadline_tick}),
        )),
        Err(error) => {
            crate::server::forward_to_leader(&state, error, "/v1/leases/renew", json!(req)).await
        }
    }
}

/// Logical time is proposed only by a data leader, one tick per scheduled
/// interval with no catch-up loop. Followers never expire from local clocks.
pub(crate) fn start_expiry(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            let metrics = state.control.metrics();
            if metrics.current_leader != Some(metrics.id) {
                continue;
            }
            let (clock, active, _) = state.control.lease_expiry_work();
            if active && state.control.advance_lease_clock(clock).await.is_err() {
                continue;
            }
            let (_, _, due) = state.control.lease_expiry_work();
            for (id, deadline) in due {
                let _ = state
                    .control
                    .expire_upload_lease(id, deadline.generation, deadline.deadline_tick)
                    .await;
            }
        }
    });
}

pub(crate) async fn cancel(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Cancel>,
) -> Result<Json<serde_json::Value>, ApiError> {
    verify(
        &req.agent,
        &kalpak_core::signing::lease_cancel_message(&req.agent, &req.lease),
        &req.signature,
    )?;
    match state
        .control
        .cancel_upload_lease(req.lease, req.agent)
        .await
    {
        Ok(()) => Ok(Json(
            json!({"cancelled": true, "lease": req.lease, "guards_released": false}),
        )),
        Err(error) => {
            crate::server::forward_to_leader(&state, error, "/v1/leases/cancel", json!(req)).await
        }
    }
}

fn verify(agent: &AgentId, message: &[u8], signature: &str) -> Result<(), ApiError> {
    let signature = kalpak_core::signing::signature_from_hex(signature)
        .map_err(|_| ApiError(StatusCode::UNAUTHORIZED, "malformed lease signature".into()))?;
    agent
        .verify(message, &signature)
        .map_err(|_| ApiError(StatusCode::UNAUTHORIZED, "invalid lease signature".into()))
}

pub(crate) async fn begin(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Begin>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if req.blocks.is_empty() || req.blocks.len() > kalpak_control::MAX_LEASE_BLOCKS {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "lease requires 1..256 block hashes".into(),
        ));
    }
    verify(
        &req.agent,
        &kalpak_core::signing::lease_begin_message(&req.agent, &req.lease, &req.blocks),
        &req.signature,
    )?;
    match state
        .control
        .begin_upload_lease(req.lease, req.agent, req.blocks.clone())
        .await
    {
        Ok(()) => Ok(Json(json!({"reserved": true, "lease": req.lease}))),
        Err(error) => {
            crate::server::forward_to_leader(&state, error, "/v1/leases/begin", json!(req)).await
        }
    }
}

pub(crate) async fn commit(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Commit>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if req.binding.blocks.is_empty() || req.binding.blocks.len() > kalpak_control::MAX_LEASE_BLOCKS
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "lease requires 1..256 block hashes".into(),
        ));
    }
    verify(
        &req.agent,
        &kalpak_core::signing::lease_commit_message(
            &req.agent,
            &req.lease,
            &req.binding.key,
            &req.binding.blocks,
            req.binding.parent.as_ref(),
            req.expected_revision,
        ),
        &req.signature,
    )?;
    // Do not trust the legacy forwarding header to bypass this check. Both
    // ingress and leader need a local copy for this experimental API.
    if req
        .binding
        .blocks
        .iter()
        .any(|id| !state.store.contains(id))
    {
        return Err(ApiError(
            StatusCode::UNPROCESSABLE_ENTITY,
            "upload leased blocks to the committing leader first".into(),
        ));
    }
    match state
        .control
        .commit_upload_lease(
            req.lease,
            req.agent,
            req.binding.clone(),
            req.expected_revision,
        )
        .await
    {
        Ok(()) => Ok(Json(json!({"bound": true, "lease": req.lease}))),
        Err(error) => {
            crate::server::forward_to_leader(&state, error, "/v1/leases/commit", json!(req)).await
        }
    }
}
