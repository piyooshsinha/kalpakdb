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
        Ok(()) => Ok(Json(
            json!({"reserved": true, "lease": req.lease, "expires": false}),
        )),
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
