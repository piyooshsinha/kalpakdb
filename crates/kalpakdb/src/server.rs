//! The Kalpak memory API: HTTP + WebSocket server over the data and control
//! planes.
//!
//! Data plane endpoints move raw bytes (KV blocks) through the tiered store;
//! control plane endpoints replicate metadata (agents, prefix bindings)
//! through Raft. `/v1/stats` and the `/v1/ws` stream feed the dashboard.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Path, State, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use kalpak_control::ControlPlane;
use kalpak_core::{AgentId, BlockId, CacheKey};
use kalpak_storage::TieredStore;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower_http::cors::CorsLayer;

pub struct AppState {
    pub store: TieredStore,
    pub control: ControlPlane,
}

type Shared = Arc<AppState>;

pub struct ServeOpts {
    pub data_dir: String,
    pub addr: String,
    pub warm_bytes: u64,
    pub node_id: u64,
    /// Form a single-voter cluster on boot. Disable when this node will be
    /// joined to an existing cluster via `/v1/cluster/*`.
    pub bootstrap: bool,
}

pub async fn serve(opts: ServeOpts) -> Result<(), Box<dyn std::error::Error>> {
    let store = TieredStore::open(&opts.data_dir, opts.warm_bytes)?;
    let control =
        ControlPlane::start_node(opts.node_id, Some(std::path::Path::new(&opts.data_dir))).await?;
    if opts.bootstrap {
        let members = std::collections::BTreeMap::from([(opts.node_id, opts.addr.clone())]);
        // Re-initialization of an already-formed cluster fails benignly on
        // restart: membership is already in the durable log.
        if let Err(e) = control.init_cluster(members).await {
            eprintln!("kalpakdb: cluster already initialized ({e})");
        }
    }
    let state = Arc::new(AppState { store, control });

    let app = router(state);
    let listener = tokio::net::TcpListener::bind(&opts.addr).await?;
    eprintln!(
        "kalpakdb node {} listening on http://{} (data dir: {})",
        opts.node_id, opts.addr, opts.data_dir
    );
    axum::serve(listener, app).await?;
    Ok(())
}

pub fn router(state: Shared) -> Router {
    Router::new()
        .route("/v1/blocks", post(put_block))
        .route("/v1/blocks/{id}", get(get_block))
        .route("/v1/agents", post(register_agent))
        .route("/v1/manifest/bind", post(bind_prefix))
        .route("/v1/manifest/lookup", post(lookup_prefix))
        .route("/v1/cluster/init", post(cluster_init))
        .route("/v1/cluster/add-learner", post(cluster_add_learner))
        .route("/v1/cluster/promote", post(cluster_promote))
        .route("/v1/stats", get(stats))
        .route("/v1/ws", get(ws_stats))
        .route("/raft/append", post(raft_append))
        .route("/raft/vote", post(raft_vote))
        .route("/raft/snapshot", post(raft_snapshot))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<kalpak_core::Error> for ApiError {
    fn from(e: kalpak_core::Error) -> Self {
        let code = match &e {
            kalpak_core::Error::BlockNotFound(_) => StatusCode::NOT_FOUND,
            kalpak_core::Error::InvalidId(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        ApiError(code, e.to_string())
    }
}

impl From<kalpak_control::ControlError> for ApiError {
    fn from(e: kalpak_control::ControlError) -> Self {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}

async fn put_block(
    State(s): State<Shared>,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    let id = s.store.put(&body)?;
    Ok(Json(json!({ "id": id.to_string(), "bytes": body.len() })))
}

async fn get_block(State(s): State<Shared>, Path(id): Path<String>) -> Result<Vec<u8>, ApiError> {
    let id: BlockId = id.parse()?;
    Ok(s.store.get(&id)?.as_ref().clone())
}

#[derive(Deserialize)]
struct RegisterAgentReq {
    agent: AgentId,
    display_name: String,
}

async fn register_agent(
    State(s): State<Shared>,
    Json(req): Json<RegisterAgentReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    s.control
        .register_agent(req.agent, req.display_name)
        .await?;
    Ok(Json(json!({ "registered": req.agent.to_string() })))
}

#[derive(Deserialize)]
struct BindReq {
    agent: AgentId,
    key: CacheKey,
    blocks: Vec<BlockId>,
}

async fn bind_prefix(
    State(s): State<Shared>,
    Json(req): Json<BindReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    for b in &req.blocks {
        if !s.store.contains(b) {
            return Err(ApiError(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("block {b} is not stored; upload blocks before binding"),
            ));
        }
    }
    s.control
        .bind_prefix(req.agent, req.key, req.blocks)
        .await?;
    Ok(Json(json!({ "bound": true })))
}

/// Probe a root-first chain of cache keys; returns the deepest bound prefix.
#[derive(Deserialize)]
struct LookupReq {
    chain: Vec<CacheKey>,
}

#[derive(Serialize)]
struct LookupResp {
    /// Index into the request chain of the deepest hit, if any.
    hit_depth: Option<usize>,
    blocks: Vec<String>,
}

async fn lookup_prefix(State(s): State<Shared>, Json(req): Json<LookupReq>) -> Json<LookupResp> {
    let mut best = None;
    for (i, key) in req.chain.iter().enumerate() {
        match s.control.lookup(key) {
            Some(rec) => best = Some((i, rec.blocks)),
            None => break,
        }
    }
    match best {
        Some((i, blocks)) => {
            // Speculative retrieval: the client will fetch these blocks
            // next, so promote them into the warm tier now, overlapping
            // disk I/O with the client's round trip.
            let warm = s.clone();
            let prefetch = blocks.clone();
            tokio::task::spawn_blocking(move || {
                for id in &prefetch {
                    let _ = warm.store.get(id);
                }
            });
            Json(LookupResp {
                hit_depth: Some(i),
                blocks: blocks.iter().map(|b| b.to_string()).collect(),
            })
        }
        None => Json(LookupResp {
            hit_depth: None,
            blocks: vec![],
        }),
    }
}

// ---- Cluster management ----

#[derive(Deserialize)]
struct ClusterInitReq {
    /// node id -> advertise address ("host:port") of every initial voter.
    members: std::collections::BTreeMap<u64, String>,
}

async fn cluster_init(
    State(s): State<Shared>,
    Json(req): Json<ClusterInitReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    s.control.init_cluster(req.members).await?;
    Ok(Json(json!({ "initialized": true })))
}

#[derive(Deserialize)]
struct AddLearnerReq {
    node_id: u64,
    addr: String,
}

async fn cluster_add_learner(
    State(s): State<Shared>,
    Json(req): Json<AddLearnerReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    s.control.add_learner(req.node_id, req.addr).await?;
    Ok(Json(json!({ "learner": req.node_id })))
}

#[derive(Deserialize)]
struct PromoteReq {
    voters: std::collections::BTreeSet<u64>,
}

async fn cluster_promote(
    State(s): State<Shared>,
    Json(req): Json<PromoteReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    s.control.change_membership(req.voters.clone()).await?;
    Ok(Json(json!({ "voters": req.voters })))
}

// ---- Raft RPC passthrough (node-to-node) ----

async fn raft_append(
    State(s): State<Shared>,
    Json(req): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    s.control
        .handle_append_entries(req)
        .await
        .map(Json)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))
}

async fn raft_vote(
    State(s): State<Shared>,
    Json(req): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    s.control
        .handle_vote(req)
        .await
        .map(Json)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))
}

async fn raft_snapshot(
    State(s): State<Shared>,
    Json(req): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    s.control
        .handle_install_snapshot(req)
        .await
        .map(Json)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))
}

fn stats_payload(s: &AppState) -> serde_json::Value {
    let cold = s.store.cold().stats();
    let tier = s.store.tier_stats();
    let raft = s.control.metrics();
    json!({
        "data_plane": {
            "blocks": cold.blocks,
            "segments": cold.segments,
            "bytes_on_disk": cold.bytes_on_disk,
            "warm_blocks": tier.warm_blocks,
            "warm_bytes": tier.warm_bytes,
            "warm_budget": tier.warm_budget,
            "hits": tier.hits,
            "misses": tier.misses,
        },
        "control_plane": {
            "node_id": raft.id,
            "leader": raft.current_leader,
            "term": raft.current_term,
            "last_log_index": raft.last_log_index,
            "last_applied": raft.last_applied.map(|l| l.index),
            "agents": s.control.agent_count(),
            "bindings": s.control.binding_count(),
        },
    })
}

async fn stats(State(s): State<Shared>) -> Json<serde_json::Value> {
    Json(stats_payload(&s))
}

async fn ws_stats(ws: WebSocketUpgrade, State(s): State<Shared>) -> Response {
    ws.on_upgrade(move |socket| stream_stats(socket, s))
}

async fn stream_stats(mut socket: WebSocket, s: Shared) {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tick.tick().await;
        let payload = stats_payload(&s).to_string();
        if socket.send(Message::Text(payload.into())).await.is_err() {
            break;
        }
    }
}
