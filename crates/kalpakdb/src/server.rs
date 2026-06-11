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
    pub control: Arc<ControlPlane>,
    /// Client for node-to-node calls (leader forwarding, peer block fetch).
    pub http: reqwest::Client,
    /// Cumulative GC telemetry (manual + scheduled compactions).
    pub gc_runs: std::sync::atomic::AtomicU64,
    pub gc_blocks_dropped: std::sync::atomic::AtomicU64,
    pub gc_bytes_reclaimed: std::sync::atomic::AtomicU64,
}

impl AppState {
    fn record_gc(&self, st: &kalpak_storage::CompactStats) {
        use std::sync::atomic::Ordering::Relaxed;
        self.gc_runs.fetch_add(1, Relaxed);
        self.gc_blocks_dropped.fetch_add(st.blocks_dropped, Relaxed);
        self.gc_bytes_reclaimed
            .fetch_add(st.bytes_reclaimed, Relaxed);
    }
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
    /// Bind address for the gRPC streaming data plane (None disables it).
    pub grpc_addr: Option<String>,
    /// Run GC automatically every N seconds (0 disables; the
    /// `/v1/admin/compact` endpoint always works).
    pub compact_secs: u64,
}

async fn boot_control(opts: &ServeOpts) -> Result<Arc<ControlPlane>, Box<dyn std::error::Error>> {
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
    Ok(Arc::new(control))
}

pub async fn serve(opts: ServeOpts) -> Result<(), Box<dyn std::error::Error>> {
    let store = TieredStore::open(&opts.data_dir, opts.warm_bytes)?;
    let control = boot_control(&opts).await?;
    let state = Arc::new(AppState {
        store,
        control,
        http: reqwest::Client::new(),
        gc_runs: Default::default(),
        gc_blocks_dropped: Default::default(),
        gc_bytes_reclaimed: Default::default(),
    });

    if opts.compact_secs > 0 {
        let state = state.clone();
        let every = std::time::Duration::from_secs(opts.compact_secs);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.tick().await; // first tick fires immediately; skip it
            loop {
                tick.tick().await;
                let live = state.control.bound_blocks();
                let store_state = state.clone();
                let swept = tokio::task::spawn_blocking(move || {
                    store_state.store.compact(|id| live.contains(id))
                })
                .await;
                match swept {
                    Ok(Ok(st)) if st.blocks_dropped > 0 => {
                        state.record_gc(&st);
                        eprintln!(
                            "kalpakdb: gc reclaimed {} bytes ({} blocks, {} segments)",
                            st.bytes_reclaimed, st.blocks_dropped, st.segments_rewritten
                        )
                    }
                    Ok(Ok(st)) => state.record_gc(&st),
                    Ok(Err(e)) => eprintln!("kalpakdb: gc failed: {e}"),
                    Err(e) => eprintln!("kalpakdb: gc task panicked: {e}"),
                }
            }
        });
    }

    if let Some(grpc_addr) = &opts.grpc_addr {
        let svc = crate::grpc::service(state.clone());
        let bind: std::net::SocketAddr = grpc_addr.parse()?;
        eprintln!("kalpakdb node {} gRPC data plane on {bind}", opts.node_id);
        tokio::spawn(async move {
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(svc)
                .serve(bind)
                .await
            {
                eprintln!("kalpakdb: gRPC server exited: {e}");
            }
        });
    }

    let app = router(state);
    let listener = tokio::net::TcpListener::bind(&opts.addr).await?;
    eprintln!(
        "kalpakdb node {} listening on http://{} (data dir: {})",
        opts.node_id, opts.addr, opts.data_dir
    );
    axum::serve(listener, app).await?;
    Ok(())
}

/// Run a witness: a consensus-only node that votes and replicates metadata
/// but stores no data-plane blocks. This is the lightweight third vote that
/// gives a two-box deployment strict quorum without a third storage machine.
pub async fn serve_witness(opts: ServeOpts) -> Result<(), Box<dyn std::error::Error>> {
    let control = boot_control(&opts).await?;
    let app = witness_router(control.clone());
    let listener = tokio::net::TcpListener::bind(&opts.addr).await?;
    eprintln!(
        "kalpakdb witness {} listening on http://{} (control dir: {})",
        opts.node_id, opts.addr, opts.data_dir
    );
    axum::serve(listener, app).await?;
    Ok(())
}

/// Consensus routes, shared by full nodes and witnesses.
fn raft_router(control: Arc<ControlPlane>) -> Router {
    Router::new()
        .route("/raft/append", post(raft_append))
        .route("/raft/vote", post(raft_vote))
        .route("/raft/snapshot", post(raft_snapshot))
        .route("/v1/cluster/init", post(cluster_init))
        .route("/v1/cluster/add-learner", post(cluster_add_learner))
        .route("/v1/cluster/promote", post(cluster_promote))
        .with_state(control)
}

pub fn router(state: Shared) -> Router {
    let raft = raft_router(state.control.clone());
    Router::new()
        .route("/v1/blocks", post(put_block))
        .route("/v1/blocks/batch", post(put_blocks_batch))
        .route("/v1/blocks/{id}", get(get_block))
        .route("/v1/agents", post(register_agent))
        .route("/v1/keys", post(make_key))
        .route("/v1/manifest/bind", post(bind_prefix))
        .route("/v1/manifest/lookup", post(lookup_prefix))
        .route("/v1/stats", get(stats))
        .route("/v1/ws", get(ws_stats))
        .route("/v1/admin/compact", post(admin_compact))
        .route("/v1/agents/list", get(list_agents))
        .route("/v1/agents/{id}/bindings", get(agent_bindings))
        .with_state(state)
        .merge(raft)
        .layer(CorsLayer::permissive())
}

fn witness_router(control: Arc<ControlPlane>) -> Router {
    let raft = raft_router(control.clone());
    Router::new()
        .route("/v1/stats", get(witness_stats))
        .with_state(control)
        .merge(raft)
        .layer(CorsLayer::permissive())
}

async fn witness_stats(State(c): State<Arc<ControlPlane>>) -> Json<serde_json::Value> {
    let raft = c.metrics();
    Json(json!({
        "role": "witness",
        "control_plane": {
            "node_id": raft.id,
            "leader": raft.current_leader,
            "term": raft.current_term,
            "last_log_index": raft.last_log_index,
            "last_applied": raft.last_applied.map(|l| l.index),
            "agents": c.agent_count(),
            "bindings": c.binding_count(),
        },
    }))
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
        let code = match &e {
            kalpak_control::ControlError::NotLeader { .. } => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        ApiError(code, e.to_string())
    }
}

/// Marks node-to-node requests so they never cascade (a peer asked to help
/// must answer from local state only).
const INTERNAL_HEADER: &str = "x-kalpak-internal";

/// If a write failed because this node is a follower, transparently retry it
/// against the leader and relay the leader's response.
async fn forward_to_leader(
    s: &AppState,
    err: kalpak_control::ControlError,
    path: &str,
    body: serde_json::Value,
) -> Result<Json<serde_json::Value>, ApiError> {
    let kalpak_control::ControlError::NotLeader {
        leader_addr: Some(addr),
    } = &err
    else {
        return Err(err.into());
    };
    let resp = s
        .http
        .post(format!("http://{addr}{path}"))
        .header(INTERNAL_HEADER, "1")
        .json(&body)
        .send()
        .await
        .map_err(|e| ApiError(StatusCode::BAD_GATEWAY, format!("leader forward: {e}")))?;
    let status = resp.status();
    let value: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| ApiError(StatusCode::BAD_GATEWAY, format!("leader response: {e}")))?;
    if status.is_success() {
        Ok(Json(value))
    } else {
        let msg = value["error"].as_str().unwrap_or("forwarded write failed");
        Err(ApiError(
            StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
            msg.to_string(),
        ))
    }
}

async fn put_block(
    State(s): State<Shared>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    let id = s.store.put(&body)?;
    // Proactive replication: push the block to every data peer in the
    // background so reads anywhere are local. Witnesses simply reject the
    // route. Internal pushes never fan out further.
    if !headers.contains_key(INTERNAL_HEADER) {
        let state = s.clone();
        let payload = body.clone();
        tokio::spawn(async move {
            for peer in state.control.peer_addrs() {
                let _ = state
                    .http
                    .post(format!("http://{peer}/v1/blocks"))
                    .header(INTERNAL_HEADER, "1")
                    .body(payload.clone())
                    .send()
                    .await;
            }
        });
    }
    Ok(Json(json!({ "id": id.to_string(), "bytes": body.len() })))
}

/// Batch block upload with one group-committed fsync.
///
/// Body framing (binary, little-endian): `u32 count`, then per block
/// `u32 len` + `len` payload bytes. Replies with the ids in input order.
async fn put_blocks_batch(
    State(s): State<Shared>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    let bad = |m: &str| ApiError(StatusCode::BAD_REQUEST, m.to_string());
    if body.len() < 4 {
        return Err(bad("truncated batch header"));
    }
    let count = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
    let mut payloads = Vec::with_capacity(count);
    let mut at = 4usize;
    for _ in 0..count {
        if at + 4 > body.len() {
            return Err(bad("truncated block length"));
        }
        let len = u32::from_le_bytes(body[at..at + 4].try_into().unwrap()) as usize;
        at += 4;
        if at + len > body.len() {
            return Err(bad("truncated block payload"));
        }
        payloads.push(&body[at..at + len]);
        at += len;
    }
    let ids = s.store.put_many(payloads.iter().copied())?;

    if !headers.contains_key(INTERNAL_HEADER) {
        let state = s.clone();
        let raw = body.clone();
        tokio::spawn(async move {
            for peer in state.control.peer_addrs() {
                let _ = state
                    .http
                    .post(format!("http://{peer}/v1/blocks/batch"))
                    .header(INTERNAL_HEADER, "1")
                    .body(raw.clone())
                    .send()
                    .await;
            }
        });
    }
    Ok(Json(json!({
        "ids": ids.iter().map(|i| i.to_string()).collect::<Vec<_>>()
    })))
}

async fn get_block(
    State(s): State<Shared>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Result<Vec<u8>, ApiError> {
    let id: BlockId = id.parse()?;
    match s.store.get(&id) {
        Ok(payload) => Ok(payload.as_ref().clone()),
        Err(kalpak_core::Error::BlockNotFound(_)) if !headers.contains_key(INTERNAL_HEADER) => {
            // Data plane is not (yet) proactively replicated: fetch the block
            // from a peer, keep a local copy (replicate-on-read), and serve.
            for peer in s.control.peer_addrs() {
                let Ok(resp) = s
                    .http
                    .get(format!("http://{peer}/v1/blocks/{id}"))
                    .header(INTERNAL_HEADER, "1")
                    .send()
                    .await
                else {
                    continue;
                };
                if !resp.status().is_success() {
                    continue;
                }
                let Ok(bytes) = resp.bytes().await else {
                    continue;
                };
                // Content addressing makes peer data self-verifying.
                if BlockId::of(&bytes) != id {
                    continue;
                }
                s.store.put(&bytes)?;
                return Ok(bytes.to_vec());
            }
            Err(kalpak_core::Error::BlockNotFound(id).into())
        }
        Err(e) => Err(e.into()),
    }
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
    match s.control.register_agent(req.agent, &req.display_name).await {
        Ok(()) => Ok(Json(json!({ "registered": req.agent.to_string() }))),
        Err(e) => {
            let body = json!({ "agent": req.agent, "display_name": req.display_name });
            forward_to_leader(&s, e, "/v1/agents", body).await
        }
    }
}

#[derive(Deserialize)]
struct BindReq {
    agent: AgentId,
    key: CacheKey,
    blocks: Vec<BlockId>,
    #[serde(default)]
    parent: Option<CacheKey>,
}

async fn bind_prefix(
    State(s): State<Shared>,
    headers: axum::http::HeaderMap,
    Json(req): Json<BindReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // Validate block existence on the node the client talked to; a
    // leader-forwarded bind skips this (the leader may not hold the blocks —
    // the data plane is per-node until proactive replication lands).
    if !headers.contains_key(INTERNAL_HEADER) {
        for b in &req.blocks {
            if !s.store.contains(b) {
                return Err(ApiError(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    format!("block {b} is not stored; upload blocks before binding"),
                ));
            }
        }
    }
    match s
        .control
        .bind_prefix(
            req.agent,
            req.key.clone(),
            req.blocks.clone(),
            req.parent.clone(),
        )
        .await
    {
        Ok(()) => Ok(Json(json!({ "bound": true }))),
        Err(e) => {
            let body = json!({
                "agent": req.agent,
                "key": req.key,
                "blocks": req.blocks,
                "parent": req.parent,
            });
            forward_to_leader(&s, e, "/v1/manifest/bind", body).await
        }
    }
}

/// Compute a chained cache key server-side, so thin clients (e.g. the
/// zero-dependency Python SDK) need no local BLAKE3.
#[derive(Deserialize)]
struct MakeKeyReq {
    fingerprint: kalpak_core::ModelFingerprint,
    tokens: Vec<u32>,
    #[serde(default)]
    parent: Option<CacheKey>,
}

async fn make_key(Json(req): Json<MakeKeyReq>) -> Json<CacheKey> {
    let key = match req.parent {
        Some(parent) => parent.extend(&req.tokens),
        None => CacheKey::root(req.fingerprint, &req.tokens),
    };
    Json(key)
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
            // Speculative retrieval, two steps: warm the hit's own blocks
            // (the client fetches them next), then warm the blocks of the
            // hit's CHILDREN in the prefix tree — the prefixes the agent is
            // most likely to extend into (cf. SpeCache / CXL-SpecKV).
            let lookahead = s.control.child_blocks(&req.chain[i]);
            let warm = s.clone();
            let prefetch = blocks.clone();
            tokio::task::spawn_blocking(move || {
                for id in prefetch.iter().chain(&lookahead) {
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
    State(c): State<Arc<ControlPlane>>,
    Json(req): Json<ClusterInitReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    c.init_cluster(req.members).await?;
    Ok(Json(json!({ "initialized": true })))
}

#[derive(Deserialize)]
struct AddLearnerReq {
    node_id: u64,
    addr: String,
}

async fn cluster_add_learner(
    State(c): State<Arc<ControlPlane>>,
    Json(req): Json<AddLearnerReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    c.add_learner(req.node_id, req.addr).await?;
    Ok(Json(json!({ "learner": req.node_id })))
}

#[derive(Deserialize)]
struct PromoteReq {
    voters: std::collections::BTreeSet<u64>,
}

async fn cluster_promote(
    State(c): State<Arc<ControlPlane>>,
    Json(req): Json<PromoteReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    c.change_membership(req.voters.clone()).await?;
    Ok(Json(json!({ "voters": req.voters })))
}

// ---- Raft RPC passthrough (node-to-node) ----

async fn raft_append(
    State(c): State<Arc<ControlPlane>>,
    Json(req): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    c.handle_append_entries(req)
        .await
        .map(Json)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))
}

async fn raft_vote(
    State(c): State<Arc<ControlPlane>>,
    Json(req): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    c.handle_vote(req)
        .await
        .map(Json)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))
}

async fn raft_snapshot(
    State(c): State<Arc<ControlPlane>>,
    Json(req): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    c.handle_install_snapshot(req)
        .await
        .map(Json)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))
}

/// Garbage-collect sealed segments: every block referenced by a
/// Raft-replicated binding is live; everything else in sealed segments is
/// swept. The active segment is never touched, which is the grace window
/// for the put-then-bind two-phase write.
async fn admin_compact(State(s): State<Shared>) -> Result<Json<serde_json::Value>, ApiError> {
    let live = s.control.bound_blocks();
    let state = s.clone();
    let stats = tokio::task::spawn_blocking(move || state.store.compact(|id| live.contains(id)))
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;
    s.record_gc(&stats);
    Ok(Json(json!({
        "segments_rewritten": stats.segments_rewritten,
        "blocks_dropped": stats.blocks_dropped,
        "bytes_reclaimed": stats.bytes_reclaimed,
    })))
}

/// The "mind explorer" listing: every registered agent, oldest first.
async fn list_agents(State(s): State<Shared>) -> Json<serde_json::Value> {
    let agents: Vec<_> = s
        .control
        .agents_list()
        .into_iter()
        .map(|(id, rec)| {
            json!({
                "agent": id.to_string(),
                "display_name": rec.display_name,
                "registered_at": rec.registered_at,
                "bindings": s.control.bindings_of(&id).len(),
            })
        })
        .collect();
    Json(json!({ "agents": agents }))
}

/// Audit one agent's memory: which prefix keys it bound to which blocks.
async fn agent_bindings(
    State(s): State<Shared>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let agent: AgentId = id.parse()?;
    let bindings: Vec<_> = s
        .control
        .bindings_of(&agent)
        .into_iter()
        .map(|(key, blocks)| {
            json!({
                "key": serde_json::from_str::<serde_json::Value>(&key)
                    .unwrap_or(serde_json::Value::String(key)),
                "blocks": blocks.iter().map(|b| b.to_string()).collect::<Vec<_>>(),
            })
        })
        .collect();
    Ok(Json(
        json!({ "agent": agent.to_string(), "bindings": bindings }),
    ))
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
            "peers": s.control.peer_addrs(),
            "replication": s.control.replication_state(),
        },
        "gc": {
            "runs": s.gc_runs.load(std::sync::atomic::Ordering::Relaxed),
            "blocks_dropped": s.gc_blocks_dropped.load(std::sync::atomic::Ordering::Relaxed),
            "bytes_reclaimed": s.gc_bytes_reclaimed.load(std::sync::atomic::Ordering::Relaxed),
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
