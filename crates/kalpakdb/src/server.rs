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
    pub data_dir: String,
    pub control: Arc<ControlPlane>,
    /// Client for node-to-node calls (leader forwarding, peer block fetch);
    /// carries the mesh identity when mesh mTLS is enabled.
    pub http: reqwest::Client,
    /// URL scheme for node-to-node calls ("https" under mesh mTLS).
    pub peer_scheme: &'static str,
    /// Reject unsigned metadata mutations.
    pub require_signatures: bool,
    /// Max bytes for a single streamed gRPC block.
    pub max_block_bytes: usize,
    /// Bearer token demanded for observability reads, when set.
    pub read_token: Option<String>,
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
    /// Reject metadata mutations (register/bind) that are not signed by the
    /// owning agent's Ed25519 key.
    pub require_signatures: bool,
    /// Max bytes for a single streamed gRPC block before it is rejected
    /// (None = 256 MiB default). A safety bound on unbounded accumulation,
    /// also an ops knob for unusually large or memory-constrained nodes.
    pub max_block_bytes: Option<usize>,
    /// Serve the client-facing API over TLS (PEM paths). Node-to-node
    /// traffic stays plain HTTP on the cluster network; generate dev certs
    /// with `kalpakdb cert`.
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    /// When set, observability reads (/v1/stats, /v1/ws, /v1/agents/*)
    /// require `Authorization: Bearer <token>` (or `?token=` for the
    /// WebSocket). /metrics stays open for Prometheus scrapers.
    pub read_token: Option<String>,
    /// Mutually-authenticated mesh listener for node-to-node traffic.
    /// When set, cluster membership addresses must point here, and internal
    /// trust (forwarding, replication) only exists on this listener —
    /// the public listener strips internal semantics. Generate material
    /// with `kalpakdb mesh-ca`.
    pub mesh: Option<MeshOpts>,
}

#[derive(Clone)]
pub struct MeshOpts {
    pub addr: String,
    pub ca: String,
    pub cert: String,
    pub key: String,
}

impl MeshOpts {
    fn client_tls(&self) -> Result<kalpak_control::MeshClientTls, Box<dyn std::error::Error>> {
        let cert = std::fs::read(&self.cert)?;
        let key = std::fs::read(&self.key)?;
        let mut identity_pem = cert.clone();
        identity_pem.extend_from_slice(b"\n");
        identity_pem.extend_from_slice(&key);
        Ok(kalpak_control::MeshClientTls {
            ca_pem: std::fs::read(&self.ca)?,
            identity_pem,
        })
    }

    /// rustls server config demanding a client certificate signed by the
    /// cluster CA — presenting one IS mesh membership.
    fn server_config(&self) -> Result<rustls::ServerConfig, Box<dyn std::error::Error>> {
        let ca_pem = std::fs::read(&self.ca)?;
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut ca_pem.as_slice()) {
            roots.add(cert?)?;
        }
        let verifier =
            rustls::server::WebPkiClientVerifier::builder(std::sync::Arc::new(roots)).build()?;
        let certs = rustls_pemfile::certs(&mut std::fs::read(&self.cert)?.as_slice())
            .collect::<Result<Vec<_>, _>>()?;
        let key = rustls_pemfile::private_key(&mut std::fs::read(&self.key)?.as_slice())?
            .ok_or("no private key in mesh key file")?;
        Ok(rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)?)
    }
}

async fn boot_control(opts: &ServeOpts) -> Result<Arc<ControlPlane>, Box<dyn std::error::Error>> {
    let mesh_tls = opts.mesh.as_ref().map(|m| m.client_tls()).transpose()?;
    let control = ControlPlane::start_node_with_mesh(
        opts.node_id,
        Some(std::path::Path::new(&opts.data_dir)),
        mesh_tls.as_ref(),
    )
    .await?;
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
    let (http, peer_scheme) = match &opts.mesh {
        Some(m) => {
            let tls = m.client_tls()?;
            let client = reqwest::Client::builder()
                .add_root_certificate(reqwest::Certificate::from_pem(&tls.ca_pem)?)
                .identity(reqwest::Identity::from_pem(&tls.identity_pem)?)
                .build()?;
            (client, "https")
        }
        None => (reqwest::Client::new(), "http"),
    };
    let state = Arc::new(AppState {
        store,
        data_dir: opts.data_dir.clone(),
        control,
        http,
        peer_scheme,
        require_signatures: opts.require_signatures,
        max_block_bytes: opts.max_block_bytes.unwrap_or(256 * 1024 * 1024),
        read_token: opts.read_token.clone(),
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

    if let Some(mesh) = &opts.mesh {
        // The mesh listener serves the FULL router behind mutual TLS: a
        // peer certificate signed by the cluster CA is mesh membership,
        // which is what justifies INTERNAL_HEADER trust there.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cfg = mesh.server_config()?;
        let mesh_app = router(state.clone());
        let bind: std::net::SocketAddr = mesh.addr.parse()?;
        eprintln!("kalpakdb node {} mesh (mTLS) on {bind}", opts.node_id);
        tokio::spawn(async move {
            let rustls_cfg =
                axum_server::tls_rustls::RustlsConfig::from_config(std::sync::Arc::new(cfg));
            if let Err(e) = axum_server::bind_rustls(bind, rustls_cfg)
                .serve(mesh_app.into_make_service())
                .await
            {
                eprintln!("kalpakdb: mesh listener exited: {e}");
            }
        });
    }

    let mut app = router(state);
    if opts.mesh.is_some() {
        // With a mesh listener present, internal semantics never apply to
        // public traffic: strip the header so it cannot be spoofed to skip
        // signature checks or fan-out suppression.
        app = app.layer(axum::middleware::from_fn(strip_internal_header));
    }
    serve_app(app, &opts).await
}

async fn strip_internal_header(
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    req.headers_mut().remove(INTERNAL_HEADER);
    next.run(req).await
}

/// Bind the API over TLS when certificates are configured, plain HTTP
/// otherwise.
async fn serve_app(app: Router, opts: &ServeOpts) -> Result<(), Box<dyn std::error::Error>> {
    match (&opts.tls_cert, &opts.tls_key) {
        (Some(cert), Some(key)) => {
            // Two rustls crypto providers exist in the dep tree (ring via
            // reqwest, aws-lc-rs via axum-server), so the process default
            // must be chosen explicitly; "already installed" is fine.
            let _ = rustls::crypto::ring::default_provider().install_default();
            let config = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?;
            let addr: std::net::SocketAddr = opts.addr.parse()?;
            eprintln!(
                "kalpakdb node {} listening on https://{} (data dir: {})",
                opts.node_id, opts.addr, opts.data_dir
            );
            axum_server::bind_rustls(addr, config)
                .serve(app.into_make_service())
                .await?;
        }
        (None, None) => {
            let listener = tokio::net::TcpListener::bind(&opts.addr).await?;
            eprintln!(
                "kalpakdb node {} listening on http://{} (data dir: {})",
                opts.node_id, opts.addr, opts.data_dir
            );
            axum::serve(listener, app).await?;
        }
        _ => return Err("--tls-cert and --tls-key must be given together".into()),
    }
    Ok(())
}

/// Run a witness: a consensus-only node that votes and replicates metadata
/// but stores no data-plane blocks. This is the lightweight third vote that
/// gives a two-box deployment strict quorum without a third storage machine.
pub async fn serve_witness(opts: ServeOpts) -> Result<(), Box<dyn std::error::Error>> {
    let control = boot_control(&opts).await?;
    let app = witness_router(control.clone());
    eprintln!(
        "kalpakdb witness {} (control dir: {})",
        opts.node_id, opts.data_dir
    );
    serve_app(app, &opts).await
}

/// Consensus + health routes, shared by full nodes and witnesses. Health
/// endpoints live here (rather than the data-plane router) so both node
/// types expose them, and they are deliberately unauthenticated — an
/// orchestrator's probe must not need a `--read-token`.
fn raft_router(control: Arc<ControlPlane>) -> Router {
    Router::new()
        .route("/raft/append", post(raft_append))
        .route("/raft/vote", post(raft_vote))
        .route("/raft/snapshot", post(raft_snapshot))
        .route("/v1/cluster/init", post(cluster_init))
        .route("/v1/cluster/add-learner", post(cluster_add_learner))
        .route("/v1/cluster/promote", post(cluster_promote))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(control)
}

pub fn router(state: Shared) -> Router {
    let raft = raft_router(state.control.clone());

    // Block ingest carries real KV tensors, routinely larger than axum's
    // 2 MiB default body limit — which would silently 413 any realistic
    // block. Raise these two routes to the same per-block ceiling the gRPC
    // path enforces (`max_block_bytes`); every other route keeps the small
    // default, bounding how much a metadata/JSON request can buffer.
    let block_ingest = Router::new()
        .route("/v1/blocks", post(put_block))
        .route("/v1/blocks/batch", post(put_blocks_batch))
        .layer(axum::extract::DefaultBodyLimit::max(state.max_block_bytes))
        .with_state(state.clone());

    Router::new()
        .route("/v1/blocks/{id}", get(get_block))
        .route("/v1/agents", post(register_agent))
        .route("/v1/keys", post(make_key))
        .route("/v1/manifest/bind", post(bind_prefix))
        .route("/v1/manifest/bind-chain", post(bind_chain))
        .route("/v1/manifest/lookup", post(lookup_prefix))
        .route("/v1/stats", get(stats))
        .route("/metrics", get(metrics))
        .route("/v1/ws", get(ws_stats))
        .route("/v1/admin/compact", post(admin_compact))
        .route("/v1/admin/backup", get(admin_backup))
        .route("/v1/agents/list", get(list_agents))
        .route("/v1/agents/{id}/bindings", get(agent_bindings))
        .with_state(state)
        .merge(block_ingest)
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

/// Liveness: the process is up and serving HTTP. Always 200 — a probe
/// failure here means the node is dead or wedged, and the orchestrator
/// should restart it.
async fn healthz() -> &'static str {
    "ok"
}

/// Readiness: the node can actually serve — it knows a current leader
/// (elected, not mid-election or partitioned). 200 when ready, 503 when
/// not, so a load balancer / `depends_on` gate holds traffic until the
/// cluster has settled. Unauthenticated, like `/healthz` and `/metrics`.
async fn readyz(State(c): State<Arc<ControlPlane>>) -> Response {
    let m = c.metrics();
    if m.current_leader.is_some() {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "no leader").into_response()
    }
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

/// After a bind commits, pin any of its blocks that are now referenced by
/// multiple bindings: structural importance (shared prefixes, system
/// prompts) earns eviction immunity (IMPRESS-style placement).
fn pin_shared_blocks(s: &Shared, blocks: &[BlockId]) {
    let state = s.clone();
    let blocks = blocks.to_vec();
    tokio::task::spawn_blocking(move || {
        for id in blocks {
            if state.control.block_ref_count(&id) >= 2 {
                let _ = state.store.pin(&id);
            }
        }
    });
}

/// Guard for observability reads when `--read-token` is set. Accepts the
/// token as a Bearer header or (for WebSocket clients, which cannot set
/// headers from browsers) a `?token=` query parameter.
fn check_read_token(
    s: &AppState,
    headers: &axum::http::HeaderMap,
    query_token: Option<&str>,
) -> Result<(), ApiError> {
    let Some(expected) = &s.read_token else {
        return Ok(());
    };
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .or(query_token);
    if presented == Some(expected.as_str()) {
        Ok(())
    } else {
        Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "this node requires a read token: Authorization: Bearer <token>".to_string(),
        ))
    }
}

/// Marks node-to-node requests so they never cascade (a peer asked to help
/// must answer from local state only).
const INTERNAL_HEADER: &str = "x-kalpak-internal";

/// Verify a mutation's signature when the node demands signed writes.
///
/// Node-to-node requests (leader forwarding, replication) skip
/// re-verification: the ingress node already verified before forwarding,
/// matching the existing internal trust model.
fn verify_signature(
    s: &AppState,
    headers: &axum::http::HeaderMap,
    agent: &AgentId,
    message: &[u8],
    signature: Option<&str>,
) -> Result<(), ApiError> {
    if !s.require_signatures || headers.contains_key(INTERNAL_HEADER) {
        return Ok(());
    }
    let Some(sig_hex) = signature else {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "this node requires signed writes: missing 'signature'".to_string(),
        ));
    };
    let sig = kalpak_core::signing::signature_from_hex(sig_hex)
        .map_err(|_| ApiError(StatusCode::UNAUTHORIZED, "malformed signature".to_string()))?;
    agent.verify(message, &sig).map_err(|_| {
        ApiError(
            StatusCode::UNAUTHORIZED,
            "signature does not verify against the agent's public key".to_string(),
        )
    })
}

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
        .post(format!("{}://{addr}{path}", s.peer_scheme))
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
                    .post(format!("{}://{peer}/v1/blocks", state.peer_scheme))
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
    // `count` is attacker-controlled: never pre-allocate from it directly,
    // or a 4-byte body claiming u32::MAX blocks would try to reserve tens of
    // GB and abort the process. Every block contributes at least a 4-byte
    // length prefix, so a body can hold at most body.len()/4 blocks — cap the
    // reservation there. Malformed counts then fail cleanly in the loop below.
    let mut payloads = Vec::with_capacity(count.min(body.len() / 4));
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
                    .post(format!("{}://{peer}/v1/blocks/batch", state.peer_scheme))
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
                    .get(format!("{}://{peer}/v1/blocks/{id}", s.peer_scheme))
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
    #[serde(default)]
    signature: Option<String>,
}

async fn register_agent(
    State(s): State<Shared>,
    headers: axum::http::HeaderMap,
    Json(req): Json<RegisterAgentReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let msg = kalpak_core::signing::register_message(&req.agent, &req.display_name);
    verify_signature(&s, &headers, &req.agent, &msg, req.signature.as_deref())?;
    match s.control.register_agent(req.agent, &req.display_name).await {
        Ok(()) => Ok(Json(json!({ "registered": req.agent.to_string() }))),
        Err(e) => {
            let body = json!({
                "agent": req.agent,
                "display_name": req.display_name,
                "signature": req.signature,
            });
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
    #[serde(default)]
    signature: Option<String>,
}

async fn bind_prefix(
    State(s): State<Shared>,
    headers: axum::http::HeaderMap,
    Json(req): Json<BindReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let msg =
        kalpak_core::signing::bind_message(&req.agent, &req.key, &req.blocks, req.parent.as_ref());
    verify_signature(&s, &headers, &req.agent, &msg, req.signature.as_deref())?;
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
        Ok(()) => {
            pin_shared_blocks(&s, &req.blocks);
            Ok(Json(json!({ "bound": true })))
        }
        Err(e) => {
            let body = json!({
                "agent": req.agent,
                "key": req.key,
                "blocks": req.blocks,
                "parent": req.parent,
                "signature": req.signature,
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

#[derive(Deserialize)]
struct BindChainReq {
    agent: AgentId,
    bindings: Vec<kalpak_control::ChainBinding>,
    #[serde(default)]
    signature: Option<String>,
}

/// Bind a whole prefix chain in one consensus round (one Raft fsync) —
/// the fast path for committing a multi-depth context.
async fn bind_chain(
    State(s): State<Shared>,
    headers: axum::http::HeaderMap,
    Json(req): Json<BindChainReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let links: Vec<_> = req
        .bindings
        .iter()
        .map(|b| (b.key.clone(), b.blocks.clone(), b.parent.clone()))
        .collect();
    let msg = kalpak_core::signing::chain_message(&req.agent, &links);
    verify_signature(&s, &headers, &req.agent, &msg, req.signature.as_deref())?;
    if !headers.contains_key(INTERNAL_HEADER) {
        for b in &req.bindings {
            for blk in &b.blocks {
                if !s.store.contains(blk) {
                    return Err(ApiError(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        format!("block {blk} is not stored; upload blocks before binding"),
                    ));
                }
            }
        }
    }
    match s.control.bind_chain(req.agent, req.bindings.clone()).await {
        Ok(()) => {
            let all: Vec<BlockId> = req.bindings.iter().flat_map(|b| b.blocks.clone()).collect();
            pin_shared_blocks(&s, &all);
            Ok(Json(json!({ "bound": req.bindings.len() })))
        }
        Err(e) => {
            let body = json!({
                "agent": req.agent,
                "bindings": req.bindings,
                "signature": req.signature,
            });
            forward_to_leader(&s, e, "/v1/manifest/bind-chain", body).await
        }
    }
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
async fn list_agents(
    State(s): State<Shared>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    check_read_token(&s, &headers, None)?;
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
    Ok(Json(json!({ "agents": agents })))
}

/// Audit one agent's memory: which prefix keys it bound to which blocks.
async fn agent_bindings(
    State(s): State<Shared>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    check_read_token(&s, &headers, None)?;
    let agent: AgentId = id.parse()?;
    let parents = s.control.parent_index();
    let bindings: Vec<_> = s
        .control
        .bindings_of(&agent)
        .into_iter()
        .map(|(key, blocks)| {
            let extends = parents.get(&key).map(|p| {
                serde_json::from_str::<serde_json::Value>(p)
                    .ok()
                    .and_then(|v| v["prefix_hash"].as_str().map(String::from))
                    .unwrap_or_default()
            });
            json!({
                "key": serde_json::from_str::<serde_json::Value>(&key)
                    .unwrap_or(serde_json::Value::String(key)),
                "blocks": blocks.iter().map(|b| b.to_string()).collect::<Vec<_>>(),
                "extends": extends,
            })
        })
        .collect();
    Ok(Json(
        json!({ "agent": agent.to_string(), "bindings": bindings }),
    ))
}

/// Stream a crash-consistent tar of the data directory from a LIVE node.
///
/// Ordering is the correctness argument: the control plane (Raft log +
/// snapshot) is archived BEFORE the segments. Bindings only reference
/// blocks that were durable before the bind (the two-phase write), so
/// every binding in the archived metadata has its blocks in the archived
/// segments. A torn segment/log tail from writes racing the copy is
/// dropped on open by the existing recovery paths — the restored state is
/// exactly "the node as of some moment during the backup".
async fn admin_backup(State(s): State<Shared>) -> Result<Response, ApiError> {
    let data_dir = s.data_dir.clone();
    let tar_path =
        tokio::task::spawn_blocking(move || -> Result<std::path::PathBuf, std::io::Error> {
            let tmp = tempfile::NamedTempFile::new()?;
            let (file, path) = tmp.keep().map_err(|e| e.error)?;
            let mut builder = tar::Builder::new(std::io::BufWriter::new(file));
            let root = std::path::Path::new(&data_dir);
            // 1. control plane first (see ordering argument above)
            let control = root.join("control");
            if control.is_dir() {
                builder.append_dir_all("control", &control)?;
            }
            // 2. then data-plane segments
            for entry in std::fs::read_dir(root)? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with("seg-") && name.ends_with(".klpk") {
                    builder.append_path_with_name(entry.path(), &name)?;
                }
            }
            builder.into_inner()?;
            Ok(path)
        })
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("backup: {e}")))?;

    let file = tokio::fs::File::open(&tar_path)
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // Unlink now; the open handle keeps the bytes alive until streamed.
    let _ = tokio::fs::remove_file(&tar_path).await;
    let stream = tokio_util::io::ReaderStream::new(file);
    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/x-tar")],
        axum::body::Body::from_stream(stream),
    )
        .into_response())
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
            "pinned_blocks": tier.pinned_blocks,
            "pinned_bytes": tier.pinned_bytes,
            "pinned_budget": tier.pinned_budget,
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

async fn stats(
    State(s): State<Shared>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    check_read_token(&s, &headers, None)?;
    Ok(Json(stats_payload(&s)))
}

/// Prometheus text exposition of the same numbers `/v1/stats` reports,
/// so the node drops into existing monitoring stacks unchanged.
async fn metrics(State(s): State<Shared>) -> ([(axum::http::HeaderName, &'static str); 1], String) {
    use std::fmt::Write as _;
    use std::sync::atomic::Ordering::Relaxed;

    let cold = s.store.cold().stats();
    let tier = s.store.tier_stats();
    let raft = s.control.metrics();

    let mut out = String::with_capacity(2048);
    let mut m = |name: &str, kind: &str, help: &str, value: u64| {
        let _ = writeln!(
            out,
            "# HELP kalpak_{name} {help}\n# TYPE kalpak_{name} {kind}\nkalpak_{name} {value}"
        );
    };

    m(
        "blocks",
        "gauge",
        "Content-addressed blocks on this node",
        cold.blocks,
    );
    m(
        "segments",
        "gauge",
        "Segment files on disk",
        cold.segments as u64,
    );
    m(
        "disk_bytes",
        "gauge",
        "Bytes on disk across segments",
        cold.bytes_on_disk,
    );
    m(
        "warm_blocks",
        "gauge",
        "Blocks resident in the warm tier",
        tier.warm_blocks,
    );
    m(
        "warm_bytes",
        "gauge",
        "Warm tier bytes in use",
        tier.warm_bytes,
    );
    m(
        "warm_budget_bytes",
        "gauge",
        "Warm tier byte budget",
        tier.warm_budget,
    );
    m(
        "pinned_blocks",
        "gauge",
        "Importance-pinned blocks (refcount >= 2)",
        tier.pinned_blocks,
    );
    m(
        "pinned_bytes",
        "gauge",
        "Importance-pinned bytes",
        tier.pinned_bytes,
    );
    m("warm_hits_total", "counter", "Warm tier hits", tier.hits);
    m(
        "warm_misses_total",
        "counter",
        "Warm tier misses (disk reads)",
        tier.misses,
    );
    m(
        "gc_runs_total",
        "counter",
        "Compaction runs",
        s.gc_runs.load(Relaxed),
    );
    m(
        "gc_blocks_dropped_total",
        "counter",
        "Blocks swept by GC",
        s.gc_blocks_dropped.load(Relaxed),
    );
    m(
        "gc_bytes_reclaimed_total",
        "counter",
        "Bytes reclaimed by GC",
        s.gc_bytes_reclaimed.load(Relaxed),
    );
    m("raft_term", "gauge", "Current Raft term", raft.current_term);
    m(
        "raft_last_log_index",
        "gauge",
        "Last Raft log index",
        raft.last_log_index.unwrap_or(0),
    );
    m(
        "raft_last_applied",
        "gauge",
        "Last applied Raft log index",
        raft.last_applied.map(|l| l.index).unwrap_or(0),
    );
    m(
        "raft_is_leader",
        "gauge",
        "1 when this node is the Raft leader",
        u64::from(raft.current_leader == Some(raft.id)),
    );
    m(
        "agents",
        "gauge",
        "Registered agent identities",
        s.control.agent_count() as u64,
    );
    m(
        "bindings",
        "gauge",
        "Prefix bindings in the state machine",
        s.control.binding_count() as u64,
    );

    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        out,
    )
}

#[derive(Deserialize)]
struct WsQuery {
    #[serde(default)]
    token: Option<String>,
}

async fn ws_stats(
    ws: WebSocketUpgrade,
    State(s): State<Shared>,
    headers: axum::http::HeaderMap,
    axum::extract::Query(q): axum::extract::Query<WsQuery>,
) -> Result<Response, ApiError> {
    check_read_token(&s, &headers, q.token.as_deref())?;
    Ok(ws.on_upgrade(move |socket| stream_stats(socket, s)))
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
