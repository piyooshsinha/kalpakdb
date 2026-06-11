//! Rust client for KalpakDB.
//!
//! Typical agent workflow:
//!
//! ```no_run
//! # async fn example() -> Result<(), kalpak_client::ClientError> {
//! use kalpak_client::KalpakClient;
//! use kalpak_core::{CacheKey, ModelFingerprint};
//!
//! let db = KalpakClient::new("http://127.0.0.1:7411");
//!
//! // Chain cache keys over the token stream, chunk by chunk.
//! let fp = ModelFingerprint::new("meta-llama/Llama-3.1-8B", "tok-hash", "fp16/paged-16");
//! let k0 = CacheKey::root(fp, &[1, 2, 3]);
//! let k1 = k0.extend(&[4, 5]);
//!
//! // Ask for the longest already-materialized prefix before prefilling.
//! if let Some(hit) = db.lookup(&[k0.clone(), k1.clone()]).await? {
//!     // reuse hit.blocks, prefill only the suffix
//! }
//!
//! // After prefill, offload the new KV chunk and bind the deeper key.
//! let agent = "07".repeat(32).parse().unwrap();
//! let id = db.put_block(b"...kv tensor bytes...".to_vec()).await?;
//! db.bind_prefix(agent, k1, vec![id]).await?;
//! # Ok(()) }
//! ```

use kalpak_core::{AgentId, BlockId, CacheKey};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("server: {status}: {message}")]
    Server { status: u16, message: String },
    #[error("decode: {0}")]
    Decode(String),
}

pub struct KalpakClient {
    base: String,
    http: reqwest::Client,
}

/// The longest cached prefix found for a key chain.
#[derive(Debug, Clone)]
pub struct PrefixHit {
    /// Index into the queried chain of the deepest bound key.
    pub depth: usize,
    /// Ordered blocks materializing that prefix.
    pub blocks: Vec<BlockId>,
}

impl KalpakClient {
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            base: base.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    async fn check(resp: reqwest::Response) -> Result<reqwest::Response, ClientError> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let message = resp
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|v| v["error"].as_str().map(String::from))
            .unwrap_or_else(|| status.to_string());
        Err(ClientError::Server {
            status: status.as_u16(),
            message,
        })
    }

    /// Store a block; returns its content address. Idempotent.
    pub async fn put_block(&self, payload: Vec<u8>) -> Result<BlockId, ClientError> {
        #[derive(Deserialize)]
        struct Resp {
            id: String,
        }
        let resp = self
            .http
            .post(format!("{}/v1/blocks", self.base))
            .body(payload)
            .send()
            .await?;
        let r: Resp = Self::check(resp).await?.json().await?;
        r.id.parse().map_err(|_| ClientError::Decode(r.id))
    }

    /// Store a batch of blocks under one group-committed fsync — the fast
    /// path for offloading a multi-chunk context.
    pub async fn put_blocks(&self, payloads: &[Vec<u8>]) -> Result<Vec<BlockId>, ClientError> {
        #[derive(Deserialize)]
        struct Resp {
            ids: Vec<String>,
        }
        let mut body = Vec::with_capacity(4 + payloads.iter().map(|p| 4 + p.len()).sum::<usize>());
        body.extend_from_slice(&(payloads.len() as u32).to_le_bytes());
        for p in payloads {
            body.extend_from_slice(&(p.len() as u32).to_le_bytes());
            body.extend_from_slice(p);
        }
        let resp = self
            .http
            .post(format!("{}/v1/blocks/batch", self.base))
            .body(body)
            .send()
            .await?;
        let r: Resp = Self::check(resp).await?.json().await?;
        r.ids
            .into_iter()
            .map(|id| id.parse().map_err(|_| ClientError::Decode(id.clone())))
            .collect()
    }

    /// Fetch a block by content address.
    pub async fn get_block(&self, id: &BlockId) -> Result<Vec<u8>, ClientError> {
        let resp = self
            .http
            .get(format!("{}/v1/blocks/{id}", self.base))
            .send()
            .await?;
        Ok(Self::check(resp).await?.bytes().await?.to_vec())
    }

    pub async fn register_agent(
        &self,
        agent: AgentId,
        display_name: &str,
    ) -> Result<(), ClientError> {
        let resp = self
            .http
            .post(format!("{}/v1/agents", self.base))
            .json(&json!({ "agent": agent, "display_name": display_name }))
            .send()
            .await?;
        Self::check(resp).await.map(|_| ())
    }

    /// Bind a prefix key to its ordered blocks. Blocks must be stored first.
    pub async fn bind_prefix(
        &self,
        agent: AgentId,
        key: CacheKey,
        blocks: Vec<BlockId>,
    ) -> Result<(), ClientError> {
        self.bind(agent, key, blocks, None).await
    }

    /// Bind a key that extends `parent`, linking it into the server-side
    /// prefix tree: a future lookup that hits `parent` will speculatively
    /// warm this key's blocks too (one-step lookahead).
    pub async fn bind_prefix_under(
        &self,
        agent: AgentId,
        parent: CacheKey,
        key: CacheKey,
        blocks: Vec<BlockId>,
    ) -> Result<(), ClientError> {
        self.bind(agent, key, blocks, Some(parent)).await
    }

    /// Bind a whole root-first chain atomically: one consensus round (and
    /// one Raft log fsync) instead of one per depth. Entries are
    /// `(key, blocks, parent)`.
    pub async fn bind_chain(
        &self,
        agent: AgentId,
        chain: Vec<(CacheKey, Vec<BlockId>, Option<CacheKey>)>,
    ) -> Result<(), ClientError> {
        let bindings: Vec<_> = chain
            .into_iter()
            .map(|(key, blocks, parent)| json!({ "key": key, "blocks": blocks, "parent": parent }))
            .collect();
        let resp = self
            .http
            .post(format!("{}/v1/manifest/bind-chain", self.base))
            .json(&json!({ "agent": agent, "bindings": bindings }))
            .send()
            .await?;
        Self::check(resp).await.map(|_| ())
    }

    async fn bind(
        &self,
        agent: AgentId,
        key: CacheKey,
        blocks: Vec<BlockId>,
        parent: Option<CacheKey>,
    ) -> Result<(), ClientError> {
        let resp = self
            .http
            .post(format!("{}/v1/manifest/bind", self.base))
            .json(&json!({ "agent": agent, "key": key, "blocks": blocks, "parent": parent }))
            .send()
            .await?;
        Self::check(resp).await.map(|_| ())
    }

    /// Probe a root-first key chain for the longest cached prefix. The
    /// server speculatively warms the returned blocks into RAM, so the
    /// subsequent `get_block` calls are warm-tier hits.
    pub async fn lookup(&self, chain: &[CacheKey]) -> Result<Option<PrefixHit>, ClientError> {
        #[derive(Deserialize)]
        struct Resp {
            hit_depth: Option<usize>,
            blocks: Vec<String>,
        }
        let resp = self
            .http
            .post(format!("{}/v1/manifest/lookup", self.base))
            .json(&json!({ "chain": chain }))
            .send()
            .await?;
        let r: Resp = Self::check(resp).await?.json().await?;
        match r.hit_depth {
            None => Ok(None),
            Some(depth) => {
                let blocks = r
                    .blocks
                    .into_iter()
                    .map(|b| b.parse().map_err(|_| ClientError::Decode(b.clone())))
                    .collect::<Result<_, _>>()?;
                Ok(Some(PrefixHit { depth, blocks }))
            }
        }
    }

    /// Node statistics (data plane + Raft control plane).
    pub async fn stats(&self) -> Result<serde_json::Value, ClientError> {
        let resp = self
            .http
            .get(format!("{}/v1/stats", self.base))
            .send()
            .await?;
        Ok(Self::check(resp).await?.json().await?)
    }
}

/// gRPC streaming client for the data plane (`--features grpc`).
///
/// Use this instead of the HTTP block endpoints when moving large KV
/// tensors: blocks stream in fixed-size chunks (so a tensor larger than any
/// single message never materializes as one frame) and the server commits
/// the whole stream under a single fsync. Metadata operations (register,
/// bind, lookup) stay on [`KalpakClient`] — the two-phase write: stream
/// blocks here first, then bind the returned ids through consensus.
#[cfg(feature = "grpc")]
pub mod grpc {
    use kalpak_core::BlockId;
    use kalpak_proto::v1::block_service_client::BlockServiceClient;
    use kalpak_proto::v1::{GetBlockRequest, PutBlockChunk};

    use crate::ClientError;

    /// Chunk size for outbound block streams.
    const CHUNK_BYTES: usize = 1024 * 1024;

    pub struct KalpakGrpcClient {
        inner: BlockServiceClient<tonic::transport::Channel>,
    }

    impl From<tonic::Status> for ClientError {
        fn from(s: tonic::Status) -> Self {
            ClientError::Server {
                status: s.code() as u16,
                message: s.message().to_string(),
            }
        }
    }

    impl KalpakGrpcClient {
        /// Connect to a node's gRPC data plane (`kalpakdb serve --grpc-addr`).
        pub async fn connect(endpoint: impl Into<String>) -> Result<Self, ClientError> {
            let inner = BlockServiceClient::connect(endpoint.into())
                .await
                .map_err(|e| ClientError::Decode(format!("grpc connect: {e}")))?;
            Ok(Self { inner })
        }

        /// Stream a batch of blocks; the server group-commits the whole
        /// stream under one fsync. Returns content ids in input order.
        pub async fn put_blocks(
            &mut self,
            payloads: &[Vec<u8>],
        ) -> Result<Vec<BlockId>, ClientError> {
            let mut chunks = Vec::new();
            for payload in payloads {
                if payload.is_empty() {
                    chunks.push(PutBlockChunk {
                        data: Default::default(),
                        last_chunk: true,
                    });
                    continue;
                }
                let mut pieces = payload.chunks(CHUNK_BYTES).peekable();
                while let Some(piece) = pieces.next() {
                    chunks.push(PutBlockChunk {
                        data: piece.to_vec().into(),
                        last_chunk: pieces.peek().is_none(),
                    });
                }
            }
            let resp = self
                .inner
                .put_blocks(tokio_stream::iter(chunks))
                .await?
                .into_inner();
            resp.ids
                .into_iter()
                .map(|id| id.parse().map_err(|_| ClientError::Decode(id.clone())))
                .collect()
        }

        /// Stream a block back, reassembling its chunks.
        pub async fn get_block(&mut self, id: &BlockId) -> Result<Vec<u8>, ClientError> {
            let mut stream = self
                .inner
                .get_block(GetBlockRequest { id: id.to_string() })
                .await?
                .into_inner();
            let mut out = Vec::new();
            while let Some(chunk) = stream.message().await? {
                out.extend_from_slice(&chunk.data);
                if chunk.last_chunk {
                    break;
                }
            }
            Ok(out)
        }
    }
}
