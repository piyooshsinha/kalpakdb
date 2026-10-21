//! gRPC streaming data plane.
//!
//! Carries ONLY block bytes — metadata stays on the Raft-backed HTTP path.
//! `PutBlocks` accumulates a chunked client stream and commits every block
//! in bounded group-commit batches. Large streams may require multiple
//! flushes and are not atomic. `GetBlock` streams payloads out in
//! fixed-size chunks; the chunks are zero-copy slices of the warm-tier
//! buffer (`Bytes::slice` over one shared allocation).

use std::sync::Arc;

use bytes::Bytes;
use kalpak_core::BlockId;
use kalpak_proto::v1::block_service_server::{BlockService, BlockServiceServer};
use kalpak_proto::v1::{BlockChunk, GetBlockRequest, PutBlockChunk, PutBlocksResponse};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::server::AppState;

/// Outbound chunk size for GetBlock streams.
const CHUNK_BYTES: usize = 1024 * 1024;
const MAX_STREAM_BLOCKS: usize = 4096;
const MAX_BATCH_BLOCKS: usize = 256;
const MAX_STREAM_CHUNKS: usize = 65_536;
const CHUNK_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub struct BlockGrpc {
    state: Arc<AppState>,
}

pub fn service(state: Arc<AppState>) -> BlockServiceServer<BlockGrpc> {
    BlockServiceServer::new(BlockGrpc { state })
}

#[tonic::async_trait]
impl BlockService for BlockGrpc {
    async fn put_blocks(
        &self,
        request: Request<Streaming<PutBlockChunk>>,
    ) -> Result<Response<PutBlocksResponse>, Status> {
        /// Completed blocks are group-committed whenever this much payload
        /// has accumulated, so an arbitrarily long stream needs bounded
        /// memory. Deliberately NOT implemented by holding the append lock
        /// across the network stream — a slow client must never head-of-line
        /// block other writers.
        const FLUSH_BYTES: usize = 64 * 1024 * 1024;
        // A single block is bounded too: FLUSH_BYTES only caps *completed*
        // blocks, so without this a client that streams chunks forever
        // without ever setting last_chunk would grow one block unbounded and
        // exhaust memory. No legitimate KV block approaches the default.
        let max_block_bytes = self.state.max_block_bytes;
        let replicas = crate::server::parse_replicas(
            request
                .metadata()
                .get("x-kalpak-replicas")
                .map(|v| v.to_str()),
        )
        .map_err(Status::invalid_argument)?;
        let permit = self
            .state
            .uploads
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("shared upload budget exhausted"))?;

        let mut stream = request.into_inner();
        let mut blocks: Vec<Vec<u8>> = Vec::new();
        let mut staged_bytes = 0usize;
        let mut current: Vec<u8> = Vec::new();
        let mut ids: Vec<String> = Vec::new();
        let mut block_count = 0usize;
        let mut chunk_count = 0usize;
        let mut block_open = false;

        let flush = |blocks: Vec<Vec<u8>>,
                     state: Arc<AppState>,
                     permit: Arc<tokio::sync::OwnedSemaphorePermit>| async move {
            let disk_state = state.clone();
            let disk_permit = permit.clone();
            let (ids, blocks) = tokio::task::spawn_blocking(move || {
                // Cancellation cannot release admission while an already
                // submitted blocking disk operation is still running.
                let _permit = disk_permit;
                let guarded: Vec<_> = blocks.iter().map(|block| BlockId::of(block)).collect();
                disk_state
                    .retention
                    .protect(&guarded)
                    .map_err(kalpak_core::Error::from)?;
                if replicas > 1 {
                    let ids: Vec<_> = blocks.iter().map(|block| BlockId::of(block)).collect();
                    disk_state
                        .repairs
                        .record(&ids, replicas)
                        .map_err(kalpak_core::Error::from)?;
                }
                disk_state
                    .store
                    .put_many(blocks.iter().map(|b| b.as_slice()))
                    .and_then(|ids| {
                        if replicas > 1 {
                            for (id, block) in ids.iter().zip(&blocks) {
                                disk_state.store.repair(id, block)?;
                            }
                        }
                        Ok((ids, blocks))
                    })
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::internal(e.to_string()))?;
            if replicas > 1 {
                for (id, block) in ids.iter().zip(blocks) {
                    crate::server::acknowledge_replicas(
                        &state,
                        "/v1/blocks",
                        Bytes::from(block),
                        serde_json::json!({"id": id.to_string()}),
                        replicas,
                        permit.clone(),
                    )
                    .await
                    .map_err(Status::unavailable)?;
                }
            }
            Ok::<_, Status>(ids)
        };

        while let Some(chunk) = tokio::time::timeout(CHUNK_IDLE_TIMEOUT, stream.message())
            .await
            .map_err(|_| Status::deadline_exceeded("block upload idle timeout"))??
        {
            chunk_count += 1;
            if chunk_count > MAX_STREAM_CHUNKS {
                return Err(Status::resource_exhausted("too many chunks in upload"));
            }
            if block_count == MAX_STREAM_BLOCKS {
                return Err(Status::resource_exhausted("too many blocks in upload"));
            }
            if chunk.data.len() > max_block_bytes.saturating_sub(current.len()) {
                return Err(Status::resource_exhausted(format!(
                    "block exceeds {max_block_bytes} bytes"
                )));
            }
            current.extend_from_slice(&chunk.data);
            block_open = !chunk.last_chunk;
            if chunk.last_chunk {
                block_count += 1;
                staged_bytes += current.len();
                blocks.push(std::mem::take(&mut current));
                if staged_bytes >= FLUSH_BYTES || blocks.len() >= MAX_BATCH_BLOCKS {
                    let batch = std::mem::take(&mut blocks);
                    staged_bytes = 0;
                    for id in flush(batch, self.state.clone(), permit.clone()).await? {
                        ids.push(id.to_string());
                    }
                }
            }
        }
        if block_open {
            return Err(Status::invalid_argument(
                "stream ended mid-block: missing last_chunk",
            ));
        }
        if !blocks.is_empty() {
            for id in flush(blocks, self.state.clone(), permit.clone()).await? {
                ids.push(id.to_string());
            }
        }

        Ok(Response::new(PutBlocksResponse { ids }))
    }

    type GetBlockStream = ReceiverStream<Result<BlockChunk, Status>>;

    async fn get_block(
        &self,
        request: Request<GetBlockRequest>,
    ) -> Result<Response<Self::GetBlockStream>, Status> {
        let id: BlockId = request
            .into_inner()
            .id
            .parse()
            .map_err(|_| Status::invalid_argument("malformed block id"))?;

        let payload = self.state.store.get(&id).map_err(|e| match e {
            kalpak_core::Error::BlockNotFound(_) => Status::not_found(e.to_string()),
            other => Status::internal(other.to_string()),
        })?;

        // One allocation shared by every outbound chunk.
        let shared = Bytes::from(payload.as_ref().clone());
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let total = shared.len();
            let mut at = 0;
            loop {
                let end = (at + CHUNK_BYTES).min(total);
                let chunk = BlockChunk {
                    data: shared.slice(at..end),
                    last_chunk: end == total,
                };
                if tx.send(Ok(chunk)).await.is_err() {
                    return; // client went away
                }
                if end == total {
                    return;
                }
                at = end;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
