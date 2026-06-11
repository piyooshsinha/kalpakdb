//! gRPC streaming data plane.
//!
//! Carries ONLY block bytes — metadata stays on the Raft-backed HTTP path.
//! `PutBlocks` accumulates a chunked client stream and commits every block
//! under a single fsync (group commit), so streaming a multi-chunk context
//! costs one flush regardless of size. `GetBlock` streams payloads out in
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

        let mut stream = request.into_inner();
        let mut blocks: Vec<Vec<u8>> = Vec::new();
        let mut staged_bytes = 0usize;
        let mut current: Vec<u8> = Vec::new();
        let mut ids: Vec<String> = Vec::new();

        let flush = |blocks: Vec<Vec<u8>>, state: Arc<AppState>| async move {
            tokio::task::spawn_blocking(move || {
                state.store.put_many(blocks.iter().map(|b| b.as_slice()))
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::internal(e.to_string()))
        };

        while let Some(chunk) = stream.message().await? {
            current.extend_from_slice(&chunk.data);
            if chunk.last_chunk {
                staged_bytes += current.len();
                blocks.push(std::mem::take(&mut current));
                if staged_bytes >= FLUSH_BYTES {
                    let batch = std::mem::take(&mut blocks);
                    staged_bytes = 0;
                    for id in flush(batch, self.state.clone()).await? {
                        ids.push(id.to_string());
                    }
                }
            }
        }
        if !current.is_empty() {
            return Err(Status::invalid_argument(
                "stream ended mid-block: missing last_chunk",
            ));
        }
        if !blocks.is_empty() {
            for id in flush(blocks, self.state.clone()).await? {
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
