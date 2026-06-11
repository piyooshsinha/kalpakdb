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
        let mut stream = request.into_inner();
        let mut blocks: Vec<Vec<u8>> = Vec::new();
        let mut current: Vec<u8> = Vec::new();

        while let Some(chunk) = stream.message().await? {
            current.extend_from_slice(&chunk.data);
            if chunk.last_chunk {
                blocks.push(std::mem::take(&mut current));
            }
        }
        if !current.is_empty() {
            return Err(Status::invalid_argument(
                "stream ended mid-block: missing last_chunk",
            ));
        }

        let state = self.state.clone();
        let ids = tokio::task::spawn_blocking(move || {
            state.store.put_many(blocks.iter().map(|b| b.as_slice()))
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(PutBlocksResponse {
            ids: ids.iter().map(|i| i.to_string()).collect(),
        }))
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
