//! Raft network transport: JSON over HTTP.
//!
//! Each peer exposes `/raft/append`, `/raft/vote`, and `/raft/snapshot`
//! (wired up by the node's API server). Handlers return the remote node's
//! `Result<_, RaftError<..>>` verbatim as JSON, so Raft-level errors (e.g. a
//! higher vote) propagate as `RemoteError` while transport failures map to
//! `NetworkError` and trigger openraft's retry/backoff.

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::types::{NodeId, TypeConfig};

#[derive(Default, Clone)]
pub struct HttpNetworkFactory {
    client: reqwest::Client,
}

pub struct HttpNetwork {
    client: reqwest::Client,
    target: NodeId,
    base: String,
}

impl RaftNetworkFactory<TypeConfig> for HttpNetworkFactory {
    type Network = HttpNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        HttpNetwork {
            client: self.client.clone(),
            target,
            base: format!("http://{}", node.addr),
        }
    }
}

impl HttpNetwork {
    async fn rpc<Req, Resp, E>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp, RPCError<NodeId, BasicNode, E>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        E: std::error::Error + DeserializeOwned,
    {
        let url = format!("{}/raft/{}", self.base, path);
        let resp = self
            .client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let result: Result<Resp, E> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        result.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

impl RaftNetwork<TypeConfig> for HttpNetwork {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.rpc("append", &req).await
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.rpc("snapshot", &req).await
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.rpc("vote", &req).await
    }
}
