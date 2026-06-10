//! Network layer.
//!
//! The single-node deployment never sends an RPC (a sole voter has no
//! replication targets), so `LocalOnlyNetwork` reports every target as
//! unreachable. The multi-node gRPC transport replaces this factory without
//! touching storage or the state machine.

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;

use crate::types::{NodeId, TypeConfig};

#[derive(Default, Clone)]
pub struct LocalOnlyNetwork;

fn unreachable_err<E: std::error::Error>() -> RPCError<NodeId, BasicNode, E> {
    RPCError::Network(NetworkError::new(&std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "kalpak-control is running in local-only (single node) mode",
    )))
}

impl RaftNetworkFactory<TypeConfig> for LocalOnlyNetwork {
    type Network = LocalOnlyNetwork;

    async fn new_client(&mut self, _target: NodeId, _node: &BasicNode) -> Self::Network {
        LocalOnlyNetwork
    }
}

impl RaftNetwork<TypeConfig> for LocalOnlyNetwork {
    async fn append_entries(
        &mut self,
        _req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        Err(unreachable_err())
    }

    async fn install_snapshot(
        &mut self,
        _req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        Err(unreachable_err())
    }

    async fn vote(
        &mut self,
        _req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        Err(unreachable_err())
    }
}
