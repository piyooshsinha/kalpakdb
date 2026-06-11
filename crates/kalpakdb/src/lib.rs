//! Kalpak node library: the API server is exposed here so integration tests
//! (and embedders) can boot nodes in-process; `main.rs` is a thin CLI over it.

pub mod grpc;
pub mod server;
pub mod stress;
