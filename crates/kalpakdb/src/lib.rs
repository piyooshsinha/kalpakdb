//! Kalpak node library: the API server is exposed here so integration tests
//! (and embedders) can boot nodes in-process; `main.rs` is a thin CLI over it.

mod admission;
pub mod grpc;
mod online_scrub;
pub mod pki;
mod repair;
pub mod server;
pub mod stress;
