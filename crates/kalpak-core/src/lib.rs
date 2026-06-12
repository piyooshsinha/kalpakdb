//! Core types for Kalpak, the distributed agentic context fabric.
//!
//! Everything stored in Kalpak is immutable and content-addressed: a block's
//! identity *is* the BLAKE3 hash of its bytes. Agents are identified by
//! Ed25519 public keys, not network addresses, so state survives
//! infrastructure churn.

mod block;
mod cache_key;
mod error;
mod identity;
pub mod signing;

pub use block::BlockId;
pub use cache_key::{CacheKey, ModelFingerprint};
pub use error::Error;
pub use identity::AgentId;
