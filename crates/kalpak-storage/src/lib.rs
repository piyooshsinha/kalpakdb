//! Kalpak's local storage engine.
//!
//! Blocks are immutable and content-addressed ([`kalpak_core::BlockId`]).
//! They are appended to segment files in 4 KiB-aligned records and located
//! through an in-memory index that is rebuilt by scanning segments on open —
//! the segments themselves are the source of truth, so there is no separate
//! index file to corrupt.
//!
//! I/O goes through the [`io::IoBackend`] trait. The default backend uses
//! portable positioned reads/writes; a Linux `io_uring` backend slots in
//! behind the `uring` feature without touching the engine.

pub mod io;
mod manifest;
mod segment;
mod store;
mod tiered;

pub use manifest::PrefixManifest;
pub use store::{BlockStore, CompactStats, StoreStats};
pub use tiered::{TierStats, TieredStore};

/// All records are aligned to this boundary so direct-I/O backends can read
/// them without bounce buffers.
pub const BLOCK_ALIGN: u64 = 4096;
