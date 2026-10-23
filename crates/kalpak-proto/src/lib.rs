//! Generated gRPC types for KalpakDB's streaming data plane.

// Tonic's async_trait expansion adds must_use to an already-must-use boxed
// Future. Rust 1.99 flags generated code; keep handwritten code lint-strict.
#[allow(clippy::double_must_use)]
pub mod v1 {
    tonic::include_proto!("kalpak.v1");
}
