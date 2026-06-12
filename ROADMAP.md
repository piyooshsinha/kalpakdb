# Roadmap

Status legend: ✅ shipped · 🔜 next · 🧭 needs data/hardware · 💤 deliberately not planned

## Near term 🔜
- Mesh mTLS: mutually-authenticated node-to-node transport (today: private network + ingress-verified signatures)
- GPU validation of the vLLM connector (`integrations/vllm`), then layer-wise streaming loads
- crates.io / PyPI publication

## Needs real-workload data 🧭
- Two-box benchmarks vs LMCache (docs/DEPLOYMENT.md is the protocol)
- Importance-aware warm-tier placement (IMPRESS-style) — validated against measured access patterns
- Learned lookahead predictor (CXL-SpecKV-style) — trained on real prefix traces
- gRPC chunks streamed directly into group-commit buffers (skip reassembly)

## Shipped ✅
Storage: content-addressed segments, crash recovery, group commit, moka warm tier,
mark-and-sweep GC, io_uring + O_DIRECT. Consensus: durable Raft log, dynamic
membership, witness nodes, leader forwarding, failover/rejoin/partition-proven.
Serving: HTTP/WS + gRPC streaming, speculative + lookahead prefetch, signed
writes, TLS, Prometheus metrics. SDKs: Rust, Python. Tooling: dashboard with
memory explorer + lineage, bench/stress/cert, Docker, vLLM connector (pre-GPU).

## Out of scope 💤
- Memory ontologies (episodic/semantic/procedural) — that's the framework layer
  above KalpakDB; we expose primitives (blocks, prefix chains, signed metadata)
- General-purpose SQL/queries — this is a substrate, not a query engine
