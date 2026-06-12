# Changelog

## 0.3.0 — 2026-06-12

The hardening release: the security and operations gaps between
"feature-complete" and "deployable" are closed.

### Signed writes
- `--require-signatures`: every metadata mutation (register, bind,
  chain-bind) must carry an Ed25519 signature over a canonical binary
  message, verified against the agent's public key before entering Raft
- Canonical messages live in `kalpak-core::signing`: raw fixed-width
  fields (never JSON), domain-separated per operation, byte layout
  locked by test and reproduced exactly by both SDKs
- Rust SDK signs via `KalpakClient::with_signer`; Python via the
  optional `Ed25519Signer` (needs `cryptography`; the SDK stays
  stdlib-only otherwise)
- Unsigned/forged/wrong-message mutations get 401; reads stay open;
  replay of a captured signature only reproduces an idempotent mutation

### TLS
- `kalpakdb serve|witness --tls-cert/--tls-key` (rustls): client-facing
  API over HTTPS with no cleartext fallback on a TLS port
- `kalpakdb cert <dir> [--hosts ...]`: self-signed dev certificates
- CA-aware SDKs: `with_options(url, signer, ca_pem)` / `cafile=...`
- Scope: client-facing API; node-to-node mesh stays on the private
  cluster network (mTLS is future work)

### Monitoring
- `GET /metrics`: Prometheus text exposition (data plane, warm tier,
  GC totals, Raft state, agents, bindings), zero new dependencies


## 0.2.0 — 2026-06-12

First feature-complete release: every phase of the original architecture
plan, built and verified.

### Storage engine (data plane)
- Content-addressed block store: append-only 4 KiB-aligned segments,
  self-verifying records, crash recovery (torn-write truncation, index
  rebuild on open), hash verification on every read
- Group commit: `put_many` batches under a single fsync (31x write
  throughput over per-block fsync on the dev bench)
- Two-tier store: moka-based concurrent warm tier (TinyLFU, size-aware,
  lock-free reads — ~10M warm reads/s), write-through, cold-hit promotion
- Lock split: an in-flight fsync never blocks readers
- Segment GC: mark-and-sweep over sealed segments, live set derived from
  replicated bindings, the active segment as the two-phase-write grace
  window; manual endpoint + scheduled loop (`--compact-secs`)
- Linux `io_uring` backend (`--features uring`): batched group-commit
  submission (N writes + drain-ordered fsync in one ring enter)

### Consensus (control plane)
- openraft state machine for agent identities and prefix bindings,
  metadata only — tensors never enter the Raft log
- Durable, replayed Raft log; bincode snapshots with log compaction
- Dynamic membership (init → add-learner → promote), JSON-over-HTTP
  transport, consensus-only witness nodes for two-box quorum
- Leader forwarding: write to any node
- Proven by integration tests: 3-node replication, leader failover,
  crash-restart-rejoin catch-up, witness quorum survival

### Serving & retrieval
- HTTP/WebSocket memory API + gRPC streaming data plane (chunked block
  streams, bounded ingest memory, zero-copy outbound chunks)
- Speculative prefetch on lookup + prefix-tree lookahead (bindings declare
  parents; a hit warms its children's blocks)
- Cross-node block fetch with replicate-on-read; proactive replication
  on put
- Rust SDK (`kalpak-client`) and zero-dependency Python client
  (`clients/python`) covering the full agent workflow

### Operations
- React dashboard: replication lag, agent memory explorer, GC gauges,
  warm-tier telemetry over a live WebSocket
- Docker image + compose topology (2 data nodes + witness)
- `kalpakdb bench`, `kalpakdb key`, CI on Linux/macOS + Docker smoke +
  uring + Python suites

## 0.1.0 — 2026-06-11

Initial scaffold: workspace, core types (BLAKE3 block identity, chained
prefix cache keys, Ed25519 agent identity), local block store, CLI.
