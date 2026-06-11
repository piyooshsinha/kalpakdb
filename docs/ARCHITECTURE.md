# Kalpak Architecture

## Thesis

Agent state is one workload that the industry serves with four databases, and
the bugs live in the gaps between them: no transaction spans a KV-cache
invalidation, a vector write, and a graph-edge update; no shared lineage
answers "why does the agent believe X?"; and the KV cache — the state that
actually determines TTFT and GPU cost — has no home in the traditional stack
at all. Kalpak makes the gaps disappear by being one substrate with one
consistency model and one identity system.

The wedge is the content-addressed KV-cache/state store (the part nobody else
has); the destination is the unified fabric. Higher-level memory ontologies
(episodic / semantic / procedural) belong to the frameworks above us — Kalpak
exposes primitives (immutable blocks, prefix chains, signed metadata) that
frameworks map their ontologies onto, rather than hardcoding one.

## Planes

### Data plane (`kalpak-storage`, Rust)

Append-only segment files of 4 KiB-aligned, self-verifying records:

```
header(64B): magic | version | payload_len | blake3(payload) | reserved
payload, zero-padded to 4 KiB
```

- The segments are the only source of truth. The block index is rebuilt by a
  forward scan on open; a torn tail-write is detected (header without full
  payload) and overwritten on the next append. There is no separate index
  file to corrupt.
- `put` is idempotent (content addressing = dedup); `get` re-verifies the
  payload hash on every read so corruption surfaces at the read site.
- All I/O goes through the `IoBackend` trait. Default backend: portable
  positioned reads/writes (runs on the macOS dev node). Planned: Linux
  `io_uring` + `O_DIRECT` backend behind the `uring` feature for NVMe nodes —
  record alignment is already direct-I/O compatible.
- Segments roll at 256 MiB; sealed segments are immutable, which is the unit
  for future tiering, replication, and compaction. Batched appends +
  log-structured layout keep write amplification (SSD wear) low.

### Control plane (`kalpak-control`, `openraft`)

Raft carries **metadata only** — agent identity records and the mapping from
cache keys to block ids. Raw tensors never enter the Raft log. The state
machine (agents + bindings) is implemented with snapshot-based compaction.
The Raft log and vote are durable (JSON-lines log + atomic meta file,
fsynced before the append callback, torn tails dropped on replay) and the
state machine recovers from the log on restart. Inter-node RPCs travel as
JSON over HTTP (`/raft/append|vote|snapshot`); membership is dynamic
(init -> add-learner -> promote). A lightweight witness process gives a
two-box cluster its third vote without split brain.

### Management plane (React dashboard + memory API)

`kalpakdb serve` exposes the memory API (axum): block put/get, agent
registration, prefix bind/lookup (longest-prefix probing of a key chain,
with speculative warm-tier prefetch of the hit's blocks), cluster
management (`/v1/cluster/*`), `/v1/stats`, and a 1 Hz `/v1/ws` stats
stream. `kalpak-client` is the Rust SDK. The React dashboard in
`dashboard/` is optional tooling rendering both planes live over the
WebSocket — the database core is engine + protocol + SDK.

## Key types (`kalpak-core`)

- `BlockId` — BLAKE3 content address of an immutable block.
- `ModelFingerprint` — `(model_id, tokenizer_hash, kv_layout)`. KV caches are
  not portable across models/tokenizers/quantizations; every cache entry is
  scoped to an exact fingerprint so incompatible blocks can never collide.
- `CacheKey` — `(fingerprint, prefix_hash)` where `prefix_hash` is a chained
  BLAKE3 over token-id chunks: equal prefixes converge (cross-agent reuse),
  diverging prefixes split, and extending a context never rehashes history.
- `AgentId` — Ed25519 public key. State mutations are signed and
  attributable; identity survives infrastructure churn.

## Phased roadmap

1. **Storage engine (now)** — local block store ✅, prefix-chain manifest
   (CacheKey → block list) ✅, two-tier store (RAM warm buffer / SSD cold
   store, write-through LRU) ✅, then: `io_uring` backend, cross-node
   tiering, importance-aware placement (IMPRESS-style).
2. **Consensus** — `openraft` state machine for agent metadata and cache-key
   bindings ✅, durable Raft log ✅, multi-node HTTP transport with dynamic
   membership ✅, leader forwarding (write to any node) ✅, leader-failover
   survival ✅, witness node (consensus-only voter giving a two-box
   deployment quorum) ✅; then: network-partition simulation, group commit
   for the fsync-bound write path.
3. **Memory API & speculative retrieval** — HTTP/WS endpoints ✅, Rust
   client SDK ✅, lookup-triggered warm-tier prefetch ✅, cross-node block
   fetch with replicate-on-read ✅, proactive block replication on put ✅,
   `kalpakdb bench` ✅; then: gRPC, model-based lookahead prediction
   (cf. SpeCache, CXL-SpecKV).
4. **Dashboard** — React + WebSocket live metrics ✅; then: per-agent
   lineage views.

## Design constraints learned from the literature

- ObjectCache (arXiv:2605.22850) validates object storage as a runtime KV
  backend but achieved its latency numbers on 100 Gbps RoCE; commodity-network
  deployments validate architecture and correctness, not those headlines. Be
  honest about this in benchmarks.
- LMCache is the system to benchmark against.
- `io_uring` is Linux-only; the I/O backend abstraction exists precisely so
  macOS dev nodes and Linux NVMe nodes run the same engine.
