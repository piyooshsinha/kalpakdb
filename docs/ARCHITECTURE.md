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

### Control plane (planned: `openraft`)

Raft carries **metadata only** — cluster topology, agent identity records,
and the mapping from cache keys / block ids to physical locations. Raw
tensors never enter the Raft log. A lightweight witness process provides the
third vote so a two-box cluster keeps strict quorum without split brain.

### Management plane (planned: React dashboard)

Real-time observability over WebSocket: Raft commits, data-plane throughput,
per-agent lineage. Lives in `dashboard/`.

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
   (CacheKey → block list) ✅, then: `io_uring` backend, tier abstraction
   (RAM warm buffer / SSD cold store), importance-aware placement
   (IMPRESS-style).
2. **Consensus** — embed `openraft`; custom state machine for agent metadata
   and cache-key indices; partition/failure simulation locally.
3. **Memory API & speculative retrieval** — gRPC/REST endpoints for
   offload/retrieve; background prefetcher streaming predicted KV blocks from
   the storage node into the compute node's RAM, overlapping network I/O with
   inference (cf. SpeCache, CXL-SpecKV's lookahead predictor).
4. **Dashboard** — React + WebSocket streams.

## Design constraints learned from the literature

- ObjectCache (arXiv:2605.22850) validates object storage as a runtime KV
  backend but achieved its latency numbers on 100 Gbps RoCE; commodity-network
  deployments validate architecture and correctness, not those headlines. Be
  honest about this in benchmarks.
- LMCache is the system to benchmark against.
- `io_uring` is Linux-only; the I/O backend abstraction exists precisely so
  macOS dev nodes and Linux NVMe nodes run the same engine.
