# Kalpak

[![CI](https://github.com/piyooshsinha/kalpakdb/actions/workflows/ci.yml/badge.svg)](https://github.com/piyooshsinha/kalpakdb/actions/workflows/ci.yml)

**A content-addressed, replicated state substrate for AI agents — the storage layer agent frameworks build on.**

The industry currently duct-tapes traditional databases together to handle AI memory and state: Redis for hot state, a vector database for embeddings, a graph database for facts, object storage for blobs — and the KV cache, the hottest and most expensive state in the entire stack, has no home at all and gets recomputed on every call. The bugs live in the seams between these systems: no atomicity across them, no shared lineage, no common identity.

Kalpak is one substrate for agent state, built so the seams disappear.

## Core ideas

- **Everything is an immutable, content-addressed block.** A block's identity is the BLAKE3 hash of its bytes. Deduplication, integrity verification, and cache coherence fall out of the design instead of being features.
- **KV caches are first-class citizens.** Cached prefixes are keyed by `(model fingerprint, chained prefix hash)` so equal prefixes converge across agents and incompatible models can never collide. Serving prefix-selected KV blocks to the inference engine cuts time-to-first-token for repetitive agentic workloads.
- **Agents are keypairs, not addresses.** State is owned by Ed25519 identities, so an agent's memory and trust relationships survive restarts, migrations, and infrastructure churn.
- **Metadata and data never mix.** Consensus (Raft) carries only metadata; tensors and blobs move on a separate direct-I/O data plane.

## Status

Early development. Working today:

- `kalpak-core` — block identity, chained prefix cache keys, Ed25519 agent identity
- `kalpak-storage` — append-only, content-addressed local block store with crash recovery (self-verifying records, torn-write truncation, index rebuild on open) and a pluggable I/O backend (portable positioned I/O now; Linux `io_uring`/`O_DIRECT` planned behind the `uring` feature), plus the durable prefix manifest (`CacheKey → block list`, longest-prefix probing for cache hits) and a two-tier store (byte-budgeted LRU warm buffer in RAM over the durable cold store, write-through, hit/miss accounting)
- `kalpak-control` — the Raft control plane (`openraft`): a replicated metadata state machine for agent registrations and prefix bindings, with snapshot-based log compaction. Strictly metadata — tensors never enter the Raft log
- `kalpakdb` — the node binary: an HTTP + WebSocket memory API (`serve`) over both planes, plus local CLI tools (`put` / `get` / `stat` / `key`)
- `dashboard/` — React control dashboard: live data-plane and Raft metrics over WebSocket

Planned next: multi-node Raft transport, speculative prefix prefetching, the Linux `io_uring` backend. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Quick start

```sh
cargo test --workspace
cargo build --release

# run a node
./target/release/kalpakdb serve /tmp/kalpak-data --addr 127.0.0.1:7411 --warm-mb 256

# store a KV block
curl -X POST --data-binary @block.bin http://127.0.0.1:7411/v1/blocks
# register an agent (Ed25519 public key, hex)
curl -X POST -H 'content-type: application/json' \
  -d '{"agent":"<pubkey-hex>","display_name":"researcher"}' http://127.0.0.1:7411/v1/agents
# compute chained prefix keys for a token stream, chunked as you like
./target/release/kalpakdb key meta-llama/Llama-3.1-8B tok-hash fp16/paged-16 1,2,3 4,5
# bind a prefix to its blocks, then probe a chain for the longest cached prefix
curl -X POST -H 'content-type: application/json' -d '{"agent":"…","key":…,"blocks":["…"]}' \
  http://127.0.0.1:7411/v1/manifest/bind
curl -X POST -H 'content-type: application/json' -d '{"chain":[…,…]}' \
  http://127.0.0.1:7411/v1/manifest/lookup

# dashboard (proxies /v1 to the node)
cd dashboard && npm install && npm run dev
```

## License

Apache-2.0. See [LICENSE](LICENSE).
