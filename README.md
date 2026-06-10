# Kalpak

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
- `kalpak-storage` — append-only, content-addressed local block store with crash recovery (self-verifying records, torn-write truncation, index rebuild on open) and a pluggable I/O backend (portable positioned I/O now; Linux `io_uring`/`O_DIRECT` planned behind the `uring` feature), plus the durable prefix manifest (`CacheKey → block list`, longest-prefix probing for cache hits)
- `kalpakdb` — minimal CLI: `put` / `get` / `stat` against a local store

Planned next: Raft control plane (`openraft`), the agent memory API (gRPC), speculative prefix prefetching, and the React observability dashboard. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Quick start

```sh
cargo test --workspace
cargo build --release

echo -n "the agent remembers" | ./target/release/kalpakdb put /tmp/kalpak-data
./target/release/kalpakdb get /tmp/kalpak-data <block-id>
./target/release/kalpakdb stat /tmp/kalpak-data
```

## License

Apache-2.0. See [LICENSE](LICENSE).
