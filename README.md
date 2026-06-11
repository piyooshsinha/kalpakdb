# KalpakDB

[![CI](https://github.com/piyooshsinha/kalpakdb/actions/workflows/ci.yml/badge.svg)](https://github.com/piyooshsinha/kalpakdb/actions/workflows/ci.yml)

**A content-addressed, Raft-replicated database for AI agent state — the storage layer agent frameworks build on.**

The industry currently duct-tapes traditional databases together to handle AI memory and state: Redis for hot state, a vector database for embeddings, a graph database for facts, object storage for blobs — and the KV cache, the hottest and most expensive state in the entire stack, has no home at all and gets recomputed on every call. The bugs live in the seams between these systems: no atomicity across them, no shared lineage, no common identity.

KalpakDB is one substrate for agent state, built so the seams disappear.

## Why "Kalpak"?

The name draws on Sanskrit roots that describe exactly what this database is for:

- **Kalpa (कल्प)** — an aeon in Hindu cosmology, a vast unit of cosmic time. Agent memory should endure across sessions, restarts, and infrastructure churn — state that persists on the scale of aeons, not requests.
- **Kalpaka (कल्पक)** — derived from *kalpa*, meaning "conforming to a settled rule or standard." A distributed database is precisely that: every node applying the same ordered log to reach the same state. Consensus is the settled rule.
- **Kalpavriksha (कल्पवृक्ष)** — the wish-fulfilling tree of Indian tradition, which grants what is asked of it. Ask Kalpak for a prefix, and it returns the longest cached one it holds — ideally before you finish asking (speculative prefetch).

## Core ideas

- **Everything is an immutable, content-addressed block.** A block's identity is the BLAKE3 hash of its bytes. Deduplication, integrity verification, and cache coherence fall out of the design instead of being features.
- **KV caches are first-class citizens.** Cached prefixes are keyed by `(model fingerprint, chained prefix hash)` so equal prefixes converge across agents and incompatible models can never collide. Serving prefix-selected KV blocks back to the inference engine cuts time-to-first-token for repetitive agentic workloads, and lookups speculatively warm the blocks into RAM before the client asks for them.
- **Agents are keypairs, not addresses.** State is owned by Ed25519 identities, so an agent's memory and trust relationships survive restarts, migrations, and infrastructure churn.
- **Metadata and data never mix.** Raft replicates only metadata (agents, prefix bindings) with a durable log; tensors and blobs move on a separate direct-I/O data plane and are referenced by content address.

## What works today

| Component | Status |
|---|---|
| `kalpak-core` | Block identity, chained prefix cache keys, Ed25519 agent identity |
| `kalpak-storage` | Append-only content-addressed segments, crash recovery, two-tier RAM/SSD store, prefix manifest, pluggable I/O backend |
| `kalpak-control` | Raft control plane (`openraft`): durable log, dynamic membership, snapshot compaction, JSON-over-HTTP transport |
| `kalpakdb` | Node binary: HTTP/WebSocket memory API, cluster management, speculative prefetch, local CLI tools |
| `kalpak-proto` | gRPC streaming data plane (chunked block streams, group-committed) |
| `kalpak-client` | Rust SDK for the full agent workflow |
| `dashboard/` | Optional React observability UI (live metrics over WebSocket) |

The three-node replication path is covered by an integration test that forms a real cluster over HTTP (init → learners → voters) and verifies state-machine convergence on every node.

## Quick start

### Single node

```sh
cargo build --release
./target/release/kalpakdb serve /tmp/kalpak-n1 --addr 127.0.0.1:7411
```

### Rust SDK

```rust
use kalpak_client::KalpakClient;
use kalpak_core::{CacheKey, ModelFingerprint};

let db = KalpakClient::new("http://127.0.0.1:7411");

// chain cache keys over the token stream, chunk by chunk
let fp = ModelFingerprint::new("meta-llama/Llama-3.1-8B", "tok-hash", "fp16/paged-16");
let k0 = CacheKey::root(fp, &[1, 2, 3]);
let k1 = k0.extend(&[4, 5]);

// reuse the longest cached prefix, prefill only the suffix
if let Some(hit) = db.lookup(&[k0.clone(), k1.clone()]).await? {
    for id in &hit.blocks {
        let kv_bytes = db.get_block(id).await?; // warm: prefetched at lookup
    }
}

// offload the newly computed KV chunk and bind the deeper key
let id = db.put_block(kv_bytes).await?;
db.bind_prefix(agent, k1, vec![id]).await?;
```

### Three-node cluster

```sh
./target/release/kalpakdb serve /data/n1 --addr 10.0.0.1:7411 --node-id 1
./target/release/kalpakdb serve /data/n2 --addr 10.0.0.2:7411 --node-id 2 --join
./target/release/kalpakdb serve /data/n3 --addr 10.0.0.3:7411 --node-id 3 --join

# or, for a two-box deployment: a consensus-only witness as the third vote
./target/release/kalpakdb witness /data/w --addr 10.0.0.3:7412 --node-id 3

# from node 1: grow the cluster
curl -X POST -H 'content-type: application/json' \
  -d '{"node_id":2,"addr":"10.0.0.2:7411"}' http://10.0.0.1:7411/v1/cluster/add-learner
curl -X POST -H 'content-type: application/json' \
  -d '{"node_id":3,"addr":"10.0.0.3:7411"}' http://10.0.0.1:7411/v1/cluster/add-learner
curl -X POST -H 'content-type: application/json' \
  -d '{"voters":[1,2,3]}' http://10.0.0.1:7411/v1/cluster/promote
```

### Docker

```sh
docker compose up -d        # two data nodes + a witness (see docker-compose.yml
                            # for the cluster-formation curl commands)
```

### Runnable example

```sh
cargo run -p kalpakdb -- serve /tmp/kalpak-demo &
cargo run -p kalpak-client --example agent_workflow   # run twice: miss, then hit
```

### Optional dashboard

KalpakDB is a database — the core is the engine, the wire protocol, and the client SDK. The React dashboard is optional tooling for humans:

```sh
cd dashboard && npm install && npm run dev   # proxies /v1 to 127.0.0.1:7411
```

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the thesis, the three planes, and the roadmap (multi-node data-plane replication, gRPC, `io_uring` backend, importance-aware tiering).

## License

Apache-2.0. See [LICENSE](LICENSE).
