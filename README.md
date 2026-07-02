# KalpakDB

[![CI](https://github.com/piyooshsinha/kalpakdb/actions/workflows/ci.yml/badge.svg)](https://github.com/piyooshsinha/kalpakdb/actions/workflows/ci.yml) [![Release](https://img.shields.io/github/v/release/piyooshsinha/kalpakdb)](https://github.com/piyooshsinha/kalpakdb/releases)

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
- **Agents are keypairs, not addresses.** State is owned by Ed25519 identities, so an agent's memory and trust relationships survive restarts, migrations, and infrastructure churn. With `--require-signatures`, every metadata mutation must be signed by the owning agent's key — attribution is enforced, not assumed.
- **Metadata and data never mix.** Raft replicates only metadata (agents, prefix bindings) with a durable log; tensors and blobs move on a separate direct-I/O data plane and are referenced by content address.

## What works today

| Component | Status |
|---|---|
| `kalpak-core` | Block identity, chained prefix cache keys, Ed25519 agent identity, canonical signed-write messages |
| `kalpak-storage` | Append-only content-addressed segments, crash recovery, group commit, two-tier RAM/SSD store (moka), mark-and-sweep GC, Linux `io_uring` + `O_DIRECT` backend (`--features uring`) |
| `kalpak-control` | Raft control plane (`openraft`): durable log, dynamic membership, witness nodes, leader forwarding, snapshot compaction, atomic chain binds |
| `kalpakdb` | Node binary: HTTP/WS memory API, gRPC data plane, cluster management, speculative + lookahead prefetch, scheduled GC, TLS, signed-write enforcement, Prometheus metrics, `/healthz` + `/readyz` probes, `bench`/`stress`/`cert` tools |
| `kalpak-proto` | gRPC streaming protocol (chunked block streams into one group commit) |
| `kalpak-client` | Rust SDK: full agent workflow, transparent signing, TLS root-CA support, optional gRPC streaming (`--features grpc`) |
| `clients/python` | Zero-dependency Python client (same workflow, server-side key hashing; optional Ed25519 signing via `cryptography`) |
| `clients/typescript` | Zero-dependency TypeScript/Node client (Node 18+ fetch; signed writes via built-in Ed25519) |
| `clients/go` | Zero-dependency Go client (stdlib net/http; signed writes via crypto/ed25519) |
| `dashboard/` | Optional React observability UI: live metrics, replication lag, agent memory explorer with lineage |
| `integrations/vllm` | vLLM KV connector (V1 interface): prefix reuse + offload through KalpakDB; protocol logic CI-tested, GPU validation pending |
| `integrations/langchain` | LangChain chat-message history: sessions as prefix chains, cross-session dedup, server-side resume |

Failure modes are tested, not assumed: integration tests form real three-node clusters over HTTP and verify state-machine convergence, kill the leader and confirm re-election, crash and rejoin a node from its durable log, run two data nodes on a witness's quorum, and exercise signed-write rejection and TLS handshakes. Crash safety in the storage engine is covered by torn-write and corruption tests.

## Quick start

### Single node

```sh
cargo build --release
./target/release/kalpakdb serve /tmp/kalpak-n1 --addr 127.0.0.1:7411

# tune the warm-tier budget and the max accepted block size (default 256 MiB,
# the ceiling for both HTTP and gRPC ingest):
./target/release/kalpakdb serve /tmp/kalpak-n1 --warm-mb 512 --max-block-mb 512
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

### Python

```python
from kalpakdb import KalpakClient, ModelFingerprint   # clients/python

db = KalpakClient("http://127.0.0.1:7411")
fp = ModelFingerprint("meta-llama/Llama-3.1-8B", "tok-hash", "fp16/paged-16")
k0 = db.cache_key(fp, [1, 2, 3])
hit = db.lookup([k0])
```

### TypeScript / Node

```ts
import { KalpakClient } from "kalpakdb";   // clients/typescript

const db = new KalpakClient("http://127.0.0.1:7411");
const fp = { model_id: "meta-llama/Llama-3.1-8B", tokenizer_hash: "tok-hash", kv_layout: "fp16/paged-16" };
const k0 = await db.cacheKey(fp, [1, 2, 3]);
const hit = await db.lookup([k0]);
```

### Go

```go
import kalpak "github.com/piyooshsinha/kalpakdb/clients/go"

db := kalpak.New("http://127.0.0.1:7411")
fp := kalpak.ModelFingerprint{ModelID: "meta-llama/Llama-3.1-8B", TokenizerHash: "tok-hash", KVLayout: "fp16/paged-16"}
k0, _ := db.CacheKey(fp, []uint32{1, 2, 3})
hit, _ := db.Lookup([]kalpak.CacheKey{k0})
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

### Built-in tools

```sh
kalpakdb bench /tmp --blocks 2000 --size-kb 64      # storage throughput (put/batch/warm/cold)
kalpakdb stress http://127.0.0.1:7411 --agents 8    # concurrent agent workload against a node
kalpakdb cert ./pki                                  # self-signed TLS certs
kalpakdb fsck /tmp/kalpak-data                       # offline integrity check (hash-verify every block)
kalpakdb backup http://127.0.0.1:7411 backup.tar     # crash-consistent online backup from a live node
kalpakdb restore backup.tar /data/restored           # unpack + fsck; then serve the restored dir
kalpakdb key <model> <tok> <layout> 1,2,3 4,5        # chained CacheKeys offline
```

## Signed writes

Run a node with `--require-signatures` and every register/bind must carry an Ed25519 signature over a canonical binary message, verified against the agent's public key before the mutation enters Raft. Both SDKs sign transparently:

```rust
let db = KalpakClient::with_signer("http://127.0.0.1:7411", signing_key);
db.register_agent(db.agent_id().unwrap(), "researcher").await?;   // signed
```

```python
db = KalpakClient("http://127.0.0.1:7411", signer=Ed25519Signer(private_key_bytes))
db.register_agent(db.signer.agent, "researcher")                   # signed
```

Unsigned or forged mutations get `401`; reads stay open. Replay of a captured signature only reproduces the identical (idempotent) mutation — confidentiality and capture-resistance on the wire are TLS's job:

## TLS

```sh
./target/release/kalpakdb cert ./pki                       # self-signed dev cert
./target/release/kalpakdb serve /data --addr 0.0.0.0:7411 \
    --tls-cert ./pki/kalpak-cert.pem --tls-key ./pki/kalpak-key.pem
```

Clients pass the CA: `KalpakClient::with_options(url, signer, Some(ca_pem))` in Rust, `KalpakClient(url, cafile="…")` in Python, `curl --cacert …`. There is no cleartext fallback on a TLS port.

Observability reads can demand a token: `--read-token <t>` guards `/v1/stats`, `/v1/ws`, and the agent explorer with `Authorization: Bearer <t>` (the dashboard picks it up from `?token=`); `/metrics` stays open for Prometheus scrapers and the data path is governed by signatures, not the read token.

Node-to-node traffic gets **mutual TLS**: `kalpakdb mesh-ca` generates a cluster CA and node certificates, and `--mesh <addr>,<ca>,<cert>,<key>` moves all Raft/replication RPCs onto a dedicated mTLS listener where presenting a CA-signed certificate *is* mesh membership — both sides authenticate, so neither a rogue client nor a rogue server can join the cluster path:

```sh
./target/release/kalpakdb mesh-ca ./pki --hosts 10.0.0.1,10.0.0.2
./target/release/kalpakdb serve /data/n1 --addr 10.0.0.1:7411 --node-id 1 \
    --mesh 10.0.0.1:7415,./pki/mesh-ca.pem,./pki/mesh-cert.pem,./pki/mesh-key.pem
```

## Monitoring

`GET /healthz` (liveness, always 200 while the process serves) and `GET /readyz` (readiness, 200 only once the node has a leader and can serve; 503 otherwise) are unauthenticated so orchestrator probes need no `--read-token`. The Docker image ships a `HEALTHCHECK` on `/readyz`; `docker-compose.yml` uses `/healthz` (its nodes await manual cluster formation before they are ready).

`GET /metrics` serves Prometheus text exposition (blocks, warm-tier hit/miss counters, importance-pinned tier, GC totals, Raft term/log/leader, agents, bindings) — point a scraper at any node and import [docs/grafana-dashboard.json](docs/grafana-dashboard.json) into Grafana. The same numbers feed `/v1/stats` (JSON) and the dashboard's WebSocket stream.

Placement is importance-aware (IMPRESS-style): blocks referenced by **multiple bindings** — shared prefixes, common system prompts — are pinned into a dedicated tier that a flood of one-off reads can never evict. The signal is structural (replicated binding refcounts), so it needs no workload tuning.

## Research foundations

KalpakDB's design decisions trace back to recent systems research. Papers that directly shaped this build:

| Paper | What it showed | Where it landed in KalpakDB |
|---|---|---|
| [ObjectCache: Layerwise Object-Storage Retrieval for KV Cache Reuse](https://arxiv.org/abs/2605.22850) | Object storage can serve as a runtime KV-cache backend for LLM inference; agent histories outgrow GPU/DRAM | The core thesis: a content-addressed block store serving prefix-selected KV blocks. Also our honesty rule — their latency numbers came from 100 Gbps RoCE, so commodity deployments validate architecture, not headlines |
| [LMCache: An Efficient KV Cache Layer for Enterprise-Scale LLM Inference](https://arxiv.org/abs/2510.09665) | End-to-end KV-cache offloading across hierarchical storage (local disk, remote CPU/disk) | The system KalpakDB benchmarks against; validated the tiered-storage shape |
| [SpeCache: Speculative Key-Value Caching](https://arxiv.org/abs/2503.16163) | Predict which KV pairs the next step needs and prefetch them, overlapping I/O with compute | Lookup-triggered speculative prefetch: a prefix hit warms its blocks into RAM during the client's round trip |
| [CXL-SpecKV: A Disaggregated FPGA Speculative KV-Cache](https://arxiv.org/abs/2512.11920) | A lightweight LSTM next-token predictor reaches a 94.7% prefetch hit rate | The roadmap's model-based lookahead predictor for cross-node block streaming |
| [Asynchronous KV Cache Prefetching](https://arxiv.org/abs/2504.06319) / [PRESERVE](https://arxiv.org/abs/2501.08192) | Hide memory-access latency behind computation/communication overlap | The computation-transfer overlap pattern behind the prefetcher and the gRPC streaming plane |
| [IMPRESS: Importance-Informed Multi-Tier Prefix KV Storage](https://www.usenix.org/conference/fast25/presentation/chen-weijian) (FAST '25) | Not all KV blocks deserve the fast tier — importance-aware placement beats plain recency | Why the warm tier uses TinyLFU (frequency-aware) rather than strict LRU; full importance-aware placement is on the roadmap |
| [Multi-Tier Dynamic Storage for KV Cache](https://link.springer.com/article/10.1007/s40747-025-02200-4) | KV-cache tiering under resource-constrained (edge) conditions | Validates the two-box dev topology: RAM warm buffer over SSD cold store on commodity hardware |
| [io_uring for High-Performance DBMSs](https://arxiv.org/abs/2512.04859) | Properly tuned io_uring lifts a storage engine from 16.5K to 546.5K TPS | The Linux `io_uring` backend (`--features uring`): batched group-commit submission + `O_DIRECT` page-cache bypass over the pre-aligned 4 KiB records |
| [MAGMA](https://arxiv.org/abs/2601.03236) / [MIRIX](https://arxiv.org/abs/2507.07957) / [A-Mem](https://openreview.net/forum?id=FiM0M8gcct) (agentic memory architectures) | The memory-framework layer is crowded; frameworks differ in ontology (episodic/semantic/procedural) but all need durable substrates | Why KalpakDB exposes primitives (blocks, prefix chains, signed metadata) instead of hardcoding one memory ontology — it aims to be the substrate *under* these frameworks |

## Architecture

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for the two-box reference deployment runbook (with `scripts/bench_cluster.py` for the network-path numbers), and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the thesis, the three planes, and what remains on the roadmap (importance-aware tier placement, model-based lookahead prediction, real-network benchmarks vs LMCache). [docs/BENCHMARKS.md](docs/BENCHMARKS.md) records what's measured (single-node storage and prefix-reuse latencies) and what awaits the hardware run; [CHANGELOG.md](CHANGELOG.md) tracks releases; [docs/LAUNCH.md](docs/LAUNCH.md) holds the announcement drafts for when those benchmarks land.

## License

Apache-2.0. See [LICENSE](LICENSE).
