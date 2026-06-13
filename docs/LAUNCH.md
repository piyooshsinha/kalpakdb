# Launch announcement (draft)

Ready-to-post copy for when you launch. Three variants below tuned to
each venue's norms — Show HN wants restraint and a "why I built this,"
r/rust wants the engineering, r/LocalLLaMA wants the inference angle.
All three deliberately avoid performance numbers we haven't measured on
real hardware yet (see the "honest benchmarks" note at the bottom).

---

## Blog post / long form

### KalpakDB: a database for AI agent state

AI agents have a memory problem, and the industry is solving it with duct
tape. The typical stack: Redis for hot state, a vector database for
embeddings, a graph database for facts, object storage for blobs — and the
KV cache, the single most expensive piece of state in the whole system, has
no home at all. It gets recomputed on every call.

The bugs don't live in any one of those systems. They live in the **seams
between them**. No transaction spans a KV-cache invalidation, a vector
write, and a graph-edge update. No shared lineage answers "why does this
agent believe X?" No common identity ties an agent's memory together as it
moves across infrastructure. Four databases, four failure domains, and the
gaps between them are where agent memory silently corrupts.

KalpakDB is a bet that agent state is **one workload**, not four, and
deserves one substrate built for it.

#### What it is

A content-addressed, Raft-replicated database where:

- **Everything is an immutable block addressed by its BLAKE3 hash.**
  Deduplication, integrity, and cache coherence stop being features and
  become properties of the design. Identical context from two different
  agents is stored once. A corrupt read is caught at the read, because the
  address *is* the checksum.

- **KV caches are first-class.** Cached prefixes are keyed by `(model
  fingerprint, chained prefix hash)`, so equal prefixes converge across
  agents and incompatible models can never collide. Ask for a prefix and
  KalpakDB returns the longest cached one it holds — and speculatively warms
  the blocks you'll ask for next into RAM before you ask.

- **Agents are keypairs, not addresses.** State is owned by Ed25519
  identities. With signed writes enabled, every mutation is signed and
  attributable — identity that survives restarts, migrations, and
  infrastructure churn.

- **Metadata and data never mix.** Raft replicates only lightweight
  metadata (agents, prefix bindings); tensors move on a separate direct-I/O
  data plane and are referenced by content address. Consensus never ingests
  a tensor.

#### What's actually built

This isn't a design doc. It's six releases of working, CI-tested code:

- A storage engine with crash recovery, group commit, a concurrent
  RAM/SSD tier (moka), mark-and-sweep GC, and a Linux `io_uring` + `O_DIRECT`
  backend.
- A Raft control plane (durable log, dynamic membership, witness nodes for
  two-box quorum, leader forwarding) — with failure modes *tested*, not
  assumed: integration tests kill the leader and confirm re-election, crash
  and rejoin nodes from their durable logs, and `kill -9` the real process
  mid-write-storm to prove acknowledged writes survive.
- A complete security model: signed writes, client TLS, and mutually-
  authenticated mesh TLS — with the signing contract verified across **four
  independent SDK implementations** (Rust, Python, TypeScript, Go) that all
  agree on the exact wire bytes.
- Operations tooling: fsck, crash-consistent online backup/restore,
  Prometheus metrics with a Grafana dashboard.
- Two framework integrations: a vLLM KV connector and LangChain memory.

#### What's honest about it

KalpakDB runs today on commodity hardware, and the design is validated by
recent systems research (ObjectCache, LMCache, SpeCache, IMPRESS — the
README maps each to a design decision). But the headline KV-cache latency
numbers in those papers came from 100 Gbps datacenter networking. **We
haven't published our own benchmarks yet** because the honest place to
measure them is real hardware over a real network, and that run is the next
step, not a claim we're making today. When the numbers come, they'll come
with the exact model, disks, and network, reproducible from the runbook in
the repo.

#### Try it

```sh
cargo build --release
./target/release/kalpakdb serve /tmp/kalpak --addr 127.0.0.1:7411
```

Apache-2.0. SDKs for Rust, Python, TypeScript, and Go. The repo:
https://github.com/piyooshsinha/kalpakdb

---

## Show HN

**Title:** Show HN: KalpakDB – A database for AI agent state (Rust, Apache-2.0)

**Body:**

The industry handles AI agent memory by duct-taping databases together:
Redis for hot state, a vector DB for embeddings, a graph DB for facts, S3
for blobs — and the KV cache, the most expensive state of all, has no home
and gets recomputed every call. The bugs live in the seams: no atomicity
across those systems, no shared lineage, no common identity.

KalpakDB is one substrate for agent state. Everything is an immutable
content-addressed block (BLAKE3), so dedup and integrity are free. KV
caches are first-class — keyed by model fingerprint + chained prefix hash,
served back by longest-prefix match with speculative prefetch. Agents are
Ed25519 keypairs, not connection strings. Metadata goes through Raft;
tensors never do.

It's six releases of CI-tested Rust: storage engine (group commit, moka
tier, io_uring/O_DIRECT), Raft control plane (witness nodes, proven
failover and crash-recovery — there's a test that SIGKILLs the real process
mid-write and checks acknowledged writes survive), full security (signed
writes + TLS + mesh mTLS, verified across four SDK languages), and
operations tooling (fsck, online backup, Prometheus/Grafana). Plus a vLLM
connector and LangChain integration.

I haven't published performance numbers yet — on purpose. The papers this
draws from (ObjectCache, LMCache) hit their latency figures on 100Gbps
datacenter fabric; the honest place to benchmark is real hardware over a
real network, which is the next step. Happy to talk architecture in the
meantime.

https://github.com/piyooshsinha/kalpakdb

---

## r/rust

**Title:** KalpakDB – a content-addressed, Raft-replicated database for AI
agent state, in Rust

I've been building a distributed database for AI agent memory and just cut
the sixth release. Sharing for the Rust-systems angle.

The core is a content-addressed block store: append-only 4 KiB-aligned
segments, self-verifying records (the BLAKE3 hash is the address, so there's
no separate checksum to corrupt), crash recovery by forward scan with
torn-tail truncation. Group commit batches writes under one fsync. The warm
tier is `moka` (concurrent, lock-free reads); the lock architecture is split
so an in-flight fsync never blocks a reader. There's a Linux `io_uring` +
`O_DIRECT` backend behind a feature flag — built against a trait so the
macOS dev box and Linux NVMe nodes run the same engine.

Consensus is `openraft` carrying metadata only. The fun part was the
testing: real multi-node clusters over HTTP, kill-the-leader failover,
crash-and-rejoin from durable logs, a `kill -9`-the-real-binary durability
test, and property-based model testing of the storage engine against a
HashMap model.

Notable Rust bits: the canonical signed-write message format is a byte-
locked binary layout (`kalpak-core::signing`) that four SDKs (Rust, Python,
TS, Go) reproduce exactly — verified by cross-language tests. Two rustls
crypto providers in the dep tree (ring via reqwest, aws-lc-rs via
axum-server) meant an explicit `install_default()` — a fun one to debug via
a Docker smoke test.

Apache-2.0, 60+ tests, CI on Linux + macOS + Docker.
https://github.com/piyooshsinha/kalpakdb

---

## r/LocalLLaMA

**Title:** KalpakDB – an open-source KV-cache + memory database for
multi-agent setups (vLLM connector included)

If you run agents locally, you've felt this: long agent histories blow past
what GPU/DRAM can hold, and recomputing KV-cache prefixes on every call
burns TTFT. Vector DBs store embeddings, not the KV cache itself — so the
hottest, most expensive state has nowhere to live.

KalpakDB is a database built for exactly that. It stores KV-cache blocks
content-addressed and keyed by `(model fingerprint, chained prefix hash)`,
so:

- Identical system prompts / shared context across agents are stored **once**
  and reused (the fingerprint makes sure an incompatible model's cache never
  collides).
- You ask for a token prefix and get the **longest cached one** back, then
  prefill only the suffix.
- A lookup speculatively warms the blocks you're about to request — and the
  blocks your *next, deeper* request will need (binding-refcount-pinned
  shared prefixes survive cache floods).

There's a vLLM KV-connector (V1 interface) in the repo. It runs on the
two-box homelab topology it was designed for — a storage node, a compute
node, and a tiny witness for quorum — with a deployment runbook. SDKs in
Python, TypeScript, Go, and Rust.

Caveat I'll be upfront about: I haven't posted TTFT numbers yet. The right
way to measure is real inference on real hardware (the connector needs GPU
validation — it's an open issue), and I'd rather ship that with honest
numbers than hand-wave. If anyone wants to run it under vLLM and compare
against LMCache, that's issue #3 and I'll help.

Apache-2.0: https://github.com/piyooshsinha/kalpakdb

---

## A note on honest benchmarks (keep this discipline)

Every variant above refuses to claim a performance number. That's
deliberate and it's the project's credibility on the line:

- The research KalpakDB draws on (ObjectCache: 5.6% over local DRAM)
  measured on 100 Gbps RoCE. Our dev hardware is commodity GbE. Quoting
  their numbers as ours would be dishonest and would get caught on the first
  reproduction.
- The local `kalpakdb bench` figures (warm reads ~10M/s, group-commit writes
  ~500 MiB/s on the dev Mac) are **single-node storage-engine** numbers, not
  end-to-end TTFT — fine to mention as "the storage layer is fast" but not as
  "KalpakDB makes inference N× faster."
- The number that matters — TTFT with vs. without KalpakDB under vLLM on the
  real cluster — doesn't exist yet. When it does (issues #3, #5), it gets
  published with the full setup so anyone can reproduce it.

The launch is stronger for waiting: "here's a database, and here are
reproducible numbers" beats "here's a database, trust me."
