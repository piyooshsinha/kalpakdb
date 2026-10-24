# KalpakDB

[![CI](https://github.com/piyooshsinha/kalpakdb/actions/workflows/ci.yml/badge.svg)](https://github.com/piyooshsinha/kalpakdb/actions/workflows/ci.yml) [![Release](https://img.shields.io/github/v/release/piyooshsinha/kalpakdb)](https://github.com/piyooshsinha/kalpakdb/releases)

**A content-addressed, Raft-replicated database for AI agent state — the storage layer agent frameworks build on.**

> **Status: experimental, not production-ready.** Correctness and security
> stabilization is in progress. See [the stabilization checklist](docs/STABILIZATION.md)
> for implemented fixes, known risks, and release-blocking work. In particular,
> daemon compaction is blocked while coordinated GC safety remains unresolved
> (`--compact-secs` must be zero). Do not expose current administrative/Raft routes to
> untrusted networks.

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
| `kalpakdb` | Node binary: HTTP/WS memory API, gRPC data plane, cluster management, speculative + lookahead prefetch, durable upload retention (daemon GC blocked), TLS, signed-write enforcement, Prometheus metrics, `/healthz` + `/readyz` probes, `bench`/`stress`/`cert` tools |
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
docker build -t kalpakdb:local .
docker run -d --name kalpak kalpakdb:local
docker exec kalpak curl -fsS http://127.0.0.1:7411/readyz
```

The image defaults to container-local loopback. A published Docker port does
not expose this development listener. Remote/container-to-container HTTP
requires explicit mesh mTLS configuration; the legacy non-mesh Compose
example must not be used unchanged with the hardened listener policy.

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

## Offline physical scrub

Stop **all** processes using the data directory, then run:

```sh
kalpakdb scrub /data/kalpak --offline --mib-per-sec 8
```

This reads canonical segment files directly, including records not reachable
through the current index. Payload hashing uses a 64 KiB buffer. Read pacing
charges page-rounded coverage, including header probes; the configurable
range is 1–1024 MiB/s. Filesystem read-ahead, metadata I/O and checkpoint
writes are outside that application-level budget. Segment enumeration is
bounded to 65,536 files. This is an offline tool, not an online background
worker: `--offline` acknowledges that writers are stopped; it is **not** an
exclusive filesystem lock.

Findings and a final summary are JSON lines. Invalid magic/version, reserved
header bytes, overflowing/truncated lengths, short headers and payload hash
mismatches produce findings. After damage, the scanner examines subsequent
4 KiB-aligned candidate boundaries rather than trusting the damaged length.
These candidates are diagnostic, not a safe reconstruction of the index;
embedded record-like payload bytes and obsolete damaged records can appear
in physical findings. A nonzero exit means findings or an operational error.
No segment bytes are changed. Preserve damaged files and investigate before
starting writers; structural recovery is **not** implemented by this command.

Checksummed `scrub-progress.json` is atomically published and synced at
completed-record boundaries. Re-running resumes an interrupted sweep;
unfinished records are rehashed, so a restart can repeat one large record.
Previous findings remain in the cumulative summary but are not re-emitted.
An uncheckpointed finding can be emitted again after a crash.
Findings should therefore be saved separately when an audit trail is needed.
Changed file identities/sizes/modification times or damaged checkpoints fail
closed. Start a deliberately new sweep with `--restart` after investigating
changes; a completed checkpoint does not perform another sweep by default.
This tool does not scrub Raft metadata or automatically repair structural
damage.

### Opt-in online sealed-segment scrub

```sh
kalpakdb serve /data/kalpak --scrub-mib-per-sec 8
```

The default is off. The online worker continuously inspects **sealed segments
only**, with one header or at most 64 KiB of payload per blocking task. It
paces page-rounded reads at 1–1024 MiB/s, yields when foreground cold reads
or append/compaction activity is detected, and shares a verification slot
with peer probes. This is best-effort scheduling, not a device bandwidth or
foreground latency guarantee; read-ahead, metadata and durable checkpoint
writes are outside the read budget. Sustained foreground load can starve it.

Checksummed `online-scrub-progress.json` resumes at record boundaries.
Compaction changes the segment generation and restarts that segment's scan;
an old descriptor's results cannot advance the replacement's checkpoint.
The active append tail is excluded: use offline scrub for a full physical
sweep. `/v1/stats.online_scrub` reports running status, bytes read, verified
records, physical findings, generation resets, yields and errors. Counters
are process-local and count repeated sweeps, not unique blocks. The first
16 findings per process are logged; operational errors stop the worker and
increment its error counter. Findings can include obsolete records and are
diagnostic only: they do not quarantine logical blocks or trigger repair.
No Raft metadata scrub or automatic structural recovery is provided.

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

Unsigned or forged mutations get `401`; reads stay open. The current signing
format does **not** provide replay protection: an old signed bind can overwrite
a newer binding. TLS protects transport but does not solve stored-request replay
or ownership authorization. These remain stabilization blockers.

## TLS

```sh
./target/release/kalpakdb cert ./pki                       # self-signed dev cert
./target/release/kalpakdb serve /data --addr 127.0.0.1:7411 \
    --tls-cert ./pki/kalpak-cert.pem --tls-key ./pki/kalpak-key.pem
```

Clients pass the CA: `KalpakClient::with_options(url, signer, Some(ca_pem))` in Rust, `KalpakClient(url, cafile="…")` in Python, `curl --cacert …`. There is no cleartext fallback on a TLS port.

Without `--mesh`, HTTP must bind to a literal loopback address (`127.0.0.1`
or `[::1]`): the development Raft/admin API is not authenticated. This also
applies to witnesses and to server-only TLS. Remote HTTP bindings require
mesh mTLS; privileged routes are then available only on the mesh listener.
The optional gRPC listener is plaintext and must always bind to loopback,
even when HTTP TLS or mesh is enabled. These restrictions are intentional
breaking changes; legacy wildcard-bind/container examples need mesh setup.
Loopback is not protection against other users or processes on the host.

HTTP and gRPC uploads share a 512 MiB logical reservation pool. Each upload
reserves its configured maximum block size plus a 64 MiB batch allowance,
rounded up to MiB, with a minimum 128 MiB reservation. At the default block
size, one upload is admitted at a time; small-block configurations allow up
to four. `--max-block-mb` cannot exceed 448. HTTP overload returns 503 and
gRPC overload returns `RESOURCE_EXHAUSTED`; retry with backoff. Reservations
also cover pending best-effort replication, with a 30-second timeout per
peer. HTTP uploads have a 60-second handler deadline. This is not a hard
RSS cap: caches, reads, storage copies and allocator overhead are separate.

gRPC uploads have additional safety limits:
4,096 blocks and 65,536 chunks per request, and a 30-second inter-chunk idle
timeout. Completed blocks flush at 64 MiB or 256 blocks; the configured
per-block limit is checked before appending payload bytes. Split larger
uploads into separate requests. Uploads are **not atomic**: an error or
cancellation may leave earlier batches persisted. Content-addressed retries
deduplicate those blocks. Read streams are not yet admission-controlled.
HTTP binary batches are capped at 4,096 blocks and reject trailing bytes.

### Opt-in acknowledged replicas

Send `x-kalpak-replicas: N` on HTTP block/batch uploads or as gRPC
`PutBlocks` request metadata (`N` is 1–16, default 1). A successful request
with `N > 1` waits for the local write and matching content acknowledgements
from `N-1` distinct membership nodes. Witnesses do not count. HTTP failures
return 503; gRPC failures return `UNAVAILABLE`. Invalid policies return
400 / `INVALID_ARGUMENT`. Peer requests have five-second deadlines and each
acknowledgement operation has a 30-second deadline.

```sh
curl --fail-with-body -X POST http://127.0.0.1:7411/v1/blocks \
    -H 'x-kalpak-replicas: 2' --data-binary @tensor.bin
```

Write-time acknowledgements require a **Raft-committed replica requirement**
as well as durable peer copies. This is not yet a replica-location catalog.
Partial writes may remain after errors or cancellation; retry identical
content. Mixed-version peers lacking node-ID, storage-generation, cold-verification or retention
acknowledgements do not count.
gRPC replicates completed blocks individually over the HTTP peer transport;
it does not put tensors in Raft. Keep GC disabled. Use mesh mTLS for trusted
peer identities; local development HTTP cannot protect against malicious
local processes. Default HTTP replication remains best-effort, and default
gRPC writes remain local-only unless a replica count is requested.

For opt-in replicated uploads, requirements are persisted before local
storage/acknowledgement in `replica-requirements.json` using file sync,
atomic rename and directory sync. Before returning replicated success, the
desired copy count must also commit through Raft (five-second metadata
deadline); no tensor bytes enter the log. A serial leader worker resumes after restart,
visiting up to 16 records per five-second scan and re-uploading content to
revalidate copies through the current membership. It shares upload admission
and acknowledgement deadlines. The ledger holds at most 16,384 distinct
blocks; retries cannot lower a recorded target. A failed upload can leave
repair intent and partial data. An intent recorded before a failed local
write cannot be repaired until that source block is uploaded successfully.

Acknowledged peers persist retention pins before accepting a copy. All daemon
compaction is now refused, even when no replica pins exist: a local binding
snapshot is not a distributed GC watermark. There is no release API yet.
Online backups include published repair metadata and upload retention,
though concurrent backup consistency remains unproven.

### Upload retention and GC safety

Before HTTP single/batch, gRPC batch, repair and replicate-on-read payload
writes, the daemon syncs non-expiring block guards to `upload-retention.log`.
Only content hashes enter this journal; tensors still bypass Raft. A guard
is durable before the payload append or response. Retried hashes deduplicate;
cancelled/failed requests may leave conservative guards for absent data.
Bindings do not release guards. These are local retention intents, not yet
a distributed lease/commit protocol or a guarantee of maintained replication.

Checksummed, sequenced journal batches recover across process crashes. A
partial final record is truncated and synced before new appends; complete
corrupt records fail startup. This is process-crash recovery, not proof
against post-fsync media loss. A write/sync error poisons further guarded
writes until restart. Capacity is bounded to 1,048,576 distinct hashes and
128 MiB of journal data; new guards fail closed at capacity. Existing guards
never expire automatically. `/v1/stats.upload_retention` exposes counts,
capacity, write-failure status and the GC-blocked reason.

`POST /v1/admin/compact` returns 409; nonzero `--compact-secs` is rejected
before storage initialization. This protects both new uploads and legacy
unguarded blocks from unsafe daemon reclamation. The storage library's
low-level compaction primitive remains available for controlled tests, not
as an online GC workaround. Older/offline CLI writes do not create daemon
guards. Do not share a data directory between processes or delete retention
metadata to reclaim capacity. There is no safe reclamation workflow yet.

`GET /v1/reclamation/{block_id}` is a read-only, per-block retention diagnostic
(requires the configured observability bearer token). It reports binding
references, pending/committed/cancelled/expired lease counts, desired replica
count, local guard journal sequence and indexed payload presence. Metadata
fields share one local state-machine observation; storage and journal samples
are separate and may race. The report always returns `authoritative: false`,
`reclaimable: false` and `gc_enabled: false`. It does not verify payload bytes
or replica health. In-flight writes, legacy provenance, block reuse generations
and the cluster GC watermark remain unknown. Zero references or expired leases
are never permission to remove data or guards.

Each new guard batch adds a metadata fsync before the payload fsync; no
throughput improvement is claimed. Backups capture a stable journal after
segment traversal; the journal-copy lock can delay uploads. Timed
lease release, safe migration of legacy inventories and a Raft-coordinated
GC watermark are still required before reclamation can be enabled.

### Replicated upload reservations (opt-in)

The Rust SDK now supports `begin_upload_lease` followed by
`commit_upload_lease`. Reserve hashes **before** uploading bytes, then commit
the exact ordered hashes and one binding in a single Raft operation:

```rust,ignore
let db = KalpakClient::with_signer(base_url, signing_key);
let agent = db.agent_id().unwrap();
db.register_agent(agent, "my-agent").await?;
let block = BlockId::of(&payload);
let lease = BlockId(unique_random_32_bytes); // application-generated, never reused
db.begin_upload_lease(lease, vec![block]).await?;
db.put_block(payload).await?;
db.commit_upload_lease(lease, key, vec![block], None, 0).await?;
```

HTTP equivalents are `POST /v1/leases/begin` with
`{lease, agent, blocks, signature}` and `POST /v1/leases/commit` with
`{lease, agent, binding: {key, blocks, parent}, expected_revision, signature}`.
Both endpoints **always** verify Ed25519 signatures, even without
`--require-signatures` and on forwarded requests. Canonical messages are
defined by `lease_begin_message` and `lease_commit_message` in `kalpak-core`:
separate domains, length-prefixed fingerprint strings, ordered hashes,
complete parent keys and the expected revision. An agent must be registered.
Fingerprint fields are limited to 1,024 bytes each and parent fingerprints
must match. These signatures are not cluster-bound; cross-cluster replay and
the older signing APIs' broader replay limitations remain unresolved.

Reservation IDs are globally single-use within the cluster. An exact begin
retry acknowledges the existing reservation without reopening it, unless it
has been cancelled (409); a changed owner or block list conflicts. Commit atomically consumes the reservation
and updates the binding/refcounts. Exact commit retries acknowledge only:
they never reapply an older binding. Reusing a consumed ID for a different
commit, mismatched ownership/hashes or a stale revision returns 409.
Concurrent first commits at one expected revision cannot both succeed.

Use `lease_revision` from `GET /v1/agents/{agent}/bindings` for updates,
including its usual read-token protection. Each binding and revision are
captured under one state lock; the listing is still a local, potentially
stale read. All binding mutations (including legacy bind and chain-bind)
advance revisions after the first valid reservation activates the protocol.
That common Raft command initializes fences at zero on every node, regardless
of older snapshot positions. Read the revision **after reserving**. Zero
means an absent key or an imported key not mutated since activation.
A stale read is rejected at the commit's Raft CAS.
Legacy writes themselves still do not enforce CAS or namespace ownership;
this is not a completed multi-tenant authorization protocol.

The current commit endpoint requires blocks locally on both the ingress
data node and committing data leader; it checks index presence, not fresh
replica durability. Prefer a data leader with the uploaded bytes; a witness
leader cannot serve this endpoint. Reservations replicate only metadata and
do not prove that an upload completed or the requested replica count exists.
Bytes continue through the existing HTTP/gRPC data plane and local guards.

Limits are 4,096 reservations total, 256 ordered block entries per reservation
and 65,536 reserved block entries across the cluster. Consumed and cancelled
records, including expired reservations, remain as replay tombstones and still count toward both limits.
There is no guard release, tombstone-pruning or lease-capacity recovery API yet.
`/v1/stats.replicated_leases` reports local pending/committed/cancelled/expired counts. Leases
survive log replay, snapshots and leader changes. No timer releases local
guards; **all daemon GC remains blocked** pending a safe reclamation protocol.

#### Cancelling a reservation

After `begin_upload_lease` has succeeded, the owning signed Rust client can
call `cancel_upload_lease(lease)`. The HTTP equivalent is
`POST /v1/leases/cancel` with `{lease, agent, signature}`. The signature covers
the owner and exact lease ID under the separate `KLPK/lease/cancel/v1\0`
domain; it is always verified, including on forwarded/internal requests.
No payload needs to be present, so a failed or abandoned upload can be closed.

Cancellation is a Raft state transition, not a local timer. Before a timed
deadline has elapsed, the first applied commit or cancel wins. Cancelling a pending reservation returns 200 with
`cancelled: true` and `guards_released: false`; exact owner retries return the
same acknowledgement without changing the original cancellation position.
Unknown IDs, other owners and committed reservations return 409. A cancelled
ID can neither be reopened by a begin retry nor committed by a delayed signed
request. Existing committed bindings and their revision/refcounts are never
removed by cancellation. Wrong or cross-operation signatures return 401.

Closing a reservation does **not** cancel an in-flight byte transfer, reject
ordinary later uploads of the same hashes, release local retention, reduce
replica requirements, reclaim lease capacity or authorize deleting data.
Tombstones survive snapshots, restart and leader changes. Optional expiry is
described below; guard release and a coordinated GC watermark remain separate
release-blocking work. Do not delete tombstones or local
retention files to simulate cancellation or reclaim capacity.

#### Opt-in logical deadlines and renewal

Reservations begin without a deadline. A signed Rust client can call
`renew_upload_lease(lease, expected_generation, ttl_ticks)`, starting with
generation zero. The reply contains `generation` and `deadline_tick`; use the
returned generation for the next renewal. HTTP uses `POST /v1/leases/renew`
with `{lease, agent, expected_generation, ttl_ticks, signature}`. The
`KLPK/lease/renew/v1\0` message signs both integers as little-endian u64 after
the owner and lease ID. All requests require the owner's signature.

TTL is 1–86,400 **logical ticks**, not Unix time or guaranteed seconds. A data
leader proposes at most one clock advance per scheduled one-second interval
while future timed reservations exist, with missed intervals skipped. Every
advance commits through Raft with an expected-tick fence: stale duplicate
proposals do not advance time twice. Followers never expire using local wall
clocks. Ticks can pause during downtime, quorum loss, slow I/O or witness
leadership; leader changes can change tick phase. TTL is not a wall-time SLA,
and expiry does not catch up to elapsed downtime. No assumption about clock
skew is used in state-machine decisions. Embedded control-plane users must
drive the trusted coordinator methods themselves.

Renewals must extend the deadline and match the current generation. An exact
retry acknowledges the original generation/deadline without extending it
again; stale/conflicting generations return 409. At the replicated deadline,
new commits, renewals, cancellations and begin retries reject—even before the
worker records an expiry marker. Old committed/cancelled reservations retain
their terminal status when their former deadline passes. Expiry proposals
also carry the exact generation and deadline, so an earlier proposal cannot
close a successfully renewed lease. A renewal ordered after the deadline
cannot resurrect it. Clock and expiry commands have no public HTTP routes.

The leader finalizes up to 16 due markers per interval. Stats count a due
reservation as expired immediately from replicated time; `expiry_markers`
counts finalized markers and may lag. `current_tick` exposes the logical
clock. State and tombstones survive snapshots and restart. There is no expiry
of legacy untimed reservations unless their owner explicitly opts in.
`begin` replies acknowledge only the reservation and no longer promise a
fixed `expires: false` value after renewal support was introduced.

Expiry does **not** release local guards, bytes, replica requirements or lease
capacity. Ordinary byte uploads remain possible; only the closed lease ID is
fenced. All daemon GC remains blocked until guard release, legacy-inventory
migration and the distributed reclamation watermark are implemented.

Committed requirements survive origin loss and leader changes. The leader
can fetch a missing local source from retained peers, verify its content
hash and persist it before repairing other copies. Uncommitted local
intents from failed/older uploads are promoted when a leader is reachable;
only these uncommitted intents still depend on the source ledger surviving.
Each ledger and the replicated requirement map are capped at 16,384 blocks.
The local ledger also persists bounded block/node/storage-generation
observations (at most 16 locations per block). Each data directory has a
durable `storage-generation` ID; a fresh directory gets a new ID. A new peer
generation invalidates its old observations across blocks. Cached locations
never count alone: fresh, retained-presence probes are required before
skipping an upload. Missing or unretained copies are uploaded; a partly
missing HTTP batch may still resend its complete batch payload.

Peer probes use `GET /v1/admin/blocks/{id}/presence`, restricted to the mesh
listener in mTLS deployments. Probes now read cold payloads and verify their
content hashes, bypassing warm and pinned caches. One peer verification per
node runs at a time on the blocking pool; probes wait up to 100 ms for the
shared verification slot before returning 503. Repair scans remain bounded
by the existing batch/admission limits; the optional sealed-segment worker
above does not rate-limit repair probes. Verification costs disk reads even
when it avoids network tensor transfers.
Observations are local hints, not a Raft-replicated placement catalog or
cryptographic storage proofs. A cold verification confirms bytes at that
instant, not future survival. `/v1/stats` exposes acknowledged replication
upload attempts so redundant tensor transfers can be checked. Backups
include the storage-generation ID and observations.

Detected payload corruption is quarantined from normal cache reads and stops
counting as a healthy copy. Repair verifies incoming bytes, bypasses normal
deduplication for damaged records, appends and syncs a replacement, then
publishes its new location. The damaged record is not overwritten in place.
The repair worker can retrieve a healthy copy from another peer. Runtime
quarantine flags are not persisted; cold verification redetects damage after
restart. Structural/header corruption and general I/O failures still need
broader recovery handling; if no healthy source survives, repair cannot
invent the missing data. A failed append blocks further writes until restart,
preventing later writes from being acknowledged beyond an uncertain tail.
This is not yet an always-maintained replica-count guarantee.

Snapshot payloads now carry a `KLPKSM05` version prefix, including replicated
logical time, deadlines, expiry/cancellation tombstones and binding revisions.
The new binary reads `KLPKSM04`, `KLPKSM03`, `KLPKSM02` and
legacy snapshots, but old binaries cannot read new commands/snapshots:
upgrade the entire experimental cluster together, with verified backups;
mixed-version rolling upgrades and downgrades are unsupported.

The `replica_crash` integration test exercises acknowledged HTTP single,
HTTP batch and gRPC writes with two real node processes. It kills the
receiver, verifies the peer without read fallback, kills both processes,
checks both stores offline, then restarts each node alone and checks every
payload again. It also checks that failed replication resumes after an
origin crash and restores the returning peer without client resubmission.
Isolated nodes must reject new two-replica writes. This is
process-crash evidence, not a power-loss simulation or a maintained-replica
guarantee.

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
