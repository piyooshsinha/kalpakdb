# Benchmarks

What KalpakDB's performance is, measured — and, just as importantly, what
has *not* been measured yet and why. The rule here: every number says where
it came from and what it does not claim.

## What these numbers are (and are not)

- **Single-node, KalpakDB-side latencies.** They measure what the database
  itself costs on the local machine. They are **not** end-to-end
  time-to-first-token under a real model, and they are not a speedup
  multiplier over recomputing a KV cache.
- The cross-system comparison that *would* be a TTFT claim — KalpakDB vs
  LMCache under vLLM on real hardware over a real network — is the
  hardware run, tracked in [#3](https://github.com/piyooshsinha/kalpakdb/issues/3)
  and [#5](https://github.com/piyooshsinha/kalpakdb/issues/5). The template
  table for it is at the bottom, empty, waiting for those numbers.
- Why the discipline: the research KalpakDB builds on (ObjectCache: 5.6%
  over local DRAM) measured on 100 Gbps RoCE. Quoting datacenter-fabric
  figures as commodity-hardware results would get caught on the first
  reproduction. Better to ship a database with reproducible numbers than a
  database with borrowed ones.

## Dev machine

The numbers below are from the development machine, not a deployment target:

| | |
|---|---|
| CPU | Apple M5 |
| OS | macOS 26.5.1 |
| Storage | built-in SSD (APFS) |
| Backend | portable positioned I/O (the `io_uring`/`O_DIRECT` backend is Linux-only) |

A two-box deployment (Mac Mini storage node + Intel NUC compute node + witness)
over real Ethernet will produce different — and more meaningful — numbers;
that is what the runbook in [DEPLOYMENT.md](DEPLOYMENT.md) is for.

## Storage engine

```sh
cargo build --release -p kalpakdb
./target/release/kalpakdb bench /tmp --blocks 2000 --size-kb 64
```

| Path | Throughput | Notes |
|---|---|---|
| put (fsync per block) | ~255 blk/s, ~16 MiB/s | the naive path |
| put batch (group commit, 64/batch) | ~6,800 blk/s, ~425 MiB/s | one fsync amortized over 64 blocks — the real write path |
| get warm | ~6.7M blk/s | served from the moka RAM tier |
| get cold (reopen, disk + hash verify) | ~23K blk/s, ~1.4 GiB/s | disk read + BLAKE3 verification |

Takeaway: group commit is the write story (≈27× the per-block-fsync path),
and the warm tier makes reads effectively free. The cold path is bounded by
disk plus the hash verification every read performs.

## Prefix reuse (the agentic path)

```sh
KALPAKDB_BIN=./target/release/kalpakdb \
  BENCH_ITERS=500 BENCH_DEPTH=10 \
  python3 scripts/bench_prefix_reuse.py
```

This measures the two paths that decide KalpakDB's contribution to TTFT:
a **miss** (the database stores new KV blocks: put + bind) and a **hit**
(the database serves a previously-seen prefix: lookup + warm fetch). The
deep-chain rows model the real workload — a multi-turn agent reusing a prior
session's context via longest-prefix match.

| Scenario | p50 | p95 | p99 |
|---|---|---|---|
| single-block miss (put + bind) | ~4.4 ms | ~5.2 ms | ~5.3 ms |
| single-block hit (lookup + warm fetch) | ~0.5 ms | ~0.7 ms | ~0.8 ms |
| 10-level chain build (put + bind ×10) | ~63 ms | ~69 ms | ~70 ms |
| 10-level chain **reuse** (lookup + warm fetch ×10) | ~1.0 ms | ~1.5 ms | ~1.6 ms |

The bottom row is the point: reusing a 10-turn cached conversation prefix
costs ~1 ms on the KalpakDB side, versus ~63 ms to materialize the same
chain fresh.

**What this does not claim:** the ~63 ms "build fresh" is KalpakDB writing
blocks, not a model prefilling tokens. In a real deployment the cost the
reuse path *replaces* is a GPU prefill of those prefix tokens — typically far
more than 63 ms, and model-dependent. So the honest statement is "reusing a
deep cached prefix is ~1 ms on the KalpakDB side"; the end-to-end TTFT
speedup is the hardware run's to measure, not this harness's to assert.

## Pending: the hardware run (issues #3, #5)

The table the launch waits on. Same prefix-reuse workload, run under vLLM,
comparing the LMCache connector against `integrations/vllm`, reporting
**end-to-end TTFT** — the number that includes the GPU prefill the cache
avoids. To be filled in from the two-box (or GPU) deployment:

| Workload | Backend | TTFT p50 | TTFT p95 | Notes |
|---|---|---|---|---|
| cache miss (cold prefix) | vLLM + LMCache | _pending_ | _pending_ | |
| cache miss (cold prefix) | vLLM + KalpakDB | _pending_ | _pending_ | |
| cache hit (warm prefix) | vLLM + LMCache | _pending_ | _pending_ | |
| cache hit (warm prefix) | vLLM + KalpakDB | _pending_ | _pending_ | |

Reproduction context to record alongside any numbers: exact model, GPU,
network (bandwidth + RTT between storage and compute nodes), disks, prefix
length, and batch size. Architecture-validating, not headline-chasing —
report the setup so the number means something.
