#!/usr/bin/env python3
"""Prefix-reuse latency benchmark for a single KalpakDB node.

Measures the two paths that decide an agent's time-to-first-token when
KalpakDB sits under an inference engine:

  MISS path  — a new prefix: put the KV blocks, then bind them.
               (the cost KalpakDB *adds* on a cache miss)
  HIT  path  — a seen prefix: look it up, then fetch its warm blocks.
               (the cost KalpakDB *replaces a prefill with* on a hit)

The HIT path is the win: on a cache hit the inference engine skips
recomputing the prefix's KV cache and instead pays only this lookup +
warm-fetch. Reporting both distributions (p50/p95/p99) quantifies the
KalpakDB-side overhead honestly, without claiming a TTFT speedup we
haven't measured end-to-end under a real model (see issue #3 / #5).

This is the *KalpakDB half* of the LMCache comparison: it needs no GPU
and no vLLM. The cross-system TTFT comparison is the hardware run.

Usage:
    # against a running node:
    python3 scripts/bench_prefix_reuse.py http://127.0.0.1:7411

    # or let it spawn one (needs a built binary):
    KALPAKDB_BIN=./target/release/kalpakdb python3 scripts/bench_prefix_reuse.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

# Reuse the stdlib client from the Python SDK.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "clients", "python"))
from kalpakdb import KalpakClient, ModelFingerprint  # noqa: E402


def percentile(sorted_ms: list[float], p: float) -> float:
    if not sorted_ms:
        return float("nan")
    k = (len(sorted_ms) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_ms) - 1)
    return sorted_ms[lo] + (sorted_ms[hi] - sorted_ms[lo]) * (k - lo)


def summarize(name: str, samples_ms: list[float]) -> dict:
    s = sorted(samples_ms)
    return {
        "path": name,
        "n": len(s),
        "p50_ms": round(percentile(s, 50), 3),
        "p95_ms": round(percentile(s, 95), 3),
        "p99_ms": round(percentile(s, 99), 3),
        "mean_ms": round(sum(s) / len(s), 3) if s else float("nan"),
        "max_ms": round(s[-1], 3) if s else float("nan"),
    }


def run(base: str, *, iters: int, chunk_kb: int, depth: int) -> dict:
    db = KalpakClient(base)
    fp = ModelFingerprint("bench/model", "tok-v1", "fp16/paged-16")
    agent = "0b" * 32
    db.register_agent(agent, "bench-agent")
    payload = (b"x" * 64) * (chunk_kb * 1024 // 64)

    miss_ms: list[float] = []
    hit_ms: list[float] = []

    # Warm-up so the first measurements aren't dominated by connection setup.
    warm_key = db.cache_key(fp, [0, 0, 0])
    wid = db.put_block(payload)
    db.bind_prefix(agent, warm_key, [wid])
    db.lookup([warm_key])

    for i in range(iters):
        # --- MISS path: a brand-new prefix (put + bind) ---
        key = db.cache_key(fp, [1, 2, 3, i])
        t0 = time.perf_counter()
        bid = db.put_block(payload)
        db.bind_prefix(agent, key, [bid])
        miss_ms.append((time.perf_counter() - t0) * 1000.0)

        # --- HIT path: look the prefix up and fetch its (warm) blocks ---
        t0 = time.perf_counter()
        hit = db.lookup([key])
        assert hit is not None, "just-bound prefix must hit"
        for b in hit.blocks:
            db.get_block(b)
        hit_ms.append((time.perf_counter() - t0) * 1000.0)

    # --- DEEP-CHAIN reuse: the real agentic workload ---------------------
    # Session 1 builds a `depth`-level conversation (each level a chunk of
    # KV). Session 2 shares the first `depth` levels and would, without a
    # cache, re-prefill all of them. With KalpakDB it fetches the whole
    # reused prefix instead. We measure that fetch (the HIT) against the
    # cost of materializing the chain fresh (the MISS) — the longest-
    # prefix-match win that depth-0 reuse can't show.
    deep_build_ms: list[float] = []
    deep_reuse_ms: list[float] = []
    chain_iters = max(1, iters // 10)
    for i in range(chain_iters):
        # Build a fresh `depth`-level chain (session 1) and time it.
        keys = []
        block_ids = []
        t0 = time.perf_counter()
        prev = None
        for d in range(depth):
            k = db.cache_key(fp, [9, i, d]) if prev is None else db.extend_key(prev, [d])
            bid = db.put_block(payload)
            db.bind_prefix(agent, k, [bid], parent=prev)
            keys.append(k)
            block_ids.append(bid)
            prev = k
        deep_build_ms.append((time.perf_counter() - t0) * 1000.0)

        # Session 2: reuse the whole chain — one lookup finds the deepest
        # bound prefix, then fetch every reused block (all warm).
        t0 = time.perf_counter()
        hit = db.lookup(keys)
        assert hit is not None and hit.depth == depth - 1, "deep chain must fully hit"
        for b in hit.blocks:
            db.get_block(b)
        deep_reuse_ms.append((time.perf_counter() - t0) * 1000.0)

    stats = db.stats()["data_plane"]
    return {
        "config": {
            "iters": iters,
            "chunk_kb": chunk_kb,
            "depth": depth,
            "base": base,
        },
        "miss": summarize("miss (put+bind)", miss_ms),
        "hit": summarize("hit (lookup+warm fetch)", hit_ms),
        "deep_build": summarize(f"build {depth}-level chain (put+bind x{depth})", deep_build_ms),
        "deep_reuse": summarize(f"reuse {depth}-level chain (lookup+warm fetch x{depth})", deep_reuse_ms),
        "warm_hit_rate": (
            stats["hits"] / (stats["hits"] + stats["misses"])
            if (stats["hits"] + stats["misses"])
            else None
        ),
        "note": (
            "Single-node KalpakDB-side latencies, not end-to-end TTFT. The "
            "HIT / deep_reuse paths are what an inference engine pays "
            "instead of recomputing a prefix's KV cache on a cache hit; the "
            "deep-chain rows show the longest-prefix-match win for a "
            "multi-turn agent reusing a prior session's context. The cross-"
            "system TTFT comparison vs LMCache under vLLM is the hardware "
            "run (issues #3, #5)."
        ),
    }


def maybe_spawn() -> tuple[str, subprocess.Popen | None, tempfile.TemporaryDirectory | None]:
    bin_path = os.environ.get("KALPAKDB_BIN")
    if not bin_path:
        print("error: pass a node URL or set KALPAKDB_BIN to spawn one", file=sys.stderr)
        sys.exit(2)
    tmp = tempfile.TemporaryDirectory()
    addr = "127.0.0.1:17651"
    proc = subprocess.Popen(
        [bin_path, "serve", tmp.name, "--addr", addr, "--compact-secs", "0"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base = f"http://{addr}"
    deadline = time.time() + 15
    while True:
        try:
            urllib.request.urlopen(f"{base}/v1/stats", timeout=1)
            break
        except OSError:
            if time.time() > deadline:
                raise RuntimeError("node did not start")
            time.sleep(0.2)
    return base, proc, tmp


def main() -> None:
    iters = int(os.environ.get("BENCH_ITERS", "500"))
    chunk_kb = int(os.environ.get("BENCH_CHUNK_KB", "64"))
    depth = int(os.environ.get("BENCH_DEPTH", "10"))

    proc = tmp = None
    if len(sys.argv) > 1:
        base = sys.argv[1].rstrip("/")
    else:
        base, proc, tmp = maybe_spawn()

    try:
        result = run(base, iters=iters, chunk_kb=chunk_kb, depth=depth)
    finally:
        if proc:
            proc.terminate()
            proc.wait(timeout=10)
        if tmp:
            tmp.cleanup()

    print(json.dumps(result, indent=2))
    h, m = result["hit"], result["miss"]
    print(
        f"\nKalpakDB prefix reuse ({result['config']['iters']} iters, "
        f"{result['config']['chunk_kb']} KiB blocks):",
        file=sys.stderr,
    )
    print(f"  miss (put+bind):          p50 {m['p50_ms']:.2f} ms  p95 {m['p95_ms']:.2f} ms  p99 {m['p99_ms']:.2f} ms", file=sys.stderr)
    print(f"  hit  (lookup+warm fetch): p50 {h['p50_ms']:.2f} ms  p95 {h['p95_ms']:.2f} ms  p99 {h['p99_ms']:.2f} ms", file=sys.stderr)
    db_, dr_ = result["deep_build"], result["deep_reuse"]
    print(f"  {result['config']['depth']}-level chain build:      p50 {db_['p50_ms']:.2f} ms  p95 {db_['p95_ms']:.2f} ms", file=sys.stderr)
    print(f"  {result['config']['depth']}-level chain reuse:      p50 {dr_['p50_ms']:.2f} ms  p95 {dr_['p95_ms']:.2f} ms  <- the longest-prefix-match win", file=sys.stderr)


if __name__ == "__main__":
    main()
