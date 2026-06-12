#!/usr/bin/env python3
"""Cluster benchmark: concurrent agent workloads against a KalpakDB node,
measuring the full network path (offload throughput, lookup latency, warm
reuse). Zero dependencies — uses the repo's Python SDK.

    python3 scripts/bench_cluster.py http://127.0.0.1:7411 --agents 8 --secs 30
    python3 scripts/bench_cluster.py https://mini.local:7411 --ca pki/kalpak-cert.pem
"""

from __future__ import annotations

import argparse
import os
import secrets
import statistics
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "clients", "python"))
from kalpakdb import KalpakClient, ModelFingerprint  # noqa: E402


def worker(base, ca, idx, secs, chunk_kb, out):
    db = KalpakClient(base, cafile=ca)
    agent = secrets.token_hex(32)
    db.register_agent(agent, f"bench-agent-{idx}")
    fp = ModelFingerprint("bench/model", "tok", "fp16/paged-16")

    offload_bytes = 0
    lookups, hits = [], 0
    contexts = []
    deadline = time.time() + secs
    rng = secrets.SystemRandom()

    while time.time() < deadline:
        if contexts and rng.random() < 0.5:
            # Re-lookup a known context: the warm/TTFT path.
            chain = rng.choice(contexts)
            t = time.perf_counter()
            hit = db.lookup(chain)
            lookups.append((time.perf_counter() - t) * 1000)
            if hit is not None:
                hits += 1
                for b in hit.blocks[-2:]:
                    db.get_block(b)
        else:
            # New context: chain 3 chunks, offload, bind atomically.
            base_tokens = [rng.randrange(1 << 30) for _ in range(3)]
            k0 = db.cache_key(fp, base_tokens * 64)
            k1 = db.extend_key(k0, base_tokens * 64)
            k2 = db.extend_key(k1, base_tokens * 64)
            payloads = [secrets.token_bytes(chunk_kb * 1024) for _ in range(3)]
            ids = db.put_blocks(payloads)
            offload_bytes += sum(len(p) for p in payloads)
            db.bind_chain(
                agent,
                [(k0, [ids[0]], None), (k1, ids[:2], k0), (k2, ids, k1)],
            )
            contexts.append([k0, k1, k2])

    out[idx] = (offload_bytes, lookups, hits)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("base")
    ap.add_argument("--agents", type=int, default=8)
    ap.add_argument("--secs", type=int, default=30)
    ap.add_argument("--chunk-kb", type=int, default=64)
    ap.add_argument("--ca", default=None)
    args = ap.parse_args()

    print(f"benchmarking {args.base}: {args.agents} agents x {args.secs}s, "
          f"{args.chunk_kb} KiB chunks")
    out: dict = {}
    threads = [
        threading.Thread(
            target=worker,
            args=(args.base, args.ca, i, args.secs, args.chunk_kb, out),
        )
        for i in range(args.agents)
    ]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.time() - t0

    offload = sum(o for o, _, _ in out.values())
    lookups = [l for _, ls, _ in out.values() for l in ls]
    hits = sum(h for _, _, h in out.values())

    print(f"\n  offload      {offload / (1024 * 1024) / wall:8.1f} MiB/s "
          f"({offload / (1024 * 1024):.0f} MiB total)")
    if lookups:
        lookups.sort()
        print(f"  lookups      {len(lookups)} total, hit rate "
              f"{hits / len(lookups) * 100:.1f}%")
        print(f"  lookup p50   {statistics.median(lookups):8.2f} ms")
        print(f"  lookup p99   {lookups[int(len(lookups) * 0.99) - 1]:8.2f} ms")

    stats = KalpakClient(args.base, cafile=args.ca).stats()
    d = stats["data_plane"]
    total = d["hits"] + d["misses"]
    print(f"  server warm  {d['hits']}/{total} "
          f"({d['hits'] / max(total, 1) * 100:.1f}% of all reads)")


if __name__ == "__main__":
    main()
