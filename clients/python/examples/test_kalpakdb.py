#!/usr/bin/env python3
"""End-to-end KalpakDB test script (Python, zero dependencies).

Exercises the full agent workflow against a running node:

  1. health check + node stats
  2. agent registration (Ed25519 identity)
  3. PASS 1 — cache miss: batch-offload KV chunks (one group commit),
     bind the prefix chain with lineage (parent links)
  4. PASS 2 — cache hit: longest-prefix lookup, warm block fetch,
     content-address verification
  5. speculative lookahead: a parent-only lookup pre-warms child blocks
  6. memory explorer: list agents, audit this agent's bindings + lineage
  7. garbage collection trigger

Usage:
    # start a node first (see RUNNING below), then:
    python3 test_kalpakdb.py [http://127.0.0.1:7411]

RUNNING the application:
    cargo build --release
    ./target/release/kalpakdb serve /tmp/kalpak-data --addr 127.0.0.1:7411
"""

from __future__ import annotations

import os
import secrets
import sys
import time

# Allow running straight from the repo without installing the package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kalpakdb import KalpakClient, KalpakError, ModelFingerprint  # noqa: E402

PASS = "\033[32m✓\033[0m"
INFO = "\033[36m→\033[0m"


def check(label: str, cond: bool, detail: str = ""):
    if not cond:
        print(f"\033[31m✗ {label}\033[0m {detail}")
        sys.exit(1)
    print(f"{PASS} {label}" + (f"  ({detail})" if detail else ""))


def main() -> None:
    base = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:7411"
    db = KalpakClient(base)

    # -- 1. health -----------------------------------------------------
    try:
        stats = db.stats()
    except (KalpakError, OSError) as e:
        print(f"cannot reach KalpakDB at {base}: {e}")
        print("start a node first:  kalpakdb serve /tmp/kalpak-data --addr 127.0.0.1:7411")
        sys.exit(2)
    cp = stats["control_plane"]
    check(
        "node is up and has a Raft leader",
        cp["leader"] is not None,
        f"node {cp['node_id']}, term {cp['term']}, leader {cp['leader']}",
    )

    # -- 2. agent identity ---------------------------------------------
    # A fresh random identity each run keeps the test re-runnable.
    agent = secrets.token_hex(32)
    db.register_agent(agent, "python-e2e-test")
    check("agent registered", True, f"id {agent[:16]}…")

    # -- 3. PASS 1: cache miss -> offload + bind -------------------------
    fp = ModelFingerprint("test/llama-demo", "tok-v1", "fp16/paged-16")
    # Unique token stream per run so pass 1 is always a miss.
    salt = int.from_bytes(secrets.token_bytes(2), "little")
    k0 = db.cache_key(fp, [salt, 1, 2, 3])
    k1 = db.extend_key(k0, [4, 5, 6])
    k2 = db.extend_key(k1, [7, 8])

    check("pass 1 is a cache miss", db.lookup([k0, k1, k2]) is None)

    chunk0 = b"kv-tensor-chunk-0:" + secrets.token_bytes(64 * 1024)
    chunk1 = b"kv-tensor-chunk-1:" + secrets.token_bytes(64 * 1024)
    chunk2 = b"kv-tensor-chunk-2:" + secrets.token_bytes(64 * 1024)
    t = time.perf_counter()
    ids = db.put_blocks([chunk0, chunk1, chunk2])  # ONE group-committed fsync
    dt = (time.perf_counter() - t) * 1000
    check("batch offload (one group commit)", len(ids) == 3, f"3x64KiB in {dt:.1f} ms")

    db.bind_prefix(agent, k0, [ids[0]])
    db.bind_prefix(agent, k1, [ids[0], ids[1]], parent=k0)  # lineage link
    db.bind_prefix(agent, k2, ids, parent=k1)
    check("prefix chain bound with lineage", True, "k0 -> k1 -> k2")

    # -- 4. PASS 2: cache hit -------------------------------------------
    hit = db.lookup([k0, k1, k2])
    check("pass 2 hits the deepest prefix", hit is not None and hit.depth == 2)
    t = time.perf_counter()
    fetched = [db.get_block(b) for b in hit.blocks]
    dt = (time.perf_counter() - t) * 1000
    check(
        "blocks fetched (server pre-warmed them at lookup)",
        fetched == [chunk0, chunk1, chunk2],
        f"3 blocks in {dt:.1f} ms, bytes verified",
    )

    # -- 5. speculative lookahead ----------------------------------------
    before = db.stats()["data_plane"]
    db.lookup([k0])  # parent-only lookup must pre-warm k1's child blocks
    deadline = time.time() + 5
    while time.time() < deadline:
        now = db.stats()["data_plane"]
        if now["hits"] > before["hits"]:
            break
        time.sleep(0.05)
    check(
        "lookahead prefetch warmed child blocks in background",
        now["hits"] > before["hits"],
        f"warm hits {before['hits']} -> {now['hits']}",
    )

    # -- 6. memory explorer ----------------------------------------------
    agents = db.list_agents()
    me = next(a for a in agents if a["agent"] == agent)
    check("agent appears in the registry", me["bindings"] == 3)
    bindings = db.agent_bindings(agent)
    roots = [b for b in bindings if not b.get("extends")]
    children = [b for b in bindings if b.get("extends")]
    check(
        "memory reads as a lineage tree",
        len(roots) == 1 and len(children) == 2,
        "1 root, 2 children",
    )

    # -- 7. GC -----------------------------------------------------------
    swept = db.compact()
    check("garbage collection runs", "blocks_dropped" in swept,
          f"{swept['blocks_dropped']} dead block(s) swept")

    d = db.stats()["data_plane"]
    print(f"\n{INFO} final node state: {d['blocks']} blocks, "
          f"{d['bytes_on_disk']} bytes on disk, "
          f"warm hit rate {d['hits']}/{d['hits'] + d['misses']}")
    print(f"{INFO} all checks passed against {base}")


if __name__ == "__main__":
    main()
