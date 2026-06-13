// Dashboard <-> server JSON contract test.
//
// The dashboard (src/App.tsx) is compile-checked by tsc, but tsc only knows
// the *declared* shapes — it cannot know whether the running server actually
// emits them. This test closes that gap: it drives a real node into a
// representative state, fetches every endpoint the dashboard consumes, and
// asserts each field App.tsx reads is present with the right type. Shape
// drift between the Rust server and the TS dashboard fails here instead of
// silently rendering `undefined` in someone's browser.
//
// Run: KALPAKDB_BIN=../target/debug/kalpakdb node --test test/contract.test.mjs
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const BIN = process.env.KALPAKDB_BIN ?? "../target/debug/kalpakdb";
const ADDR = "127.0.0.1:17661";
const BASE = `http://${ADDR}`;

let proc;
let dir;

function isNum(v) {
  return typeof v === "number";
}
function isNumOrNull(v) {
  return v === null || typeof v === "number";
}

before(async () => {
  dir = mkdtempSync(join(tmpdir(), "kalpak-contract-"));
  proc = spawn(BIN, ["serve", dir, "--addr", ADDR, "--compact-secs", "0"], { stdio: "ignore" });
  const deadline = Date.now() + 15000;
  for (;;) {
    try {
      const r = await fetch(`${BASE}/v1/stats`);
      if (r.ok) break;
    } catch {
      /* not up yet */
    }
    if (Date.now() > deadline) throw new Error("node did not start");
    await new Promise((r) => setTimeout(r, 150));
  }

  // Drive representative state so every field the dashboard reads is
  // exercised with non-trivial values: an agent, blocks, a parent/child
  // binding pair (so `extends` is populated), and a GC run.
  const agent = "0c".repeat(32);
  await fetch(`${BASE}/v1/agents`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ agent, display_name: "contract-agent" }),
  });
  const put = async (body) =>
    (await (await fetch(`${BASE}/v1/blocks`, { method: "POST", body })).json()).id;
  const id0 = await put("contract-block-0");
  const key = async (tokens, parent) =>
    await (
      await fetch(`${BASE}/v1/keys`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          fingerprint: { model_id: "m", tokenizer_hash: "t", kv_layout: "l" },
          tokens,
          parent,
        }),
      })
    ).json();
  const k0 = await key([1, 2]);
  const k1 = await key([3], k0);
  const bind = (k, blocks, parent) =>
    fetch(`${BASE}/v1/manifest/bind`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ agent, key: k, blocks, parent: parent ?? null }),
    });
  await bind(k0, [id0]);
  await bind(k1, [id0], k0); // child -> populates `extends`
  await fetch(`${BASE}/v1/admin/compact`, { method: "POST" }); // exercise gc counters
});

after(() => {
  proc?.kill("SIGKILL");
  if (dir) rmSync(dir, { recursive: true, force: true });
});

test("/v1/stats matches the dashboard's Stats type", async () => {
  const s = await (await fetch(`${BASE}/v1/stats`)).json();

  // data_plane — every field App.tsx's Stats.data_plane declares.
  const d = s.data_plane;
  assert.ok(d, "data_plane missing");
  for (const f of ["blocks", "segments", "bytes_on_disk", "warm_blocks", "warm_bytes", "warm_budget", "pinned_blocks", "pinned_bytes", "hits", "misses"]) {
    assert.ok(isNum(d[f]), `data_plane.${f} must be a number, got ${typeof d[f]}`);
  }

  // control_plane.
  const c = s.control_plane;
  assert.ok(c, "control_plane missing");
  for (const f of ["node_id", "term", "agents", "bindings"]) {
    assert.ok(isNum(c[f]), `control_plane.${f} must be a number`);
  }
  for (const f of ["leader", "last_log_index", "last_applied"]) {
    assert.ok(isNumOrNull(c[f]), `control_plane.${f} must be number|null`);
  }
  assert.ok(Array.isArray(c.peers), "control_plane.peers must be an array");
  // replication is Record<string, number|null> | null
  assert.ok(c.replication === null || typeof c.replication === "object", "control_plane.replication must be object|null");

  // gc.
  const g = s.gc;
  assert.ok(g, "gc missing");
  for (const f of ["runs", "blocks_dropped", "bytes_reclaimed"]) {
    assert.ok(isNum(g[f]), `gc.${f} must be a number`);
  }
});

test("/v1/agents/list matches the dashboard's AgentRow type", async () => {
  const { agents } = await (await fetch(`${BASE}/v1/agents/list`)).json();
  assert.ok(Array.isArray(agents) && agents.length >= 1, "expected at least one agent");
  const a = agents[0];
  assert.equal(typeof a.agent, "string", "agent must be a string");
  assert.equal(typeof a.display_name, "string", "display_name must be a string");
  assert.ok(isNum(a.registered_at), "registered_at must be a number");
  assert.ok(isNum(a.bindings), "bindings count must be a number");
});

test("/v1/agents/{id}/bindings matches the dashboard's Binding type", async () => {
  const agent = "0c".repeat(32);
  const { bindings } = await (await fetch(`${BASE}/v1/agents/${agent}/bindings`)).json();
  assert.ok(Array.isArray(bindings) && bindings.length === 2, `expected 2 bindings, got ${bindings?.length}`);
  for (const b of bindings) {
    assert.ok("key" in b, "binding.key missing");
    assert.ok(Array.isArray(b.blocks), "binding.blocks must be an array");
    // extends is optional string|null; when present it's a string (the child)
    if (b.extends != null) assert.equal(typeof b.extends, "string", "binding.extends must be a string when set");
  }
  // exactly one child binding carries `extends`
  assert.equal(bindings.filter((b) => b.extends).length, 1, "exactly one binding should extend a parent");
});
