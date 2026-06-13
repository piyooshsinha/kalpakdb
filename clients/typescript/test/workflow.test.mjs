// End-to-end tests against a real kalpakdb node (KALPAKDB_BIN), covering
// the full agent workflow and cross-language signed writes (Node's
// built-in Ed25519 vs the Rust verifier).
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { KalpakClient, Ed25519Signer, KalpakError } from "../dist/index.js";

const BIN = process.env.KALPAKDB_BIN ?? "../../target/debug/kalpakdb";
const PLAIN = "127.0.0.1:17631";
const SIGNED = "127.0.0.1:17632";

const procs = [];
const dirs = [];

function spawnNode(addr, extra = []) {
  const dir = mkdtempSync(join(tmpdir(), "kalpak-ts-"));
  dirs.push(dir);
  const p = spawn(BIN, ["serve", dir, "--addr", addr, "--compact-secs", "0", ...extra], {
    stdio: "ignore",
  });
  procs.push(p);
}

async function waitOnline(db) {
  const deadline = Date.now() + 15000;
  for (;;) {
    try {
      await db.stats();
      return;
    } catch {
      if (Date.now() > deadline) throw new Error("node did not start");
      await new Promise((r) => setTimeout(r, 150));
    }
  }
}

before(async () => {
  spawnNode(PLAIN);
  spawnNode(SIGNED, ["--require-signatures"]);
  await waitOnline(new KalpakClient(`http://${PLAIN}`));
  await waitOnline(new KalpakClient(`http://${SIGNED}`));
});

after(() => {
  for (const p of procs) p.kill("SIGKILL");
  for (const d of dirs) rmSync(d, { recursive: true, force: true });
});

test("full agent workflow: miss, offload, chain bind, hit, byte-identical reads", async () => {
  const db = new KalpakClient(`http://${PLAIN}`);
  const agent = "0e".repeat(32);
  await db.registerAgent(agent, "ts-agent");

  const fp = { model_id: "test/model", tokenizer_hash: "tok", kv_layout: "fp16/paged-16" };
  const k0 = await db.cacheKey(fp, [1, 2, 3]);
  const k1 = await db.extendKey(k0, [4, 5]);

  assert.equal(await db.lookup([k0, k1]), null, "fresh prefix must miss");

  const chunk0 = new TextEncoder().encode("kv-ts-chunk-0");
  const chunk1 = new TextEncoder().encode("kv-ts-chunk-1");
  const ids = await db.putBlocks([chunk0, chunk1]); // one group commit
  assert.equal(ids.length, 2);

  await db.bindChain(agent, [
    { key: k0, blocks: [ids[0]] },
    { key: k1, blocks: ids, parent: k0 },
  ]);

  const hit = await db.lookup([k0, k1]);
  assert.ok(hit, "bound prefix must hit");
  assert.equal(hit.depth, 1);
  assert.deepEqual(hit.blocks, ids);

  assert.deepEqual(await db.getBlock(ids[0]), chunk0);
  assert.deepEqual(await db.getBlock(ids[1]), chunk1);

  // memory explorer sees the lineage
  const agents = await db.listAgents();
  assert.ok(agents.some((a) => a.agent === agent));
  const bindings = await db.agentBindings(agent);
  assert.equal(bindings.length, 2);
  assert.ok(bindings.some((b) => b.extends));
});

test("signed writes: Node-signed mutations verify against the Rust node", async () => {
  const signer = new Ed25519Signer(new Uint8Array(32).fill(7));
  const db = new KalpakClient(`http://${SIGNED}`, { signer });

  // Unsigned client is rejected with 401...
  const unsigned = new KalpakClient(`http://${SIGNED}`);
  await assert.rejects(
    () => unsigned.registerAgent(signer.agent, "nope"),
    (e) => e instanceof KalpakError && e.status === 401,
  );

  // ...while the Node signer completes the whole workflow: the canonical
  // message bytes and signature must match the Rust verifier exactly.
  await db.registerAgent(signer.agent, "ts-signed");
  const id = await db.putBlock(new TextEncoder().encode("kv-signed-ts"));
  const fp = { model_id: "test/model", tokenizer_hash: "tok", kv_layout: "fp16/paged-16" };
  const k0 = await db.cacheKey(fp, [9, 9]);
  const k1 = await db.extendKey(k0, [8]);
  await db.bindPrefix(signer.agent, k0, [id]);
  await db.bindChain(signer.agent, [{ key: k1, blocks: [id], parent: k0 }]);

  const hit = await db.lookup([k0, k1]);
  assert.equal(hit.depth, 1);

  // A different key cannot write as this agent.
  const intruder = new KalpakClient(`http://${SIGNED}`, {
    signer: new Ed25519Signer(new Uint8Array(32).fill(8)),
  });
  await assert.rejects(
    () => intruder.registerAgent(signer.agent, "forged"),
    (e) => e instanceof KalpakError && e.status === 401,
  );
});
