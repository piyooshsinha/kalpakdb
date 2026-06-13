/**
 * KalpakDB TypeScript/Node client — zero runtime dependencies.
 *
 * Uses the global `fetch` (Node 18+) and Node's built-in Ed25519 for
 * signed writes. Mirrors the Python SDK: key hashing happens server-side
 * (`/v1/keys`), so the client needs no BLAKE3.
 *
 * Typical agent workflow:
 *
 * ```ts
 * import { KalpakClient } from "kalpakdb";
 *
 * const db = new KalpakClient("http://127.0.0.1:7411");
 * const fp = { model_id: "meta-llama/Llama-3.1-8B", tokenizer_hash: "tok", kv_layout: "fp16/paged-16" };
 *
 * const k0 = await db.cacheKey(fp, [1, 2, 3]);
 * const k1 = await db.extendKey(k0, [4, 5]);
 *
 * const hit = await db.lookup([k0, k1]);
 * if (!hit) {
 *   const ids = await db.putBlocks([chunk0, chunk1]);   // one group commit
 *   await db.bindChain(agent, [
 *     { key: k0, blocks: [ids[0]] },
 *     { key: k1, blocks: ids, parent: k0 },             // lookahead link
 *   ]);
 * } else {
 *   const blocks = await Promise.all(hit.blocks.map((b) => db.getBlock(b)));
 * }
 * ```
 */

import { createPrivateKey, createPublicKey, sign as edSign, KeyObject } from "node:crypto";

export interface ModelFingerprint {
  model_id: string;
  tokenizer_hash: string;
  kv_layout: string;
}

export interface CacheKey {
  fingerprint: ModelFingerprint;
  prefix_hash: string;
}

export interface PrefixHit {
  /** Index into the queried chain of the deepest bound key. */
  depth: number;
  /** Hex BLAKE3 content ids materializing that prefix. */
  blocks: string[];
}

export interface ChainBinding {
  key: CacheKey;
  blocks: string[];
  parent?: CacheKey;
}

export class KalpakError extends Error {
  constructor(
    message: string,
    public readonly status?: number,
  ) {
    super(message);
    this.name = "KalpakError";
  }
}

/** PKCS#8 DER prefix for a raw 32-byte Ed25519 private key. */
const PKCS8_ED25519_PREFIX = Buffer.from("302e020100300506032b657004220420", "hex");

/**
 * Signs metadata mutations for nodes running `--require-signatures`,
 * using Node's built-in Ed25519 (no dependencies).
 */
export class Ed25519Signer {
  private readonly key: KeyObject;
  /** Hex public key — the agent identity this signer writes as. */
  public readonly agent: string;

  constructor(privateKey: Uint8Array) {
    if (privateKey.length !== 32) {
      throw new KalpakError("Ed25519 private key must be 32 bytes");
    }
    const der = Buffer.concat([PKCS8_ED25519_PREFIX, Buffer.from(privateKey)]);
    this.key = createPrivateKey({ key: der, format: "der", type: "pkcs8" });
    const spki = createPublicKey(this.key).export({ format: "der", type: "spki" });
    // Raw public key = last 32 bytes of the SPKI encoding.
    this.agent = Buffer.from(spki.subarray(spki.length - 32)).toString("hex");
  }

  signHex(message: Uint8Array): string {
    return edSign(null, message, this.key).toString("hex");
  }
}

// ---- Canonical signed-write messages -------------------------------------
// Byte-identical to kalpak-core::signing (raw fixed-width fields, never
// JSON): see crates/kalpak-core/src/signing.rs for the locked layout.

function u32le(n: number): Buffer {
  const b = Buffer.alloc(4);
  b.writeUInt32LE(n);
  return b;
}

function msgKey(key: CacheKey): Buffer {
  const fp = key.fingerprint;
  return Buffer.concat([
    Buffer.from(fp.model_id, "utf8"),
    Buffer.from([0]),
    Buffer.from(fp.tokenizer_hash, "utf8"),
    Buffer.from([0]),
    Buffer.from(fp.kv_layout, "utf8"),
    Buffer.from([0]),
    Buffer.from(key.prefix_hash, "hex"),
  ]);
}

function msgLink(key: CacheKey, blocks: string[], parent?: CacheKey): Buffer {
  const parts = [msgKey(key), u32le(blocks.length)];
  for (const b of blocks) parts.push(Buffer.from(b, "hex"));
  if (parent) {
    parts.push(Buffer.from([1]), Buffer.from(parent.prefix_hash, "hex"));
  } else {
    parts.push(Buffer.from([0]));
  }
  return Buffer.concat(parts);
}

export function registerMessage(agent: string, displayName: string): Buffer {
  return Buffer.concat([
    Buffer.from("KLPK/reg/v1\0", "utf8"),
    Buffer.from(agent, "hex"),
    Buffer.from(displayName, "utf8"),
  ]);
}

export function bindMessage(
  agent: string,
  key: CacheKey,
  blocks: string[],
  parent?: CacheKey,
): Buffer {
  return Buffer.concat([
    Buffer.from("KLPK/bind/v1\0", "utf8"),
    Buffer.from(agent, "hex"),
    msgLink(key, blocks, parent),
  ]);
}

export function chainMessage(agent: string, links: ChainBinding[]): Buffer {
  const parts = [
    Buffer.from("KLPK/chain/v1\0", "utf8"),
    Buffer.from(agent, "hex"),
    u32le(links.length),
  ];
  for (const l of links) parts.push(msgLink(l.key, l.blocks, l.parent));
  return Buffer.concat(parts);
}

// ---- Client ----------------------------------------------------------------

export interface KalpakClientOptions {
  /** Sign every metadata mutation (required by `--require-signatures` nodes). */
  signer?: Ed25519Signer;
  /** Per-request timeout in milliseconds (default 30000). */
  timeoutMs?: number;
}

export class KalpakClient {
  private readonly base: string;
  readonly signer?: Ed25519Signer;
  private readonly timeoutMs: number;

  constructor(base: string, opts: KalpakClientOptions = {}) {
    this.base = base.replace(/\/+$/, "");
    this.signer = opts.signer;
    this.timeoutMs = opts.timeoutMs ?? 30_000;
  }

  private async request(
    method: string,
    path: string,
    body?: Uint8Array | object,
  ): Promise<Response> {
    const headers: Record<string, string> = {};
    let payload: BodyInit | undefined;
    if (body instanceof Uint8Array) {
      headers["content-type"] = "application/octet-stream";
      payload = body as unknown as BodyInit;
    } else if (body !== undefined) {
      headers["content-type"] = "application/json";
      payload = JSON.stringify(body);
    }
    const resp = await fetch(`${this.base}${path}`, {
      method,
      headers,
      body: payload,
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    if (!resp.ok) {
      let message = `${resp.status}`;
      try {
        const err = (await resp.json()) as { error?: string };
        if (err.error) message = err.error;
      } catch {
        /* non-JSON error body */
      }
      throw new KalpakError(message, resp.status);
    }
    return resp;
  }

  private async json<T>(method: string, path: string, body?: object): Promise<T> {
    return (await (await this.request(method, path, body)).json()) as T;
  }

  // -- keys (hashed server-side, like the Python SDK) --

  cacheKey(fingerprint: ModelFingerprint, tokens: number[]): Promise<CacheKey> {
    return this.json("POST", "/v1/keys", { fingerprint, tokens });
  }

  extendKey(parent: CacheKey, tokens: number[]): Promise<CacheKey> {
    return this.json("POST", "/v1/keys", {
      fingerprint: parent.fingerprint,
      tokens,
      parent,
    });
  }

  // -- data plane --

  /** Store one block; returns its content address. Idempotent. */
  async putBlock(payload: Uint8Array): Promise<string> {
    const resp = await this.request("POST", "/v1/blocks", payload);
    const r = (await resp.json()) as { id: string };
    return r.id;
  }

  /**
   * Store a batch under one group-committed fsync — the fast path for
   * offloading a multi-chunk context. Returns content ids in input order.
   */
  async putBlocks(payloads: Uint8Array[]): Promise<string[]> {
    const parts: Buffer[] = [u32le(payloads.length)];
    for (const p of payloads) {
      parts.push(u32le(p.length), Buffer.from(p));
    }
    const resp = await this.request("POST", "/v1/blocks/batch", Buffer.concat(parts));
    const r = (await resp.json()) as { ids: string[] };
    return r.ids;
  }

  /** Fetch a block by content address. */
  async getBlock(id: string): Promise<Uint8Array> {
    const resp = await this.request("GET", `/v1/blocks/${id}`);
    return new Uint8Array(await resp.arrayBuffer());
  }

  // -- control plane --

  async registerAgent(agent: string, displayName: string): Promise<void> {
    const body: Record<string, unknown> = { agent, display_name: displayName };
    if (this.signer) {
      body.signature = this.signer.signHex(registerMessage(agent, displayName));
    }
    await this.request("POST", "/v1/agents", body);
  }

  /** Bind a prefix to its blocks; `parent` links the prefix tree for
   *  one-step-ahead speculative prefetch. */
  async bindPrefix(
    agent: string,
    key: CacheKey,
    blocks: string[],
    parent?: CacheKey,
  ): Promise<void> {
    const body: Record<string, unknown> = { agent, key, blocks, parent: parent ?? null };
    if (this.signer) {
      body.signature = this.signer.signHex(bindMessage(agent, key, blocks, parent));
    }
    await this.request("POST", "/v1/manifest/bind", body);
  }

  /** Bind a whole root-first chain atomically (one consensus round). */
  async bindChain(agent: string, bindings: ChainBinding[]): Promise<void> {
    const body: Record<string, unknown> = {
      agent,
      bindings: bindings.map((b) => ({
        key: b.key,
        blocks: b.blocks,
        parent: b.parent ?? null,
      })),
    };
    if (this.signer) {
      body.signature = this.signer.signHex(chainMessage(agent, bindings));
    }
    await this.request("POST", "/v1/manifest/bind-chain", body);
  }

  /** Probe a root-first key chain for the longest cached prefix. The server
   *  speculatively warms the hit's blocks (and its children's) into RAM. */
  async lookup(chain: CacheKey[]): Promise<PrefixHit | null> {
    const r = await this.json<{ hit_depth: number | null; blocks: string[] }>(
      "POST",
      "/v1/manifest/lookup",
      { chain },
    );
    return r.hit_depth === null ? null : { depth: r.hit_depth, blocks: r.blocks };
  }

  // -- observability / admin --

  stats(): Promise<Record<string, unknown>> {
    return this.json("GET", "/v1/stats");
  }

  async listAgents(): Promise<Array<Record<string, unknown>>> {
    const r = await this.json<{ agents: Array<Record<string, unknown>> }>(
      "GET",
      "/v1/agents/list",
    );
    return r.agents;
  }

  async agentBindings(agent: string): Promise<Array<Record<string, unknown>>> {
    const r = await this.json<{ bindings: Array<Record<string, unknown>> }>(
      "GET",
      `/v1/agents/${agent}/bindings`,
    );
    return r.bindings;
  }

  compact(): Promise<Record<string, unknown>> {
    return this.json("POST", "/v1/admin/compact");
  }
}
