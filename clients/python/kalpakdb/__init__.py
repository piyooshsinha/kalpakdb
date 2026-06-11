"""KalpakDB Python client — zero dependencies (stdlib urllib only).

Typical agent workflow::

    from kalpakdb import KalpakClient, ModelFingerprint

    db = KalpakClient("http://127.0.0.1:7411")
    fp = ModelFingerprint("meta-llama/Llama-3.1-8B", "tok-hash", "fp16/paged-16")

    k0 = db.cache_key(fp, [1, 2, 3])          # root prefix
    k1 = db.extend_key(k0, [4, 5])            # deeper prefix

    hit = db.lookup([k0, k1])                 # longest cached prefix
    if hit is None:
        ids = db.put_blocks([kv_chunk_0, kv_chunk_1])   # one group commit
        db.bind_prefix(agent, k0, [ids[0]])
        db.bind_prefix(agent, k1, ids, parent=k0)       # lookahead link
    else:
        blocks = [db.get_block(b) for b in hit.blocks]  # warm: prefetched
"""

from __future__ import annotations

import json
import struct
import urllib.error
import urllib.request
from dataclasses import dataclass
from hashlib import blake2b  # only for docs; key hashing is server-compatible blake3 below

__all__ = [
    "KalpakClient",
    "KalpakError",
    "ModelFingerprint",
    "PrefixHit",
]

__version__ = "0.2.0"


class KalpakError(Exception):
    """Server or transport error, with the HTTP status when available."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class ModelFingerprint:
    """KV caches are only valid for an exact model/tokenizer/layout triple."""

    model_id: str
    tokenizer_hash: str
    kv_layout: str

    def to_dict(self) -> dict:
        return {
            "model_id": self.model_id,
            "tokenizer_hash": self.tokenizer_hash,
            "kv_layout": self.kv_layout,
        }


@dataclass(frozen=True)
class PrefixHit:
    """The longest cached prefix found for a key chain."""

    depth: int
    blocks: list[str]


class KalpakClient:
    def __init__(self, base: str, timeout: float = 30.0):
        self.base = base.rstrip("/")
        self.timeout = timeout

    # ---- transport ----

    def _request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        content_type: str = "application/octet-stream",
    ) -> bytes:
        req = urllib.request.Request(
            f"{self.base}{path}",
            data=body,
            method=method,
            headers={"content-type": content_type} if body is not None else {},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "replace")
            try:
                detail = json.loads(detail).get("error", detail)
            except (json.JSONDecodeError, AttributeError):
                pass
            raise KalpakError(detail, status=e.code) from None
        except urllib.error.URLError as e:
            raise KalpakError(f"transport: {e.reason}") from None

    def _json(self, method: str, path: str, payload: dict | None = None) -> dict:
        body = json.dumps(payload).encode() if payload is not None else None
        raw = self._request(method, path, body, "application/json")
        return json.loads(raw)

    # ---- cache keys (server computes the chained hash) ----

    def cache_key(self, fingerprint: ModelFingerprint, tokens: list[int]) -> dict:
        """Root cache key for the first chunk of a token stream.

        The chained BLAKE3 prefix hash is computed server-side-compatible by
        delegating to the node (`/v1/keys`), keeping this client dependency-
        free. For hot paths, compute keys natively with the `blake3` package
        and the documented chaining scheme.
        """
        return self._key_request(fingerprint, tokens, parent=None)

    def extend_key(self, parent: dict, tokens: list[int]) -> dict:
        """Key for the next chunk, chained from `parent`."""
        fp = ModelFingerprint(**parent["fingerprint"])
        return self._key_request(fp, tokens, parent=parent)

    def _key_request(
        self, fingerprint: ModelFingerprint, tokens: list[int], parent: dict | None
    ) -> dict:
        return self._json(
            "POST",
            "/v1/keys",
            {
                "fingerprint": fingerprint.to_dict(),
                "tokens": tokens,
                "parent": parent,
            },
        )

    # ---- data plane ----

    def put_block(self, payload: bytes) -> str:
        """Store one block; returns its content address (hex). Idempotent."""
        raw = self._request("POST", "/v1/blocks", payload)
        return json.loads(raw)["id"]

    def put_blocks(self, payloads: list[bytes]) -> list[str]:
        """Store a batch under one group-committed fsync."""
        frame = bytearray(struct.pack("<I", len(payloads)))
        for p in payloads:
            frame += struct.pack("<I", len(p))
            frame += p
        raw = self._request("POST", "/v1/blocks/batch", bytes(frame))
        return json.loads(raw)["ids"]

    def get_block(self, block_id: str) -> bytes:
        return self._request("GET", f"/v1/blocks/{block_id}")

    # ---- control plane ----

    def register_agent(self, agent: str, display_name: str) -> None:
        self._json(
            "POST", "/v1/agents", {"agent": agent, "display_name": display_name}
        )

    def bind_prefix(
        self,
        agent: str,
        key: dict,
        blocks: list[str],
        parent: dict | None = None,
    ) -> None:
        """Bind a prefix to its blocks; `parent` links the prefix tree for
        one-step-ahead speculative prefetch."""
        self._json(
            "POST",
            "/v1/manifest/bind",
            {"agent": agent, "key": key, "blocks": blocks, "parent": parent},
        )

    def bind_chain(
        self,
        agent: str,
        bindings: list[tuple[dict, list[str], dict | None]],
    ) -> None:
        """Bind a root-first chain atomically: one consensus round (one
        Raft log fsync) instead of one per depth. Entries are
        ``(key, block_ids, parent_key_or_None)``."""
        payload = [
            {"key": key, "blocks": blocks, "parent": parent}
            for key, blocks, parent in bindings
        ]
        self._json("POST", "/v1/manifest/bind-chain",
                   {"agent": agent, "bindings": payload})

    def lookup(self, chain: list[dict]) -> PrefixHit | None:
        """Probe a root-first key chain for the longest cached prefix."""
        resp = self._json("POST", "/v1/manifest/lookup", {"chain": chain})
        if resp.get("hit_depth") is None:
            return None
        return PrefixHit(depth=resp["hit_depth"], blocks=resp["blocks"])

    # ---- observability / admin ----

    def stats(self) -> dict:
        return self._json("GET", "/v1/stats")

    def list_agents(self) -> list[dict]:
        return self._json("GET", "/v1/agents/list")["agents"]

    def agent_bindings(self, agent: str) -> list[dict]:
        return self._json("GET", f"/v1/agents/{agent}/bindings")["bindings"]

    def compact(self) -> dict:
        """Trigger GC: sweep sealed segments not referenced by any binding."""
        return self._json("POST", "/v1/admin/compact", {})
