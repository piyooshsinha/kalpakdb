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
    "Ed25519Signer",
    "KalpakClient",
    "KalpakError",
    "ModelFingerprint",
    "PrefixHit",
    "bind_message",
    "chain_message",
    "register_message",
]

__version__ = "0.2.0"


class Ed25519Signer:
    """Signs metadata mutations for nodes running ``--require-signatures``.

    Needs the optional ``cryptography`` package (the only situation in
    which this SDK is not stdlib-only)::

        pip install cryptography
        signer = Ed25519Signer(private_key_bytes_32)
        db = KalpakClient("http://...", signer=signer)
    """

    def __init__(self, private_key: bytes):
        try:
            from cryptography.hazmat.primitives.asymmetric.ed25519 import (
                Ed25519PrivateKey,
            )
        except ImportError as e:  # pragma: no cover
            raise KalpakError(
                "signed writes need the 'cryptography' package: pip install cryptography"
            ) from e
        self._key = Ed25519PrivateKey.from_private_bytes(private_key)

    @property
    def agent(self) -> str:
        """Hex public key — the agent identity this signer writes as."""
        from cryptography.hazmat.primitives.serialization import (
            Encoding,
            PublicFormat,
        )
        return self._key.public_key().public_bytes(
            Encoding.Raw, PublicFormat.Raw
        ).hex()

    def sign_hex(self, message: bytes) -> str:
        return self._key.sign(message).hex()


# Canonical signed-write messages — byte-identical to kalpak-core's
# `signing` module (raw fixed-width fields, never JSON).

def _msg_key(key: dict) -> bytes:
    fp = key["fingerprint"]
    return (
        fp["model_id"].encode() + b"\0"
        + fp["tokenizer_hash"].encode() + b"\0"
        + fp["kv_layout"].encode() + b"\0"
        + bytes.fromhex(key["prefix_hash"])
    )


def _msg_link(key: dict, blocks: list[str], parent: dict | None) -> bytes:
    out = _msg_key(key) + struct.pack("<I", len(blocks))
    for b in blocks:
        out += bytes.fromhex(b)
    if parent is not None:
        out += b"\x01" + bytes.fromhex(parent["prefix_hash"])
    else:
        out += b"\x00"
    return out


def register_message(agent: str, display_name: str) -> bytes:
    return b"KLPK/reg/v1\0" + bytes.fromhex(agent) + display_name.encode()


def bind_message(agent: str, key: dict, blocks: list[str], parent: dict | None) -> bytes:
    return b"KLPK/bind/v1\0" + bytes.fromhex(agent) + _msg_link(key, blocks, parent)


def chain_message(agent: str, links: list[tuple[dict, list[str], dict | None]]) -> bytes:
    out = b"KLPK/chain/v1\0" + bytes.fromhex(agent) + struct.pack("<I", len(links))
    for key, blocks, parent in links:
        out += _msg_link(key, blocks, parent)
    return out


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
    def __init__(self, base: str, timeout: float = 30.0,
                 signer: "Ed25519Signer | None" = None,
                 cafile: str | None = None):
        self.signer = signer
        # Extra root CA for self-signed/private-CA TLS deployments.
        self._ssl_ctx = None
        if cafile is not None:
            import ssl
            self._ssl_ctx = ssl.create_default_context(cafile=cafile)
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
            with urllib.request.urlopen(
                req, timeout=self.timeout, context=self._ssl_ctx
            ) as resp:
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
        body = {"agent": agent, "display_name": display_name}
        if self.signer is not None:
            body["signature"] = self.signer.sign_hex(
                register_message(agent, display_name)
            )
        self._json("POST", "/v1/agents", body)

    def bind_prefix(
        self,
        agent: str,
        key: dict,
        blocks: list[str],
        parent: dict | None = None,
    ) -> None:
        """Bind a prefix to its blocks; `parent` links the prefix tree for
        one-step-ahead speculative prefetch."""
        body = {"agent": agent, "key": key, "blocks": blocks, "parent": parent}
        if self.signer is not None:
            body["signature"] = self.signer.sign_hex(
                bind_message(agent, key, blocks, parent)
            )
        self._json("POST", "/v1/manifest/bind", body)

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
        body = {"agent": agent, "bindings": payload}
        if self.signer is not None:
            body["signature"] = self.signer.sign_hex(
                chain_message(agent, list(bindings))
            )
        self._json("POST", "/v1/manifest/bind-chain", body)

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
