"""KalpakDB KV connector for vLLM (KVConnectorBase_V1).

Maps vLLM's connector contract onto KalpakDB's primitives:

    scheduler  get_num_new_matched_tokens  ->  longest-prefix lookup
    scheduler  build_connector_meta        ->  per-request load/save plan
    worker     start_load_kv               ->  get_block (warm: the lookup
                                               already prefetched server-side)
    worker     save_kv_layer/wait_for_save ->  put_blocks (one group commit)
                                               + bind_chain (one Raft round)

Chunking: token streams are split into fixed CHUNK_TOKENS chunks and hashed
into KalpakDB's chained CacheKeys. With the optional `blake3` package the
chain is computed locally (byte-identical to the server); otherwise each key
costs one round-trip to `/v1/keys`.

Status: the protocol logic (chunking, chaining, lookup math, save planning,
tensor codec) is unit-tested without a GPU in `test_connector_logic.py`.
The vLLM-coupled glue is written against the documented V1 interface and
needs validation on a GPU box — see README.md in this directory.
"""

from __future__ import annotations

import os
import struct
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

# The kalpakdb Python SDK (clients/python) must be importable.
try:
    from kalpakdb import KalpakClient, ModelFingerprint
except ImportError:  # pragma: no cover - convenience for in-repo use
    sys.path.insert(
        0,
        os.path.join(os.path.dirname(__file__), "..", "..", "clients", "python"),
    )
    from kalpakdb import KalpakClient, ModelFingerprint

try:
    import blake3  # type: ignore

    _HAVE_BLAKE3 = True
except ImportError:
    _HAVE_BLAKE3 = False

if TYPE_CHECKING:  # real vLLM types only needed at runtime inside vLLM
    import torch

# Tokens per Kalpak chunk. Must be a multiple of the vLLM block size in use;
# 256 amortizes per-chunk overhead while keeping reuse granularity useful.
CHUNK_TOKENS = int(os.environ.get("KALPAK_CHUNK_TOKENS", "256"))


# ---------------------------------------------------------------------------
# Key chaining (mirror of kalpak-core's CacheKey, local when blake3 exists)
# ---------------------------------------------------------------------------

def chain_keys(
    db: KalpakClient,
    fingerprint: ModelFingerprint,
    token_ids: list[int],
    chunk: int = CHUNK_TOKENS,
) -> list[dict]:
    """Chained CacheKeys for every complete chunk of `token_ids`."""
    keys: list[dict] = []
    parent: Optional[dict] = None
    for at in range(0, len(token_ids) - len(token_ids) % chunk, chunk):
        tokens = token_ids[at : at + chunk]
        if _HAVE_BLAKE3:
            h = blake3.blake3()
            if parent is not None:
                h.update(bytes.fromhex(parent["prefix_hash"]))
            for t in tokens:
                h.update(struct.pack("<I", t))
            key = {
                "fingerprint": fingerprint.to_dict(),
                "prefix_hash": h.hexdigest(),
            }
        else:
            key = (
                db.extend_key(parent, tokens)
                if parent is not None
                else db.cache_key(fingerprint, tokens)
            )
        keys.append(key)
        parent = key
    return keys


# ---------------------------------------------------------------------------
# Tensor codec: vLLM paged KV layers <-> Kalpak block payloads
# ---------------------------------------------------------------------------

class PagedKvCodec:
    """Serializes the KV pages of one chunk across all layers.

    vLLM paged layers are tensors whose second dimension indexes blocks
    (pages). One Kalpak block = the bytes of one chunk's pages across every
    layer, concatenated in registration order — so a prefix chunk is one
    content-addressed unit, and the fingerprint's kv_layout pins dtype/shape
    compatibility.
    """

    def __init__(self) -> None:
        self.layer_names: list[str] = []
        self.kv_caches: dict[str, "torch.Tensor"] = {}

    def register(self, kv_caches: dict[str, "torch.Tensor"]) -> None:
        self.kv_caches = dict(kv_caches)
        self.layer_names = list(kv_caches.keys())

    def encode_chunk(self, block_ids: list[int]) -> bytes:
        """Extract `block_ids` pages from every layer into one payload."""
        parts: list[bytes] = []
        for name in self.layer_names:
            layer = self.kv_caches[name]
            pages = layer[:, block_ids] if layer.dim() >= 2 else layer[block_ids]
            parts.append(pages.contiguous().cpu().numpy().tobytes())
        return b"".join(parts)

    def decode_chunk(self, payload: bytes, block_ids: list[int]) -> None:
        """Write a payload back into `block_ids` pages of every layer."""
        import numpy as np
        import torch

        at = 0
        for name in self.layer_names:
            layer = self.kv_caches[name]
            pages = layer[:, block_ids] if layer.dim() >= 2 else layer[block_ids]
            n = pages.numel() * pages.element_size()
            flat = np.frombuffer(payload[at : at + n], dtype=np.uint8)
            src = torch.from_numpy(flat.copy()).view(pages.dtype).view(pages.shape)
            if layer.dim() >= 2:
                layer[:, block_ids] = src.to(layer.device)
            else:
                layer[block_ids] = src.to(layer.device)
            at += n


# ---------------------------------------------------------------------------
# Pure-python planning core (unit-tested without vLLM)
# ---------------------------------------------------------------------------

@dataclass
class LoadPlan:
    """One request's reusable prefix: kalpak block per chunk, in order."""

    kalpak_ids: list[str] = field(default_factory=list)
    vllm_block_ids: list[list[int]] = field(default_factory=list)


@dataclass
class SavePlan:
    """One request's chunks to offload after prefill."""

    keys: list[dict] = field(default_factory=list)
    vllm_block_ids: list[list[int]] = field(default_factory=list)
    parents: list[Optional[dict]] = field(default_factory=list)


class KalpakPlanner:
    """Scheduler-side brain: lookup math and save planning."""

    def __init__(self, db: KalpakClient, fingerprint: ModelFingerprint):
        self.db = db
        self.fingerprint = fingerprint
        self._chains: dict[str, list[dict]] = {}
        self._hits: dict[str, LoadPlan] = {}

    def matched_tokens(self, req_id: str, token_ids: list[int], computed: int) -> int:
        """How many prompt tokens KalpakDB can supply beyond `computed`.

        Also stages the LoadPlan for the hit (the server has already begun
        warming those blocks plus their lookahead children).
        """
        chain = chain_keys(self.db, self.fingerprint, token_ids)
        self._chains[req_id] = chain
        if not chain:
            return 0
        hit = self.db.lookup(chain)
        if hit is None:
            return 0
        matched = (hit.depth + 1) * CHUNK_TOKENS
        if matched <= computed:
            return 0
        plan = LoadPlan(kalpak_ids=list(hit.blocks))
        self._hits[req_id] = plan
        return matched - computed

    def save_plan(self, req_id: str, prefill_chunks: int) -> SavePlan:
        """Chunks (and their parent links) to offload for a finished prefill."""
        chain = self._chains.get(req_id, [])[:prefill_chunks]
        plan = SavePlan()
        for i, key in enumerate(chain):
            plan.keys.append(key)
            plan.parents.append(chain[i - 1] if i > 0 else None)
        return plan

    def take_load_plan(self, req_id: str) -> Optional[LoadPlan]:
        return self._hits.pop(req_id, None)

    def forget(self, req_id: str) -> None:
        self._chains.pop(req_id, None)
        self._hits.pop(req_id, None)


# ---------------------------------------------------------------------------
# The vLLM connector proper (importable only inside a vLLM process)
# ---------------------------------------------------------------------------

def build_connector_class():  # pragma: no cover - requires vLLM at runtime
    """Construct the KVConnectorBase_V1 subclass lazily, so this module
    imports (and its planner is testable) without vLLM installed."""
    from vllm.distributed.kv_transfer.kv_connector.v1.base import (
        KVConnectorBase_V1,
        KVConnectorMetadata,
        KVConnectorRole,
    )

    class KalpakConnectorMetadata(KVConnectorMetadata):
        def __init__(self, loads: dict, saves: dict):
            self.loads = loads  # req_id -> (kalpak_ids, vllm_block_ids)
            self.saves = saves  # req_id -> SavePlan

    class KalpakConnector(KVConnectorBase_V1):
        def __init__(self, vllm_config, role, kv_cache_config):
            super().__init__(vllm_config, role, kv_cache_config)
            cfg = vllm_config.kv_transfer_config.kv_connector_extra_config or {}
            base = cfg.get("kalpak_url", "http://127.0.0.1:7411")
            model = vllm_config.model_config.model
            layout = f"{vllm_config.model_config.dtype}/paged-{vllm_config.cache_config.block_size}"
            self.db = KalpakClient(base)
            self.fingerprint = ModelFingerprint(model, cfg.get("tokenizer_hash", model), layout)
            self.agent = cfg.get("agent", "00" * 32)
            if role == KVConnectorRole.SCHEDULER:
                self.planner = KalpakPlanner(self.db, self.fingerprint)
            self.codec = PagedKvCodec()
            self._pending_saves: dict[str, SavePlan] = {}
            self._loads: dict = {}

        # ---- scheduler side ----
        def get_num_new_matched_tokens(self, request, num_computed_tokens):
            n = self.planner.matched_tokens(
                request.request_id, list(request.prompt_token_ids), num_computed_tokens
            )
            return (n if n > 0 else 0), False

        def update_state_after_alloc(self, request, blocks, num_external_tokens):
            plan = self.planner.take_load_plan(request.request_id)
            if plan is None or num_external_tokens == 0:
                return
            ids = blocks.get_block_ids()[0]
            per = CHUNK_TOKENS // self._block_size()
            plan.vllm_block_ids = [
                ids[i * per : (i + 1) * per] for i in range(len(plan.kalpak_ids))
            ]
            self._loads[request.request_id] = plan

        def build_connector_meta(self, scheduler_output):
            loads, self._loads = self._loads, {}
            saves, self._pending_saves = self._pending_saves, {}
            return KalpakConnectorMetadata(loads, saves)

        def request_finished(self, request, block_ids):
            chunks = len(request.prompt_token_ids) // CHUNK_TOKENS
            plan = self.planner.save_plan(request.request_id, chunks)
            per = CHUNK_TOKENS // self._block_size()
            plan.vllm_block_ids = [
                list(block_ids[i * per : (i + 1) * per]) for i in range(chunks)
            ]
            if plan.keys:
                self._pending_saves[request.request_id] = plan
            self.planner.forget(request.request_id)
            return False, None

        def _block_size(self):
            return self._kv_cache_config.kv_cache_groups[0].kv_cache_spec.block_size  # type: ignore[attr-defined]

        # ---- worker side ----
        def register_kv_caches(self, kv_caches):
            self.codec.register(kv_caches)

        def start_load_kv(self, forward_context, **kwargs):
            meta = self._get_connector_metadata()
            for _req, plan in meta.loads.items():
                for kalpak_id, vblocks in zip(plan.kalpak_ids, plan.vllm_block_ids):
                    payload = self.db.get_block(kalpak_id)
                    self.codec.decode_chunk(payload, vblocks)

        def wait_for_layer_load(self, layer_name):
            return  # loads are synchronous in v0

        def save_kv_layer(self, layer_name, kv_layer, attn_metadata, **kwargs):
            return  # saves happen wholesale in wait_for_save

        def wait_for_save(self):
            meta = self._get_connector_metadata()
            for _req, plan in meta.saves.items():
                payloads = [self.codec.encode_chunk(v) for v in plan.vllm_block_ids]
                ids = self.db.put_blocks(payloads)  # one group commit
                chain, acc = [], []
                for key, parent, bid in zip(plan.keys, plan.parents, ids):
                    acc.append(bid)
                    chain.append((key, list(acc), parent))
                self.db.bind_chain(self.agent, chain)  # one Raft round

    return KalpakConnector


# vLLM loads connectors by "module.path:ClassName"; resolve lazily.
def __getattr__(name: str):  # pragma: no cover
    if name == "KalpakConnector":
        return build_connector_class()
    raise AttributeError(name)
