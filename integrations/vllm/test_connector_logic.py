"""GPU-free tests for the vLLM connector's protocol logic, against a real
KalpakDB node: key chaining parity, lookup math, save planning, and the
paged tensor codec (numpy stand-in for torch)."""

import os
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "clients", "python"))
sys.path.insert(0, os.path.dirname(__file__))

from kalpakdb import KalpakClient, ModelFingerprint  # noqa: E402
from kalpak_connector import CHUNK_TOKENS, KalpakPlanner, chain_keys  # noqa: E402

BIN = os.environ.get("KALPAKDB_BIN", "target/debug/kalpakdb")
ADDR = "127.0.0.1:17631"


class ConnectorLogicTest(unittest.TestCase):
    proc: subprocess.Popen
    tmp: tempfile.TemporaryDirectory

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.proc = subprocess.Popen(
            [BIN, "serve", cls.tmp.name, "--addr", ADDR],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 15
        while True:
            try:
                urllib.request.urlopen(f"http://{ADDR}/v1/stats", timeout=1)
                break
            except OSError:
                if time.time() > deadline:
                    raise RuntimeError("node did not start") from None
                time.sleep(0.2)
        cls.db = KalpakClient(f"http://{ADDR}")
        cls.fp = ModelFingerprint("test/vllm-model", "tok", "fp16/paged-16")
        cls.agent = "0a" * 32
        cls.db.register_agent(cls.agent, "vllm-connector-test")

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        cls.proc.wait(timeout=10)
        cls.tmp.cleanup()

    def test_local_chaining_matches_server(self):
        """With blake3 installed, local chaining must be byte-identical to
        the server's chain (same CacheKey hashes)."""
        try:
            import blake3  # noqa: F401
        except ImportError:
            self.skipTest("blake3 not installed")
        tokens = list(range(CHUNK_TOKENS * 3 + 17))  # trailing partial chunk
        local = chain_keys(self.db, self.fp, tokens)
        # Server-side chain over the same chunks.
        server, parent = [], None
        for at in range(0, CHUNK_TOKENS * 3, CHUNK_TOKENS):
            chunk = tokens[at : at + CHUNK_TOKENS]
            key = (
                self.db.extend_key(parent, chunk)
                if parent
                else self.db.cache_key(self.fp, chunk)
            )
            server.append(key)
            parent = key
        self.assertEqual(local, server)

    def test_lookup_math_and_save_plan(self):
        tokens = list(range(1000, 1000 + CHUNK_TOKENS * 4))
        planner = KalpakPlanner(self.db, self.fp)

        # Cold: nothing matched, regardless of computed tokens.
        self.assertEqual(planner.matched_tokens("r1", tokens, 0), 0)

        # Offload the first two chunks, as the worker would after prefill.
        plan = planner.save_plan("r1", prefill_chunks=2)
        self.assertEqual(len(plan.keys), 2)
        self.assertIsNone(plan.parents[0])
        self.assertEqual(plan.parents[1], plan.keys[0])
        payloads = [b"chunk-0" * 100, b"chunk-1" * 100]
        ids = self.db.put_blocks(payloads)
        chain, acc = [], []
        for key, parent, bid in zip(plan.keys, plan.parents, ids):
            acc.append(bid)
            chain.append((key, list(acc), parent))
        self.db.bind_chain(self.agent, chain)

        # Warm: a new request with the same prompt matches 2 chunks...
        planner2 = KalpakPlanner(self.db, self.fp)
        matched = planner2.matched_tokens("r2", tokens, 0)
        self.assertEqual(matched, 2 * CHUNK_TOKENS)
        load = planner2.take_load_plan("r2")
        self.assertEqual(load.kalpak_ids, ids)

        # ...and already-computed tokens are not double-counted.
        planner3 = KalpakPlanner(self.db, self.fp)
        self.assertEqual(
            planner3.matched_tokens("r3", tokens, CHUNK_TOKENS),
            CHUNK_TOKENS,
        )
        self.assertEqual(
            planner3.matched_tokens("r4", tokens, 3 * CHUNK_TOKENS), 0
        )

    def test_paged_codec_roundtrip(self):
        """The codec must reproduce pages exactly through encode/decode.
        numpy arrays mimic torch paged layers ([2, blocks, bs, h, d])."""
        try:
            import numpy as np
            import torch
        except ImportError:
            self.skipTest("torch/numpy not installed")
        from kalpak_connector import PagedKvCodec

        torch.manual_seed(7)
        layers = {
            f"layer.{i}": torch.randn(2, 8, 16, 4, 8, dtype=torch.float32)
            for i in range(3)
        }
        codec = PagedKvCodec()
        codec.register(layers)

        original = {k: v.clone() for k, v in layers.items()}
        payload = codec.encode_chunk([1, 3])

        # Scramble the target pages, then restore them from the payload.
        for v in layers.values():
            v[:, [1, 3]] = -1.0
        codec.decode_chunk(payload, [1, 3])
        for name in layers:
            self.assertTrue(
                torch.equal(layers[name], original[name]),
                f"codec did not restore {name} exactly",
            )


if __name__ == "__main__":
    unittest.main()
