"""End-to-end agent workflow through the Python client, against a real node."""

import os
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from kalpakdb import KalpakClient, KalpakError, ModelFingerprint  # noqa: E402

BIN = os.environ.get("KALPAKDB_BIN", "target/debug/kalpakdb")
ADDR = "127.0.0.1:17581"


class WorkflowTest(unittest.TestCase):
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

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        cls.proc.wait(timeout=5)
        cls.tmp.cleanup()

    def test_full_workflow(self):
        db = KalpakClient(f"http://{ADDR}")
        agent = "0f" * 32
        db.register_agent(agent, "py-agent")

        fp = ModelFingerprint("test/model", "tok", "fp16/paged-16")
        k0 = db.cache_key(fp, [1, 2, 3])
        k1 = db.extend_key(k0, [4, 5])
        self.assertNotEqual(k0["prefix_hash"], k1["prefix_hash"])
        # Determinism: same inputs, same key.
        self.assertEqual(k0, db.cache_key(fp, [1, 2, 3]))

        # Miss before binding.
        self.assertIsNone(db.lookup([k0, k1]))

        # Batch offload (one group commit), then bind both depths with a
        # lookahead parent link.
        ids = db.put_blocks([b"py-kv-chunk-0", b"py-kv-chunk-1"])
        self.assertEqual(len(ids), 2)
        db.bind_prefix(agent, k0, [ids[0]])
        db.bind_prefix(agent, k1, ids, parent=k0)

        hit = db.lookup([k0, k1])
        self.assertIsNotNone(hit)
        self.assertEqual(hit.depth, 1)
        self.assertEqual(hit.blocks, ids)
        self.assertEqual(db.get_block(ids[1]), b"py-kv-chunk-1")

        # Single put dedups against the batch.
        again = db.put_block(b"py-kv-chunk-0")
        self.assertEqual(again, ids[0])

        # Observability: the agent shows up with its bindings.
        agents = db.list_agents()
        self.assertEqual(len(agents), 1)
        self.assertEqual(agents[0]["display_name"], "py-agent")
        self.assertEqual(agents[0]["bindings"], 2)
        bindings = db.agent_bindings(agent)
        self.assertEqual(len(bindings), 2)

        # Stats and GC respond.
        stats = db.stats()
        self.assertEqual(stats["control_plane"]["agents"], 1)
        gc = db.compact()
        self.assertEqual(gc["segments_rewritten"], 0)  # all in active segment

        # Typed errors.
        with self.assertRaises(KalpakError) as ctx:
            db.get_block("ab" * 32)
        self.assertEqual(ctx.exception.status, 404)


if __name__ == "__main__":
    unittest.main()
