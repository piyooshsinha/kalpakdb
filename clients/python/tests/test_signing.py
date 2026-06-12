"""Signed writes through the Python client against --require-signatures.

Skipped when the optional 'cryptography' package is absent (the SDK is
otherwise stdlib-only); CI installs it so this always runs there.
"""

import os
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from kalpakdb import KalpakClient, KalpakError, ModelFingerprint  # noqa: E402

try:
    from kalpakdb import Ed25519Signer
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (  # noqa: F401
        Ed25519PrivateKey,
    )
    HAVE_CRYPTO = True
except ImportError:
    HAVE_CRYPTO = False

BIN = os.environ.get("KALPAKDB_BIN", "target/debug/kalpakdb")
ADDR = "127.0.0.1:17592"


@unittest.skipUnless(HAVE_CRYPTO, "needs the optional 'cryptography' package")
class SignedWritesTest(unittest.TestCase):
    proc: subprocess.Popen
    tmp: tempfile.TemporaryDirectory

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.proc = subprocess.Popen(
            [BIN, "serve", cls.tmp.name, "--addr", ADDR, "--require-signatures"],
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
        cls.proc.wait(timeout=10)
        cls.tmp.cleanup()

    def test_signed_workflow_and_rejection(self):
        signer = Ed25519Signer(bytes([7]) * 32)
        agent = signer.agent

        # Unsigned writes are rejected with 401...
        unsigned = KalpakClient(f"http://{ADDR}")
        with self.assertRaises(KalpakError) as ctx:
            unsigned.register_agent(agent, "unsigned")
        self.assertEqual(ctx.exception.status, 401)

        # ...while the signing client completes the whole workflow.
        db = KalpakClient(f"http://{ADDR}", signer=signer)
        db.register_agent(agent, "py-signed")

        ids = db.put_blocks([b"kv-py-0", b"kv-py-1"])
        fp = ModelFingerprint("test/model", "tok", "fp16/paged-16")
        k0 = db.cache_key(fp, [1, 2])
        k1 = db.extend_key(k0, [3])
        db.bind_prefix(agent, k0, [ids[0]])
        db.bind_chain(agent, [(k1, ids, k0)])

        hit = db.lookup([k0, k1])
        self.assertIsNotNone(hit)
        self.assertEqual(hit.depth, 1)

        # A signer with a different key cannot write as this agent.
        intruder = KalpakClient(f"http://{ADDR}", signer=Ed25519Signer(bytes([8]) * 32))
        with self.assertRaises(KalpakError) as ctx:
            intruder.register_agent(agent, "forged")
        self.assertEqual(ctx.exception.status, 401)

        # Reads stay open to everyone.
        self.assertIn("control_plane", unsigned.stats())
