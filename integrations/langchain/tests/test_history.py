"""KalpakMessageHistory against a real node: append, replay, dedup."""

import os
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from kalpakdb_langchain import KalpakMessageHistory  # noqa: E402

BIN = os.environ.get("KALPAKDB_BIN", "target/debug/kalpakdb")
ADDR = "127.0.0.1:17593"
AGENT = "0b" * 32


class HistoryTest(unittest.TestCase):
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
        # the history binds as this agent; register it once
        from kalpakdb import KalpakClient

        KalpakClient(f"http://{ADDR}").register_agent(AGENT, "langchain-history")

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        cls.proc.wait(timeout=10)
        cls.tmp.cleanup()

    def test_append_and_read_back(self):
        h = KalpakMessageHistory(f"http://{ADDR}", AGENT, "thread-1")
        h.add_message("user", "What is the capital of France?")
        h.add_message("assistant", "Paris.")
        h.add_message("user", "And of Italy?")
        self.assertEqual(
            [m["content"] for m in h.messages()],
            ["What is the capital of France?", "Paris.", "And of Italy?"],
        )
        self.assertEqual(h.messages()[0]["role"], "user")
        self.assertEqual(h.messages()[1]["role"], "assistant")

    def test_replay_resumes_an_existing_session(self):
        h1 = KalpakMessageHistory(f"http://{ADDR}", AGENT, "thread-2")
        h1.add_message("user", "remember me")
        h1.add_message("assistant", "always")

        # A brand-new object for the same session replays the chain.
        h2 = KalpakMessageHistory(f"http://{ADDR}", AGENT, "thread-2")
        self.assertEqual(
            [m["content"] for m in h2.messages()], ["remember me", "always"]
        )
        # ...and continues appending where it left off.
        h2.add_message("user", "still there?")
        self.assertEqual(len(h2.messages()), 3)

    def test_sessions_are_isolated(self):
        a = KalpakMessageHistory(f"http://{ADDR}", AGENT, "thread-a")
        b = KalpakMessageHistory(f"http://{ADDR}", AGENT, "thread-b")
        a.add_message("user", "only in a")
        self.assertEqual(b.messages(), [])

    def test_clear_starts_a_fresh_chain(self):
        h = KalpakMessageHistory(f"http://{ADDR}", AGENT, "thread-3")
        h.add_message("user", "old life")
        h.clear()
        self.assertEqual(h.messages(), [])
        h.add_message("user", "new life")
        self.assertEqual([m["content"] for m in h.messages()], ["new life"])

    def test_identical_turns_deduplicate_storage(self):
        from kalpakdb import KalpakClient

        db = KalpakClient(f"http://{ADDR}")
        before = db.stats()["data_plane"]["blocks"]
        # Two different sessions with an identical first message: the
        # message block is content-addressed, so it is stored once.
        h1 = KalpakMessageHistory(f"http://{ADDR}", AGENT, "dedup-x")
        h2 = KalpakMessageHistory(f"http://{ADDR}", AGENT, "dedup-y")
        h1.add_message("user", "shared system prompt")
        mid = db.stats()["data_plane"]["blocks"]
        h2.add_message("user", "shared system prompt")
        after = db.stats()["data_plane"]["blocks"]
        self.assertEqual(mid - before, 1)
        self.assertEqual(after, mid, "identical message must deduplicate")


if __name__ == "__main__":
    unittest.main()
