"""KalpakDB chat-message history for LangChain agents.

The thesis in practice: agent frameworks keep their ontologies, KalpakDB is
the substrate. This adapter stores conversation history as content-addressed
blocks with a chained prefix key per turn, so:

- identical conversation prefixes deduplicate across agents (content
  addressing), and equal histories converge on the same chain;
- the server's prefix tree gives lookahead prefetch on replay;
- every turn is bound to the agent's Ed25519 identity — auditable in the
  dashboard's memory explorer like any other agent state.

Works with or without LangChain installed:

- ``KalpakMessageHistory`` is dependency-free (message dicts in/out)
- ``KalpakChatMessageHistory`` (exported only when ``langchain-core`` is
  importable) plugs into ``RunnableWithMessageHistory`` as a drop-in
  ``BaseChatMessageHistory``.

Usage::

    from kalpakdb_langchain import KalpakMessageHistory

    history = KalpakMessageHistory(
        "http://127.0.0.1:7411", agent=agent_hex, session_id="thread-42"
    )
    history.add_message("user", "What is the capital of France?")
    history.add_message("assistant", "Paris.")
    history.messages()   # -> [{"role": "user", ...}, {"role": "assistant", ...}]
"""

from __future__ import annotations

import json
import os
import sys

# The zero-dependency Python SDK ships in this repo; installed deployments
# get it from the kalpakdb package instead.
_REPO_SDK = os.path.join(os.path.dirname(__file__), "..", "..", "..", "clients", "python")
if os.path.isdir(_REPO_SDK):
    sys.path.insert(0, _REPO_SDK)

from kalpakdb import KalpakClient, ModelFingerprint  # noqa: E402

__all__ = ["KalpakMessageHistory"]

# Conversation history is text, not KV tensors, but it rides the same
# substrate: the fingerprint namespaces chat blocks away from cache blocks.
_CHAT_FINGERPRINT = ModelFingerprint("kalpak/chat-history", "json-v1", "utf8")


def _token_of(payload: bytes) -> list[int]:
    """Stable pseudo-token for chaining: first 8 bytes of the payload as
    two u32s. Chaining only needs determinism, not tokenization."""
    import hashlib

    h = hashlib.blake2b(payload, digest_size=8).digest()
    return [int.from_bytes(h[:4], "little"), int.from_bytes(h[4:], "little")]


class KalpakMessageHistory:
    """Append-only conversation history on KalpakDB, one chain per session.

    Each message becomes a content-addressed block; the session's chain key
    deepens turn by turn (parent links feed the server's prefix tree). The
    full history is the chain's deepest binding.
    """

    def __init__(
        self,
        base: str,
        agent: str,
        session_id: str,
        client: KalpakClient | None = None,
    ):
        self.db = client or KalpakClient(base)
        self.agent = agent
        self.session_id = session_id
        # Root key derives from the session id, so the same session always
        # lands on the same chain.
        self._root = self.db.cache_key(
            _CHAT_FINGERPRINT, _token_of(session_id.encode())
        )
        self._tip, self._block_ids = self._replay()

    # -- internal -------------------------------------------------------

    def _replay(self) -> tuple[dict, list[str]]:
        """Resume an existing session by walking the server-side prefix
        tree: every bind carried a parent link, and the agent's bindings
        expose those edges (`extends`), so the chain is recoverable without
        knowing any message content up front."""
        tip, blocks = self._root, []
        if self.db.lookup([self._root]) is None:
            return tip, blocks
        bindings = self.db.agent_bindings(self.agent)
        by_parent = {
            b.get("extends"): b for b in bindings if b.get("extends")
        }
        cur_hash = self._root["prefix_hash"]
        while cur_hash in by_parent:
            child = by_parent[cur_hash]
            tip = child["key"]
            blocks = child["blocks"]
            cur_hash = child["key"]["prefix_hash"]
        return tip, blocks

    # -- public ---------------------------------------------------------

    def add_message(self, role: str, content: str) -> str:
        """Append a turn; returns the block id holding it."""
        next_index = len(self._block_ids)
        # Session identity lives in the CHAIN (root key), deliberately not
        # in the block: identical turns across sessions then deduplicate to
        # one content-addressed block (e.g. a shared system prompt).
        payload_dict = {
            "role": role,
            "content": content,
            "index": next_index,
        }
        # chain tokens must be recorded inside the block so replay can
        # recompute the next key without guessing.
        probe = json.dumps(payload_dict, separators=(",", ":")).encode()
        payload_dict["chain"] = _token_of(probe)
        payload = json.dumps(payload_dict, separators=(",", ":")).encode()

        block_id = self.db.put_blocks([payload])[0]
        new_tip = self.db.extend_key(self._tip, payload_dict["chain"])
        self.db.bind_prefix(
            self.agent,
            new_tip,
            self._block_ids + [block_id],
            parent=self._tip if self._block_ids else self._root,
        )
        # Root must exist for future replays to find the chain.
        if not self._block_ids:
            self.db.bind_prefix(self.agent, self._root, [block_id])
        self._tip = new_tip
        self._block_ids = self._block_ids + [block_id]
        return block_id

    def messages(self) -> list[dict]:
        """Full history, oldest first, verified by content address."""
        out = []
        for bid in self._block_ids:
            msg = json.loads(bytes(self.db.get_block(bid)))
            out.append({"role": msg["role"], "content": msg["content"]})
        return out

    def clear(self) -> None:
        """Start a fresh chain for this session (old blocks stay until GC;
        immutability means 'clear' is a new chain, not an erase)."""
        self.session_id = f"{self.session_id}#cleared"
        self._root = self.db.cache_key(
            _CHAT_FINGERPRINT, _token_of(self.session_id.encode())
        )
        self._tip, self._block_ids = self._root, []


# Optional LangChain shim: only exported when langchain-core is present.
try:  # pragma: no cover - exercised in CI where langchain-core is installed
    from langchain_core.chat_history import BaseChatMessageHistory
    from langchain_core.messages import (
        AIMessage,
        BaseMessage,
        HumanMessage,
        SystemMessage,
    )

    _TO_ROLE = {"human": "user", "ai": "assistant", "system": "system"}
    _FROM_ROLE = {
        "user": HumanMessage,
        "assistant": AIMessage,
        "system": SystemMessage,
    }

    class KalpakChatMessageHistory(BaseChatMessageHistory):
        """Drop-in `BaseChatMessageHistory` backed by KalpakDB."""

        def __init__(self, base: str, agent: str, session_id: str):
            self._inner = KalpakMessageHistory(base, agent, session_id)

        @property
        def messages(self) -> list[BaseMessage]:  # type: ignore[override]
            return [
                _FROM_ROLE.get(m["role"], HumanMessage)(content=m["content"])
                for m in self._inner.messages()
            ]

        def add_message(self, message: BaseMessage) -> None:
            role = _TO_ROLE.get(message.type, message.type)
            self._inner.add_message(role, str(message.content))

        def clear(self) -> None:
            self._inner.clear()

    __all__.append("KalpakChatMessageHistory")
except ImportError:
    pass
