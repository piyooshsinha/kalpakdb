# KalpakDB × LangChain

Conversation memory for LangChain agents on KalpakDB — the thesis in
practice: the framework keeps its ontology, KalpakDB is the substrate.

Each turn is a content-addressed block; each session is a prefix chain with
parent links, so:

- **identical turns deduplicate across sessions** (a shared system prompt is
  stored once, cluster-wide),
- **sessions resume from the server**: a new process replays the chain via
  the replicated prefix tree — no local state,
- every turn is **bound to the agent's Ed25519 identity** and auditable in
  the dashboard's memory explorer (lineage = conversation order).

```python
from kalpakdb_langchain import KalpakMessageHistory

history = KalpakMessageHistory("http://127.0.0.1:7411", agent=agent_hex,
                               session_id="thread-42")
history.add_message("user", "What is the capital of France?")
history.add_message("assistant", "Paris.")
history.messages()
```

With `langchain-core` installed, `KalpakChatMessageHistory` is a drop-in
`BaseChatMessageHistory` for `RunnableWithMessageHistory`. Without it, the
dependency-free `KalpakMessageHistory` works standalone.

Run the tests (spawns a real node):

```sh
cargo build -p kalpakdb
KALPAKDB_BIN=$PWD/target/debug/kalpakdb python3 -m unittest discover -s integrations/langchain/tests
```
