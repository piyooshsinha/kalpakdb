# kalpakdb (Python client)

Zero-dependency Python client for [KalpakDB](https://github.com/piyooshsinha/kalpakdb).

```python
from kalpakdb import KalpakClient, ModelFingerprint

db = KalpakClient("http://127.0.0.1:7411")
agent = "07" * 32  # the agent's Ed25519 public key, hex

db.register_agent(agent, "py-agent")

fp = ModelFingerprint("meta-llama/Llama-3.1-8B", "tok-hash", "fp16/paged-16")
k0 = db.cache_key(fp, [1, 2, 3])
k1 = db.extend_key(k0, [4, 5])

hit = db.lookup([k0, k1])
if hit is None:
    ids = db.put_blocks([b"kv-chunk-0", b"kv-chunk-1"])  # one group commit
    db.bind_prefix(agent, k0, [ids[0]])
    db.bind_prefix(agent, k1, ids, parent=k0)            # lookahead link
else:
    blocks = [db.get_block(b) for b in hit.blocks]       # warm: prefetched
```

Run tests against a local node:

```sh
KALPAKDB_BIN=../../target/debug/kalpakdb python3 -m unittest discover tests
```
