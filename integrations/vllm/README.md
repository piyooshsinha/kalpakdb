# KalpakDB ⇄ vLLM connector

A [KVConnectorBase_V1](https://docs.vllm.ai/en/latest/api/vllm/distributed/kv_transfer/kv_connector/v1/base/)
implementation that lets vLLM offload and reuse KV caches through KalpakDB —
the integration that turns the prefix store into TTFT savings.

| vLLM contract | KalpakDB primitive |
|---|---|
| `get_num_new_matched_tokens` | longest-prefix lookup over the chained CacheKeys of the prompt |
| `update_state_after_alloc` / `build_connector_meta` | per-request load/save plan |
| `start_load_kv` | `get_block` per chunk (the lookup already pre-warmed them server-side, children included) |
| `wait_for_save` after prefill | `put_blocks` (one group commit) + `bind_chain` (one Raft round) |

Chunking: prompts are split into `KALPAK_CHUNK_TOKENS` (default 256) chunks;
one Kalpak block = one chunk's KV pages across all layers. With `pip install
blake3` the key chain is computed locally (byte-identical to the server);
otherwise keys come from `/v1/keys`.

## Usage

```sh
pip install blake3                         # optional but recommended
export PYTHONPATH=/path/to/kalpakdb/clients/python:/path/to/kalpakdb/integrations/vllm

vllm serve meta-llama/Llama-3.1-8B-Instruct \
  --kv-transfer-config '{
    "kv_connector": "KalpakConnector",
    "kv_role": "kv_both",
    "kv_connector_module_path": "kalpak_connector",
    "kv_connector_extra_config": {"kalpak_url": "http://127.0.0.1:7411"}
  }'
```

## Status — read this before relying on it

- **Tested without a GPU** (`test_connector_logic.py`, runs in CI against a
  real KalpakDB node): key-chain parity with the server, lookup/matched-token
  math, save planning with parent lineage, and exact tensor-codec roundtrips.
- **Needs GPU validation**: the vLLM-coupled glue (scheduler/worker wiring,
  block-id bookkeeping against a live engine). The V1 connector API is
  marked experimental by vLLM and drifts between releases — expect to pin a
  vLLM version and adjust signatures.
- v0 loads synchronously in `start_load_kv`; layer-wise streaming overlap
  (the ObjectCache trick) is the natural upgrade once the basics validate.
