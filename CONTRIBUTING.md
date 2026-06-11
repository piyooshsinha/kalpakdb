# Contributing to KalpakDB

Thanks for your interest! KalpakDB is early and moving fast — small, focused
contributions land easiest.

## Getting started

```sh
git clone https://github.com/piyooshsinha/kalpakdb
cd kalpakdb
cargo test --workspace          # full suite, includes multi-node cluster tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

CI runs exactly those three commands on Linux and macOS, plus a Docker image
build and smoke test. Green locally means green in CI.

## Project layout

| Crate | What it is |
|---|---|
| `crates/kalpak-core` | Content addressing, cache keys, agent identity. No I/O. |
| `crates/kalpak-storage` | The data plane: segments, tiering, prefix manifest. |
| `crates/kalpak-control` | The control plane: openraft state machine, log store, transport. |
| `crates/kalpakdb` | The node binary: HTTP/WS API, cluster management, CLI. |
| `crates/kalpak-client` | The Rust SDK. |
| `dashboard/` | Optional React observability UI. |

Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) first — especially the
thesis and the metadata/data split, which most design questions come back to.

## Ground rules

- **Metadata and data never mix.** Nothing tensor-sized goes through Raft.
- **Blocks are immutable and content-addressed.** No update-in-place, ever.
- **Crash safety is tested, not assumed.** A change to a persistence path
  needs a test that simulates the crash (see the torn-write tests in
  `kalpak-storage` and the durable-log tests in `kalpak-control`).
- **The witness stays thin.** Anything data-plane on the witness is a bug.
- Tests use real I/O in temp dirs and real HTTP between in-process nodes —
  follow that pattern rather than mocking.

## Pull requests

- One logical change per PR, with tests.
- `cargo fmt` + clippy clean (`-D warnings` is enforced).
- Commit messages: imperative summary line, body explains *why*.

## License

By contributing you agree your work is licensed under Apache-2.0.
