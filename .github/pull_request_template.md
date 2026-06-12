**What & why**

**Checklist**
- [ ] `cargo test --workspace` green
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean
- [ ] `cargo fmt --all --check` clean
- [ ] Persistence-path changes include a crash-simulation test
- [ ] No tensor-sized data on the Raft path (see CONTRIBUTING.md ground rules)
