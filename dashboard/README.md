# Kalpak Dashboard

React control dashboard for a Kalpak node: live data-plane metrics (blocks,
warm-tier occupancy, hit rate) and Raft control-plane state (leader, term,
log/applied indices, agents, bindings) streamed over `/v1/ws`.

```sh
# in one terminal: a Kalpak node on the default address
cargo run -p kalpakdb -- serve /tmp/kalpak-data

# in another: the dashboard (Vite proxies /v1 to 127.0.0.1:7411)
npm install
npm run dev
```

`npm run build` produces a static bundle in `dist/`.
