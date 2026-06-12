# Deploying KalpakDB on the two-box topology

The runbook for the reference deployment: a storage-heavy node (Mac Mini,
1TB SSD), a compute node (Intel NUC, 16GB RAM), and a witness for quorum.
Everything below is copy-paste; adjust addresses.

## 0. Build per node

```sh
# Mac Mini (macOS): portable backend
cargo build --release

# NUC (Linux): io_uring + O_DIRECT backend
cargo build --release --features kalpak-storage/uring
```

## 1. Certificates (once, any machine)

```sh
./target/release/kalpakdb cert ./pki --hosts mini.local,nuc.local,127.0.0.1
# copy ./pki to both boxes; clients get kalpak-cert.pem
```

## 2. Start the nodes

```sh
# Mac Mini — storage node, node 1 (bootstrap)
kalpakdb serve /var/lib/kalpak --addr 0.0.0.0:7411 --node-id 1 \
  --warm-mb 1024 --compact-secs 3600 \
  --tls-cert pki/kalpak-cert.pem --tls-key pki/kalpak-key.pem

# NUC — compute node, node 2 (16GB RAM: big warm tier)
kalpakdb serve /var/lib/kalpak --addr 0.0.0.0:7411 --node-id 2 --join \
  --warm-mb 8192 --grpc-addr 0.0.0.0:7412 \
  --tls-cert pki/kalpak-cert.pem --tls-key pki/kalpak-key.pem

# Witness — third vote, runs fine beside either node or on a Pi
kalpakdb witness /var/lib/kalpak-witness --addr 0.0.0.0:7413 --node-id 3
```

Note: cluster-formation calls below use the node-to-node (Raft) addresses,
which stay on the private network per the TLS scope in the README.

## 3. Form the cluster (from anywhere that reaches node 1)

```sh
M=https://mini.local:7411; CA="--cacert pki/kalpak-cert.pem"
curl $CA -X POST -H 'content-type: application/json' \
  -d '{"node_id":2,"addr":"nuc.local:7411"}'    $M/v1/cluster/add-learner
curl $CA -X POST -H 'content-type: application/json' \
  -d '{"node_id":3,"addr":"mini.local:7413"}'   $M/v1/cluster/add-learner
curl $CA -X POST -H 'content-type: application/json' \
  -d '{"voters":[1,2,3]}'                        $M/v1/cluster/promote
```

## 4. Verify health

```sh
curl $CA $M/v1/stats | jq .control_plane     # leader, peers, replication
curl $CA $M/metrics | grep kalpak_raft        # or point Prometheus here
cd dashboard && npm run dev                   # visual: lag, agents, GC
```

## 5. Benchmark (the numbers that go in the README)

```sh
# storage ceiling per node (local, no network):
kalpakdb bench /tmp --blocks 2000 --size-kb 64

# cluster under concurrent agent load, from the NUC against the Mini —
# this measures the REAL network path:
python3 scripts/bench_cluster.py https://mini.local:7411 \
  --ca pki/kalpak-cert.pem --agents 8 --secs 30

# then the same against the local node for the contrast:
python3 scripts/bench_cluster.py https://nuc.local:7411 \
  --ca pki/kalpak-cert.pem --agents 8 --secs 30
```

Record: offload MB/s, lookup p50/p99, warm-hit rate after a re-run (the
TTFT story), and `kalpak_gc_*` after compaction. Compare against an
LMCache setup on the same hardware for the honest table.

## 6. Failure drills (each should be a non-event)

```sh
# kill the leader -> the survivor + witness keep committing (re-run step 4)
# pull the NUC's cable for 30s -> majority keeps serving; plug back; converges
# restart any node -> rejoins from its durable log, no operator action
```

These mirror the CI tests (failover, witness, rejoin, partition) — running
them on real hardware is the final sign-off.
