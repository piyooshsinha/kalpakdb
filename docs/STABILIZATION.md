# Correctness and security stabilization

KalpakDB is experimental. A release tag or passing happy-path tests is not
a production-readiness guarantee. This checklist supersedes earlier claims
that the only remaining work needs external hardware.

## Implemented in the current working change

- Persist locally built and received Raft snapshots before advertising them
  to OpenRaft; recover applied state and membership before opening Raft.
- Publish control files using write, file sync, rename, directory sync.
- Persist the committed log position as well as votes and purge metadata.
- Remove an unfinished log suffix before appending again; reject complete
  malformed log records rather than silently hiding subsequent entries.
- Fail startup when purged logs have no covering durable snapshot.
- Capture a storage record location and its segment generation together;
  publish rewritten segment handles and index locations under the same locks.
- Reverify signed mutations even when callers present the internal forwarding
  header. This closes that exemption, not the broader public Raft/admin issue.
- Default CLI automatic GC to disabled; operators must explicitly opt in.
- Hide Raft, cluster-management and admin namespaces on public listeners
  when mesh mTLS is configured, including witnesses. Bootstrap advertises
  the mesh address; witnesses start a mutually authenticated peer listener.
- Reject non-loopback HTTP binds without mesh and all non-loopback plaintext
  gRPC binds before opening storage or starting background tasks. Hostnames
  are rejected for these transports to avoid resolution/bind races.
- Bound gRPC upload concurrency, total chunk/block counts, batch block counts
  and idle waits. Keep admission held until submitted blocking flushes finish,
  even if the client cancels. Reject an unfinished empty block as torn framing.

The snapshot envelope is versioned (`raft-snapshot.json`, version 1) and
contains the existing bincode state-machine payload plus Raft metadata.
Temporary `.pending` files are never recovery sources. Older directories
without snapshots are supported only when their logs have not been purged.
Lost snapshots from older versions cannot be recreated by this change.

## Release-blocking work still pending

1. **Public/internal isolation:** mesh deployments now restrict privileged
   routes to the certificate-authenticated listener. Non-mesh deployments
   are restricted to loopback but still expose unauthenticated Raft/admin
   operations to local processes and trust an internal header; replace
   this legacy development behavior with authenticated local access.
   Expand witness, bootstrap and forwarding negative tests.
2. **GC safety:** replace the active-segment-only grace assumption with
   upload leases and a coordinated GC watermark. Bind-versus-GC and follower
   lag races are not solved by the reader-generation fix. Until then, run
   with `--compact-secs 0` and avoid manual compaction on live deployments.
3. **Data durability contract:** choose acknowledged replica counts, add
   retry/repair and location tracking, and bring gRPC onto the same path.
   Metadata replication alone does not guarantee surviving block copies.
4. **Authorization and signing v2:** namespace ownership, replay-resistant
   revisions/nonces, unambiguous length-prefixed encoding and parent coverage.
5. **Backup consistency:** coordinate metadata and segment generations with
   compaction and replica placement. Current live-tar ordering is not a
   demonstrated concurrent snapshot protocol. Keep offline, verified copies.
6. **Admission control:** gRPC upload counts and idle waits are bounded;
   still add aggregate byte budgets shared with HTTP and reads, absolute
   request deadlines, bounded replication queues and cancellation coverage.
7. **Deployment and CI:** resolve the failed master run, validate feature
   combinations and restart behavior on Linux/macOS, wire backend selection
   into the daemon, and pin a supported inference-engine configuration.
8. **Workload validation:** vLLM GPU correctness and reproducible TTFT/memory/
   throughput comparisons against no external cache and LMCache.

## Verification rules

- Run commands directly and gate on exit status; never infer a green suite
  by counting `test result: ok` lines in a failed process.
- Include restart after actual log purging, repeated torn-tail recovery,
  received-snapshot recovery, corrupt-snapshot rejection, and concurrent
  compaction readers in the durability checks.
- SIGKILL tests do not simulate power loss. Sync ordering is implemented,
  but device/filesystem power-loss validation remains a separate exercise.
- Do not publish a new release or claim production readiness until the
  release-blocking invariants above have adversarial regression coverage.
