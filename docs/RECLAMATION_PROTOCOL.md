# Reclamation safety review

Status: design review, not an implemented distributed release protocol.
Daemon compaction remains disabled. Local primitives and diagnostics must not
be interpreted as deletion authority. This review covers the combined work in
PRs #13–#20, not the default branch alone.

## Implemented foundations

- Replicated reservations, revision-fenced lease commits, terminal cancellation,
  and opt-in logical expiry. Terminal records retain their hashes and capacity.
- Durable local upload generations and replicated block-reference positions.
  Missing historical evidence is unknown, never a safe zero.
- Observational per-block diagnostics and a dry-run validator that always deny
  release. Samples across storage, journal and Raft are not atomic.
- Per-block active local writer counts, including cancellation-safe blocking
  disk workers; internal generation-checked release-check exclusion under the
  same lock as guarded writer admission.

These protect different boundaries. The local exclusion guard prevents local
guarded payload entry while held; it does not stop a Raft binding or lease from
creating a new reference. A Raft position identifies order within one history,
not a globally unique cluster or restore epoch.

## Release-blocking gaps

| Gap | Why current checks are insufficient | Required property |
| --- | --- | --- |
| Cluster and restore identity | Hashes, local generations and log positions can recur in a cloned or rolled-back history | Decisions bound to cluster, storage incarnation and a non-reusable recovery epoch |
| Metadata exclusion | References can be created after a local snapshot/check | Raft-ordered prepare/commit fence respected by every binding, lease and replica-requirement mutation |
| Local write coordination | Internal exclusion is not connected to a coordinator; network staging is not yet covered | A documented retry/abort contract connecting local exclusion to the metadata fence |
| Stale followers and leaders | Local reads can lag; an isolated node may remember an old decision | Fresh authority and explicit applied-position requirements, with fail-closed partition behavior |
| Legacy inventory | Older/offline writes lack complete guard and reference history | Verified migration/inventory before eligibility, not a missing-entry default |
| Replica responsibility | Desired replica count is not verified current placement | Defined responsibility transfer and verified surviving copies, or explicit end-of-life authorization |
| Crash and restore recovery | Ephemeral reservations disappear; backups may rewind durable evidence | Durable intent/outcome recovery that cannot reuse an old decision after restart or restore |
| Journal and tombstone capacity | Upload, lease and fence maps retain history without reclamation | A separately proven safe compaction/checkpoint protocol; never discard replay protection to free space |

## Candidate coordinator contract (not implemented)

1. Establish authenticated cluster/recovery identity and inventory eligibility.
2. Prepare a Raft-ordered per-block decision against the expected reference
   fence. Preparation must reject live references/responsibilities and prevent
   new ones, or explicitly invalidate/abort the decision when they arrive.
3. Acquire local exclusion against the expected durable upload generation.
   Busy or stale state aborts/retries without modifying durable guards.
4. Revalidate authority and prepare identity after local acquisition. Do not
   hold a blocking mutex across network I/O; the RAII token is not that mutex.
5. Persist an idempotent, identity-bound local intent and resolve its Raft
   outcome using a specified crash-recovery protocol before changing guards.
6. Treat physical segment reclamation as a separate generation/watermark
   operation. Dropping a guard alone must never bypass reference or reader
   safety. The existing low-level compactor is not an online workaround.

The ordering above is a discussion checklist, not a correctness proof or an
API to implement piecemeal. In particular, a quorum read followed by an
uncoordinated local delete still has a time-of-check/time-of-use race.

## Required adversarial tests before enabling any release

- A bind, reservation, renewal, replica requirement or payload reuse races
  each prepare/commit/recovery boundary; at most one compatible outcome wins.
- Losing leadership, quorum or connectivity after preparation never grants
  fresh local deletion authority; lagging followers reject old decisions.
- SIGKILL at every persistence boundary preserves acknowledged live data and
  replay protection, including a second restart after recovery.
- Restore an older backup, clone a storage directory, reuse a node ID, and
  replay a decision from another cluster: all stale identities fail closed.
- Duplicate decisions, coordinator cancellation, partial batches, journal
  exhaustion and I/O errors preserve guards without partial publication.
- Legacy data and replicas with uncertain health remain ineligible. Readers
  spanning a segment replacement retain the correct file generation.

Process-crash tests are not device power-loss validation. No production GC
claim follows from passing local unit tests or a green platform CI run.

## Consolidation evidence and merge gate

On 2026-10-23, GitHub showed PRs #13, #14 and #18 green. PRs #15–#17 had
macOS failures in the same gRPC shared-admission test: the slow HTTP request
could be rejected while a prior gRPC operation was still releasing admission.
PRs #19–#20 were still running when the initial audit was captured. A later
check confirmed #19 green; #20 had macOS and Docker green with Linux pending
before the consolidation correction was pushed. New commits require fresh CI.

The test now uses HTTP `Expect: 100-continue` to confirm that the HTTP handler
has reached its body read after admission. It retries only an explicit 503
from the previous operation, then requires gRPC to return ResourceExhausted
while that body remains withheld. It still checks admission release after
disconnect; it does not weaken the memory-budget assertion.

Before merging, propagate the test correction to the earliest affected branch
and integrate it through the stack, then obtain fresh green checks for each
reviewed head. Historical failures are not retroactively green because a
later branch passes. Merge only in dependency order after explicit approval.
