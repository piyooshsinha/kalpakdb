# Security Policy

## Reporting

Report vulnerabilities privately via GitHub Security Advisories
("Report a vulnerability" on the repo's Security tab). Please do not open
public issues for exploitable bugs. Expect an acknowledgement within a week.

## Experimental security model

KalpakDB is not production-ready. See [STABILIZATION.md](docs/STABILIZATION.md)
for release blockers. Keep all listeners on a trusted private network.

- **Write attribution**: with `--require-signatures`, every metadata
  mutation must carry an Ed25519 signature by the owning agent, verified
  before entering consensus. There is no nonce/revision enforcement; replay
  can restore stale state, and identity verification is not ownership ACLs.
- **Transport**: client-facing API serves TLS (`--tls-cert/--tls-key`),
  no cleartext fallback. Optional mesh mTLS authenticates the peer listener,
  and mesh deployments hide Raft, cluster-management and administrative
  routes on the public listener; use a cluster client certificate on the
  mesh listener for these operations. Without mesh, those routes remain
  unauthenticated: this mode is for trusted development networks only.
  The internal HTTP header never exempts signed writes from verification.
  Non-mesh internal-header trust remains a release blocker. The optional
  gRPC listener is not covered by the HTTP TLS options.
- **Reads are open** (stats, metrics, lookups, block fetch by content
  hash). Do not expose nodes holding sensitive context to untrusted
  networks; block ids are unguessable but lookups are not access-checked.
- Blocks are content-addressed: payload integrity is verified on every
  read; a peer cannot poison the store with mismatched bytes.
