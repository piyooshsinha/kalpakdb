# Security Policy

## Reporting

Report vulnerabilities privately via GitHub Security Advisories
("Report a vulnerability" on the repo's Security tab). Please do not open
public issues for exploitable bugs. Expect an acknowledgement within a week.

## Model (current, v0.3.x)

- **Write attribution**: with `--require-signatures`, every metadata
  mutation must carry an Ed25519 signature by the owning agent, verified
  before entering consensus. Replay reproduces only idempotent state.
- **Transport**: client-facing API serves TLS (`--tls-cert/--tls-key`),
  no cleartext fallback. **Node-to-node traffic is NOT yet mutually
  authenticated** — run the cluster mesh on a trusted private network.
  Mesh mTLS is on the roadmap.
- **Reads are open** (stats, metrics, lookups, block fetch by content
  hash). Do not expose nodes holding sensitive context to untrusted
  networks; block ids are unguessable but lookups are not access-checked.
- Blocks are content-addressed: payload integrity is verified on every
  read; a peer cannot poison the store with mismatched bytes.
