# ferrika-governance

The governance layer of [Ferrika](https://github.com/Jangmyun/ferrika), on
top of [`ferrika-core`](https://crates.io/crates/ferrika-core):

- `policy`: a company policy signed with Ed25519 that decides what a command
  may do (read-only clusters, forbidden actions, mandatory PII masking, no
  record export). A tampered policy switches to the strictest mode.
- `audit`: a hash-chained local audit log of operations that changed a
  cluster.
- `audit_anchor`: writes the newest chain position to the Windows Event Log or
  syslog after every append, so a log cut short or replaced is detected.
- `audit_forward`: forwards each audit entry to a SIEM (syslog over UDP, TCP or
  TLS, or a spool folder).

## Features

- `dev-policy-dir`: honour `FERRIKA_POLICY_DIR` in optimized builds (demos and
  end-to-end tests). Do not enable it in a shipped binary.
- `test-util`: hand-built policies and an in-memory audit sink for tests.
  Builds without debug assertions refuse it.

## Stability

Pre-1.0: public items can change between minor versions.

## License

Apache-2.0
