# Security Policy

## Reporting a vulnerability

Please report security problems privately through GitHub:
[**Report a vulnerability**](https://github.com/Jangmyun/ferrika/security/advisories/new)
(the repository's Security tab). Only the maintainers see the report, and the
fix and advisory are prepared there. Do not open a public issue.

Include what you can:

- the Ferrika version (`ferrika --version`) and operating system
- steps to reproduce, or a proof of concept
- the impact you see (for example: credentials exposed, a read-only profile
  bypassed, a crash from a crafted broker response)

## What to expect

| Step | Target |
| --- | --- |
| Acknowledge the report | within 7 days |
| First assessment (confirmed or not, severity) | within 14 days |
| Fix for a confirmed critical or high issue | within 30 days, sooner when actively exploited |

You will be credited in the release notes unless you ask not to be.
Ferrika is maintained by one person, so these are targets, not guarantees.

## Supported versions

Ferrika is pre-1.0 (`0.x`). Only the latest release receives security fixes.

## Scope

In scope:

- the `ferrika` binary (the CLI, its MCP server and its Prometheus exporter)
  and the `ferrika-core` engine in this repository
- handling of credentials and profiles (`~/.ferrika/`)
- parsing of data from brokers, Schema Registry and Kafka Connect
- read-only profile enforcement, masking, the signed company policy and the
  audit log

Out of scope:

- the separately distributed commercial desktop app, which is not in this
  repository (a flaw in the shared engine that the app also uses is in scope;
  if you are unsure where a problem belongs, use the same private form)
- a build modified to skip the policy checks: Ferrika is open source, so
  anyone can build such a binary. Enforcing a policy relies on allowing only
  signed official binaries; see
  [docs/POLICY.md](docs/POLICY.md#threat-model-and-what-open-source-changes)

- vulnerabilities in Kafka brokers or other servers Ferrika connects to
- issues that need an attacker who already controls the user's account or
  `~/.ferrika/` directory
- third-party dependencies, unless Ferrika uses them in an unsafe way
  (please report those upstream as well)

## Known limitations

These are known and tracked; reports about them are welcome but not new:

- Profile secrets (SASL and SSL key passwords, and passwords embedded in
  Schema Registry or Connect URLs) are kept in the operating system's
  credential store (Windows Credential Manager, macOS Keychain, Linux Secret
  Service). They fall back to plain text in `~/.ferrika/profiles.json` when no
  store is available or `FERRIKA_SECRET_STORE=file` is set.
- The local audit log (`~/.ferrika/audit.log`) can be edited by the user who
  owns it. Its lines are hash-chained, so `ferrika audit verify` detects an
  edited, inserted or removed line, but truncating the end or deleting the
  file is not detected locally.
- A signed company policy ([`docs/POLICY.md`](docs/POLICY.md)) is trusted from a machine-wide
  directory; a local administrator can replace or remove it.
- Release binaries are not code-signed yet.

## Dependencies

A CycloneDX SBOM and a license summary of the CLI are in [`docs/sbom/`](docs/sbom/) (`ferrika-cli*.cdx.json`, `LICENSES-ferrika-cli*.md`).
Regenerate them with `python3 tools/sbom.py --package ferrika-cli` (and `--features ssl`) after changing dependencies.
