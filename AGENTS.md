# AGENTS.md

Guidance for AI coding agents (and humans) working in this repository.

## Project

Ferrika (Ferris + Kafka) is a single-binary Kafka command-line client written
in Rust, with no JVM. It uses `librdkafka` (through the `rdkafka` crate) for
producing, consuming and metadata, and native Rust code for admin requests,
Schema Registry, Kafka Connect, the policy and the audit log. Usage is in the
[README](README.md) and [docs/CLI.md](docs/CLI.md).

## Build and test

Run these before you finish a change:

```bash
cargo fmt --all --check
cargo check --all-targets
cargo clippy --all-targets -- -D warnings
cargo test
cargo test -p ferrika-core -p ferrika-governance -p ferrika-ops -p ferrika-mcp -p ferrika-cli -p ferrika-rabbitmq
```

The root manifest is virtual. Build the CLI with
`cargo build --release -p ferrika-cli` (add `--features ssl` for TLS and SCRAM
through librdkafka; it needs Perl). `rdkafka` builds the bundled `librdkafka`
with CMake, so a C compiler and CMake must be installed.

End-to-end checks need a throw-away broker (the script creates and deletes
topics, so never point it at a real cluster):

```bash
docker run -d --name ferrika-kafka-test -p 9092:9092 apache/kafka:3.9.0
python3 tools/e2e/smoke.py            # 35 checks against target/release/ferrika
```

## Crates and dependency direction

| Crate | Contents |
| --- | --- |
| `crates/ferrika-core` | `src/error.rs`, `src/model` (domain data, no I/O), `src/codec` (Base64, Avro, Protobuf, JSON Schema, wire format, PII masking; no I/O), `src/files.rs`, `src/notice.rs` and `src/kafka` (cluster, Schema Registry and Connect clients) |
| `crates/ferrika-governance` | Company policy, hash-chained audit log, audit anchors, SIEM forwarding |
| `crates/ferrika-ops` | Operations on top of the clients: backup, mirror, DLQ, diff, lint, health, report, change history and undo |
| `crates/ferrika-mcp` | The MCP server (`ferrika mcp`): read tools (Kafka, transform preview, RabbitMQ listings), policy-gated write tools |
| `crates/ferrika-cli` | `src/cli` (command definitions, console I/O, the policy gate in `cli/app.rs`) and `src/main.rs` (the `ferrika` binary) |
| `crates/ferrika-rabbitmq` | Preview: RabbitMQ management HTTP API client (`client`), typed responses and `x-death` (`model`), RabbitMQ profiles in `rabbit-profiles.json` (`profile`), AMQP dead-letter redrive with lapin (`amqp`, default feature `amqp`); no policy, audit or masking (the CLI applies them). See `docs/plans/multi-broker.md` |

Dependencies point one way: `ferrika-cli` → `ferrika-mcp` → `ferrika-ops` →
`ferrika-governance` → `ferrika-core`; `ferrika-cli` and `ferrika-mcp` →
`ferrika-rabbitmq` → `ferrika-core` (`ferrika-mcp` without the `amqp`
feature). Inside `ferrika-core`, `model` and `codec` never use `kafka`. The CLI
crate root re-exports `ferrika_core::{codec, error, files, kafka, model, notice}` and
the other crates as `governance` and `ops`, so CLI code writes `crate::kafka::...`,
`crate::governance::policy::...` and `crate::ops::mirror::...`.

The library crates are meant for crates.io. Until 1.0 their public items can
still change. `rdkafka` is not re-exported and public signatures use Ferrika
types (`StartOffset`, `CommitMode`, `ClusterMetadata`, offset maps). Keep new public signatures free of `rdkafka` types.

## Policy gate: no wildcard arms

Every command that changes a cluster passes the company-policy and read-only
checks in `crates/ferrika-cli/src/cli/app.rs` (`write_action`, `export_gate`).
These `match` expressions have **no wildcard (`_`) arm, at any level, on
purpose**: adding a command or subcommand must stop compilation until the
author decides whether it writes or exports, so a check cannot be skipped by
forgetting to register it. Keep it that way. If you add a command, classify it
in both functions and add a test for the decision.

## Code rules

- Follow `cargo fmt` and keep `cargo clippy -- -D warnings` clean.
- Avoid `unwrap()` and `expect()` in production code; return `Result` and map
  errors. Tests may use them.
- Prefer small, focused changes and say what changed and why.
- Do not log or print secrets (SASL and SSL passwords, tokens, record values in
  audit entries). Profile secrets belong in the OS credential store.
- `ferrika-core` features `test-util` (hand-built policies for tests) and
  `dev-policy-dir` (reads `FERRIKA_POLICY_DIR` in optimized builds) are for
  tests and demos only. Never enable them in a shipped binary.
- New behaviour needs a unit test; a behaviour that needs a real broker goes
  into `tools/e2e/smoke.py`.
- Documentation is in English. Examples must run against the current CLI;
  check them with `ferrika <command> --help`.

## Releasing

1. Bump `version` in the root `Cargo.toml` (the `[workspace.package]` value
   and the five `ferrika-*` entries under `[workspace.dependencies]`) and run
   `cargo check` so `Cargo.lock` follows; merge that.
2. `cargo publish --workspace` from the merged commit (crates.io).
3. Tag the same commit `v<version>` and push the tag. The release workflow
   (`.github/workflows/release.yml`) builds the CLI for macOS, Linux and
   Windows, creates the GitHub release with the install scripts
   (`tools/release/install.sh`, `install.ps1`), updates the formula in
   [Jangmyun/homebrew-tap](https://github.com/Jangmyun/homebrew-tap)
   (`tools/release/homebrew-formula.sh`) and installs the result with the
   scripts and with brew. The scripts and the formula expect the archive
   layout the workflow builds; change them together.
