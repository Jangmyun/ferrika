# Ferrika

**A single-binary Kafka command-line client, written in Rust. No JVM.**

Ferrika (Ferris + Kafka) is a native CLI for working with Apache Kafka
clusters: topics, producing and consuming, consumer group lag, ACLs,
Schema Registry, Kafka Connect, health and config checks, backup and restore,
copying records with filters and PII masking (`transform`), and a terminal
dashboard (`top`). It starts in milliseconds, links `librdkafka` statically
and needs no Java runtime.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

> Status: pre-1.0 (`0.x`). Command names and flags can still change.

Works with Apache Kafka and Kafka-compatible brokers and services:
Redpanda (tested in CI), and Confluent Cloud, Amazon MSK, Redpanda Cloud,
WarpStream and AutoMQ through connection presets
(`ferrika profile add --preset`). What is checked and the known limitations
are in [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md).

A commercial desktop app is available separately; it is not part of this
repository.

## Install

Linux and macOS (x86_64 and arm64), into `~/.local/bin`:

```bash
curl -fsSL https://github.com/Jangmyun/ferrika/releases/latest/download/install.sh | sh
```

Windows (PowerShell), into `%LOCALAPPDATA%\Programs\ferrika\bin`, added to
the user `PATH`:

```powershell
irm https://github.com/Jangmyun/ferrika/releases/latest/download/install.ps1 | iex
```

The scripts check the archive's sha256 before installing. Set
`FERRIKA_VERSION` (for example `v0.2.0`; binaries exist from `v0.1.1` on) to
pin a release,
`FERRIKA_INSTALL_DIR` to install elsewhere, and on Windows
`FERRIKA_NO_MODIFY_PATH=1` to leave `PATH` alone.

Homebrew (macOS, Linux):

```bash
brew install jangmyun/tap/ferrika
```

Shell completion: `ferrika completions bash|zsh|fish|powershell` prints the
script (`ferrika completions --help` says where to put it); Homebrew installs
it for you.

The archives the scripts and Homebrew install are on the
[releases page](https://github.com/Jangmyun/ferrika/releases). They are built
with the `ssl` feature (TLS and SCRAM, see [Building](#building)).

From crates.io, with a recent stable Rust toolchain (1.89 or newer), CMake and
a C compiler:

```bash
cargo install ferrika-cli
```

Or build from source:

```bash
git clone https://github.com/Jangmyun/ferrika.git
cd ferrika
cargo build --release -p ferrika-cli
./target/release/ferrika --version
```

The binary is `target/release/ferrika` (`ferrika.exe` on Windows). See
[Building](#building) for the TLS/SCRAM build and platform notes.

## Quick start

```bash
# 1. Save a cluster profile (stored in ~/.ferrika/profiles.json) and make it the default
ferrika profile add local -b localhost:9092 --set-active

# 2. List the topics
ferrika topics list

# 3. Read the first 10 records of a topic from the beginning
ferrika consume orders -b -n 10 --pretty-json
```

Global options go before the subcommand: `-p/--profile <name>` picks a saved
profile, `-b/--bootstrap-servers` overrides it, `-f json` switches the output
to JSON, `--timeout <s>` sets the request timeout. `ferrika <command> --help`
lists every option.

At a terminal, a command that needs the cluster stops within seconds when
nothing listens at the bootstrap servers (connection refused). In scripts
(stderr redirected) it waits up to `--timeout` instead, so a broker that is
still starting can answer.

## Main commands

| Task | Commands |
| --- | --- |
| Clusters and profiles | `profile` (add, use, show, import a client `.properties` file, read-only guard), `cluster`, `topology` |
| Topics | `topics` (list, describe, create, clone, config, purge, delete, histogram, analyze, ...), `compact`, `reassign` |
| Records | `produce`, `consume` (regex, key, header and `--where` expression filters), `generate`, `mirror`, `transform` (filter, reshape and mask while copying), `dump`, `restore`, `diff`, `dlq`, `trace`, `probe` |
| Consumer groups | `monitor`, `top`, `offset` (inspect and reset) |
| Checks | `health` (exit code for CI), `lint` (config rules with fixes), `report`, `crypto audit` (PII scan), `bench` |
| Schemas and integrations | `schema`, `connect`, `acl`, `quota`, `user` |
| Change safety | `audit`, `history`, `undo`, `policy` |
| AI assistants | `mcp` |
| RabbitMQ (preview) | `rabbit` (queues, exchanges, bindings, peek, publish, DLQ inspect and redrive; separate RabbitMQ profiles) |

Examples for each are in the [command reference](docs/CLI.md).

## Terminal dashboard

`ferrika top` (alias `ferrika tui`) opens a live, read-only dashboard in the
terminal (Windows Terminal, PowerShell and Unix terminals):

```bash
ferrika -p prod top                 # refresh every 5 s
ferrika -p prod top --interval 2 --sort-by throughput
```

| View | Shows |
| --- | --- |
| 1 Overview | Cluster id, brokers online and offline, controller, topic and partition counts, under-replicated and leaderless partitions, total lag, produce rate, the first health findings |
| 2 Topics | Partitions, replication factor, under-replicated and offline partitions, records retained, produce rate (from watermark changes between refreshes) |
| 3 Consumer groups | State, members, total lag and its trend since the last refresh |
| 4 Findings | The `ferrika health` findings, most severe first |

Keys: Tab or Left/Right switch views (or 1-4), Up/Down or j/k move, `/`
filters by name, `s` sorts by the next column and `S` reverses it, `r`
refreshes now, `?` lists the keys, `q` or Esc quits. The cluster is read in
the background, so a slow or unreachable cluster leaves the last data on
screen with its age and the error in the status line. The dashboard reads
metadata, watermarks and committed offsets only; it never changes the cluster
and shows no record contents.

Without a terminal, or with `--plain`, `-n <count>` or `--json`, `top` prints
a refreshing text view or one JSON snapshot instead. The dashboard is the
default-on `tui` cargo feature of `ferrika-cli` (ratatui and crossterm);
build with `--no-default-features` to leave it out.

## Stream transform

`ferrika transform` copies records to another topic or cluster through a
short pipeline of steps, applied in the order given: filter, drop, rename,
set, rekey, mask and sample. It previews by default and writes only with
`--execute`:

```bash
# Preview, then copy a masked subset of production orders to staging
ferrika -p prod transform orders --to orders --dst-profile staging \
  --mask --filter '$.amount > 100' --drop user.email --from-beginning --until-end
ferrika -p prod transform orders --to orders --dst-profile staging \
  --mask --filter '$.amount > 100' --drop user.email --from-beginning --until-end --execute
```

Writes go through the same policy and audit checks as `mirror`. It is a
stateless, at-least-once copy tool for debugging and test data, not a stream
processor: no joins, windows or exactly-once delivery. See
[the command reference](docs/CLI.md#stream-transform-ferrika-transform).

## Policy, audit and MCP

- **Read-only profiles and an audit log.** `ferrika profile read-only prod`
  refuses every command that changes that cluster. Each change is appended to
  `~/.ferrika/audit.log` as a hash-chained line, and `ferrika audit verify`
  detects an edited, inserted or removed line.
- **Signed company policy.** A security team can sign one policy file
  (Ed25519) with `ferrika policy keygen | sign | verify | show`. Installed
  machine-wide, it makes read-only targets, forbidden actions, PII masking
  and record-export limits mandatory, and can forward audit entries to a
  SIEM or anchor them in the OS log. Environments (dev, staging, prod
  tiers matched by broker or profile name) can add their own read-only and
  forbid rules and refuse or require masking for copies from a higher tier
  to a lower one (`copy_down`); `mcp_write` limits what AI assistants may
  change. Read the
  [threat model in docs/POLICY.md](docs/POLICY.md#threat-model-and-what-open-source-changes)
  first: because the source is open, enforcement relies on allowing only
  signed official binaries.
- **History and undo.** Changes are recorded with the state they replaced;
  `ferrika history` lists them and `ferrika undo` reverts one after checking
  that nothing changed since.
- **MCP server.** `ferrika mcp` serves read-only cluster tools to AI assistants
  over stdin/stdout, including a transform preview and (preview) RabbitMQ
  queue, exchange and binding listings. `--allow-write` adds a small set of
  write tools (produce, create topic, change config, reset offsets, run a
  transform within the cluster) that are planned, confirmed and audited, and
  still pass the same policy and read-only checks. Their changes land in the
  operation history, so `ferrika undo` reverts what an assistant changed.

```bash
claude mcp add ferrika-prod -- ferrika -p prod mcp
```

## Building

The repository is a Cargo workspace. The root manifest is virtual, so select
the crate with `-p`:

| Crate | What it is |
| --- | --- |
| `crates/ferrika-core` | Kafka clients, record formats (Avro, Protobuf, JSON Schema, PII masking) and the data model. |
| `crates/ferrika-governance` | Signed company policy, hash-chained audit log, audit anchors and SIEM forwarding. |
| `crates/ferrika-ops` | Backup, mirror, DLQ, diff, lint, health, reports and change history with undo. |
| `crates/ferrika-mcp` | The MCP server behind `ferrika mcp`. |
| `crates/ferrika-cli` | The `ferrika` command line. |
| `crates/ferrika-rabbitmq` | Preview: RabbitMQ management API client, RabbitMQ profiles and AMQP dead-letter redrive behind `ferrika rabbit`. |

The crates are published on [crates.io](https://crates.io/crates/ferrika-core); until 1.0 their API can change.

Requirements:

- Rust 1.89 or newer
- CMake and a C compiler (the bundled `librdkafka` is built from source)
- Windows: MSVC build tools with `rc.exe` on `PATH`
- Optional `ssl` feature: Perl, to build OpenSSL from source (on Windows a
  native Perl such as Strawberry Perl, not the one bundled with Git)

```bash
cargo build --release -p ferrika-cli                 # default build
cargo build --release -p ferrika-cli --features ssl  # TLS (ssl, sasl_ssl) and SCRAM through librdkafka
```

The default build has no OpenSSL dependency. Its native admin requests and the
Schema Registry and Connect clients already speak TLS through rustls;
producing and consuming over `ssl`, `sasl_ssl` or SCRAM need the `ssl`
feature.

Verification:

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
```

An end-to-end smoke test (Python 3, standard library only) runs 35 checks of
the built binary against a throw-away Kafka:

```bash
docker run -d --name ferrika-kafka-test -p 9092:9092 apache/kafka:3.9.0
cargo build --release -p ferrika-cli
python3 tools/e2e/smoke.py            # or: tools/e2e/smoke.py path/to/ferrika --broker host:9092
```

## Exit codes

A failed command exits 1 and prints `Error: ...`; a bad argument exits 2
(clap usage errors, `audit verify`). Checks meant for scripts and CI also fail
with 1, so `&&` and `set -e` work:

| Command | Exit 1 when |
| --- | --- |
| `reassign verify` | any partition is still in progress or failed |
| `trace audit` | coverage is below `--min-coverage` (default 90%); `--json` adds `passed` and `min_coverage` |
| `probe` | no probe came back; with `--max-latency-ms`, also when any probe was lost or p99 exceeds the limit |
| `monitor lag <group>` | the consumer group does not exist (no more "0 lag" for a typo) |
| `crypto audit --fail-on-leak` | PII or a secret was found |

`diff`, `cluster diff` and `schema diff-schema` take `--exit-code` (as
`git diff --exit-code`): exit 0 when there are no differences, 1 when there
are, and 2 on any error. Without it they exit 0 unless they fail.

```bash
ferrika diff orders-v1 orders-v2 --exit-code || echo "topics differ (1) or the diff failed (2)"
```

A reader that closes the pipe (`ferrika consume ... | head`) ends the command
quietly with exit 0.

## Contributing and security

Contributions are welcome under the DCO; see [CONTRIBUTING.md](CONTRIBUTING.md).
Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).
Performance measurements and how they were taken are in
[docs/bench/RESULTS.md](docs/bench/RESULTS.md). Dependency licenses and a
CycloneDX SBOM are in [docs/sbom/](docs/sbom/).

## License

Apache License 2.0, see [LICENSE](LICENSE).
