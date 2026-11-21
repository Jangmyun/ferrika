# Changelog

All notable changes to the Ferrika crates and the `ferrika` CLI. The crates
share one version. Before 1.0, a minor version (0.x) may break the library
API; a patch version (0.x.y) does not. Exception: `ferrika-cli`'s Rust `cli`
module is the command line's own code (argument structs, the command enum),
not a library API; options added to commands change it in patch versions,
and 0.2.0 makes it crate-internal.

## 0.1.1

### Added

- Install without Rust: prebuilt binaries for macOS (Apple silicon, Intel),
  Linux (x86_64, arm64) and Windows (x86_64) on the GitHub releases page,
  `brew install jangmyun/tap/ferrika`, and install scripts
  (`curl -fsSL .../install.sh | sh`, `irm .../install.ps1 | iex`) that check
  the archive's sha256.
- `ferrika mirror -g/--group`: resumable mirroring. A later run with the same
  group continues where the last one stopped.
- `ferrika audit --source cli|gui|mcp`, `--actor TEXT`, `--failed`.
- MCP: changes made by the write tools (`create_topic`, `alter_topic_config`,
  `reset_consumer_group_offsets`) are recorded in the operation history, so
  `ferrika undo` reverts what an assistant changed.
- MCP: `schema_fields` read tool (field paths of a topic's schema, usable in
  `peek_messages`' `where`).
- MCP: `produce_messages` encodes keys with `key_subject`, like values with
  `value_subject`.
- MCP: read tool results also come as `structuredContent`.
- `ferrika completions bash|zsh|fish|powershell|elvish`; Homebrew installs the
  bash, zsh and fish scripts.
- Library: `TaggedDeliveryPipeline`, `KafkaConsumerClient::store_processed_offset`,
  `generate_minimal_move_plan`, `replicas_to_move`, `Endpoint::with_timeout`,
  `SchemaRegistryClient::with_timeout`, `ConnectClient::with_timeout`,
  `audit::recent_matching`.

### Fixed

- The audit log recorded only the request of commands that write records
  (`produce`, `generate`, `restore`, `probe`, `consume --group`, ...). A
  second line now says whether the command finished without error or failed
  (`audit --failed` finds it); the error text is not logged.
- `mirror --src-profile/--dst-profile` was also checked against the global
  profile, so a read-only `-p` profile refused a copy between two other
  clusters. Only the destination is checked now, and its `read_only_brokers`
  and the source's `export_clusters` match the brokers the clusters report
  (a tunnel or an IP address no longer gets around them).
- PII masking: an undecoded Schema Registry record (Avro, Protobuf, JSON
  Schema wire format) that happened to be valid UTF-8 was scanned as free
  text, so names and passwords in it were shown; it is now hidden whole
  (`consume` without a registry or with `--format json`/`--save`, MCP
  `peek_messages`, `compact`, `crypto mask`). JSON with a byte order mark or
  several JSON lines is masked field by field; `crypto audit` samples of
  passwords and tokens no longer keep the last four characters;
  `Set-Cookie` and `Proxy-Authorization` headers are hidden.
- Audit log: a correctly chained line appended later could replace an
  operation's `op_digest` and let an edited history pass `undo`'s check;
  two different digests for one operation now refuse it. `audit verify`
  reports a line with a duplicated key (which hid the entry from readers).
- `undo` of an undo could put a Kafka Connect connector back with empty
  passwords (the undo had stored the blanked secrets); it is now refused for
  want of the secret. Undoing a change is no longer refused because a later
  change to the same thing was itself undone, or because of changes on
  another cluster.
- MCP: `--topics` patterns ignored case, so `dev-*` also allowed writes to
  `DEV-payments`; they are now case-sensitive like topic names.
  `alter_topic_config` now marks deleting a retention override, any
  `cleanup.policy` change and lower `local.retention.*` as destructive
  (stronger confirmation), like a lower `retention.ms`.
- `consume --group` exited 0 when the final offset commit failed (it only
  printed it), so a script took the run as done and the next run read the
  records again. It now fails after the summary.
- An empty `FERRIKA_PROFILE` or `KAFKA_BOOTSTRAP_SERVERS` (`export
  FERRIKA_PROFILE=`) made every command fail with "Profile '' does not
  exist"; it now means not set.
- `mirror` with a consumer group could commit offsets of records whose copy
  was still in flight or had failed, so a restart skipped them. Offsets are
  now committed only after delivery (at least once).
- `--timeout` now applies to Schema Registry, Kafka Connect and webhook
  requests (they always waited 30 s).
- `dlq redrive` and `dlq export` stopped at the first 800 ms gap, so a slow
  first fetch looked like an empty DLQ. They now read up to the high
  watermark and warn when the result is partial.
- `schema check -m *_transitive` showed the mode without `_TRANSITIVE` and
  said "incompatible with latest registered schema". The table shows
  `BACKWARD_TRANSITIVE (N versions)`; JSON keeps `mode` as before and adds
  `transitive: true` and `versions_checked`.
- `ferrika lint` printed the target name twice on every line.
- The Linux binary no longer needs `libz.so.1` (zlib is linked statically);
  release builds check that only glibc / system libraries are linked.
- Secrets (SASL password, OIDC client secret, SCRAM password, webhook URL,
  credentials in Schema Registry and Connect URLs) no longer appear in the
  `Debug` output of parsed command-line arguments.
- `ferrika --version` follows the crate version.
- `ferrika.exe` carries Windows version information (product, version, file
  description) for Explorer, AppLocker and inventory tools.
- Commands such as `health` that failed with a Kafka client connection or
  authorization error ended with a bare `Error:` line; they now show the same
  troubleshooting hints as `topics` and `consume`.
- The hint for a GSSAPI (Kerberos) profile that fails to connect had runs of
  spaces in the middle of its sentence.

### Changed

- At a terminal, commands that need the cluster (`topics`, `consume`,
  `produce`, `health`, ...) stop within seconds when nothing listens at the
  bootstrap servers (connection refused) instead of waiting `--timeout`
  (30 s). Scripts (stderr not a terminal) wait as before.
- **`ferrika reassign plan` (with a cluster) now keeps replicas that are
  already well placed and moves as few as possible** (adding a broker moves
  only its share, removing one moves only its replicas) and prints how many
  replicas move. The JSON shape is the same; `--from-scratch` gives the
  previous round-robin layout. The offline plan (`--partitions`) is unchanged.
- `ferrika --help`: one accurate line per command, no "(CLI mode)" suffix.
- `ferrika-core` docs, README and `examples/quickstart.rs` show how to use it
  as a library; CONTRIBUTING describes the five crates; issue and pull request
  templates.

## 0.1.0

First release on crates.io: `ferrika-core`, `ferrika-governance`,
`ferrika-ops`, `ferrika-mcp`, `ferrika-cli`.
