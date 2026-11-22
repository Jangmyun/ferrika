# Changelog

All notable changes to the Ferrika crates and the `ferrika` CLI. The crates
share one version. Before 1.0, a minor version (0.x) may break the library
API; a patch version (0.x.y) does not. Exception: `ferrika-cli`'s Rust `cli`
module is the command line's own code (argument structs, the command enum),
not a library API; options added to commands change it in patch versions,
and 0.2.0 makes it crate-internal.

## Unreleased

### Added

- `ferrika profile add --preset NAME` fills in the security protocol and
  SASL mechanism for Kafka-compatible services: `confluent-cloud`
  (SASL_SSL + PLAIN), `msk-iam` (SASL_SSL + AWS-MSK-IAM), `msk-scram`
  (SASL_SSL + SCRAM-SHA-512), `redpanda-cloud` (SASL_SSL + SCRAM-SHA-256),
  `warpstream` and `automq` (plaintext). Explicit flags win; the output
  lists what the preset filled in and notes for the service.
- `profile add --schema-registry-user` with `--schema-registry-password-stdin`
  (or `--schema-registry-password`): Schema Registry basic auth such as a
  Confluent Cloud Schema Registry API key, kept in the OS credential store.
- Partition reassignment, SCRAM user and client quota commands check the
  broker's ApiVersions first and say "this cluster does not support ..."
  when a Kafka-compatible broker leaves the API out, instead of failing
  with a dropped connection.
- `docs/COMPATIBILITY.md`: what is tested against which Kafka-compatible
  broker, the presets and known limitations; `docs/e2e/automq-compose.yml`
  (not run yet) for a later AutoMQ end-to-end test.
- `ferrika top` (new alias `ferrika tui`) opens an interactive, read-only
  terminal dashboard when run at a terminal: Overview (brokers online and
  offline, controller, under-replicated and leaderless partitions, health
  findings), Topics (replication, records, produce rate from watermark
  deltas), Consumer groups (state, members, lag with a trend arrow) and
  Findings, with keys to switch views, filter (`/`), sort (`s`), refresh
  (`r`) and quit (`q`). The cluster is read on a background thread, so a
  slow cluster does not freeze the screen; the status line shows the age of
  the data and the last error. The terminal is restored on exit and on a
  panic. It is the default-on `tui` feature of `ferrika-cli` (ratatui 0.30,
  crossterm 0.29). `--plain` keeps the previous printed view, which is also
  used with `-n`, `--json` or when stdout is not a terminal.

### Changed

- `ferrika top`: the refresh interval option is now `--interval` (the old
  `--interval-secs` still works) and defaults to 5 seconds instead of 2.
- Building `ferrika-cli` with default features needs Rust 1.88 (ratatui's
  minimum).

### Fixed

- `dlq export` saved only a 120-character preview of each value; records
  now carry the whole value as `payload` (or `payload_base64` when it is not
  UTF-8, with `payload` null), masked while the policy enforces masking.
- lint B003, B004 and B005 looked at the first broker's settings only, so
  a default replication factor of 1 or automatic topic creation on another
  broker went unreported; every broker is now checked and the finding names
  the brokers when only some have the setting.
- A profile's request timeout (`request_timeout_ms`) was always replaced
  by `--timeout`'s default of 30 s; it now applies unless `--timeout` is
  given.
- `monitor lag GROUP --topic NAME` with a topic that does not exist printed
  an empty table and a total lag of 0; it is now an error.
- `monitor export` answered a request whose first line arrived in several
  TCP segments with 404 (it read once); it now reads the whole line, and a
  client that stops reading no longer holds a task.
- `consume -D` applied `--grep` and `-k` to the undecoded bytes, so text of an
  Avro or Protobuf record (such as `"age":41`) was never found; they now look
  at the decoded text.
- With an unreachable Schema Registry, `consume -D` waited for every record
  (30 s each, ignoring `--timeout`); a failed lookup is now remembered for a
  minute and `--timeout` applies.
- `trace inject --sampled` and `compact analyze --from-beginning` could not
  be turned off (`--sampled false`, `--from-beginning=false`).
- `consume --save` says when it appends to a file that already has records.
- `compact` read keys as lossy text, so binary keys (Avro, varints) that
  differed only in invalid UTF-8 bytes counted as one; such keys are now
  compared and shown as `0x` and hex.
- `reassign verify` called a partition that no move was running for "in
  progress" forever; it now reports it as not moving.
- Preferred leader elections and reassignments asked the broker to wait as
  long as the client's read timeout, so a late answer could be lost; the
  broker now gets a few seconds less.

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
  `audit::recent_matching`, `exporter::scrape_cluster_metrics_checked`,
  `model::acl::kafka_principal`, `model::quota::{canonical_quota_entity,
  same_quota_entity}`, `wire::delete_acl_bindings`,
  `reassign::ensure_not_moving`,
  `SchemaRegistryClient::is_compatible_with_all_versions`,
  `dlq::names_original_topic`, `wire::reassign_or_set_replication`.
- Works with Redpanda: checked on Redpanda 26.3 (one and three nodes,
  with SASL); CI runs the end-to-end smoke test against it. See
  `docs/REDPANDA.md`.
- `dlq` reads the origin and error headers of Spring Kafka
  (`kafka_dlt-*`) and Kafka Connect (`__connect.errors.*`) dead-letter
  topics.
- `consume -D -f json` lines carry the decoded record (`decoded`,
  `decoded_key`, `decoded_with`) next to the raw one.
- `monitor export` serves `ferrika_scrape_success` (0 when a step of the last
  scrape failed). A failed scrape used to look like a cluster with no brokers
  and no lag, so other alerts went quiet.

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
- `produce` from stdin kept a UTF-8 byte order mark (Windows files) in the
  first record's key or value; it is now removed.
- `dump -b` stopped with Ctrl+C before every partition reached its end
  reported success; it now fails as incomplete. After failed records,
  `restore` no longer suggests a `--skip` resume that would pass over them.
- `lint --fix`: G002 could delete a group whose committed offsets had not
  been read (a coordinator loading after a restart); it now reads them
  again before deleting. T003 (lower `min.insync.replicas` to RF-1) is no
  longer an automatic fix: it runs only with `--rule T003`.
- `offset reset --to-timestamp`: a partition whose time lookup failed (a
  moving leader, a timeout) was planned at its end and, with `--execute`,
  committed there, skipping its unread records; the plan is now refused.
  A value in seconds (before 1973 as milliseconds) is refused instead of
  rewinding every partition to its oldest record.
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
- A line of `audit.log` or the operation history cut inside a multi-byte
  character (a crash) made `audit verify` fail and hid every later entry from
  `audit`, undo and history; trimming the history then dropped them.
- Topic create and delete, partition add, group delete and purge waited up to
  60 s for an unreachable broker whatever `--timeout` said.
- `lint` B006 proposed setting a broker config to "" when most brokers leave
  it unset; it now removes that broker's override.
- `acl create -p Group:devs` created an ACL for `User:Group:devs`, which never
  applies.
- `connect list` and `connect status` with `-f json` printed connector
  secrets that the text view masks.
- `connect delete` and `schema delete-subject` exited 0 when the
  confirmation could not be read (no terminal); they now fail.
- `user create --iterations` is checked against Kafka's 4096 to 16384.
- `quota set`/`quota remove` could record no earlier value for a user and
  client-id entity, so `undo` removed the quota instead of restoring it.
- `health --watch --webhook` lost a change when one post failed; it is sent
  again on the next poll. Log dir failures on different brokers no longer
  merge into one finding.
- `monitor lag` without a group showed every group as Stable with lag 0; it
  shows their real state, members and lag. `monitor --watch` no longer ends
  on an error after the first refresh.
- `health --watch` never ran its stalled-consumer check; it now compares
  committed offsets over at least a minute.
- `acl delete` could also delete bindings its preview did not show (User
  resources, token operations, bindings added meanwhile); it deletes only
  the listed ones.
- With `FERRIKA_SECRET_STORE=file`, saving profiles copied the passwords
  held in the OS credential store into `profiles.json` in plain text.
- `profile add`/`import` over an existing profile dropped its read-only mark
  (kept now) and silently dropped its secrets and URLs (now listed).
- `KAFKA_BOOTSTRAP_SERVERS` replacing the brokers of the profile in use is
  now warned about: that profile's credentials go to those brokers.
- `consume --to-timestamp` with a time in the past could wait forever; a
  partition whose `--from-timestamp` lookup failed was skipped silently.
- `consume` exited 0 when it read nothing and reported errors (no read
  permission).
- `reassign plan` accepted offline brokers as targets.
- `reassign plan` and `topics set-replication` read a partition being moved
  as having its old and new replicas, planning a higher replication factor
  or dropping replicas still being copied; they now refuse while a move
  runs, and the undo record keeps the pre-move replicas.
- `compact snapshot` wrote 60-character previews instead of the values.
- `schema check` without `-m` judged locally under BACKWARD and could
  disagree with the registry; it now asks the registry under the subject's
  level (`-m` keeps the local what-if).
- `dlq redrive --execute` sent records without an origin header to a topic
  guessed from the DLQ's name; that now needs `-t`.
- On a cluster without the ElectLeaders API (Redpanda), `topics
  elect-leaders` failed with a broken-connection error; it now says the
  cluster does not support it.
- Replication factor changes (`topics set-replication`, lint fix, undo)
  failed on Redpanda, which refuses them through reassignment; the topic's
  `replication.factor` is set there instead.
- `lint` reported T008 on every topic when only the cluster defaults put
  segment.ms above retention.ms (Redpanda's defaults); it is one cluster
  finding now. B003 and B005 name the Redpanda settings too.
- `consume` says which permissions a refused consumer group needs (Redpanda
  needs Describe on the group as well as Read).
- `acl --local` used `~/.ferrika/acls.json` even when `FERRIKA_CONFIG_DIR`
  moved the configuration; it sits next to `profiles.json` now.

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
