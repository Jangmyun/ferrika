# Changelog

All notable changes to the Ferrika crates and the `ferrika` CLI. The crates
share one version. Before 1.0, a minor version (0.x) may break the library
API; a patch version (0.x.y) does not. Exception: `ferrika-cli`'s Rust `cli`
module is the command line's own code (argument structs, the command enum),
not a library API; options added to commands changed it in 0.1 patch
versions, and 0.2.0 made it crate-internal.

## Unreleased

### RabbitMQ: paged listings, TLS with a private CA, quorum-queue peeks

Command line:

- `rabbit queues`, `rabbit exchanges` and `rabbit bindings` take
  `--name TEXT` (case-insensitive substring, filtered by the broker),
  `--regex` (treat `--name` as a regular expression) and `--limit N`
  (default 1000, `0` for all). Queues and exchanges are fetched from the
  management API in pages of up to 500 with only the shown columns, so a
  vhost with thousands of queues no longer comes back as one response that
  can pass the 64 MiB limit. **Changed:** the listings stop at 1000 entries
  by default (stderr says how many more match); pass `--limit 0` for the
  earlier "everything" behaviour.
- TLS options for every `rabbit` command and `rabbit profile add`:
  `--rabbit-ca-cert FILE` (a private CA for `https://` and `amqps://`),
  `--rabbit-client-cert FILE` and `--rabbit-client-key FILE` (mutual TLS),
  also as `FERRIKA_RABBIT_CA_CERT`, `FERRIKA_RABBIT_CLIENT_CERT`,
  `FERRIKA_RABBIT_CLIENT_KEY` and as the profile fields `ca_cert`,
  `client_cert`, `client_key` (absolute paths). An untrusted server
  certificate fails with a hint to give the CA file; a server that requires
  a client certificate fails with a hint to give one. There is no option to
  skip certificate verification.
- **Changed:** `rabbit peek`, `rabbit dlq inspect` and the `rabbit dlq
  redrive` preview refuse a quorum queue with a delivery limit on RabbitMQ
  before 4.0 (or when the broker version cannot be read), where every peek
  counts as a delivery attempt and can dead-letter or drop messages.
  `--allow-delivery-count` reads anyway and shows per message how many more
  peeks keep it in the queue. RabbitMQ 4.0 and later do not count a peek,
  so nothing changes there.

MCP:

- `rabbit_queues`, `rabbit_exchanges` and `rabbit_bindings` filter on the
  broker and fetch in pages. **Changed:** `filter` is now case-insensitive;
  a new `regex` argument treats it as a regular expression.
- `rabbit_peek` and `rabbit_dlq_inspect` refuse a quorum queue whose
  delivery limit a peek would count towards (RabbitMQ before 4.0), with no
  opt-in over MCP; plans of other quorum queues say that a read does not
  count.
- RabbitMQ profiles with `ca_cert` / `client_cert` / `client_key` are used
  with those files by every RabbitMQ tool.

Library (`ferrika-rabbitmq`, `ferrika-core`):

- New: `ferrika_rabbitmq::listing` (`ListQuery`, `Listing`,
  `ManagementClient::list_queues`, `list_exchanges`, `list_bindings`);
  `ferrika_rabbitmq::delivery_limit` (`PeekRisk`, `DeliveryFate`,
  `delivery_count`, `ManagementClient::peek_risk`);
  `ManagementClient::with_tls`; `ferrika_core::kafka::http::TlsFiles` and
  `Endpoint::with_tls_files` (re-exported as
  `ferrika_rabbitmq::client::TlsFiles`). The unbounded
  `ManagementClient::queues`, `exchanges` and `bindings` are unchanged.
- **Breaking** for code that builds these structs with every field listed
  (use `..Default::default()` or `..RabbitProfile::new(url)`): new fields
  `RabbitProfile::{ca_cert, client_cert, client_key}`,
  `RabbitOverrides::{ca_cert, client_cert, client_key}`,
  `ResolvedRabbit::tls` and `AmqpTarget::tls`. `rabbit-profiles.json`
  files from 0.2.0 load unchanged.
- `ResolvedRabbit::client` and `ResolvedRabbit::amqp_target` apply the TLS
  files; `client` fails when a file cannot be read.

### MQTT support (preview)

Added:

- MQTT support (preview): `ferrika mqtt` for MQTT 3.1.1 and 5 brokers
  (Mosquitto, EMQX, HiveMQ, MQTT bridges), in the new crate `ferrika-mqtt`
  behind the `ferrika-cli` cargo feature `mqtt` (on by default; a build
  with `--no-default-features` leaves it out). `mqtt sub FILTER` shows
  messages until `--count` (default 10) or `--duration` (default 30 s);
  `mqtt pub TOPIC -m TEXT` publishes (QoS 1 by default, `--retain`
  explicit; MQTT 5 user properties and content type); `mqtt retained
  FILTER` lists retained messages; `mqtt retained clear TOPIC` deletes one
  (asks first, `--yes` to skip; changes nothing when there is none). Table
  and `-f json` output (`sub` writes JSON Lines). TLS with `mqtts://` and
  the OS trust store or `--mqtt-ca-file`. Every session is clean with a
  fresh client id and no last will; shared subscriptions (`$share/`,
  `$queue/`) are refused. See `docs/plans/mqtt.md`.
- MQTT profiles: `ferrika mqtt profile add|list|use|remove|show`, stored in
  `mqtt-profiles.json` next to `profiles.json`, passwords in the OS
  credential store (`<profile>/mqtt_password`). Broker URLs never carry
  credentials.
- Policy category `messages.delete` (`mqtt retained clear`), which also
  needs `produce` allowed. `mqtt pub` is `produce`; `mqtt sub` and
  `mqtt retained` pass the export gate and mask PII (payloads, topics, MQTT
  5 user properties) under the policy. The policy's broker-address rules
  match the MQTT broker's `host:port`; read-only MQTT profiles refuse `pub`
  and `retained clear`.
- `tools/e2e/mqtt_smoke.py` and `tools/e2e/mosquitto-setup.sh`, and a CI
  job running them against `eclipse-mosquitto:2`.

Compatibility:

- A signed policy whose `forbid` names `messages.delete` is invalid for
  builds up to 0.2.0, which then run in the strictest mode. Roll it out
  only once every client runs a newer build.
- `PolicyAction` has a new variant, `MessagesDelete`; code that matches it
  exhaustively must add it.

## 0.2.0

New: connection presets for Kafka-compatible services, `ferrika transform`,
an interactive `ferrika top` dashboard, RabbitMQ support (preview), policy
environments and the `screen_capture` and `mcp_write` rules, MCP transform
and RabbitMQ tools, Schema Registry references, `-f json` for every command
and a stronger history digest. The library API no longer exposes `rdkafka`
types. This release breaks a few CLI defaults, the library API and older
builds' reading of new policies and history entries; read "Upgrading from
0.1" first.

### Upgrading from 0.1

Toolchain:

- Building from source needs Rust 1.89 or newer (`lapin` and its AMQP
  crates); the crates declare `rust-version = "1.89"`. Prebuilt binaries are
  not affected.

Command line (scripts may need a change):

- `acl delete` without a filter (every binding) needs `--all`; `-f` alone is
  refused.
- `quota set/remove --user default` and `--client-id default` now name an
  entity called `default`. For the default quota use `--user-default`,
  `--client-id-default` or `--ip-default`.
- `ferrika top` at a terminal opens the interactive dashboard. Scripts that
  read its output should pass `--plain` (or `-n`/`--json`; a stdout that is
  not a terminal still gets the printed view). `--interval-secs` is now
  `--interval` (the old name still works) and defaults to 5 s instead of 2.
- A profile's `request_timeout_ms` now applies unless `--timeout` is given;
  before, the 30 s default of `--timeout` always replaced it. Pass
  `--timeout 30` to keep the old value for such a profile.
- `--header-filter KEY=VALUE` matches the exact value (`KEY:VALUE` still
  means "contains"), and repeated filters must all match.
- `monitor lag GROUP --topic NAME` fails when the topic does not exist
  instead of reporting 0 lag.
- `dlq export` records carry the whole value (`payload` or
  `payload_base64`) besides `payload_preview`.
- `topics create` without `-p`/`-r` uses the broker's `num.partitions` and
  `default.replication.factor` instead of 1 (a broker older than Kafka 2.4
  still gets 1). Pass `-p 1 -r 1` to keep the old result.
- `health` exits with status 3 when a check could not be completed (group
  listing, committed or end offsets unreadable) and no finding failed
  `--fail-on` first; before, such a cluster passed as "no groups". Treat 3
  as "unknown" in scripts, or pass `--fail-on never`.
- `health --watch -f json` writes JSON Lines (one object per finding that
  appears or clears); `monitor --watch -f json` is refused.
- `-f json` now prints one JSON document (notes on stderr) for the commands
  that used to print text regardless, such as `acl create/delete`, `undo`,
  `profile add`, `quota set` and `schema register` (list under Fixed).
- Audit forwarding writes `ferrika-audit-<host>-<user>.<random>.jsonl`
  spool files instead of one fixed name; point collectors at
  `ferrika-audit-*.jsonl`. A shared spool folder that group or others may
  write (without the sticky bit) is refused. On Windows a spool folder
  whose DACL lets other accounts delete files, change its permissions or
  write to the files created in it (for example one inheriting broad rights
  under `C:\ProgramData`) is refused; create it with
  `Install-FerrikaPolicy.ps1 -SpoolDir` or tighten its permissions.
- Profiles in a config directory other than `~/.ferrika`
  (`FERRIKA_CONFIG_DIR`, `FERRIKA_CONFIG_PATH`) keep their secrets in the
  credential store as `<profile>/<field>@<hash>`. The first read copies an
  existing `<profile>/<field>` entry, which stays in place; a 0.1 build on
  that directory still reads only the old entry, so secrets changed with
  0.2.0 are not seen there.

Library API (Rust users of the crates):

- New public paths for logic that was private to the CLI (presets,
  transform options, the copy policy check, RabbitMQ connection and view):
  see "Library API for front ends" under Added (#96).
- `ferrika-cli` (#23): the command modules, their argument structs (with
  `ProfileAddArgs::security_protocol`, which became `Option<String>` for
  `--preset`), `Commands` and the fields of `Cli` are crate-internal. Run
  the command line in-process with `cli::app::{Cli, run}` (`Cli::parse_from`)
  or use the library crates; `rabbit` and `transform` keep the few items GUI
  front ends need.
- `ferrika-core` (#25): the SASL OAUTHBEARER and AWS MSK IAM internals
  (SigV4 presigning, credential-file and `credential_process` parsing,
  region guessing, JWT and form encoding, `OidcSource`, `MskIamSource`),
  `parse_properties` and the expression-completion helpers are
  crate-internal. Set `sasl_mechanism` (`OAUTHBEARER`, `AWS_MSK_IAM`) on a
  `ClusterProfile` and let the clients build the token source; read a
  `.properties` file with `ClusterProfile::from_properties`.
- `ferrika-governance` and `ferrika-ops` (#24): about 50 items only their
  own crate used are crate-internal, among them the policy directory
  override helpers (use `policy::policy_dir`, `load_from`),
  `resolve_brokers` and `broker_glob_match` (use `glob_match`,
  `normalize_broker`, `brokers_for_with`), syslog framing, the topology and
  time-histogram building blocks (use `topology::from_metadata` or
  `topology::collect`, `time_histogram::run`/`compute`), lint fix undo files, history constants,
  `dlq::build_redrive_plan` (use `build_redrive_plan_from`),
  `mirror::Verbatim`, `transform::MAX_STEPS` and `Pipeline::uses_fields`.
- `rdkafka` types are gone from the public API (#109, #111):
  `ferrika_core::rdkafka` is removed (depend on `rdkafka` directly if you
  need it); `kafka::consumer::StartOffset` replaces `rdkafka::Offset`,
  `CommitMode` rdkafka's commit mode and `PendingDelivery` its
  `DeliveryFuture`; `KafkaAdminClient::fetch_metadata` returns
  `ClusterMetadata` (`fetch_raw_metadata` is crate-internal) and
  `topology::from_rdkafka_metadata` becomes `from_metadata`/`collect`;
  `KafkaConsumerClient::inner` is removed and `positions()` returns a map.
  Committed offsets are maps: `fetch_committed_offsets(_for)` return
  `HashMap<(String, i32), Option<i64>>`, `commit` takes
  `&[(&str, i32, i64)]`, and `group_lag::committed_partition_list` is now
  `committed_partition_offsets`.
- `ferrika_core::kafka::bounded_read::topic_watermarks` is `async`.
- `PolicyAction` has new variants `MessagesPeek` and `MessagesMove`; a
  `match` on it needs arms for them.
- `PolicyRules` has new fields (`screen_capture`, `mcp_write`,
  `environments`, `default_environment`, `copy_down`); struct literals need
  them or `..Default::default()`.
- `MessageFilter::header_filters` is a `Vec<HeaderFilter>` instead of a
  map, and `with_header` adds a filter instead of replacing one for the
  same key.
- `ferrika_ops::report::ReportContext` takes `product` and
  `product_version`; `Report::ferrika_version` is replaced by `product`,
  `product_version` and `ops_version` (also in `report --format json`).
- `ClusterSnapshot` has a new `warnings` field; struct literals need it
  (`ClusterSnapshot::empty` leaves it empty).
- `ferrika_governance::audit_forward::spool_file` returns
  `Option<PathBuf>` (`None` before the first entry).
- `undo::DigestCheck` has a new `Legacy` variant, and
  `Operation::digest` returns the v2 digest (`digest_v1` is the old one).
  Front ends that decide an undo themselves can use the new
  `undo::prepare_request` (and `undo_eligibility` for lists) to apply the
  same checks as `ferrika undo` (additive).

Company policy:

- The new rules `screen_capture`, `mcp_write`, `environments`,
  `default_environment` and `copy_down` make a policy format 2, and the
  categories `messages.peek` and `messages.move` are new. Builds before
  0.2.0 treat a policy that uses any of them as invalid (unknown rule or
  category) and fall back to the strictest mode. Update every client (CLI,
  MCP servers and other front ends) to 0.2.0 first, then roll out such a
  policy. Policies without them keep working on both versions.

History and undo:

- Entries recorded by 0.2.0 carry a `v2:` digest. A 0.1 build refuses to
  undo them (it reports them as edited), so undo with 0.2.0 or later once
  any client was upgraded. 0.1 entries still verify and undo, shown as
  `legacy digest`; with `-y` undoing one needs `--accept-legacy`.
- `undo` refuses unless the cluster in use is the recorded one (cluster id
  when recorded, else the bootstrap set).

MCP:

- `ferrika mcp --allow-write` also offers the RabbitMQ write tools
  (`rabbit_peek`, `rabbit_dlq_inspect`, `rabbit_publish`,
  `rabbit_dlq_redrive`). Limit them with the
  new `--queues GLOB` and `--exchanges GLOB`; with either set, a redrive
  must name `to_exchange`. `mcp_write` and read-only RabbitMQ profiles
  apply.

### Security

- Audit forwarding spool: new spool files are named
  `ferrika-audit-<host>-<user>.<16 random hex>.jsonl` and created with
  create-new semantics (`0600` on Unix). An existing file is appended to
  only when it is not a link, junction or other non-regular file (opened
  without following links) and is owned by the current account (on Unix
  also: not group/world-writable, one hard link); a file that fails is left
  alone, reported (`audit forwarding: skipping spool file …`) and a new file
  is used. A missing spool folder is created `0700` on Unix; an existing one
  that group or others may write (without the sticky bit), or that another
  non-root account owns, is refused. Before, a fixed, predictable file name
  was appended to, so another account with write access to a shared spool
  folder could pre-create it or swap in a symlink to tamper with or redirect
  forwarded entries. The old fixed-name file is no longer written; point
  collectors at `ferrika-audit-*.jsonl`.
- `ferrika_governance::audit_forward::spool_file` now returns
  `Option<PathBuf>`: the file this account would append to, `None` before
  the first entry.
- Audit forwarding spool on Windows: the spool folder and spool files are
  now checked against their DACL, not only their owner and reparse points.
  A spool file whose DACL (explicit or inherited entries, evaluated in
  order with deny entries) lets another account than the current one,
  SYSTEM or Administrators write, append, delete or change its permissions
  or owner is skipped with a warning and a new file is used. An existing
  spool folder that lets other accounts delete files, change its
  permissions or owner, or write to the files created in it (an
  inheritable entry) is refused with a clear error; letting them only add
  files (the `Install-FerrikaPolicy.ps1 -SpoolDir` folder) stays accepted.
  A missing spool folder is created with a protected owner-only DACL.
  Before, a spool folder under e.g. `C:\ProgramData` that inherited broad
  permissions let other local accounts modify audit spool files already
  written. The policy directory warning of `ferrika policy show` and the
  spool checks now share one implementation, so the policy check also
  honours deny entries before allow entries and reports a symbolic link or
  junction.
- Policy directory: `packaging/windows/Install-FerrikaPolicy.ps1` verifies
  after locking that only Administrators, SYSTEM and TrustedInstaller own
  the folder and policy files or hold write/create/delete/permission rights
  on them, and fails otherwise (a folder that still let users create files
  would let anyone drop a broken `policy.keys` and force strictest mode);
  `packaging/linux/install-ferrika-policy.sh` verifies root ownership and no
  group/world write. `ferrika policy show` warns when the policy directory
  or a policy file may be written by non-administrators (`-f json`:
  `dir_warnings`; Unix: owner and mode bits, Windows: owner and DACL).
- Broker lists in policy checks are split on commas **and whitespace**, so a
  second broker after a space (`prod1:9092 mine:9092`) is no longer read as
  part of the first. `destination_within_source` (the export gate for
  `mirror` and `transform --execute`), `shares_broker`, `read_only_brokers`,
  `export_clusters` and environment broker globs compare normalized
  `host:port` sets (lowercase host, no trailing dot, default port 9092,
  bracketed IPv6, duplicates ignored), and `read_only` globs also see each
  broker in its normalized form (`KAFKA1.prod.corp.` is caught by
  `kafka1.prod.corp:*`).
- History digest version 2. The audit log's `op_digest` used to cover only
  an operation's before- and after-state, so an edited journal line could
  move a recorded change to another cluster (`scope`), mark another
  operation as undone (`undoes`, blocking its undo) or change its `action`
  or `reversibility` without being noticed. New entries record
  `op_digest: "v2:<sha256>"`, which covers every field of the entry (id,
  source, user, scope, action, target, reversibility, limits, info,
  before-state, `undoes`, final after-state and result). The audit log's
  digest decides which rule applies, so an edited journal cannot choose the
  weaker one; 0.1 readers compare the whole string and so refuse a v2
  entry instead of accepting it.
- Entries recorded by 0.1 (64 hex digits, no prefix) still verify with the
  old rule but are shown as `legacy digest (scope not covered)` in
  `history` (new AUDIT column, `digest` in JSON), `history show` and
  `audit verify`. Undoing one prints the recorded and the current cluster
  and asks again; with `-y` it needs `--accept-legacy`.
- `undo` now refuses unless the cluster in use is the recorded one: when
  the entry has a cluster id, the current cluster's id must be read and
  equal (matching bootstrap servers are no longer enough); otherwise the
  bootstrap set must match. The message names both clusters.
- An `undoes` link counts only when the undoing entry's v2 digest matches
  (or, without a policy, when the audit log has no entry for it); an edited
  or legacy link no longer blocks undoing the original, and an edited one
  is logged as `history.tampered`.
- `audit verify` also checks every history entry against its `op_digest`
  and exits 2 when one was edited.

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
  for the AutoMQ end-to-end test (see Changed).
- `schema register` and `schema check` take `--reference NAME=SUBJECT:VERSION`
  (repeatable) for Avro named types, Protobuf imports and JSON Schema `$ref`s
  in other subjects, sent as the Schema Registry API's `references`.
  `VERSION` may be `latest`; it is resolved to the current version number
  before anything is sent or recorded. `schema check` leaves the local
  field-level comparison out when the candidate or the latest version has
  references and gives the registry's verdict (JSON: `local_skipped`,
  `references`); `-m` (a local comparison) is refused with `--reference`.
  `schema get` and `schema subject` show references in the table (JSON
  already had them). Library: `SchemaReference::parse_spec`,
  `SchemaRegistryClient::register_with_references`, `check_compatibility`
  and `resolve_reference_versions`.
- `health --watch` under `-f json` writes JSON Lines, one object per finding
  that appears or clears; `monitor export` prints its URLs as JSON.
- A test walks the command tree and fails for a command that is neither
  known to print JSON under `-f json` nor listed with a reason why not.
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
- `ferrika transform SRC --to DST`: copy records to another topic (also on
  another cluster with `--dst-profile` or `--dst-bootstrap-servers`) through
  steps applied in the order given: `--filter EXPR` (the `consume --where`
  syntax), `--drop FIELD`, `--rename OLD=NEW`, `--set FIELD=JSON`,
  `--rekey FIELD`, `--mask[=redact]` and `--sample N`. Without `--execute` it
  is a dry run that prints the transformed records. Bounded runs with
  `--from-beginning`, `--from-timestamp`, `--max-records` and `--until-end`;
  `-g` commits offsets only after the destination acknowledged the record;
  `-D` decodes Schema Registry values to JSON first. Writes pass the same
  policy checks as `mirror`; under enforced PII masking a run is allowed when
  `--mask` is its first step. The audit result entry records the counts and
  a SHA-256 of the steps, never the steps or record data.
- `ferrika mirror --mask[=redact]`: masks PII in keys, values and header
  values (the transform mask step) before writing, so a mirror passes
  enforced masking and `copy_down: masked`. It cannot be combined with
  `--grep`, `-k` or `--header-filter`, which read raw values.
- Preview: RabbitMQ support in the new crate `ferrika-rabbitmq` and the
  command `ferrika rabbit`, through the management HTTP API: `queues`,
  `exchanges` and `bindings` of a vhost, `peek <queue> -n N` (reads and
  requeues, so the messages are marked redelivered) and `publish <exchange>
  -k <routing-key> -m <body>`, with table and `--format json` output.
  Commands and output may change; design and decisions in
  `docs/plans/multi-broker.md`.
- RabbitMQ profiles: `ferrika rabbit profile add|list|use|remove|show`,
  stored in `rabbit-profiles.json` next to `profiles.json` (separate from
  the Kafka profiles; `ClusterProfile` is unchanged), passwords in the OS
  credential store under `<profile>/rabbitmq_password`. Commands take
  `--rabbit-profile` (default: the active RabbitMQ profile); `--rabbit-url`,
  `--vhost`, `--rabbit-user`, `FERRIKA_RABBIT_PASSWORD` /
  `--rabbit-password-stdin` and `--amqp-url` override it. `--read-only` on
  a profile refuses its writes.
- `ferrika rabbit dlq inspect <queue>`: dead-lettered messages grouped by
  original queue, reason, exchange and routing key from `x-death`, masked
  under the policy. `ferrika rabbit dlq redrive <queue> [--to-exchange X]
  [-k K] [-n N]`: a preview of where each message would go; with
  `--execute` (asks first unless `--yes`) the messages are moved over AMQP
  0-9-1, one at a time, acknowledged only after the broker confirmed the
  republished copy, stopping at the first failure. Redriven messages keep
  their headers (`x-death` included). Credentials in `--rabbit-url` are
  used for AMQP too, with a warning that they can end up in shell
  history. New dependency `lapin` (MIT) in `ferrika-rabbitmq`, behind its
  default feature `amqp`.
- Policy categories `messages.peek` (`rabbit peek`, `rabbit dlq inspect`,
  the `rabbit dlq redrive` preview) and `messages.move` (`rabbit dlq redrive
  --execute`). `rabbit publish` is `produce`, so `forbid: ["produce"]`
  covers both brokers, and a RabbitMQ redrive needs `produce` allowed as
  well as `messages.move`. For `rabbit` commands the policy's
  broker-address rules match both the management and the AMQP
  `host:port`. `PolicyAction` gains `MessagesPeek` and `MessagesMove`.
- Policy environments (format 2): `rules.environments` names environment
  tiers (`"prod": {"tier": 3, "brokers": ["*.prod.corp"], "profiles":
  ["prod-*"]}`), each matched by broker-address and/or profile-name globs,
  with an optional `default_environment` for unmatched clusters. Each
  environment can add `read_only: true` and its own `forbid` categories on
  top of the global ones. `copy_down: allow | masked | forbid` (default
  `allow`) governs record copies from a higher tier to a lower one (or to a
  cluster in no environment): `masked` needs `transform --mask` as the
  first step or `mirror --mask`, `forbid` refuses them. Checked in the
  shared `authorize_copy` and `EffectivePolicy::check`, so `mirror`,
  `transform --execute`, MCP write tools and other front ends apply the
  same rules. `policy show` lists the environments, `profile list` gains an
  `ENV` column (and an `environment` JSON field), and an allowed change's
  audit request entry reads `requested (environment prod)`. See
  docs/POLICY.md "Environments".
- Policy rule `screen_capture: allowed | blocked` (format 2, opt-in, default
  `allowed`): `blocked` asks a desktop front end to keep its window out of
  screenshots, screen recordings and screen shares. No effect in the CLI;
  `policy show` lists it. Library: `ScreenCaptureRule` and
  `EffectivePolicy::screen_capture_blocked_at`.
- Policy rule `mcp_write: allowed | confirm_human | forbidden` (format 2,
  default `allowed`) for AI assistants using `ferrika mcp --allow-write`:
  `forbidden` leaves the write tools out of `tools/list` and refuses a call,
  audited as `policy.blocked` like other policy refusals; `confirm_human` has the server confirm every write through
  MCP elicitation whatever `--confirm` says (a client without elicitation
  gets no write tools). The CLI is unaffected; `policy show` lists it.
  Library: `McpWriteRule` and `EffectivePolicy::mcp_write_at`.
- MCP: `transform_preview` (read tool) runs `ferrika transform` steps
  (`[{"filter": ...}, {"drop": ...}, {"mask": "partial"}, ...]`) on a topic
  from the beginning or `from_timestamp` up to its current end and returns
  up to 50 transformed records with counts (read, kept, filtered, sampled
  out, not JSON); it writes nothing and joins no consumer group. It passes
  the export gate like `peek_messages` (hidden and refused under `export:
  forbidden`, recorded under `audited`), masks returned text unless
  `mask_pii` is false, returns header names but not header values, and under
  enforced masking puts a mask step first so filters see only masked data.
- MCP: `transform_execute` (write tool, `--allow-write`) copies the
  transformed records into another existing topic on the same cluster,
  through the plan/confirm/execute flow of the other write tools. It needs
  `max_records` (at most 10,000; counted against `--max-produce-records`,
  the unwritten part given back), absolute start times only, a destination
  inside `--topics`, and passes the write gate as `produce` plus
  `ferrika_ops::mirror::authorize_copy` (enforced masking refuses it unless
  the first step is `mask`). Arguments naming another cluster are refused
  (`cross_cluster_unsupported`); use the CLI for that. The audit result
  records the counts and the steps' SHA-256, never the steps or record data.
- MCP (preview): `rabbit_queues`, `rabbit_exchanges` and `rabbit_bindings`
  list a virtual host of a RabbitMQ profile (`profile` argument, else the
  active RabbitMQ profile; `vhost`, `filter`; at most 500 entries). They
  read no message bodies and take no URL, so a stored password only goes
  to its profile's broker. `ferrika-mcp` now depends on `ferrika-rabbitmq`
  without its default `amqp` feature (the workspace dependency turns default
  features off; `ferrika-cli` enables `amqp`).
- MCP (preview): RabbitMQ write tools, offered with `--allow-write` and run
  through the plan/confirm/execute flow of the other write tools:
  `rabbit_peek` (up to 20 messages) and `rabbit_dlq_inspect` (up to 50,
  grouped by `x-death` with a sample of 10) are writes because reading
  requeues the messages (marked redelivered); they are gated as
  `messages.peek`, pass the export gate, and return bodies and headers
  PII-masked unless `mask_pii` is false (always masked under enforced
  masking). `rabbit_publish` (one UTF-8 message to an existing exchange,
  gated as `produce`, counted against `--max-produce-records`) fails with
  `unroutable` when no binding takes the message. `rabbit_dlq_redrive` (up
  to 1,000 messages, optional `to_exchange`/`routing_key`) plans from the
  queue's statistics and arguments only, reading no message (so nothing is
  requeued before the move is confirmed; destinations come from each
  message's `x-death`, or `to_exchange`, as it moves, and
  `rabbit_dlq_inspect` shows them first), then moves the messages over AMQP
  with publisher confirms, stopping at the first failure, and reports the
  count per destination; it is gated as `messages.move` plus `produce`. The gates match the RabbitMQ profile and the management
  and AMQP `host:port` like `ferrika rabbit`; a read-only RabbitMQ profile
  refuses them (a read-only Kafka profile does not), and `mcp_write`
  applies. New `ferrika mcp` options `--queues GLOB` and `--exchanges GLOB`
  (repeatable) limit them; by default every queue and every exchange but
  `amq.rabbitmq.*` is allowed, and with either set a redrive must name
  `to_exchange`. Audit entries name queues, exchanges and counts, never
  bodies. `ferrika-mcp` has a new `amqp` feature (off by default, enabled
  by `ferrika-cli`) for `rabbit_dlq_redrive`; without it the crate still
  builds without the AMQP client.
- Library API for front ends (GUI front ends and other library users), so
  they reuse the CLI's own checks instead of copies that could drift. The
  CLI now calls these itself; its behaviour is unchanged (#96).
  - `ferrika_core::kafka::preset`: the connection presets with
    `display_name`, `credential_fields` (what the service calls each
    credential), `credentials_hint` and `notes` that name no CLI flags,
    `resolve` and a structured `missing_settings`.
  - `ferrika_core::kafka::profile::{url_credentials, split_url_credentials,
    url_has_credentials, percent_decode}`: the inverse of
    `url_with_basic_auth`.
  - `ferrika_core::model::policy::PolicyAction::also_requires` and
    `ferrika_governance::policy::extra_category_refusal`: a RabbitMQ redrive
    needs `produce` allowed as well as `messages.move`, in every front end.
  - `ferrika_ops::transform::{check_options, RunOptions, OptionsProblem,
    DecodingTransform, TransformSummary}`: option checks on plain data, the
    per-record decode-then-steps hook and the serializable run summary with
    its audit detail.
  - `ferrika_ops::mirror::authorize_copy` (`CopyCheck`, `CopyCluster`,
    `CopyAuthorized`): the policy checks for copying records between
    clusters (enforced masking unless the copy masks first, the export gate
    for another cluster, the destination's write gate and read-only
    profile) on plain inputs.
  - `ferrika_rabbitmq::connection` (`ResolvedRabbit::resolve`,
    `credentials`, `client`, `amqp_target`) and `ferrika_rabbitmq::view`
    (`ShownMessage` with PII masking, `mask_properties`, `Inspection`,
    `RedrivePreview`, `redrive_outcome`, `parse_headers`).
- Library: `ferrika_core::model::policy::{Environment, CopyDownRule}` and
  the `PolicyRules` fields `environments`, `default_environment` and
  `copy_down`; `ferrika_governance::policy::{EnvironmentMatch, CopyDown}`
  with `EffectivePolicy::{environment, copy_down,
  copy_down_needs_broker_metadata}`.
- Library: `ferrika_ops::history::undo::prepare_request` makes the whole
  undo decision in one place, so the CLI and other front ends cannot
  decide differently: whether the "undone by" mark is believed
  (`UndoneMark`, with the note to show and the edited undoing entry to
  record as `history.tampered`), the audit digest, the scope check against
  the cluster in use (`ScopeCheck`, naming both), the company policy,
  `force` and whether a legacy entry needs its cluster confirmed. It
  returns an `UndoRequest` with the `UndoPlan`, the refusal
  (`UndoRefusal`) with and without `force` and `needs_legacy_confirmation`.
  `undo_eligibility` makes the part of that decision a list can make
  without contacting the cluster (the scope by cluster id when known), so
  a list can mark an entry whose undo would be refused; `listed_on` is the
  rule for which entries a cluster's history lists. `ferrika undo` and
  `ferrika history` use them; their behaviour and output are unchanged.
- `ferrika-ops`: `transform` (the step pipeline), and
  `mirror::run_copy_pipeline` with a `RecordTransform` hook, a preview
  destination and `MirrorConfig::from_timestamp` / `until_end`.
- `ferrika-core`: `Expr::matches_parsed` (evaluate an expression on an
  already parsed JSON value) and `DriverError::partition_eof`.

### Changed

- **Breaking:** `topics create` without `-p/--partitions` or
  `-r/--replication` leaves them to the broker (`num.partitions`,
  `default.replication.factor`, KIP-464), like `kafka-topics.sh`, instead of
  using 1. A broker older than Kafka 2.4 (no CreateTopics v4) gets 1 for
  what was omitted, with a note. The output shows the partition count and
  replication factor the topic was created with (read back from the
  metadata); `--json` adds `broker_defaults_unsupported`. `-p`/`-r` must be
  at least 1. New `KafkaAdminClient::create_topic_with_broker_defaults`,
  `TopicCreation` and `BROKER_DEFAULT` in `ferrika_core::kafka::admin`. The
  MCP `create_topic` tool is unchanged (partitions required, replication
  factor defaults to the broker's).
- **Breaking:** `health` no longer reports a consumer group list or lag
  lookup that failed as "no groups". `ClusterSnapshot` has a new `warnings`
  field (filled by `ferrika_ops::health::snapshot` when the group listing,
  committed offsets or partition end offsets cannot be read; empty from
  `ClusterSnapshot::empty`/`demo`), and `health_checks` turns each into a
  warning finding starting with `Incomplete check:` (shown by `health`,
  `--json`, `--watch`, webhooks, `report`, the `top` dashboard and the MCP
  `cluster_health` tool). `health` then exits with status 3 unless a finding
  fails `--fail-on` first (status 1) or `--fail-on never` is given.
  `monitor lag` (all groups) prints the warnings on stderr, and plain `top`
  shows them (`TopClusterSnapshot::warnings`). The exporter's failed steps
  (`scrape_cluster_metrics_checked`) include `offsets` when some groups'
  committed offsets could not be read, so `ferrika_scrape_success` is 0 then.
  New `ferrika_ops::health::{lag_warnings, INCOMPLETE_PREFIX}`.
- **Breaking (library):** `ferrika_ops::report::ReportContext` takes the
  caller's `product` and `product_version`; the report shows them ("Made
  by", e.g. `Ferrika CLI 0.2.0`) and the ferrika-ops version on a line of
  its own, instead of calling the ops crate's version the "Ferrika" version.
  `Report::ferrika_version` is replaced by `product`, `product_version` and
  `ops_version` (also in `report --format json`); new
  `Report::product_line` and `report::OPS_VERSION`.
- Tests: CI runs two more end-to-end smoke tests (Python, standard library
  only). `tools/e2e/mcp_smoke.py` drives `ferrika mcp --allow-write` over
  stdio JSON-RPC in the Kafka job (tools/list, read tools,
  `transform_preview` masking, `transform_execute` plan, token and execute,
  the `--topics` scope, audit entries). `tools/e2e/rabbit_smoke.py` runs in
  a new RabbitMQ job (`rabbitmq:4-management`): profiles and where the
  password is kept, queues/exchanges/bindings JSON, publish (routed and
  unroutable), peek, `dlq inspect` grouping, `dlq redrive` preview and
  `--execute` (x-death kept), read-only profile refusals and an audit log
  without message bodies. `tools/e2e/policy_smoke.py` checks a signed
  company policy against two clusters (forbid rules, environments and
  `copy_down`, export and masking, `mcp_write`, history tampering), and
  every smoke command runs in a throwaway home. See `docs/e2e/README.md`.
- Building needs Rust 1.89 or newer (`lapin` and its AMQP crates); the
  crates declare `rust-version = "1.89"` and CI checks it.
- `ferrika top`: the refresh interval option is now `--interval` (the old
  `--interval-secs` still works) and defaults to 5 seconds instead of 2.
- **Breaking:** `acl delete` without a filter (which removes every binding)
  now needs `--all`; `-f/--force` alone is refused. `--all` cannot be
  combined with a filter, still only previews without `-f`, and its preview
  says that it covers all bindings and how many. As before, only the
  previewed bindings are deleted and recorded for undo.
- **Breaking:** `quota set/remove --user default` names a user called
  `default` (likewise `--client-id default`); the default quota entity is
  now `--user-default` / `--client-id-default` (and `--ip-default`;
  `--ip default` keeps its meaning). Passing `--user default` or
  `--client-id default` prints a note about the change. Listings show the
  default entity as `<default>`.
- `consume`/`mirror --header-filter`: `KEY=VALUE` matches the exact
  (case-sensitive) value besides `KEY:VALUE` (value contains, unchanged).
  Repeated filters must all match; before, a repeated key kept only the
  last filter. New `parse_header_filter`, `HeaderFilter` and
  `MessageFilter::{with_header_exact, with_header_filter}`.
- `diff`, `cluster diff` and `schema diff-schema` take `--exit-code`: exit 0
  without differences, 1 with differences, 2 on errors (like
  `git diff --exit-code`). Without it the exit codes are unchanged.
- `dlq redrive` takes `-g/--group <GROUP>`: the run starts at the group's
  committed offsets on the DLQ and commits a record's offset only after it
  was delivered to its target (as `mirror -g`), so a later run with the same
  group continues instead of resending. `--execute` without `-g` prints a
  note that a rerun resends the same records. `ferrika_ops::mirror::CommitTracker`
  is now public for this.
- AutoMQ moves from "preset available, not tested" to tested:
  `docs/e2e/automq-compose.yml` now runs (AutoMQ 1.7.5 with an
  S3-compatible store, Kafka API on `localhost:49092`), CI runs the smoke
  test against it, and `docs/COMPATIBILITY.md` lists the results and the
  limitations found. `docs/e2e/undo.sh` takes `KAFKA_BIN` for brokers whose
  Kafka tools are not in `/opt/kafka/bin`.
- The history journal no longer re-reads the whole file on every append
  once it is over 1 MiB: it is trimmed (1000 operations, 90 days) and, when
  that leaves more than 512 KiB, rotated to `history.jsonl.1`, replacing the
  previous one. `history` and `undo` read both files, so the journal keeps
  at most about 2 MiB.
- A failed multi-step undo names the steps it applied before the failure
  (left in place, not rolled back) and the ones it did not run, in its
  message and in the history entry's outcome.
- Library: `Operation::digest` returns the v2 digest; `digest_v1`,
  `verify_digest`, `DigestStatus`, `operation_digest`,
  `Scope::check_undo_target`, `undo::undo_mark_trusted`,
  `undo::execute_steps` with `undo::UndoFailure` (the applied and not-run
  steps of a failed undo as data), `Journal::rotated_path` and
  `audit::op_digests` are new, and `undo::DigestCheck` has a `Legacy`
  variant.
- `rabbit peek` is a write: it changes the messages it reads (redelivered
  flag, quorum-queue delivery count), so read-only rules and profiles refuse
  it and it is audited like other writes, besides being a record export.
- Credential store entries are per config directory. Profiles in the
  default directory (`~/.ferrika`) keep their account names
  (`<profile>/<field>`); profiles in another directory (`FERRIKA_CONFIG_DIR`,
  `FERRIKA_CONFIG_PATH`) use `<profile>/<field>@<hash>`, where `<hash>` is
  the first 16 hex digits of the SHA-256 of the directory's canonical path.
  Two directories with a profile of the same name (for example `prod` in a
  test directory and in `~/.ferrika`) no longer read and overwrite each
  other's passwords. This covers Kafka profile secrets, RabbitMQ passwords
  (`<profile>/rabbitmq_password`) and the connector secrets the history
  keeps for undo. A profile in another directory whose own entry is missing
  reads the old `<profile>/<field>` entry once and copies it to its own; the
  old entry is left for the default directory. Library: `secrets::Namespace`,
  `secrets::delete_with_legacy` and `ProfileStore::home_config_dir` are new.
- Leader-change retries: `ferrika_core::kafka::retry` has one retry policy
  (which errors, how many tries, backoff) with a blocking executor
  (`retry_leader_change`, `retry_metadata`) and a new async one
  (`retry_leader_change_async`, waits with `tokio::time::sleep`).
  `KafkaConsumerClient` gains `fetch_watermarks_async` and
  `fetch_watermarks_batch_async`; the blocking versions now document that
  they may sleep the calling thread for up to 3.5 s while a partition
  leader moves. `bounded_read::topic_watermarks` is now `async`.
- **Breaking (library):** `ferrika-cli`'s command modules, argument structs,
  `Commands` and `Cli`'s fields are crate-internal (#23).
- **Breaking (library):** 21 internals of `ferrika-core`'s AWS MSK IAM,
  OIDC and profile code (#25) and about 50 items of `ferrika-governance`
  and `ferrika-ops` that only their own crate used (#24) are crate-internal.
  See "Upgrading from 0.1" for what to use instead.
- **Breaking (library):** `ferrika_core::kafka::filter::MessageFilter::header_filters`
  is a `Vec<HeaderFilter>` instead of a map, and `with_header` adds a filter
  instead of replacing an earlier one for the same key.
- **Breaking (library):** `PolicyAction` gains `MessagesPeek` and
  `MessagesMove`; `PolicyRules` gains `screen_capture`, `mcp_write`,
  `environments`, `default_environment` and `copy_down`, so code that builds
  it without `..Default::default()` must add them.
- **Breaking (library):** `rdkafka` is no longer part of the documented API:
  - `ferrika_core::rdkafka` (the re-export) is removed.
  - New `kafka::consumer::StartOffset` (`Beginning`, `End`, `At(i64)`,
    `Tail(i64)`, `Committed`) replaces `rdkafka::Offset` in
    `Lookup::as_offset`, `timestamp_start_offsets`, `PartitionRead::start`
    and `KafkaConsumerClient::{assign_with_offsets, resolve_logical_offsets,
    assign_topic, seek, offsets_for_timestamp}`.
  - New `kafka::consumer::CommitMode` (`Sync`, `Async`) replaces rdkafka's in
    `KafkaConsumerClient::{commit_message, commit_all, commit_stored, commit}`.
  - `KafkaConsumerClient::inner` is removed; `positions()` returns the read
    positions as a `(topic, partition) -> offset` map.
  - `KafkaProducerClient::enqueue` returns a `PendingDelivery` future that
    resolves to `KafkaResult<DeliveryReceipt>` instead of rdkafka's
    `DeliveryFuture`.
  - `KafkaAdminClient::fetch_raw_metadata` is crate-internal; use the new
    `fetch_metadata`, which returns `ClusterMetadata` (brokers, topics,
    partitions). `ferrika_ops::topology::from_rdkafka_metadata` is replaced
    by `from_metadata(&ClusterMetadata, ..)` and `collect(&KafkaAdminClient)`.
  - `KafkaClientConfig::to_rdkafka_config` and
    `kafka::oauth_refresh::{prepare, OAuthSetup, with_oauth}` are
    crate-internal.
  - Committed offsets use plain maps too (see the next entry), so no public
    signature names an `rdkafka` type.
  - `ferrika-mcp` no longer depends on `rdkafka`.
- **Breaking (library):** committed offsets no longer pass through
  `rdkafka::TopicPartitionList`, so these signatures do not change with
  rdkafka. `KafkaConsumerClient::fetch_committed_offsets` and
  `fetch_committed_offsets_for` return `HashMap<(String, i32), Option<i64>>`
  (`None`: no committed offset), and `fetch_committed_offsets_for` takes the
  partitions as `&[(&str, i32)]`. `KafkaConsumerClient::commit` takes
  `&[(&str, i32, i64)]` (topic, partition, next offset to read).
  `ferrika_ops::group_lag::committed_partition_list` is now
  `committed_partition_offsets` and returns that map, which
  `apply_group_offsets` takes. Lag numbers are unchanged.

### Compatibility

- A signed policy whose `forbid` names `messages.peek` or `messages.move`
  is invalid for builds before 0.2.0 (unknown category), so they fall back
  to the strictest mode. Roll such a policy out only once every client runs
  0.2.0 or later.
- A signed policy that uses `screen_capture`, `mcp_write`, `environments`,
  `default_environment` or `copy_down` is format 2, and builds without
  these rules refuse it as having unknown rules (they do not ignore them),
  so they run in the strictest mode until updated. Update every client
  first. Policies without these fields are unchanged and still signed as
  format 1 (unless they use other format-2 rules such as `export`).
- Under enforced masking `mirror --mask` is now allowed (plain `mirror`
  is still refused).
- History entries written by 0.2.0 carry a `v2:` digest in the audit log.
  An 0.1 build reading them refuses to undo them (it reports the entry as
  edited); undo them with 0.2.0 or later. Audit logs and journals from 0.1
  stay readable and verifiable (as legacy digests).

### Fixed

- Watermark lookups (`topics describe` and `list` message counts,
  `consume`, `monitor`, `undo` of a topic creation, and the other users of
  `KafkaConsumerClient::fetch_watermarks` and `fetch_watermarks_batch`) are
  retried for a few seconds when a partition's leader is not ready yet
  (`NotLeaderForPartition`, `LeaderNotAvailable` and similar). On AutoMQ,
  which opens a new topic's partitions asynchronously, `topics describe`
  right after `topics create` showed `Messages: unknown`, and undoing the
  creation failed. Timeouts and unknown topics are not retried.
  `ferrika_core::kafka::retry` has the new `is_leader_change` and
  `retry_leader_change`.
- Async commands and MCP tools no longer sleep a tokio worker thread while
  a partition leader moves: `consume`, `dump`, `monitor`, `probe`, `top`,
  the TUI, `topics list`/`describe` message counts, `diff`, `dlq`,
  `offset reset`, `mirror`/copy pipelines, topic statistics, the metrics
  scrape behind `top`/`health`, and the MCP `list_topics`,
  `describe_topic`, `peek_messages` and `transform_preview` tools use the
  async watermark lookups, which wait between tries with
  `tokio::time::sleep` (up to 3.5 s used to block the thread).
- `consume --group` whose reader closed the pipe (`| head`) ended without
  committing, so the next run read the same records again. It now commits,
  per partition, the offset after the last record whose whole line reached
  stdout (records filtered out before it count as handled; records read but
  not written are not committed and come again), still exits 0, and the
  audit result says how many records' offsets were committed. Offsets are
  now stored as records reach stdout rather than when they are read, so the
  periodic auto-commit no longer covers records still in the output buffer.
- `produce` from stdin could not be stopped while the broker was down: with
  the in-flight window (10,000 messages) full, it waited for a delivery
  without listening for Ctrl+C, and after the first Ctrl+C it waited for
  every pending delivery. Waiting for window space now also ends on Ctrl+C,
  and a second Ctrl+C (or one after the input ended) stops waiting: the
  command reports how many messages were delivered and how many are not
  confirmed, exits with an error and is audited as interrupted.
- History: trimming the journal (`history.jsonl`) no longer rewrites the
  lines it keeps. It copied them through this version's event types, so a
  journal shared with a newer Ferrika lost the event kinds and fields this
  version does not know, and could change bytes a digest covers. Kept lines
  are now copied byte for byte; only the lines of the operations dropped by
  age or count go (whatever their event kind), and lines that are not JSON
  at all (cut short by a crash). Rotation renames the file as it is.
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
- `-f json` is now honoured by every command that printed text or tables
  regardless: `schema register`, `schema compat`, `schema delete-subject`,
  `acl create`, `acl delete`, `cluster ping` (without profiles), `compact
  snapshot`, `connect apply/validate/delete/pause/resume/restart/
  restart-task`, `dlq export`, `undo`, `lint --fix`, `policy keygen`,
  `policy sign`, `policy example --out`, `produce` from stdin, `profile
  add/use/remove/secure/read-only/import`, `quota set/remove`, `reassign
  execute/verify/rollback`, `topics set-replication`, `user create/delete`
  and `rabbit profile use/remove`. Each prints one JSON document; notes go
  to stderr. `monitor --watch` refuses `-f json` instead of mixing a
  redrawn screen into it. See "JSON output" in docs/CLI.md.
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
