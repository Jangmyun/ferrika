# Ferrika command reference

Examples for the `ferrika` command line, grouped by task. Run
`ferrika <command> --help` for the full list of options of any command. The
global options `-p/--profile`, `-b/--bootstrap-servers`, `-f/--format` and
`--timeout` go before the subcommand; some subcommands reuse `-b` or `-p` for
their own flags (`consume -b` reads from the beginning, `consume -p` picks a
partition).

## Commands

### 1. Multi-Cluster Profiles (`ferrika profile`)
```bash
# List all configured cluster profiles
ferrika profile list

# Add a new cluster profile
ferrika profile add staging -b "staging-kafka.corp:9092" --security-protocol plaintext
ferrika profile add prod -b "prod-kafka.corp:9093" --security-protocol sasl_ssl --sasl-mechanism PLAIN --sasl-username admin

# Switch active default cluster
ferrika profile use staging

# Show profile details
ferrika profile show staging

# Import a Kafka client .properties file (e.g. the one Confluent Cloud gives you)
ferrika profile import cloud -f client.properties --use

# Use a specific profile for any command with -p
ferrika -p prod topics list

# Decode Avro and Protobuf payloads (Confluent wire format) in `consume -D`
ferrika profile add prod -b "prod-kafka.corp:9093" --schema-registry http://schema-registry.corp:8081
ferrika -p prod consume orders -D --pretty-json
# HTTPS registries (e.g. Confluent Cloud) take the API key in the URL
ferrika profile add cloud -b "pkc-xxxx.confluent.cloud:9092"   --schema-registry "https://KEY:SECRET@psrc-xxxx.confluent.cloud"
# Serialize JSON with the latest schema of orders-value and produce it
ferrika -p prod produce orders --schema -m '{"id": 1, "note": null}'

# SASL OAUTHBEARER with OIDC client credentials (secret from stdin)
echo "$CLIENT_SECRET" | ferrika profile add cloud -b "pkc-xxxx.confluent.cloud:9092" \
  --security-protocol sasl_ssl --sasl-mechanism oauthbearer \
  --oauth-token-endpoint https://idp.example.com/oauth2/token \
  --oauth-client-id kafka-cli --oauth-client-secret-stdin \
  --oauth-scope kafka --oauth-extensions logicalCluster=lkc-xxxx,identityPoolId=pool-xxxx
# The token endpoint must be https://; plain http is accepted only for
# localhost, 127.0.0.1 and ::1 (the client secret would otherwise cross the
# network unencrypted).
# AWS MSK IAM (credentials from AWS_* variables, ~/.aws or credential_process)
ferrika profile add msk -b "b-1.demo.abc123.c2.kafka.us-east-1.amazonaws.com:9098" \
  --security-protocol sasl_ssl --sasl-mechanism aws-msk-iam --aws-profile prod

# Guard a production cluster: commands that change it
# (create/delete/purge topics, produce, offset resets, ACL changes, ...) are refused
ferrika profile read-only prod
ferrika profile read-only prod --off
```

OAUTHBEARER tokens are fetched by Ferrika and shared by every client in the
process: OIDC tokens with the client credentials grant (`client_secret_basic`,
or `--oauth-client-auth post`), MSK IAM tokens signed locally with SigV4.
librdkafka clients get each token through `rd_kafka_oauthbearer_set_token`
and a background thread replaces it at 80% of its lifetime. Producing,
consuming and metadata need the `--features ssl` build (librdkafka enables
OAUTHBEARER only with OpenSSL); the native admin requests (ACLs, quotas,
config edits) support OAUTHBEARER in every build. MSK IAM reads credentials
from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`, the
shared `credentials`/`config` files (`--aws-profile` or `AWS_PROFILE`) and
`credential_process`; SSO and assume-role profiles work through
`credential_process = aws configure export-credentials --profile <name> --format process`.
The region comes from `--aws-region`, `AWS_REGION` or the MSK host name.
`profile import` maps `sasl.oauthbearer.*`, `OAuthBearerLoginModule` and
`AWS_MSK_IAM`/`IAMLoginModule` settings.

Profile passwords (SASL, SSL key, OAuth client secret, and the password part of a Schema Registry
or Connect URL) are saved in the OS credential store (Windows Credential
Manager, macOS Keychain, Linux Secret Service), not in `profiles.json`. Set `FERRIKA_SECRET_STORE=file` to keep them in the file,
for example on a headless Linux host without a Secret Service. Existing
plain-text passwords move to the store the next time the profiles are saved,
or at once with `ferrika profile secure`.

### Health checks (`ferrika health`)

The cluster health checks, for scripts and CI:
offline brokers, leaderless and under-replicated partitions, uneven
leadership, idle groups with lag, topics without redundancy.

```bash
ferrika health                    # exits non-zero on a critical finding
ferrika health --fail-on warning  # stricter gate
ferrika -f json health --fail-on never

# Alerting: check every 60 s, print and post only what appears or clears
ferrika -p prod health --watch 60 --webhook https://hooks.slack.com/services/...
```

`--webhook` posts JSON with a `text` field (Slack, Microsoft Teams and
Mattermost incoming webhooks) plus the findings as `new` and `cleared`.
`--min-severity` (default `warning`) sets what is reported. Changing counts
in a finding (lag, records behind) do not count as a new finding, and an
unreachable cluster is reported as a critical finding.

### Config check (`ferrika lint`)

Where `health` looks at the cluster's state, `lint` looks at its settings:
topic, broker and consumer group configs that are risky or inconsistent, each
with a fixed rule id, a severity and a fix. `ferrika lint rules` lists the
catalog (T001-T014 topics, B001-B007 brokers, G001-G003 groups), for example
replication 1 (T001), `min.insync.replicas` above or equal to the replication
factor (T002, T003), unclean leader election (T005, B001), a dead letter
topic that keeps records for less time than its source (T013), partitions off
their preferred leader (T014), keys that differ between brokers (B006) and
groups whose committed offsets are older than the log (G001).

```bash
ferrika -p prod lint                      # findings, graded for production
ferrika lint --topic 'orders-*' --skip T007
ferrika lint --fail-on warning            # CI gate (default: never fails)
ferrika lint --fix --dry-run              # brokers validate the changes; nothing changes
ferrika lint --fix                        # apply the auto fixes after one confirmation
ferrika lint --fix --rule T001            # also the guided fix of T001 (copies data)
```

Fixes come in three classes. **auto** fixes are reversible dynamic config
changes and leader elections; `--fix` applies them. **guided** fixes move
data, change client behaviour or cannot be undone (replication, raising
`min.insync.replicas`, adding partitions, deleting a group); they run only
when their rule is named with `--rule` and are confirmed one by one, and
deleting a group asks for its name even with `--yes`. **manual** fixes
(static `server.properties` keys, business decisions such as retention)
are only explained. Before each fix the cluster is read again; a finding
that changed since the lint is skipped. Each fix goes through the company
policy and the read-only profile check as its own category (see
[POLICY.md](POLICY.md#what-is-enforced-where)) and is written to the
audit log. Each applied fix is recorded in the history with the state it
replaced, so `ferrika history` lists it and `ferrika undo` reverts it like
any other change (a `--dry-run` records nothing); the previous config
values also go to `~/.ferrika/lint/<time>.json`.

`--env auto` (the default) grades for production when the profile is
read-only, a policy read-only rule matches it, or its name or description has
the word `prod`, `prd` or `live`; the first output line says which. Resources
that could not be read (no `DESCRIBE_CONFIGS` permission) are always listed
and never fail the run.

### Incident report (`ferrika report`)

One Markdown (or JSON) document for the active cluster: header (profile,
bootstrap without credentials, time, version, policy), health checks,
consumer lag, lag trend and the audit timeline. No record contents; audit
text is PII-masked.

```bash
ferrika -p prod report --since 30m -o incident.md       # audit timeline of the last 30 min
ferrika report --samples 6 --interval 10 > lag-trend.md # sample lag for a minute
ferrika report --format json
```

Ferrika keeps no lag history, so the trend covers lag sampled during the run;
one sample is reported as a snapshot.

### AI assistants (`ferrika mcp`)

`ferrika mcp` is a [Model Context Protocol](https://modelcontextprotocol.io)
server on stdin/stdout. By default it gives an assistant read-only tools for
the profile it starts with: `cluster_health`, `config_lint` (the `ferrika lint`
findings, with the command for each fix), `list_brokers`, `list_topics`,
`describe_topic`, `list_consumer_groups`, `consumer_group_lag`,
`message_histogram` (records per time bucket from offsets) and
`peek_messages`. None of them changes the cluster, and `peek_messages` reads
without a consumer group and masks emails, card numbers and tokens unless
`mask_pii` is false. A broken `profiles.json` or an unknown `-p` profile
stops the server at start instead of falling back to another cluster.

```bash
# Claude Code
claude mcp add ferrika-prod -- ferrika -p prod mcp
```

```json
// Claude Desktop and other clients: mcpServers entry
{ "ferrika-prod": { "command": "ferrika", "args": ["-p", "prod", "mcp"] } }
```

#### Write tools (`--allow-write`)

`--allow-write` adds four write tools: `produce_messages` (up to 100 UTF-8
records to an existing topic, optionally encoded with a Schema Registry
subject), `create_topic`, `alter_topic_config` (set or reset keys; others
stay) and `reset_consumer_group_offsets` (the group must be Empty).

```bash
claude mcp add ferrika-dev -- ferrika -p dev mcp --allow-write --topics "test-*"
```

Every write takes two calls. `mode: "plan"` checks the request (the broker
validates topic creation and config changes without applying them) and
returns what would change: per-key current and new values, per-partition
offsets and the records re-read or skipped, per-record partition and size
(never the values). `mode: "execute"` plans again and refuses
(`state_changed`) when the cluster no longer matches the confirmed plan.

How an execute is confirmed (`--confirm`, default `auto`):

| Mode | Confirmation |
|---|---|
| `elicit` | Ferrika asks the user through the client (MCP elicitation): "Run it?", and for a change that deletes data (lowering `retention.ms`/`retention.bytes`) the topic name typed back. Clients without elicitation get no write tools. |
| `token` | The plan returns a one-time token (120 s, bound to the tool and the exact arguments); the execute call must carry it. |
| `auto` | `elicit` when the client supports elicitation, `token` otherwise. |

The token only makes sure a plan was made and nothing changed since; **the
model can pass it on its own, so it is not a person's approval.** For that
rely on elicitation or on the client's own tool-approval prompt (the tools
carry `readOnlyHint: false` and `destructiveHint` annotations). There is no
mode without confirmation. An offset reset that moves more than
`--max-offset-delta` records can only be confirmed by a person.

Each write call passes the same gate as the CLI commands: the company
policy (`forbid`, `read_only`, `read_only_brokers`, `audit: required`,
strictest mode) and a read-only profile refuse it, and when they refuse a
tool outright it is left out of `tools/list` (with one stderr line at
start). Internal topics (names starting with `_`) are never written; `--topics <glob>`
(repeatable) narrows the writable topics further. Limits per session:
`--max-writes-per-minute` (10), `--max-produce-records` (1,000), plans at 60
per minute, and the same refusal 5 times in a row pauses writes for 30 s.
`--allow-tools create_topic,produce_messages` offers only some tools.

Executed writes are audited as `mcp <tool>` with source `mcp` and an
`actor` such as `mcp:claude-code/2.1.4#a1b2c3d4` (client name and version as
the client reports them, plus a session id; the identity is still the OS
user), along with `mcp.session_start`, `mcp.session_end` and `mcp.declined`
for confirmations the user refused. Plans are not audited, and record
values and secret config values never are. `ferrika audit` shows an ACTOR
column.

Deleting or purging topics, ACLs, SCRAM users and quotas are not, and will
not be, MCP tools: do those in the CLI. Keep in mind that an
assistant that reads records and configs may be steered by text inside them
(prompt injection); keep `--allow-write` to development profiles or narrow
it with `--topics`, and prefer `--confirm elicit`.

### Kafka Connect (`ferrika connect`)

```bash
ferrika connect list --url http://connect:8083   # or set it once: profile add ... --connect URL
ferrika connect status orders-jdbc-sink          # config, tasks, failure traces
ferrika connect restart orders-jdbc-sink         # also pause / resume
ferrika connect apply -f s3-sink.json            # create, or replace a connector's config
ferrika connect delete s3-sink
```

### Client quotas (`ferrika quota`)

```bash
ferrika quota list
ferrika quota set --user billing-svc --key producer_byte_rate --value 1048576
ferrika quota set --client-id default --key consumer_byte_rate --value 5242880
ferrika quota remove --user billing-svc --key producer_byte_rate
```

### SCRAM users (`ferrika user`)

```bash
ferrika user list
FERRIKA_SCRAM_PASSWORD=... ferrika user create billing-svc --mechanism SCRAM-SHA-512
ferrika user delete billing-svc --mechanism SCRAM-SHA-512
```

The password is salted locally (PBKDF2); only the salted form reaches the cluster.

### Audit log (`ferrika audit`)

Every operation that changes a cluster — commands as requested — is appended to `~/.ferrika/audit.log` (JSON Lines,
next to `profiles.json`). Targets are recorded, never payloads or config values.

```bash
ferrika audit            # last 30 entries
ferrika -f json audit -n 100
ferrika audit verify     # check the hash chain; names the first edited line
```

Each line carries the OS user and a SHA-256 hash chained to the line before.
A signed company policy can make read-only targets, forbidden actions and PII
masking mandatory, and forbid or audit record export:
`ferrika policy keygen | sign | verify | show`, see
[docs/POLICY.md](POLICY.md).

### History and undo (`ferrika history`, `ferrika undo`)

Changes made from the CLI are also recorded with the state they
replaced, in `~/.ferrika/history.jsonl` (next to the audit log; config
values live here, never in the audit log). Undo puts that state back:
topic and broker configs, replication, offset resets and deletions,
deleted groups (their offsets), ACLs, quotas, new SCRAM users, schema
compatibility, versions and deleted subjects, connector configs and
pause/resume, and new topics while they are still empty. Deleting or
purging topics, adding partitions, replacing or deleting passwords and
producing cannot be undone (prompts say so).

```bash
ferrika history                    # this cluster's changes, newest first
ferrika history --all --undoable   # every cluster, only what can be undone
ferrika history show 01K9ABCDEF12  # before/after values, limits
ferrika undo --dry-run             # what undoing your latest change would do
ferrika undo 01K9AB                # id prefix (6+ characters); asks first
ferrika undo 01K9AB --force        # even though the state changed since
```

Undo reads the current state first: if someone changed it since, it shows
before / recorded / current and stops unless `--force`. It is a change like
any other (company policy, read-only profiles, audit), checked under the
category of what it does (undoing a topic creation is a topic deletion), and
it is recorded too, so undoing an undo applies the change again.

### 2. Topic Management (`ferrika topics`)
```bash
# List topics
ferrika topics list

# Include internal system topics
ferrika topics list -a

# Describe topic partitions and replicas
ferrika topics describe orders-v1

# Create a new topic
ferrika topics create user-events --partitions 6 --replication 1 -c retention.ms=604800000

# Show a topic's config overrides, then change only the listed keys
# (other overrides are kept; uses IncrementalAlterConfigs, PLAINTEXT/SASL_PLAIN)
ferrika topics config user-events
ferrika topics config user-events --set retention.ms=86400000 --delete max.message.bytes
ferrika topics config user-events --set retention.ms=abc --dry-run   # broker validates only

# New topic with the partitions, replication factor and config overrides of another
ferrika topics clone user-events user-events-v2

# Change the replication factor, moving as little data as possible
ferrika topics set-replication user-events 3

# Move partition leadership back to the preferred replicas (all topics, or the named ones)
ferrika topics elect-leaders
ferrika topics elect-leaders user-events

# Delete every record but keep the topic (all partitions, or -p N for one)
ferrika topics purge user-events --yes
ferrika topics purge user-events -p 2 --yes

# Delete a topic
ferrika topics delete old-topic --yes

# Messages over time (alias: timeline): records per time bucket from offset
# lookups, no record is read. Last 24 hours by default, about 48 buckets, UTC.
ferrika topics histogram orders
ferrika topics histogram orders --since 7d --bucket 1h -p 0 -p 1
ferrika topics histogram orders --from "2026-11-02 09:00" --to "2026-11-02 12:00" --by-partition
ferrika topics histogram orders --all -f json
# Count by the records' own timestamps instead (reads them; up to --limit records)
ferrika topics histogram orders --since 1h --exact
```

`topics histogram` asks the brokers, for every bucket boundary, which offset
is the first at or after that time, and counts offset differences. Partitions
with nothing in a span are not asked about it again, so quiet topics cost few
lookups. Counts are offset ranges: transaction markers, aborted records and
records removed by compaction are counted (`--exact` reads the records and
counts them by their timestamps). With CreateTime, a record produced with an
old timestamp is counted where it was written. Records "after range" are
usually future timestamps (clock skew). Buckets before the oldest retained
record are shown as deleted, not empty. The output ends with a `consume`
command that reads the busiest bucket. More than 200,000 partition lookups
need `--yes`.

Looking a topic up never creates it: `describe`, `consume`, `topics histogram`
and the other read and admin commands set `allow.auto.create.topics=false`, so
a typo fails with "unknown topic" instead of leaving an empty topic behind.
`produce` and `restore` follow the broker's `auto.create.topics.enable`, as a
Kafka producer normally does. Anyone embedding the client can override either
side with `KafkaClientConfig::with_property("allow.auto.create.topics", "true")`
(`extra_properties`); there is no CLI flag for it. Topics whose names start
with `_` (`__consumer_offsets`, `_schemas`, `_confluent-*`) count as internal
(topic lists and cluster comparisons hide them unless you pass `-a` /
`--include-internal`).

### 3. Message Producer (`ferrika produce`)
```bash
# Produce a single message with key and headers
ferrika produce orders-v1 -k "order-101" -m '{"status":"paid","amount":99.5}' -H "env:prod" -H "source:api"

# Stream messages from stdin
cat events.txt | ferrika produce raw-logs

# Stream key:value records from stdin
echo "user-42:login_success" | ferrika produce user-logins -s ":"
```

### 4. Message Consumer (`ferrika consume`)
```bash
# Consume from latest offsets
ferrika consume orders-v1

# Consume from the beginning with formatted JSON
ferrika consume orders-v1 -b --pretty-json

# Consume specific partition and seek offset
ferrika consume orders-v1 -p 0 --offset 12000

# Filter messages in real-time by payload content
ferrika consume orders-v1 -b -G "ERROR"

# Filter by key and export matched records to JSONL file
ferrika consume orders-v1 -k "user-" -s matched_users.jsonl

# Filter by fields of JSON records (Avro/Protobuf too, with -D)
ferrika consume orders-v1 -b -W '$.amount > 100 && $.status == "PAID"'
ferrika consume orders-v1 -b -W 'key =~ "^eu-" && headers.source exists'

# Limit message count or idle timeout
ferrika consume orders-v1 -n 100 -t 5000

# Decode Confluent Schema Registry wire-format headers (0x00 + 4-byte Schema ID)
ferrika consume orders-v1 -b -D

# Inspect raw binary payloads with canonical 16-byte hexdump
ferrika consume telemetry-v1 -b --hex
```

Filter expressions (`-W`/`--where`, expressions that start with
`$` or `?` or with a record field followed by an operator, such as
`key =~ "^eu-"`, and MCP `peek_messages`):

| Part | Meaning |
|---|---|
| `$.a.b`, `$.items[0]`, `$["odd key"]`, `$` | A field of the JSON value; `$` alone is the whole value |
| `key`, `partition`, `offset`, `timestamp`, `headers.<name>` | Record fields (timestamp in epoch ms) |
| `== != > >= < <=` | Compare; a number and a numeric string compare as numbers |
| `=~ "regex"`, `contains "text"` | Case-insensitive match; `contains` also checks array elements |
| `exists`, `&&`/`and`, `\|\|`/`or`, `!`/`not`, `( )` | Presence and logic; a bare field is true unless missing, null, false, 0 or "" |

List the paths a subject's schema gives, e.g. to write `--where`: `ferrika schema fields --topic orders-v1 --plain` (or `ferrika schema fields orders-v1-value` for types, flags and enum values).

### 5. Cluster & Consumer Group Monitor (`ferrika monitor`)
```bash
# View all consumer groups
ferrika monitor lag

# View partition lag for a specific consumer group
ferrika monitor lag billing-service

# Live continuous monitor mode (refreshes every 2 seconds)
ferrika monitor lag billing-service -w -i 2

# Inspect broker health and metadata
ferrika monitor brokers

# Start Prometheus metrics exporter & K8s healthcheck server (default port 9102)
# Binds to 127.0.0.1 by default (no auth); add --host 0.0.0.0 to expose it, e.g. in a pod
ferrika monitor export -P 9102 -i 15
# Endpoints:
#   http://127.0.0.1:9102/metrics (Prometheus exposition format)
#   http://127.0.0.1:9102/healthz (Liveness/Readiness probe)
```

### 6. Performance Benchmark (`ferrika bench`)
```bash
# Benchmark producer throughput (50,000 records of 1 KB with LZ4 compression)
ferrika bench produce perf-test -n 50000 -s 1024 -c lz4
# Codecs: none, gzip, snappy, lz4, zstd (zstd is built in from bundled sources)
ferrika bench produce perf-test -n 50000 -c zstd

# Benchmark consumer throughput
ferrika bench consume perf-test -n 50000 -t 30

# Output results in machine-readable JSON
ferrika -f json bench produce perf-test -n 10000
```

### 7. Schema Registry & Wire-Format (`ferrika schema`)
```bash
# List all registered subjects in Schema Registry
ferrika schema list -S http://localhost:8081

# Fetch schema definition by its global ID
ferrika schema get 42 -S http://localhost:8081

# Fetch latest schema for a subject
ferrika schema subject orders-value -S http://localhost:8081

# Decode hexadecimal message to inspect schema ID, headers, and payload
ferrika schema decode "00 00 00 00 2a 7b 22 69 64 22 3a 31 7d"

# Test candidate schema compatibility against latest registered subject schema (BACKWARD/FORWARD/FULL)
ferrika schema check orders-value -f ./candidate-schema.avsc -m backward --fail-on-incompatible

# Offline diff and evolutionary compatibility analysis between two local schema files
ferrika schema diff-schema ./schemas/order-v1.avsc ./schemas/order-v2.avsc -m full
```

### 8. Synthetic Mock Event Generator (`ferrika generate`)
```bash
# Generate e-commerce order events at 10 events/sec (continuous)
ferrika generate orders-v1 --template ecommerce -r 10

# Generate 500 IoT telemetry messages at 50 events/sec with device key prefix
ferrika generate iot-telemetry -T iot -r 50 -n 500 -k "dev-"

# Generate web access logs with verbose real-time output
ferrika generate web-logs -T web -r 25 -v

# Generate events wrapped in Confluent Schema Registry wire-format
ferrika generate financial-tx -T financial -r 20 --schema-id 101

# Generate custom JSON events using dynamic interpolation tokens
ferrika generate custom-events --custom '{"event_id":"{{uuid}}","seq":{{index}},"status":"{{status}}"}' -r 5 -n 50
```

### 9. Topic Mirror & Cross-Cluster Replicator (`ferrika mirror`)
```bash
# Mirror messages between topics on the same cluster
ferrika mirror orders-v1 orders-v1-backup -b

# Mirror dead-letter queue (DLQ) messages with error filter
ferrika mirror orders-dlq orders-retry -b -G "RETRYABLE"

# Cross-cluster replication from production to staging with rate throttling
ferrika mirror orders-v1 orders-v1 --src-profile prod --dst-profile staging -b -r 500

# Replicate specific partition with key filtering and limit to 10,000 messages
ferrika mirror telemetry telemetry-staging -p 0 -k "sensor-temp" -n 10000

# Resumable: a later run with the same group continues where this one stopped.
# An offset is committed only after its record reached the destination, so a
# failed or interrupted run copies records again instead of skipping them.
ferrika mirror orders-v1 orders-v1 --src-profile prod --dst-profile staging -g orders-mirror -b
```

### 10. Consumer Group Offset Reset & Replay (`ferrika offset reset`)
```bash
# Dry-run simulation: Preview resetting consumer group to the beginning (earliest)
ferrika offset reset billing-service -t orders-v1 --to-earliest

# Execute reset to high watermark (skip all accumulated consumer group lag)
ferrika offset reset billing-service -t orders-v1 --to-latest --execute

# Rewind consumer group by 1,000 messages (replay historical events)
ferrika offset reset billing-service -t orders-v1 --shift-by -1000 --execute

# Reset specific partition to explicit offset
ferrika offset reset billing-service -t orders-v1 -p 0 --to-offset 150000 --execute

# Delete idle consumer groups and their committed offsets (groups with members are refused)
ferrika offset delete-group old-billing-service stale-etl-group --yes
```

### 11. Topic Backup & Restore (`ferrika dump` & `ferrika restore`)
```bash
# Export topic data to JSON Lines file with metadata and Base64 payloads
ferrika dump orders-v1 -o ./orders_backup.jsonl -b

# Dump specific partition with message count limit
ferrika dump iot-telemetry -o ./telemetry_p0.jsonl -p 0 -n 10000

# Restore events from backup file to existing or new topic
ferrika restore orders-v1 -i ./orders_backup.jsonl

# Restore events strictly preserving their original source partition IDs
ferrika restore orders-v1 -i ./orders_backup.jsonl --preserve-partitions
```

A dump line has `headers` (text) and `headers_base64` objects. Only a record
that those objects cannot hold exactly (a repeated header key, or a binary
header before a text one) also gets `header_list`, every header in record
order, and `restore` prefers it. Records without repeated keys are written as
before, and files from older versions restore as they always did. `restore`
creates a missing topic only if the broker auto-creates topics.

### 12. Kafka ACL & Cluster Security Manager (`ferrika acl`)

Works on the cluster's ACLs (DescribeAcls/CreateAcls/DeleteAcls; PLAINTEXT or
SASL_PLAIN, and the connecting user needs DESCRIBE/ALTER on the cluster). Add
`--local` to draft or audit rules in `~/.ferrika/acls.json` instead.

```bash
# List all ACL bindings in pretty colorized table
ferrika acl list

# Every binding that applies to a topic (literal, prefixed and wildcard)
ferrika acl list -t "orders-v1"

# Filter ACLs by principal identity
ferrika acl list -p "User:analytics-reader"

# Output ACL bindings in JSON format for automated policy inspection
ferrika acl list --json

# Grant producer service ALLOW Write permission on specific topic
ferrika acl create -p "User:orders-producer" -o Write -t "orders-v1"

# Grant prefixed topic read access to consumer group
ferrika acl create -p "User:billing-worker" -o Read -t "billing-" --pattern-type prefixed

# Deny public host access to sensitive audit topics
ferrika acl create -p "User:*" -o All -t "audit-log" --deny -H "198.51.100.0"

# Delete the bindings for exactly this name (without -f it only previews)
ferrika acl delete -t "orders-v1" -p "User:old-service" -f

# Run automated security audit to detect wildcards, over-permissive cluster rules, and allow/deny conflicts
ferrika acl audit

# The same commands against the local policy file
ferrika acl --local list
```

### 13. Cluster Rebalance & Partition Reassignment Planner (`ferrika reassign`)
```bash
# Spread topics evenly over brokers 1-4 (e.g. after adding broker 4). Replicas
# already on a broker within its share stay; the plan says how many move.
ferrika reassign plan -t orders-v1,payments -b 1,2,3,4 --rf 2 -o ./reassignment.json

# Decommission broker 4: only its replicas move to 1-3
ferrika reassign plan -t orders-v1,payments -b 1,2,3

# Lay every partition out afresh (leader rotation; moves much more data)
ferrika reassign plan -t orders-v1 -b 1,2,3 --from-scratch

# Offline plan for 6 partitions, without reading the cluster (always afresh)
ferrika reassign plan -t orders-v1 -b 1,2,3 -p 6 --rf 2 --json

# Dry-run simulation: Preview partition migration plan without touching the cluster
ferrika reassign execute -i ./reassignment.json

# Execute partition reassignment with a 50 MB/s replication bandwidth throttle
ferrika reassign execute -i ./reassignment.json --throttle-bytes 52428800 --execute

# Verify ongoing replica movement and ISR synchronization progress
ferrika reassign verify -i ./reassignment.json

# Generate a safe rollback plan preserving pre-migration replica layout
ferrika reassign rollback -t orders-v1,payments -o ./rollback.json
```

### 14. Topic Compaction Analyzer & State Snapshotter (`ferrika compact`)
```bash
# Analyze key deduplication efficiency, dirty ratio, and disk savings for a compacted topic
ferrika compact analyze orders-v1 -p 0 -n 5000

# Output compaction metrics in machine-readable JSON format
ferrika compact analyze orders-v1 --json

# Extract current compacted state snapshot (latest record per key) to JSON Lines (.jsonl)
ferrika compact snapshot orders-v1 -o ./orders-state.jsonl

# Export state snapshot including active delete tombstones
ferrika compact snapshot orders-v1 -o ./orders-state-with-tombstones.jsonl --include-tombstones

# Scan and list all active tombstone markers (key deletion records)
ferrika compact tombstones orders-v1 -n 100
```

### 15. Dead Letter Queue (DLQ) Manager & Poison Pill Rescuer (`ferrika dlq`)
```bash
# Inspect dead-letter messages, root cause error categorization, and origin topics
ferrika dlq inspect orders-v1-dlq -n 100

# Filter failed messages by exception type (e.g. timeout, serde, deserializ)
ferrika dlq inspect orders-v1-dlq --filter-error "timeout"

# Safe dry-run redrive simulation (previews target routing without modifying cluster)
ferrika dlq redrive orders-v1-dlq

# Live redrive re-injecting failed messages back to origin topic with redrive provenance headers
ferrika dlq redrive orders-v1-dlq --execute

# Override target destination topic during redrive
ferrika dlq redrive orders-v1-dlq -t orders-v1-retry --execute

# Export toxic/failed messages with stacktraces to JSON file for developer debugging
ferrika dlq export orders-v1-dlq -o ./failed-orders-report.json -n 500
```

### 16. Distributed Tracing & OpenTelemetry Analyzer (`ferrika trace`)
```bash
# Inspect topic messages for distributed trace headers (W3C traceparent, B3, Jaeger)
ferrika trace inspect order-events -n 50

# Filter messages by trace propagation standard
ferrika trace inspect order-events --standard w3c

# Audit distributed trace coverage compliance and detect untraced dark traffic
ferrika trace audit order-events --min-coverage 95.0

# Generate fresh, valid W3C traceparent headers for injecting into test events
ferrika trace inject -n 3
```

### 17. Topic Data Diff & Schema Drift Detector (`ferrika diff`)
```bash
# Compare key presence and content parity between two topics
ferrika diff orders-v1 orders-v2

# Sample more messages and display identical records alongside differences
ferrika diff staging.payments prod.payments -n 500 --show-identical

# Output full JSON field-level drift tree in machine-readable JSON format
ferrika diff topic-source topic-mirror --json
```

### 18. Kafka End-to-End Latency & Heartbeat Sentinel (`ferrika probe`)
```bash
# Measure true produce-to-consume round-trip time (RTT) with canary heartbeats
ferrika probe

# Target custom topic with 10 canary pings and 200ms intervals
ferrika probe heartbeat-v1 -c 10 -i 200

# Enforce strict SLA policy check (exits with error if P99 latency exceeds 25.0ms)
ferrika probe -c 20 --max-latency-ms 25.0

# Export probe latency distributions and jitter metrics in JSON format
ferrika probe -c 5 --json
```

### 19. Kafka Cluster Top & Terminal Process Monitor (`ferrika top`)
```bash
# Launch interactive real-time cluster monitor (default 2s refresh, auto-sorted by consumer lag)
ferrika top

# Monitor with 1s refresh interval and sort topics by throughput (msg/s)
ferrika top -i 1 -s throughput

# Run 5 iterations and exit
ferrika top -i 2 -n 5

# Capture a single-shot JSON snapshot of cluster health and activity
ferrika top --json
```

### 20. Multi-Cluster Manager & Cross-Datacenter Sync Checker (`ferrika cluster`)
```bash
# Concurrently ping and benchmark health & latency across all configured cluster profiles
ferrika cluster ping

# Ping specific clusters with custom 5-second timeout and JSON output
ferrika cluster ping prod-us dr-eu --timeout-ms 5000 --json

# Compare topic inventory, partition counts, and replication factor drift between two clusters
ferrika cluster diff prod-cluster dr-cluster

# Filter topic comparison by regex/prefix and include internal topics
ferrika cluster diff prod-cluster dr-cluster --filter "orders" --include-internal

# Generate an automated remediation plan to bring destination cluster to 100% parity
ferrika cluster reconcile-plan prod-cluster dr-cluster
```

### 21. Field-Level PII Masking & Security Auditor (`ferrika crypto`)
```bash
# Masking hides a value when its JSON key looks sensitive (password, pwd, pass,
# secret, token, api-key, authorization, credential, access_key, private_key,
# card number, cvv, ssn, ...) or the text matches a pattern: emails (also with
# Hangul), card and Korean ID/phone/account numbers, AWS keys (AKIA...), Bearer
# and JWT tokens, GitHub (ghp_), Stripe (sk_live_), Slack (xox) tokens, private
# key blocks and `Basic <base64 user:password>`. Plain prose such as
# "Basic configuration" stays readable.
# Audit topic for unmasked PII, credit cards, SSNs, JWT tokens, and cloud secrets
ferrika crypto audit orders-v1 -n 200

# CI/CD Security Gate: Fail build with non-zero exit code if any PII leak is detected
ferrika crypto audit orders-v1 -n 500 --fail-on-leak

# Export full vulnerability audit report in machine-readable JSON format
ferrika crypto audit orders-v1 --json

# Real-time streaming PII masking directly from a Kafka topic
ferrika crypto mask -t orders-v1 --strategy partial

# Pipe CLI streams through PII masking filter in shell pipelines
ferrika consume orders-v1 | ferrika crypto mask --strategy redact
```

### 22. Kafka Cluster Topology & Partition Placement Visualizer (`ferrika topology`)
```bash
# Display cluster topology, broker workload distribution, and partition placement table
ferrika topology

# Filter partition view to a specific topic
ferrika topology -t orders-v1

# Inspect only degraded Under-Replicated Partitions (URP) requiring urgent attention
ferrika topology --under-replicated-only

# Identify partitions suffering from leader election drift away from preferred leader
ferrika topology --imbalanced-only

# Export complete cluster topology graph in machine-readable JSON format
ferrika topology --json
```


---

### Exit codes

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

A reader that closes the pipe (`ferrika consume ... | head`) ends the command
quietly with exit 0.
