# Multi-broker support: broker abstraction and RabbitMQ

Status: stage 2 design with a preview implementation (`ferrika rabbit`,
crate `ferrika-rabbitmq`), released in 0.2.0; phase 3 (topology writes with
history and undo, CLI and library) on `feat/rabbit-topology`. The product
decisions of 2026-11-22 are in [Decisions](#decisions-2026-11-22) and those
of phase 3 in [Decisions (2026-11-23, phase
3)](#decisions-2026-11-23-phase-3); they supersede the earlier proposals
below where they differ. Kafka behaviour is unchanged.

## Goal

Ferrika's governance layer (the signed company policy, the write and export
gates, the hash-chained audit log, operation history with undo, and PII
masking) should apply to every broker Ferrika talks to, not only Kafka. The
second broker is RabbitMQ: browse queues, exchanges and bindings, view and
publish messages, and reprocess dead-lettered messages.

The rule that drives the design: **a RabbitMQ write passes the same policy
gate and lands in the same audit log as a Kafka write**, and RabbitMQ message
bodies are masked by the same rules as Kafka records.

## How governance is wired to Kafka today

| Piece | Where | Kafka-specific parts |
| --- | --- | --- |
| Policy rules | `ferrika-core` `model::policy::PolicyRules` | `forbid` categories (`PolicyAction`) are named after Kafka resources (`topics.*`, `offsets.*`, `acl.change`...). `read_only`, `read_only_brokers` and `export_clusters` are globs over a profile name and broker addresses: plain strings. |
| Policy check | `ferrika-governance` `EffectivePolicy::check(action, profile, bootstrap)` | Takes strings. A broker address without a port is assumed to be on 9092 (`DEFAULT_BROKER_PORT`), which only matters for port-qualified globs. |
| Write gate | `governance::policy::{WriteRequest, authorize_write}` | None: `source`, `label`, `target`, `profile`, `bootstrap`, `actor`, `op` are strings; only `action: PolicyAction` is typed. |
| Export gate | `governance::policy::{ExportRequest, authorize_export}` | None, except that `ExportChannel::Cluster` is documented as "another Kafka cluster". |
| Broker list for the gates | `governance::policy::brokers_for(&KafkaClientConfig, needed)` | Kafka-typed: adds the brokers from cluster metadata when the policy matches broker addresses. |
| Audit log | `governance::audit::AuditEntry` | None: every field is a string; hash chain, anchors and SIEM forwarding do not look at the content. |
| CLI gates | `ferrika-cli` `cli/app.rs`: `write_action`, `export_gate`, `write_target` | Exhaustive matches over `Commands`; the profile and broker list come from the Kafka `GlobalOpts`. |
| History and undo | `model::history::{Scope, State}`, `ops::history::{capture, inverse, undo}` | `Scope` has `bootstrap`, `cluster_id`, `registry`, `connect`. Every `State` variant is a Kafka, Schema Registry or Connect resource, and capture and inverse call those clients. |
| Masking | `codec::pii::{mask_bytes, mask_payload, mask_header_value, mask_record}` | None: they work on bytes, text, JSON and header names. |
| MCP | `ferrika-mcp` `guard.rs`, `write_tools.rs` | Kafka write tools only; RabbitMQ read tools (`rabbit_tools.rs`) list metadata without a gate. |

So the gates and the audit log are already broker-neutral in their data;
what ties them to Kafka is (1) the policy categories, (2) how the CLI finds
the profile and broker addresses to match, and (3) the history state model.

## The abstraction

### Broker-neutral (shared by every broker)

- **Target address.** The policy matches "broker addresses". For Kafka these
  are bootstrap servers (plus metadata brokers); for RabbitMQ it is the
  `host:port` of the management API and of the AMQP listener (decision 6). `read_only`,
  `read_only_brokers` and `export_clusters` globs therefore cover RabbitMQ
  without a policy format change: `read_only_brokers: ["*.prod.corp"]`
  makes `rabbit.prod.corp:15672` read-only too.
- **Profile name.** Only a profile of the broker in question is used. A
  RabbitMQ command never borrows the active Kafka profile's name (or its
  `read_only` flag), since that would match `read_only: ["prod-*"]` globs
  against the wrong system. Without a RabbitMQ profile the audit entry's
  `profile` is the management address, as for Kafka without a profile.
- **Operation descriptors.** A write is `(label, target, PolicyAction)`:
  - `label`: the command as the audit log names it, prefixed with the broker
    command (`rabbit publish`), so `ferrika audit` can filter by broker.
  - `target`: resource kind, name and scope in words, never payloads or
    secrets: `exchange orders on vhost /, routing key 'order.created'`,
    `queue orders.dlq on vhost /`.
  - `PolicyAction`: a **semantic** category. Where a RabbitMQ operation means
    the same as a Kafka one, it reuses that category; publishing a message is
    `produce`, so one `forbid: ["produce"]` covers both brokers.
- **Exports.** Showing message bodies is a record export exactly like
  `consume`: stdout to a terminal is viewing; redirected stdout, files and
  MCP go through the export gate (`export: forbidden | audited`).
- **Masking.** Message bodies go through `mask_bytes` (JSON field by field,
  text by pattern, binary hidden whole); AMQP headers and string properties
  through `mask_header_value`, judged by their name; numbers and flags
  (`delivery_mode`, `priority`) stay. Tables such as `x-death` are masked as
  JSON text.
- **Audit log.** Unchanged format and chain. Entries from RabbitMQ commands
  differ only in `action` (`rabbit ...`), `target` and `profile`.

### Per broker

- The client and its typed resource model (`ferrika-rabbitmq`).
- The CLI commands and their classification in `write_action` /
  `export_gate` (no wildcard arms, as for Kafka).
- How the broker address list is built (`gate_brokers` in `cli/app.rs`).
- History `State` variants, capture and inverse (see below).

### Change made to shared code in this step

Minimal and additive:

1. `PolicyAction::from_cli("rabbit publish")` returns `Produce`
   (`ferrika-core`, `model/policy.rs`). Two new categories,
   `messages.peek` and `messages.move` (see [New policy
   categories](#new-policy-categories)).
2. `cli/app.rs` asks `gate_profile` and `gate_brokers` for the profile name
   and broker addresses instead of always reading the Kafka options. For
   every Kafka command they return what was used before; for `rabbit` they
   return the RabbitMQ profile and the management `host:port`, resolved
   once before the gates (`cli::rabbit::resolve`). The read-only check of
   the write gate uses the RabbitMQ profile's `read_only` flag for `rabbit`
   commands and the Kafka profile's for the others.

`WriteRequest`, `ExportRequest`, `authorize_write`, `authorize_export`, the
audit log and the masking helpers are used as they are.

### Later: a neutral target type

When a second consumer (the desktop app, MCP) needs RabbitMQ gating, replace
the `(profile, bootstrap)` string pair with a small type in
`ferrika-core::model`:

```rust
pub enum BrokerKind { Kafka, RabbitMq }
pub struct GateTarget { pub kind: BrokerKind, pub profile: String, pub addresses: String }
```

and a `brokers_for` per kind. This is not needed for the CLI preview, nor
for the phase 3 topology writes, which the CLI gates through
`gate_profile` / `gate_brokers` like the other `rabbit` commands
(decision 18).

### New policy categories

A signed policy with an unknown `forbid` entry is refused by
`parse_payload`, so a binary that does not know a new category falls back
to the strictest mode. New categories are therefore a policy-format event,
not a free addition.

Added in 0.2.0, with this branch:

| Category | Covers |
| --- | --- |
| `messages.peek` | reading messages in a way that changes them: `rabbit peek`, `rabbit dlq inspect`, the `rabbit dlq redrive` preview (management `get` with requeue) |
| `messages.move` | moving messages between queues: `rabbit dlq redrive --execute` |

`rabbit publish` stays `produce`, so one `forbid: ["produce"]` covers both
brokers. The names follow the existing `resource.verb` pattern
(`topics.delete`, `offsets.reset`) with `messages` as the resource, so a
later Kafka command that moves records (for example a redrive that deletes
what it moved) can use `messages.move` too.

Compatibility: builds before 0.2.0 do not know these two names, so a
signed policy that forbids them is invalid there and those builds run in
the strictest mode. The policy format number is not raised for this (a new
`forbid` value is not a new field); the CHANGELOG and `docs/POLICY.md` say
to roll such a policy out only once every client runs 0.2.0 or later.

Added after 0.2.0, for RabbitMQ topology writes (phase 3, decision 16):

| Category | Covers |
| --- | --- |
| `queues.create` | `rabbit queue declare`; undoing a queue deletion |
| `queues.delete` | `rabbit queue delete`; undoing a queue declaration |
| `queues.purge` | `rabbit queue purge` |
| `exchanges.create` | `rabbit exchange declare`; undoing an exchange deletion |
| `exchanges.delete` | `rabbit exchange delete`; undoing an exchange declaration |
| `bindings.change` | `rabbit bind`, `rabbit unbind`; undoing either, and restoring the bindings of a deleted queue or exchange |

The same compatibility rule applies: a build that does not know them
(0.2.0 and older) treats a policy naming them as invalid and runs in the
strictest mode. `from_cli` still maps unknown `rabbit ...` writes to
`config.change`.

## Crate layout

```
ferrika-cli ──> ferrika-mcp ──> ferrika-ops ──> ferrika-governance ──> ferrika-core
     └────────> ferrika-rabbitmq <──┘ (mcp: no amqp) ─────────────────────┘
```

- `crates/ferrika-rabbitmq`: the management API client (`client`) and the
  typed responses and request bodies (`model`). Depends on `ferrika-core`
  only (HTTP client, Base64). It does no policy, audit or masking; like the
  Schema Registry and Connect clients, it is a client.
- `ferrika-cli`: `ferrika rabbit ...` in `cli/rabbit.rs`, classified in the
  gates in `cli/app.rs`. The CLI re-exports the crate as `rabbitmq`.
- `ferrika-rabbitmq` also holds the RabbitMQ profiles (`profile`) and the
  AMQP redrive (`amqp`, behind the default feature `amqp`, with `lapin`).
- The Kafka path is untouched. The DLQ redrive lives in
  `ferrika-rabbitmq::amqp`, called by the CLI. Since phase 3 `ferrika-ops`
  depends on the crate (without `amqp`) for the RabbitMQ history and undo
  (`history::rabbit`, decision 21). `ferrika-mcp` uses it without the `amqp` feature for its read-only
  RabbitMQ listings.

New third-party dependency: `lapin` 4.13 (MIT) with its AMQP crates
(`amq-protocol*`, `async-rs`, `tcp-stream`...), only in `ferrika-rabbitmq`
and only with the `amqp` feature; TLS through rustls with the ring backend
and the OS trust store (no aws-lc, no OpenSSL). It raises the minimum Rust
version to 1.89 and adds about 130 crates to `Cargo.lock` (the SBOM in
`docs/sbom` is regenerated).

## Access method: the management HTTP API

The MVP uses the `rabbitmq_management` plugin's HTTP API through Ferrika's
own HTTP/1.1 client (`ferrika_core::kafka::http::Endpoint`, used for the
Schema Registry and Kafka Connect: http and https with the OS trust store,
basic authentication, a 30 s timeout, a 64 MiB response cap). There is no
AMQP dependency.

| Command | Request |
| --- | --- |
| `rabbit queues` | `GET /api/queues/{vhost}?page=N&page_size=S&columns=...[&name=P&use_regex=B]` until `--limit` |
| `rabbit exchanges` | `GET /api/exchanges/{vhost}?page=N&page_size=S&columns=...[&name=P&use_regex=B]` until `--limit` |
| `rabbit bindings` | `GET /api/bindings/{vhost}` (the API does not page bindings; filtered and cut locally) |
| `rabbit peek Q -n N` | `POST /api/queues/{vhost}/{Q}/get` with `{"count": N, "ackmode": "ack_requeue_true", "encoding": "auto", "truncate": 50000}` |
| `rabbit publish X -k K -m M` | `POST /api/exchanges/{vhost}/{X}/publish` with `{"properties": {...}, "routing_key": K, "payload": M, "payload_encoding": "string"}` |

Names and vhosts are percent-encoded as single path segments (the default
vhost `/` is `%2F`; the default exchange is addressed as `amq.default`).

**Peek is not side-effect free.** The API has no browse operation; `get`
with `ack_requeue_true` takes messages and requeues them:

- the broker sets `redelivered` on them, which consumers may treat as a
  retry signal;
- requeued messages go back to the head in classic queues, but ordering
  relative to messages published meanwhile is not guaranteed;
- on quorum queues with a delivery limit, RabbitMQ before 4.0 counts the
  requeue as a delivery attempt: repeated peeks push a message over the limit
  and the broker dead-letters it (or drops it without a dead-letter
  exchange). Checked on 3.13.7: `x-delivery-limit: 2`, the third peek
  dead-lettered the message (`x-death` reason `delivery_limit`). RabbitMQ
  4.0+ counts only failed deliveries; a peek's requeue is none (checked on
  4.4.1: 28 peeks with `x-delivery-limit: 2` kept the message, only
  `x-acquired-count` grew). See [Quorum queues and the delivery
  limit](#quorum-queues-and-the-delivery-limit);
- stream queues do not support `get`; reading them needs a stream consumer
  (later phase);
- messages are held briefly while being fetched; a peek on a busy queue
  competes with consumers.

The CLI caps a peek at 500 messages, truncates payloads at 50 000 bytes
(reported as `truncated` with the full `size`), and says on stderr that the
messages were requeued and marked redelivered. Because of these side
effects a peek is a write under the policy (decision 1 below).

Why not AMQP for everything: the management API covers listing, peeking
and publishing with one HTTP client we already ship and audit; AMQP 0-9-1
is used only where the management API cannot do the job safely, moving
messages (`dlq redrive --execute`).

### Paged listings

`ferrika_rabbitmq::listing` (`ManagementClient::list_queues`,
`list_exchanges`, `list_bindings` with a `ListQuery { name, regex, limit }`,
returning `Listing { items, matched, total }`) is what the CLI and the MCP
read tools use; the unbounded `queues` / `exchanges` / `bindings` stay for
callers that want everything in one request.

- Queues and exchanges: `page` / `page_size` (the page size is the limit,
  at most 500, the API's maximum), `name` with `use_regex`, and `columns`
  with only the fields Ferrika shows (the default queue object carries
  message rates, garbage-collection and backing-queue details). Pages are
  fetched one after another until the limit or the last page; a `400` on a
  later page (entries deleted meanwhile) ends the listing. `disable_stats`
  is not used: it drops `consumers`, which the table shows.
- The broker's `name` filter is a case-insensitive substring, and
  `use_regex=true` a case-insensitive regular expression (checked on
  4.4.1). The pattern is checked locally first, so a bad one never reaches
  the broker.
- Bindings: `/api/bindings/{vhost}` ignores `page` (checked on 4.4.1), so
  they are fetched whole (small objects without statistics) and filtered
  (source or destination) and cut locally with the same matching rules.
- Pages are not a snapshot: a queue created or deleted between two pages
  can shift an entry across the page boundary.

### TLS

`https://` management URLs and `amqps://` were verified end to end against
`rabbitmq:4-management` (4.4.1) with a private CA and listeners on 15671
and 5671: without the CA the commands fail with `invalid peer certificate:
UnknownIssuer` plus a hint naming `--rabbit-ca-cert`; with it `rabbit
queues` and `rabbit dlq redrive` (preview and `--execute`) work; with
`fail_if_no_peer_cert = true` the client certificate options are required
(`CertificateRequired` without them, again with a hint) and work.

- `--rabbit-ca-cert`, `--rabbit-client-cert`, `--rabbit-client-key` (and
  `FERRIKA_RABBIT_CA_CERT` / `_CLIENT_CERT` / `_CLIENT_KEY`; profile fields
  `ca_cert`, `client_cert`, `client_key`, stored as absolute paths that must
  exist). The Kafka profile's `ssl_ca_location` works the same way for the
  Kafka native requests; the Schema Registry and Connect clients have no CA
  option yet.
- The management API (Ferrika's HTTP client,
  `ferrika_core::kafka::http::Endpoint::with_tls_files`) trusts only the
  given CA, like Kafka's `ssl_ca_location`. AMQP (lapin's rustls connector)
  adds the CA to the OS trust store; lapin offers no way to replace it.
- No insecure mode: there is no option that skips certificate
  verification (decision 14 below).

## Connection and profile configuration

RabbitMQ profiles (decision 3): `rabbit-profiles.json` in the directory of
`profiles.json` (so `FERRIKA_CONFIG_DIR` and `FERRIKA_CONFIG_PATH` move
both), managed with `ferrika rabbit profile add|list|use|remove|show`:

```json
{
  "active_profile": "prod",
  "profiles": {
    "prod": {
      "management_url": "https://rabbit.prod.corp:15671",
      "vhost": "shop",
      "user": "ops",
      "password_in_store": true,
      "amqp_url": "amqps://rabbit.prod.corp:5671",
      "read_only": false
    }
  }
}
```

- The password goes to the OS credential store through
  `ferrika_core::kafka::secrets`, account `<profile>/rabbitmq_password`.
  No Kafka profile field has that name, so a Kafka and a RabbitMQ profile
  with the same name never share an entry, and removing a Kafka profile
  never deletes a RabbitMQ password. Without a credential store (or with
  `FERRIKA_SECRET_STORE=file`) the password stays in the file, as for
  Kafka profiles.
- URLs in a profile must not carry credentials (`profile add` refuses
  them), so the file never holds a password inside a URL.
- `amqp_url` is optional: by default the management host on 5672, or
  `amqps` on 5671 when the management URL is https.
- `read_only` refuses the profile's writes (`publish`, `peek`, `dlq
  inspect`, `dlq redrive`), like a read-only Kafka profile.
- `profile add NAME` takes the connection from the same options as every
  `rabbit` command (`--rabbit-url`, `--vhost`, `--rabbit-user`, the
  password options, `--amqp-url`) plus `--read-only`, `-d` and `--use`.
  Replacing a profile keeps its stored password when no new one is given
  and the user is unchanged.

Resolution (`cli::rabbit::resolve`, once, before the gates): the profile
named by `--rabbit-profile` / `FERRIKA_RABBIT_PROFILE`, else the active
RabbitMQ profile; every option below overrides the profile's value.
`--rabbit-url` without `--rabbit-profile` uses no profile, so a profile's
stored password is never sent to a URL typed by hand, and a stored
password is used only for the profile's own user.

Options and environment variables:

| Option | Environment | Default |
| --- | --- | --- |
| `--rabbit-profile NAME` | `FERRIKA_RABBIT_PROFILE` | the active RabbitMQ profile |
| `--rabbit-url URL` | `FERRIKA_RABBIT_URL` | the profile's |
| `--vhost VHOST` | `FERRIKA_RABBIT_VHOST` | the profile's, else `/` |
| `--rabbit-user USER` | `FERRIKA_RABBIT_USER` | the profile's, else the user in the URL |
| `--rabbit-password` (prefer the variable) | `FERRIKA_RABBIT_PASSWORD` | |
| `--rabbit-password-stdin` | | |
| `--amqp-url URL` | `FERRIKA_RABBIT_AMQP_URL` | the profile's, else derived |

Credentials may also be in the URL (`https://user:pass@host`), as for
Connect. They are never printed: the client's and the options' `Debug`
output redacts them, and errors name only `host:port`.

The earlier plan to add RabbitMQ fields to the Kafka `ClusterProfile` was
dropped (decision 3): no public API change there, and a RabbitMQ-only
connection does not need Kafka's required `bootstrap_servers`.

## Governance mapping of the commands

| Command | Gate | Policy category | Audit |
| --- | --- | --- | --- |
| `rabbit queues / exchanges / bindings` | none (metadata) | | none |
| `rabbit peek` | write gate, and export gate (channel `stdout`) | `messages.peek` | request and result entries (`rabbit peek`, target queue/vhost); `record.export` when exports are audited and stdout is redirected; `policy.blocked` when refused |
| `rabbit publish` | write gate | `produce` | request entry before the call, result entry after (`rabbit publish`, target exchange/vhost/routing key) |
| `rabbit dlq inspect` | write gate, and export gate (channel `stdout`) | `messages.peek` | as `peek` |
| `rabbit dlq redrive` (preview) | write gate | `messages.peek` | request and result entries; shows destinations, no bodies |
| `rabbit dlq redrive --execute` | write gate, confirmation unless `--yes` | `messages.move`, and `produce` must be allowed too | request entry (source queue, vhost, destination rule); result entry with the moved count per destination and, on failure, a fixed reason; never bodies or broker error text |
| `rabbit profile ...` | none (local configuration) | | none |

Masking applies to `peek` when the policy enforces it (or with
`--mask-pii`). A publish that no binding routes (`routed: false`) is an
error, so scripts notice the dropped message, and its audit result entry
records the failure.

## DLQ reprocessing

RabbitMQ dead-lettering: a queue with `x-dead-letter-exchange` (argument or
policy `dead-letter-exchange`) and optionally `x-dead-letter-routing-key`
republishes messages that are rejected without requeue, expire (TTL), exceed
the queue length, or exceed a quorum queue's `delivery-limit`. The broker
adds an `x-death` header: a list of tables, newest first, each with `queue`,
`reason` (`rejected | expired | maxlen | delivery_limit`), `count`,
`exchange`, `routing-keys` and `time`; it also sets
`x-first-death-queue/-reason/-exchange` (and, since 3.13, `x-last-death-*`).

`ferrika rabbit dlq` mirrors `ferrika dlq`:

- `inspect QUEUE [-n N]`: the DLQ's messages grouped by original queue,
  reason, exchange and routing key from the newest `x-death` entry, plus
  each message (payload masked under the policy). Reads through the
  management peek, so it is a `messages.peek` write and an export.
- `redrive QUEUE [--to-exchange X] [-k K] [-n N]`: a preview (management
  peek, at most 500 messages) of where each message would go, with counts
  per destination and the messages that have no `x-death`.
- `redrive ... --execute [--yes]`: moves up to N messages over AMQP
  (below), category `messages.move`. The destination is the first (newest)
  `x-death` entry's `exchange` and first `routing-keys` value; each can be
  overridden. A message without `x-death` needs `--to-exchange`. The
  redrive needs `produce` allowed as well (decision 7).

The move must not lose or duplicate silently. With the management API
alone the safest order is per message: `get` with `ack_requeue_false` is
destructive before the publish succeeds, so the plan is:

1. `get` with `ack_requeue_true` to read a batch (non-destructive);
2. publish each message to its destination (with `x-death` kept or
   stripped, and a `x-ferrika-redriven` header for traceability);
3. remove the published ones from the DLQ.

Step 3 has no "delete this message" call in the management API, so the
move uses AMQP 0-9-1 (`ferrika_rabbitmq::amqp::redrive`, lapin on tokio),
one message at a time on one channel in publisher-confirm mode:

1. `basic.get` without auto-ack (the message is held for this client; one
   at a time, so no prefetched messages are held);
2. `basic.publish` to the destination with `mandatory`, the message's
   properties and headers kept (`x-death` included) plus
   `x-ferrika-redriven-from: <queue>` (decision 8: `x-death` is kept, so
   the broker's history of the message stays and repeated failures keep
   counting), then wait for the confirm;
3. `basic.ack` only after a positive confirm with no returned message.

The first failure stops the move: an unroutable (returned) or nacked
publish, a publish error, no confirm within the timeout, or a message
without a destination is rejected with requeue (`basic.nack`, requeue) and
goes back to the queue; closing the channel returns it too if the reject
fails. A failed ack after a confirmed publish leaves the message in both
queues; the report and the audit outcome say so. A publish that timed out
waiting for its confirm may still arrive, so that case can duplicate as
well. The audit result entry records the moved count per destination and a
fixed reason text, never bodies or broker error text.

Shovel (`/api/parameters/shovel`) with `src-delete-after: queue-length` is
an alternative that moves server-side without AMQP in Ferrika, but it moves
whole batches without per-message destinations.

History and undo: a redrive is not undoable (messages are consumed
downstream), so like `dlq redrive` it is recorded in the audit log only.

## Operation history and undo for RabbitMQ (phase 3)

Topology writes are undoable and fit the existing model (implemented on
`feat/rabbit-topology`):

- `Scope` has a `rabbitmq` field: the management API's `host:port` (never
  credentials), next to `registry` and `connect`. `same_cluster` and
  `check_undo_target` compare it (case ignored); a Kafka scope never
  matches a RabbitMQ one. It is part of the version 2 digest like the rest
  of the scope.
- New `State` variants (`kind` `rabbit_queue`, `rabbit_queue_messages`,
  `rabbit_exchange`, `rabbit_binding`): `RabbitQueue { vhost, name,
  declaration: Option<{type, durable, auto_delete, arguments}> }`,
  `RabbitExchange { vhost, name, declaration: Option<{type, durable,
  auto_delete, internal, arguments}> }`, `RabbitBinding { vhost, binding:
  {source, destination, destination_type, routing_key, arguments}, present
  }` and `RabbitQueueMessages { vhost, name, messages }` (a purge, its own
  resource key so it never overlaps the queue's declaration). Capture is a
  `GET` before the change (`ferrika_ops::history::rabbit`), the after-state
  is read back after it, inverse declares, deletes, binds or unbinds.
- A queue deletion records the queue and its bindings; undo declares it
  again (with its type named, so a changed vhost default does not make it
  another kind) and restores the bindings: `Partial`, the messages are
  lost. An exchange deletion records the exchange and every binding from
  and to it: `Reversible`. A declaration is `Conditional`: undo deletes the
  queue or exchange only while it is empty and unbound. Bind and unbind
  are `Reversible`. Purge is `Irreversible`.
- Steps run in a fixed order (unbind, delete, declare, bind) whatever the
  order of the recorded states.
- Journal compatibility (checked against the 0.2.0 reader): 0.2.0
  deserialises each line into its `State` enum, so a line holding an
  unknown `kind` fails to parse and is skipped by `history`, `undo` and
  `audit verify`; its trimming copies unparsed lines byte for byte. A
  RabbitMQ line without a RabbitMQ state would parse there and show a scope
  without the `rabbitmq` field, whose digest would not match (a false
  "edited"). So every RabbitMQ operation records at least one `rabbit_*`
  state, and a change whose before-state cannot be read is refused instead
  of being recorded as not undoable (decision 20). The test
  `a_0_2_reader_skips_rabbit_operations_whole_and_keeps_the_others`
  (`ferrika-ops`, `history.rs`) parses a journal with the 0.2 shapes.

## Phases

1. **Done (preview):** `ferrika-rabbitmq` with the management client;
   `ferrika rabbit queues | exchanges | bindings | peek | publish`; table
   and `--format json` output.
2. **Done (preview, this branch, for 0.2.0):** RabbitMQ profiles and
   secrets; `messages.peek` / `messages.move`; peek as a write;
   `rabbit dlq inspect`; `rabbit dlq redrive` with an AMQP client.
   Done since: paged listings, TLS with a private CA and client
   certificates, the quorum-queue delivery-limit guard (2026-11-23).
   Still open in this phase: the neutral `GateTarget` type; e2e
   checks against a throw-away `rabbitmq:4-management` container in
   `tools/e2e` (the AMQP move is unit-tested against a fake broker only).
   MCP read tools `rabbit_queues`, `rabbit_exchanges` and `rabbit_bindings`
   (metadata only, a RabbitMQ profile by name or the active one, no URL
   argument) are done. Follow-ups for MCP: `rabbit_peek` and
   `rabbit_dlq_inspect` as write tools (a peek requeues messages, policy
   category `messages.peek`, through the write guard with plan, confirmation
   and the export gate) and `rabbit_dlq_redrive` (`messages.move` plus
   `produce`); both need the neutral gate target first, since the MCP write
   guard is built around one Kafka cluster.
3. **CLI and library done (`feat/rabbit-topology`):** topology writes
   (`rabbit queue declare|delete|purge`, `rabbit exchange declare|delete`,
   `rabbit bind|unbind`) with the new policy categories, the operation
   history and undo (`rabbit history`, `rabbit undo`), unit tests against
   an in-memory management API (`ferrika_rabbitmq::fake`, feature
   `test-util`) and checks against RabbitMQ 4.4.1 in
   `tools/e2e/rabbit_smoke.py`. Still open in this phase: MCP tools and
   desktop views (both need the neutral gate target first, see decision
   14), and the topology categories in the MCP `mcp_write` rule.
4. Stream queues (stream protocol or AMQP consumer with offsets).

## Decisions (2026-11-22)

1. **`rabbit peek` is a write**, not only an export: it marks messages
   redelivered and may count towards a quorum queue's delivery limit. It
   passes the write gate (read-only policies and profiles refuse it) and is
   audited, in addition to the export gate and masking. The stderr warning
   about requeue/redelivered stays. The same holds for `rabbit dlq inspect`
   and the `rabbit dlq redrive` preview, which read the same way.
2. **New policy categories land in 0.2.0, with this branch:**
   `messages.peek` and `messages.move` (see [New policy
   categories](#new-policy-categories)); `rabbit publish` keeps mapping to
   `produce`. Older binaries reading a signed policy that names a new
   category treat it as invalid and run in the strictest mode; documented
   in `docs/POLICY.md` and the CHANGELOG.
3. **RabbitMQ connections get a separate store**, `rabbit-profiles.json`
   next to `profiles.json`, not fields on the Kafka `ClusterProfile` (no
   public API change there). Passwords in the OS credential store under
   `<profile>/rabbitmq_password`. CLI: `ferrika rabbit profile
   add|list|use|remove|show`; commands take `--rabbit-profile` (default:
   the active RabbitMQ profile) with `--rabbit-url`, `--vhost` and
   `--rabbit-user` as overrides. The policy gate matches the
   management `host:port` (and the AMQP one, decision 6).
4. **DLQ redrive uses AMQP via `lapin`** (MIT, 4.13, tokio, rustls with
   ring), only in `ferrika-rabbitmq`, behind the default-on feature `amqp`.
   `rabbit dlq inspect` and `rabbit dlq redrive [--execute]` as described in
   [DLQ reprocessing](#dlq-reprocessing); `--execute` asks for confirmation
   unless `--yes`. The AMQP URL comes from the profile (`amqp_url`), else
   the management host on 5672 (`amqps` on 5671 when the management URL is
   https).
5. **`ferrika-rabbitmq` is published to crates.io as a preview** with the
   other crates (`cargo publish --workspace`): metadata and a README marked
   preview are in place; it is not published from this branch.

6. **The AMQP address is matched by the policy too.** The gate matches a
   comma-separated list of the management `host:port` and the AMQP
   listener's `host:port` (`--amqp-url`, the profile's `amqp_url`, or the
   derived one), so a `read_only`, `read_only_brokers` or `export_clusters`
   rule naming either applies to every `rabbit` command on that connection.
7. **A redrive needs `produce` as well as `messages.move`**, because it
   republishes. `PolicyAction::also_requires` lists the extra category and
   `ferrika_governance::policy::extra_category_refusal` checks it (the CLI
   in `cli/app.rs`, before the write gate); under `forbid: ["produce"]` a `dlq redrive
   --execute` is refused (recorded as `policy.blocked`) with a message that
   names `produce`.
8. **Redriven messages keep their headers**, `x-death` included, plus
   `x-ferrika-redriven-from`; said in `--help` and in [DLQ
   reprocessing](#dlq-reprocessing).
9. **Credentials in the management URL** are used for AMQP too (percent-
   decoded, `ferrika_rabbitmq::profile::url_credentials`) when no user is
   given otherwise. Every `rabbit` command with such a URL prints one
   warning on stderr that credentials in a URL can end up in shell history,
   recommending `rabbit profile add ... --rabbit-password-stdin`; the
   warning never names the user or password.
10. **Rust 1.89 is the minimum for 0.2.0**: `rust-version = "1.89"` in the
    workspace (every crate inherits it), a CI job `msrv` on 1.89, README and
    CONTRIBUTING.
11. **`ferrika-rabbitmq` shares the workspace version**
    (`version.workspace = true`) and is labelled preview in its README and
    crate description.

## Decisions (2026-11-23)

12. **Listings are bounded by default.** `rabbit queues | exchanges |
    bindings` show at most 1000 entries unless `--limit` says otherwise
    (`--limit 0`: all, still fetched 500 per request). A production vhost
    can hold tens of thousands of queues; a bounded default keeps the
    command fast and the response small, and stderr says how many more
    match, so nobody mistakes a cut list for the whole. Scripts that need
    everything pass `--limit 0`. The MCP read tools keep their cap of 500.
13. **`--name` is a substring by default, `--regex` opts in to regular
    expressions.** A substring is what people type most and cannot fail to
    parse; it matches the MCP tools' `filter` and the broker's own default.
    Both are case-insensitive because the broker matches that way.
14. **No insecure TLS mode.** A private CA is handled with
    `--rabbit-ca-cert`; an option to skip verification would be one typo
    away from sending the broker password to an impostor, and would need
    its own policy rule. If one is ever needed it must be explicit per
    command, refused under any company policy, and warn on every use.
15. **Peeks that can dead-letter are refused unless opted in.** On RabbitMQ
    before 4.0 (or of unknown version), `peek`, `dlq inspect` and the `dlq
    redrive` preview of a quorum queue with a delivery limit are refused;
    `--allow-delivery-count` reads anyway, warns, and shows per message how
    many more peeks keep it. The MCP tools refuse such queues with no
    opt-in: an agent should not take a step that can lose messages. On 4.0+
    nothing is refused, since a peek does not count there. The check costs
    one extra request per peek (two for a quorum queue with a limit).

## Quorum queues and the delivery limit

`ferrika_rabbitmq::delivery_limit`: `ManagementClient::peek_risk(vhost,
queue)` reads the queue (`type`, `arguments`, `effective_policy_definition`,
`delivery_limit`) and, for a quorum queue with a positive limit, the broker
version from `/api/overview`. `PeekRisk::peeks_count()` is true for a
quorum queue with a limit on a broker before 4.0 or of unknown version; the
lowest of the argument, the policy and the broker-reported limit applies.
`PeekRisk::fate(message)` turns a peeked message's `x-delivery-count` (as
read, before this peek's requeue) into the number of further peeks that
keep it, or `OverLimit` when this peek's requeue took it past the limit:
with limit L and count c the message stays while c + 1 <= L. Checked on
3.13.7 with a limit of 3: the CLI predicted 2, 1, 0 further peeks and then
"over the limit", and the fourth peek dead-lettered the message.

The AMQP redrive is not affected: it acks moved messages, and a message it
fails to move is returned with `basic.nack` and requeue, which counts as
one failed delivery on every version (one per failed redrive).

## Decisions (2026-11-23, phase 3)

16. **Exchanges get their own categories**, `exchanges.create` and
    `exchanges.delete`, next to the proposed `queues.create`,
    `queues.delete`, `queues.purge` and `bindings.change`. Deleting an
    exchange silently drops what is published to it and removes its
    bindings, which is as harmful as deleting a queue; folding it into
    `bindings.change` or a broad `topology.change` would force operators
    to allow one to allow the other. All six arrive together, so it is one
    policy-format event.
17. **CLI shape.** `rabbit queue declare|delete|purge NAME`, `rabbit
    exchange declare|delete NAME`, `rabbit bind|unbind SOURCE DESTINATION
    [-k KEY] [--destination-type queue|exchange] [--arg K=V]`, like
    `topics create|delete|purge`: changes run directly, deletions and purges
    ask first (`-y` to skip, the prompt says what undo can and cannot
    restore). No `--execute` preview: these are single, recorded and
    (except purge) undoable changes, as Kafka topic changes are. History
    and undo are `rabbit history` and `rabbit undo`, which carry the
    RabbitMQ connection options; `ferrika undo ID` on a RabbitMQ operation
    names `ferrika rabbit undo ID` instead of connecting to Kafka, and
    `ferrika history --all` lists both brokers' operations.
18. **No neutral gate target yet.** The CLI gates the new writes through
    `gate_profile` / `gate_brokers` like the other `rabbit` commands, and
    `rabbit undo` builds its `WriteRequest` from the RabbitMQ profile and
    addresses. `GateTarget` is left for the MCP tools and desktop views,
    which need it to share one write guard; no public gate API changes now.
19. **Declaring something that exists is refused**, so undoing a
    declaration never deletes a queue or exchange this command did not
    make. Undo of a queue declaration refuses a queue holding messages,
    with consumers or with bindings added since, and `--force` does not
    override it (as for an undone Kafka topic creation with records):
    purging first is a separate, audited `queues.purge` decision. Classic
    queues are deleted with the broker's `if-empty` / `if-unused`; RabbitMQ
    4 refuses both flags on quorum and stream queues, so for those the
    check reads the broker's statistics (refreshed every few seconds) just
    before deleting.
20. **A RabbitMQ change whose current state cannot be read is refused**
    (nothing is changed), unlike Kafka changes, which then run and are
    recorded as not undoable. The state read is the same management API the
    change uses, and an entry without a RabbitMQ state would be misread by
    0.2.0 (see the history section).
21. **`ferrika-ops` depends on `ferrika-rabbitmq`** (without `amqp`), so the
    RabbitMQ undo shares the Kafka undo's decision code (`check_request`,
    `decide`, `UndoPlan`, `Step`). The dependency stays one-way
    (ops → rabbitmq → core).

## Open questions

1. **Cluster node names.** Should the gate also match the cluster's nodes
   (`GET /api/nodes`), the way Kafka adds metadata brokers?
