# Multi-broker support: broker abstraction and RabbitMQ

Status: stage 2 design, with a first preview implementation
(`ferrika rabbit`, crate `ferrika-rabbitmq`). Kafka behaviour is unchanged.

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
| MCP | `ferrika-mcp` `guard.rs`, `write_tools.rs` | Kafka tools only. |

So the gates and the audit log are already broker-neutral in their data;
what ties them to Kafka is (1) the policy categories, (2) how the CLI finds
the profile and broker addresses to match, and (3) the history state model.

## The abstraction

### Broker-neutral (shared by every broker)

- **Target address.** The policy matches "broker addresses". For Kafka these
  are bootstrap servers (plus metadata brokers); for RabbitMQ it is the
  `host:port` of the management API the command talks to. `read_only`,
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
   (`ferrika-core`, `model/policy.rs`). No new category, so existing signed
   policies keep their meaning and older binaries keep accepting policies.
2. `cli/app.rs` asks `gate_profile` and `gate_brokers` for the profile name
   and broker addresses instead of always reading the Kafka options. For
   every Kafka command they return what was used before.

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

and a `brokers_for` per kind. This is not needed for the CLI preview.

### New policy categories

A signed policy with an unknown `forbid` entry is refused by
`parse_payload`, so a binary that does not know a new category falls back
to the strictest mode. New categories are therefore a policy-format event,
not a free addition. Proposed when RabbitMQ topology writes arrive (phase 3):

| Category | Covers |
| --- | --- |
| `queues.create` | declare a queue or exchange |
| `queues.delete` | delete a queue or exchange |
| `queues.purge` | purge a queue |
| `bindings.change` | add or remove a binding |
| `messages.move` | move or redrive messages between queues |

Until then, `from_cli` maps unknown `rabbit ...` writes to `config.change`
(the broadest category), as for any unknown write.

## Crate layout

```
ferrika-cli ──> ferrika-mcp ──> ferrika-ops ──> ferrika-governance ──> ferrika-core
     └────────> ferrika-rabbitmq ─────────────────────────────────────────┘
```

- `crates/ferrika-rabbitmq`: the management API client (`client`) and the
  typed responses and request bodies (`model`). Depends on `ferrika-core`
  only (HTTP client, Base64). It does no policy, audit or masking; like the
  Schema Registry and Connect clients, it is a client.
- `ferrika-cli`: `ferrika rabbit ...` in `cli/rabbit.rs`, classified in the
  gates in `cli/app.rs`. The CLI re-exports the crate as `rabbitmq`.
- The Kafka path is untouched. `ferrika-ops` and `ferrika-mcp` do not depend
  on the new crate yet; DLQ reprocessing (phase 2) adds `ferrika-ops →
  ferrika-rabbitmq`.

No new third-party dependency: `serde`, `serde_json` and `thiserror` are
already workspace dependencies.

## Access method: the management HTTP API

The MVP uses the `rabbitmq_management` plugin's HTTP API through Ferrika's
own HTTP/1.1 client (`ferrika_core::kafka::http::Endpoint`, used for the
Schema Registry and Kafka Connect: http and https with the OS trust store,
basic authentication, a 30 s timeout, a 64 MiB response cap). There is no
AMQP dependency.

| Command | Request |
| --- | --- |
| `rabbit queues` | `GET /api/queues/{vhost}` |
| `rabbit exchanges` | `GET /api/exchanges/{vhost}` |
| `rabbit bindings` | `GET /api/bindings/{vhost}` |
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
- on quorum queues a requeue can count as a delivery attempt towards
  `delivery-limit` (default 20 since RabbitMQ 4.0); repeated peeks can push a
  message over the limit and dead-letter or drop it;
- stream queues do not support `get`; reading them needs a stream consumer
  (later phase);
- messages are held briefly while being fetched; a peek on a busy queue
  competes with consumers.

The CLI caps a peek at 500 messages, truncates payloads at 50 000 bytes
(reported as `truncated` with the full `size`), and says on stderr that the
messages were requeued and marked redelivered. Whether peek should count as
a write under read-only rules is an open decision (below).

Why not AMQP for the MVP: the management API covers listing, peeking and
publishing with one HTTP client we already ship and audit; AMQP 0-9-1 would
add a dependency and a connection model (channels, confirms) that only pays
off for high-volume moves (bulk DLQ redrive) and stream reading. Listing is
also unpaginated for now: a vhost with tens of thousands of queues can hit
the 64 MiB cap; `?page=&page_size=` support is phase 2.

## Connection and profile configuration

Now (preview), from options and environment variables only:

| Option | Environment | Default |
| --- | --- | --- |
| `--rabbit-url URL` | `FERRIKA_RABBIT_URL` | none (required) |
| `--vhost VHOST` | `FERRIKA_RABBIT_VHOST` | `/` |
| `--rabbit-user USER` | `FERRIKA_RABBIT_USER` | user in the URL, if any |
| `--rabbit-password` (prefer the variable) | `FERRIKA_RABBIT_PASSWORD` | |
| `--rabbit-password-stdin` | | |

Credentials may also be in the URL (`https://user:pass@host`), as for
Connect. They are never printed: the client's and the options' `Debug`
output redacts them, and errors name only `host:port`.

Next (phase 2): RabbitMQ fields on a profile, so a profile names one
system and its credentials live in the OS credential store through the
existing `kafka::secrets` mechanism:

- `rabbitmq_url` (secret field, like `connect_url`, since it may carry
  credentials), `rabbitmq_vhost`, `rabbitmq_user`, `rabbitmq_password`
  (secret);
- the profile's `read_only` flag then also guards `rabbit` writes, and the
  profile name is what `read_only` globs match;
- open question whether a profile may hold only RabbitMQ (no
  `bootstrap_servers`): `ClusterProfile::bootstrap_servers` is required
  today. Adding fields to the public `ClusterProfile` struct is a
  library-API change for 0.2.0.

## Governance mapping of the commands

| Command | Gate | Policy category | Audit |
| --- | --- | --- | --- |
| `rabbit queues / exchanges / bindings` | none (metadata) | | none |
| `rabbit peek` | export gate, channel `stdout` | | `record.export` when exports are audited and stdout is redirected; `policy.blocked` when forbidden |
| `rabbit publish` | write gate | `produce` | request entry before the call, result entry after (`rabbit publish`, target exchange/vhost/routing key) |

Masking applies to `peek` when the policy enforces it (or with
`--mask-pii`). A publish that no binding routes (`routed: false`) is an
error, so scripts notice the dropped message, and its audit result entry
records the failure.

## DLQ reprocessing (phase 2, design only)

RabbitMQ dead-lettering: a queue with `x-dead-letter-exchange` (argument or
policy `dead-letter-exchange`) and optionally `x-dead-letter-routing-key`
republishes messages that are rejected without requeue, expire (TTL), exceed
the queue length, or exceed a quorum queue's `delivery-limit`. The broker
adds an `x-death` header: a list of tables, newest first, each with `queue`,
`reason` (`rejected | expired | maxlen | delivery_limit`), `count`,
`exchange`, `routing-keys` and `time`; it also sets
`x-first-death-queue/-reason/-exchange` (and, since 3.13, `x-last-death-*`).

`ferrika rabbit dlq` would mirror `ferrika dlq`:

- `inspect QUEUE`: group the DLQ's messages by original queue and reason
  from `x-death` (peek, masked under the policy, export-gated).
- `redrive QUEUE [--to-exchange X] [--routing-key K] [--execute]`: preview
  by default; with `--execute`, a write `rabbit dlq redrive --execute`,
  category `produce` (or `messages.move` once that category exists).
  The original destination is the first `x-death` entry's `exchange` and
  `routing-keys`, overridable.

The move must not lose or duplicate silently. With the management API
alone the safest order is per message: `get` with `ack_requeue_false` is
destructive before the publish succeeds, so the plan is:

1. `get` with `ack_requeue_true` to read a batch (non-destructive);
2. publish each message to its destination (with `x-death` kept or
   stripped, and a `x-ferrika-redriven` header for traceability);
3. remove the published ones from the DLQ.

Step 3 has no "delete this message" call in the management API, so a
correct move needs AMQP (consume with manual ack, publish with publisher
confirms, then ack). The redrive is therefore the point where an AMQP client
(`lapin` or a minimal 0-9-1 implementation) is added, behind a feature if
binary size matters. Shovel (`/api/parameters/shovel`) with
`src-delete-after: queue-length` is an alternative that moves server-side
without AMQP in Ferrika, but it moves whole batches without per-message
filtering.

History and undo: a redrive is not undoable (messages are consumed
downstream), so like `dlq redrive` it is recorded in the audit log only.

## Operation history and undo for RabbitMQ (phase 3)

Topology writes are undoable and fit the existing model:

- `Scope` gains a `rabbitmq` field (management address, redacted) next to
  `registry` and `connect`.
- New `State` variants: `Queue { vhost, name, kind, durable, arguments }`,
  `Exchange { vhost, name, kind, durable, internal, arguments }`,
  `Binding { vhost, source, destination, destination_type, routing_key,
  arguments }`, with capture (`GET` before the change) and inverse (declare
  or delete).
- Deleting a queue is `Partial`: undo re-declares it, the messages are gone.
  Purge is `Irreversible`.
- The journal format is versioned in `model::history`; new variants must
  be skipped (not fail) by older readers before they ship.

## Phases

1. **This step (preview):** `ferrika-rabbitmq` with the management client;
   `ferrika rabbit queues | exchanges | bindings | peek | publish`; publish
   through the write gate (`produce`), peek through the export gate and
   masking; table and `--format json` output.
2. RabbitMQ profile fields and secrets; pagination; `rabbit dlq inspect`;
   `rabbit dlq redrive` (preview without `--execute`, then with an AMQP
   client); the neutral `GateTarget` type; e2e checks against a throw-away
   `rabbitmq:4-management` container in `tools/e2e`.
3. Topology writes (declare/delete/purge queues and exchanges, bindings)
   with new policy categories, history and undo; MCP tools; desktop views.
4. Stream queues (stream protocol or AMQP consumer with offsets).

## Open decisions

1. **Peek under read-only rules.** Peek changes queue state (redelivered
   flag, quorum delivery count). Keep it a read gated as an export (current),
   or classify it as a write so `read_only` refuses it, or refuse it only on
   quorum queues with a `delivery-limit`?
2. **Policy categories.** Reuse `produce` for publish (current). Introduce
   the `queues.*`, `bindings.change` and `messages.move` categories in the
   next policy-format version, accepting that older binaries go strictest
   with such a policy?
3. **Address matching.** The policy matches the management API's
   `host:port`, not the AMQP listener (5672/5671) or the cluster's node
   names. Should RabbitMQ also add the cluster's nodes (`GET /api/nodes`)
   the way Kafka adds metadata brokers, and should policies be able to name
   AMQP addresses?
4. **Profiles.** Extend `ClusterProfile` (one profile per system, Kafka or
   RabbitMQ) or add a separate RabbitMQ profile store? Affects the public
   library API in 0.2.0.
5. **AMQP dependency** for DLQ redrive: `lapin` (adds an async AMQP stack)
   versus a minimal in-house 0-9-1 client versus Shovel only.
6. **Publishing the crate.** `ferrika-rabbitmq` is a normal workspace crate
   so `cargo publish --workspace` keeps working (the CLI depends on it).
   Publish it as a preview 0.x crate, or keep it behind a CLI feature until
   it stabilises?
