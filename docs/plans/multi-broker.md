# Multi-broker support: broker abstraction and RabbitMQ

Status: stage 2 design with a preview implementation (`ferrika rabbit`,
crate `ferrika-rabbitmq`), planned for 0.2.0. The product decisions of
2026-11-22 are in [Decisions](#decisions-2026-11-22) and supersede the
earlier proposals below where they differ. Kafka behaviour is unchanged.

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

and a `brokers_for` per kind. This is not needed for the CLI preview.

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

Still proposed, for RabbitMQ topology writes (phase 3): `queues.create`,
`queues.delete`, `queues.purge`, `bindings.change`. Until then `from_cli`
maps unknown `rabbit ...` writes to `config.change` (the broadest
category), as for any unknown write.

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
- The Kafka path is untouched. `ferrika-ops` does not depend on the new
  crate; the DLQ redrive lives in `ferrika-rabbitmq::amqp`, called by the
  CLI. `ferrika-mcp` uses it without the `amqp` feature for its read-only
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
messages were requeued and marked redelivered. Because of these side
effects a peek is a write under the policy (decision 1 below).

Why not AMQP for everything: the management API covers listing, peeking
and publishing with one HTTP client we already ship and audit; AMQP 0-9-1
is used only where the management API cannot do the job safely, moving
messages (`dlq redrive --execute`). Listing is
also unpaginated for now: a vhost with tens of thousands of queues can hit
the 64 MiB cap; `?page=&page_size=` support is phase 2.

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

1. **Done (preview):** `ferrika-rabbitmq` with the management client;
   `ferrika rabbit queues | exchanges | bindings | peek | publish`; table
   and `--format json` output.
2. **Done (preview, this branch, for 0.2.0):** RabbitMQ profiles and
   secrets; `messages.peek` / `messages.move`; peek as a write;
   `rabbit dlq inspect`; `rabbit dlq redrive` with an AMQP client.
   Still open in this phase: pagination; the neutral `GateTarget` type; e2e
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
3. Topology writes (declare/delete/purge queues and exchanges, bindings)
   with new policy categories, history and undo; MCP tools; desktop views.
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

## Open questions

1. **Cluster node names.** Should the gate also match the cluster's nodes
   (`GET /api/nodes`), the way Kafka adds metadata brokers?
