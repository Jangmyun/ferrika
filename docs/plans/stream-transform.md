# Stream transform (`ferrika transform`)

Status: implemented for 0.2.0 (CLI). MCP tool: follow-up.

## Goal

Give operators a safe, built-in way to copy records from one topic to
another while filtering and reshaping them, without writing a Kafka Streams
or Connect application. It is an operations tool, not a stream processor.

## Use cases

- Copy a filtered subset of a topic to another topic or cluster to debug or
  reproduce a problem (`--filter '$.order_id == "A-17"'`).
- Copy production data to a lower environment with PII masked
  (`--mask --dst-profile staging`).
- Re-key a topic (`--rekey customer.id`) to test a new partitioning.
- Drop, rename or add fields for a consumer under test
  (`--drop`, `--rename`, `--set`).
- Take a small deterministic sample (`--sample 100`).

## Non-goals

- Stateful processing: joins, windows, aggregations, deduplication.
- Exactly-once delivery, within or across clusters (no transactions).
- Schema evolution: values decoded with `-D` are written as JSON, not
  re-encoded with a registry schema.
- A long-running managed service. A run is a foreground command; for a
  continuous copy, run it under a supervisor with `-g`.

## Command

```
ferrika [GLOBAL] transform <SRC_TOPIC> --to <DST_TOPIC>
        [--dst-profile P | --dst-bootstrap-servers B]
        [STEP]... [-b | --from-timestamp MS] [--until-end] [-n N]
        [-g GROUP] [-p PARTITION] [-r RATE] [-D [-S URL]]
        [--dry-run | --execute]
```

The source is the cluster the global options select (`-p`, `-b`), as for
`consume`. The destination defaults to the same cluster.

### Steps

Steps are repeatable flags applied in the order they appear on the command
line (clap's argument indices give the order across different flags).

| Step | Argument | Effect |
| --- | --- | --- |
| `--filter` | `EXPR` | Keep records the expression matches |
| `--drop` | `FIELD` | Remove a value field |
| `--rename` | `OLD=NEW` | Move a value field (to a new name or path) |
| `--set` | `FIELD=JSON` | Set a value field to a JSON literal |
| `--rekey` | `FIELD` | New record key from a value field |
| `--mask` | `[=partial\|redact]` | Mask PII in key, value and header values |
| `--sample` | `N` | Keep records whose offset is a multiple of N |

`FIELD` is a dotted path (`user.address.city`, `items.0`); a leading `$.` is
accepted. `--set` creates missing parent objects; it does nothing when a
parent is a scalar or an array index is out of range. A rename that cannot
be placed leaves the value unchanged.

### Expression syntax

`--filter` reuses the record expression language of `consume --where`
(`ferrika_core::model::expr`) rather than adding a second one:

- operands: `$.a.b`, `$.items[0]`, `$["odd key"]` (value fields), `$` (the
  whole value), `key`, `partition`, `offset`, `timestamp`, `headers.<name>`;
- literals: `"text"`, `'text'`, numbers, `true`, `false`, `null`;
- operators: `== != > >= < <=`, `=~` (regex, case-insensitive), `contains`,
  `exists`, `&&`/`and`, `||`/`or`, `!`/`not`, parentheses.

Examples: `$.user.country == "KR"`, `$.amount > 100`, `key =~ "^order-"`,
`headers.source == "web"`. The parser is hand-written, bounded in length,
depth and term count, and has no new dependency. `Expr::matches_parsed` was
added so a filter after a field step sees the changed value without
re-serializing it.

### Values that are not JSON

Field steps act on values that are JSON objects (a leading byte order mark
is allowed). Text, binary, JSON scalars and arrays pass through them
unchanged and are counted as "not JSON" in the summary; a `$.` filter on
them is false, while `$` alone compares their text. Tombstones (null values)
stay tombstones and are not counted.

A value no step changed is written byte for byte as read. A changed value
is written as compact JSON (field order kept). The source timestamp and
headers are kept; the partition is left to the producer, so a new key
picks its partition the usual way.

### Schema Registry (`-D`)

With `-D` the value is decoded by the existing `PayloadDecoder` (Avro,
Protobuf, JSON Schema; schemas cached per id). A value that decodes to JSON
is processed and written as JSON text (no wire-format header). A value that
does not decode passes through unchanged. Keys are not decoded.

## Semantics

- Delivery is at least once. With `-g GROUP` (and `--execute`) the source
  offset of a record is stored only after the destination acknowledged its
  copy, or after a step dropped it, and never past a failed record; stored
  offsets are committed periodically and at the end. A failed or
  interrupted run copies some records again rather than skipping any.
- Start: the current end (default), `--from-beginning`, `--from-timestamp`
  (per partition, the first offset at or after the time; not with `-g`), or
  the group's committed offsets.
- Stop: `--max-records` written, `--until-end`, Ctrl-C, or too many failed
  deliveries (100, as for `mirror`). `--until-end` captures each partition's
  high watermark at the start and stops when every partition reached it;
  `enable.partition.eof` finishes partitions that end in transaction
  markers. Records past the captured end are left for the next run.
- Exit status is non-zero when any write failed.

## Dry run and `--execute`

Following `dlq redrive`, the command previews unless `--execute` is given.
A dry run prints up to 10 transformed records (or `-n`) as JSON lines on
stdout, the summary on stderr, writes nothing, joins no consumer group and
commits nothing (`-g` is ignored). `--dry-run` may be given explicitly and
conflicts with `--execute`.

Summary counters: read, kept by the steps, written (printed), dropped by
`--filter`, left out by `--sample`, not JSON, `--rekey` field missing,
failed deliveries.

## Governance

- `transform --execute` is a write (`PolicyAction::Produce`) in
  `write_action`, with no wildcard arm, like the other writes.
- Without cluster options the gate in `main` checks the source cluster,
  which is also the destination. With `--dst-profile` or
  `--dst-bootstrap-servers`, `mirror::authorize` (now generic over a
  `CopyRoute` that `mirror` and `transform` implement) checks the
  destination's write gate (read-only profiles and brokers,
  `forbid: produce`) and, when the destination is another cluster
  (`destination_within_source` is false), the export gate
  (`export: forbidden | audited`, `export_clusters`).
- A dry run is a record export to stdout and goes through `export_gate`
  (`transform --dry-run`).
- Enforced PII masking: `mirror` is refused outright. `transform` is
  allowed, dry or not, only when `--mask` is the first step, so no filter
  or field step sees raw values (a filter on raw values could reveal
  masked data through which records were copied). Otherwise it is refused
  with a `policy.blocked` entry.
- Audit: one request entry and one result entry per run. The result
  outcome holds the counts and the SHA-256 of the step list
  (`read N, written N, filtered N, sampled out N, not JSON N, failed N;
  steps sha256:...`). The steps themselves are not logged: a filter or
  `--set` can quote a record value. Record keys, values and header values
  are never logged.
- The policy has no "lower environment" classification today. The
  destination is judged by its profile and brokers (read-only rules) and,
  across clusters, by the export rules. A rule such as "copies to these
  clusters must be masked" is a possible follow-up in the policy format.

## Code layout

- `ferrika-core`: `Expr::matches_parsed`, `DriverError::partition_eof`.
- `ferrika-ops::transform`: field paths, step parsing and application,
  counters, the spec digest (pure, unit-tested).
- `ferrika-ops::mirror`: the copy loop became `run_copy_pipeline` with a
  `RecordTransform` hook and a `CopyDestination` (cluster or preview);
  `run_mirror_pipeline` is that loop with a verbatim transform. New
  `MirrorConfig::from_timestamp` and `until_end`.
- `ferrika-cli::cli::transform`: arguments (the ordered step flags are a
  hand-written `clap::Args`), the `-D` decoder hook, output and audit
  detail. Gate wiring in `cli/app.rs`.

## Follow-ups

- MCP tool (`transform_topic`, dry run by default, write gated like the
  other MCP write tools).
- Key decoding with `-D`, and re-encoding to a registry schema.
- A policy rule that requires masking for copies to given clusters.
- `--sample` by key hash (stable across partitions) in addition to offset.
- An end-to-end check in `tools/e2e/smoke.py` (needs a broker).
