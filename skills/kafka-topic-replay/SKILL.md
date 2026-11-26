---
name: kafka-topic-replay
description: Replay a time window or a filtered subset of a Kafka topic with the ferrika CLI - either by rewinding a consumer group or by copying the records into another topic (optionally filtered, reshaped and PII-masked), previewing first. Use when someone wants to reprocess records from a period, copy production data to a test or staging topic, rebuild a downstream system, or reproduce a bug with real records.
---

# Kafka topic replay

Two ways to replay, pick with the user:

- **Rewind the consumer group**: the application reads the window again
  from the same topic. Nothing new is written to Kafka, but every
  downstream effect happens again. Use the `kafka-safe-offset-reset`
  skill with `--to-timestamp`.
- **Copy into another topic** (this skill): `ferrika transform` copies the
  window, through filters and masking, into a replay or staging topic that
  a test consumer reads. The source topic and its consumers are untouched.

## Before you start

- `ferrika --version` works; global options go before the subcommand.
- Confirm the source cluster (`ferrika -f json profile show`) and, for a
  copy to another cluster, the destination profile.
- `TOPIC`, `REPLAY_TOPIC`, `PROFILE`, `PROD` and `STAGING` below are
  placeholders; epoch-ms values are examples.
- The destination topic must exist and should not be the source topic.

## Steps

### 1. Find the window

```bash
ferrika -f json topics histogram TOPIC --since 2d --bucket 1h
ferrika topics histogram TOPIC --from "2026-11-02 09:00" --to "2026-11-02 12:00" --by-partition
```

Counts per bucket from offsets, nothing read. Note the window's start and
end in epoch ms (`buckets[].start_ms`) and the expected number of records
(sum of `count`). Say the UTC times back to the user.

### 2. Look at the records

```bash
ferrika -f json consume TOPIC --from-timestamp 1762074000000 --to-timestamp 1762084800000 -n 5 -t 5000 --mask-pii
```

Read without a consumer group, bounded and masked. Check the record shape
before writing filters; with a registry schema,
`ferrika schema fields --topic TOPIC --plain` lists the field paths.

### 3. Preview the copy (the default)

```bash
ferrika transform TOPIC --to REPLAY_TOPIC --from-timestamp 1762074000000 \
  --until-end --filter 'timestamp <= 1762084800000' -n 20
```

Without `--execute` nothing is written: up to 20 transformed records go to
stdout as JSON lines and a summary (read, kept, filtered, not JSON) to
stderr. `--from-timestamp` starts the window; there is no end-time option,
so the `timestamp <= ...` filter ends it, and `--until-end` stops at the end
each partition had when the run started.

Steps run in the order given. Useful ones:

- `--filter EXPR`: keep matching records, same syntax as `consume --where`
  (`$.status == "FAILED"`, `key =~ "^eu-"`, `headers.source == "web"`).
- `--mask` (first, so later steps only see masked values) or `--mask=redact`.
- `--drop FIELD`, `--rename OLD=NEW`, `--set FIELD=JSON`, `--rekey FIELD`.
- `--sample N`: one record in N.

### 4. Approval and execute

Show the user the preview, the expected count and the destination. After an
explicit yes, run the same command with `--execute` and without `-n` unless
they want a limit:

```bash
ferrika -f json transform TOPIC --to REPLAY_TOPIC --from-timestamp 1762074000000 \
  --until-end --filter 'timestamp <= 1762084800000' --execute
```

The JSON summary counts the records read and written; compare it with the
expected count. A run with `--from-timestamp` takes no consumer group, so an
interrupted run starts over and writes the first records again. For a
resumable copy of a whole topic, use `-g GROUP -b` instead of
`--from-timestamp` (an offset is committed only after the destination
acknowledged the record).

### 5. Production data into a lower environment

```bash
ferrika -p PROD transform TOPIC --to TOPIC --dst-profile STAGING --mask \
  --from-timestamp 1762074000000 --until-end --filter 'timestamp <= 1762084800000' -n 20
```

Put `--mask` first. A company policy may require it (enforced masking or a
`copy_down: masked` rule) or forbid the export entirely; a refusal is final.
Add `--execute` only after approval, as in step 4. A whole-topic copy
without reshaping is `ferrika mirror`, which also takes `--mask`.

## Safety rules

- Preview first; never `--execute` before the user approved the preview.
- Records written to Kafka cannot be taken back; there is no undo for a
  copy. Double-check the destination topic and cluster.
- Never copy production records to another environment without `--mask`
  unless the user explicitly says the data is not personal.
- An error that starts with `Refused` comes from the company policy or a
  read-only profile. Stop and tell the user.
- Never print secrets; keep `--mask-pii` on when reading records.

## With the Ferrika MCP server

`transform_preview` shows the same preview (steps as one-key objects in
order). With `--allow-write`, `transform_execute` copies within the same
cluster, at most 10,000 records per call; copies to another cluster are CLI
only.
