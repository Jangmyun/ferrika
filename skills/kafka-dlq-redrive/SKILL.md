---
name: kafka-dlq-redrive
description: Inspect a Kafka dead letter topic (DLQ/DLT) and send its records back to the original topic safely with the ferrika CLI - group the failures by cause, preview the redrive, run it in small resumable batches. Use when someone asks what is in a dead letter queue, why records failed, or to replay, retry or redrive dead-lettered Kafka records.
---

# Kafka dead letter inspect and redrive

Find out why records were dead-lettered, make sure the cause is fixed, then
send them back in small, resumable batches. Redriving before the cause is
fixed only fills the dead letter topic again.

Ferrika reads the usual dead letter headers: Spring Kafka (`kafka_dlt-*`),
Kafka Connect (`__connect.errors.*`) and `x-original-topic` /
`x-exception-message` style headers.

## Before you start

- `ferrika --version` works; global options go before the subcommand.
- Confirm the cluster: `ferrika -f json profile show`; use `-p PROFILE`.
- `DLQ_TOPIC`, `TOPIC`, `RETRY_TOPIC` and `PROFILE` below are placeholders.

## Steps

### 1. What failed and why

```bash
ferrika dlq inspect DLQ_TOPIC -n 200 --json
```

- `analysis.total_messages`, `analysis.error_categories` (category and
  count, such as `Downstream Timeout` or `Deserialization Error`),
  `analysis.original_topics` (topic and count),
  `analysis.earliest_timestamp_ms` / `latest_timestamp_ms`.
- `messages[]`: `partition`, `offset`, `key`, `original_topic`,
  `original_partition`, `original_offset`, `exception_message`,
  `retry_count`, `payload_preview` and `headers`.

`payload_preview` and header values are record data and are shown unmasked
unless the company policy enforces masking. Summarize by category; do not
paste payloads into the conversation. Narrow to one cause with
`--filter-error`:

```bash
ferrika dlq inspect DLQ_TOPIC -n 200 --json --filter-error timeout
```

To see a few payloads with PII masked, read without a consumer group:

```bash
ferrika -f json consume DLQ_TOPIC -b -n 3 -t 5000 --mask-pii --show-headers
```

### 2. Is the cause fixed?

Ask the user. As a rule of thumb:

- Timeouts, unavailable downstream services: transient; redrive once the
  dependency is healthy.
- Deserialization or validation errors: the records will fail again unless
  the consumer was fixed or the records are transformed; do not redrive
  them to the original topic as they are.

Also check that the dead letter topic keeps records at least as long as
the source (`ferrika lint --rule T013`).

### 3. Preview (the default)

```bash
ferrika dlq redrive DLQ_TOPIC --json
```

Nothing is written. `total_examined`, `planned_redrives`,
`skipped_unresolved_topic` (records whose original topic is neither in a
header nor inferable from the dead letter topic's name; `-t` sends every
record to one topic instead), `skipped_already_redriven` (they carry
`x-redriven-by` from an earlier redrive) and `target_topic_distribution`.
Show this to the user.

### 4. Approval, then a small first batch

Ask the user to approve the target topics and the count. Start small and
record progress in a consumer group of its own on the dead letter topic, so
a rerun continues instead of sending the same records twice:

```bash
ferrika dlq redrive DLQ_TOPIC -g dlq-redrive-orders -n 10 --execute --json
```

The result adds `published`, `failed` and `group` to the preview's fields.
Redriven records get the `x-redriven-by` and `x-redrive-timestamp` headers.
An offset is committed only after its record was delivered. Wait for the
user to confirm the application processed them (and that they did not land
in the dead letter topic again), then continue with larger batches using the
same `-g`:

```bash
ferrika dlq redrive DLQ_TOPIC -g dlq-redrive-orders -n 500 --execute --json
```

To a retry topic instead of the original one:

```bash
ferrika dlq redrive DLQ_TOPIC -t RETRY_TOPIC -g dlq-redrive-orders -n 10 --execute --json
```

Without `-g` every run starts from the beginning of the dead letter topic,
and the `x-redriven-by` marker is the only thing that stops duplicates;
`--from-offset` (`N` or `P:N`) starts elsewhere.

### 5. Keep a copy when the user wants one

```bash
ferrika dlq export DLQ_TOPIC -o dlq-export.json -n 500
```

The file holds whole record values (masked only under an enforced masking
policy). Tell the user where it is and that it may contain personal data.

## Safety rules

- Never pass `--execute` before the user approved the preview, and never
  redrive records whose failure is not fixed.
- Produced records cannot be taken back: there is no undo for a redrive.
  Prefer `-g`, small `-n` batches, and a retry topic when unsure.
- Do not pass `--include-redriven` unless the user asks; it sends records a
  second time.
- An error that starts with `Refused` comes from the company policy or a
  read-only profile (a redrive is a `produce`). Stop and tell the user.
- Do not paste payloads, header values or stack traces with personal data;
  never print secrets.

## With the Ferrika MCP server

The MCP server has no Kafka dead letter tools; `peek_messages` (masked by
default) reads a dead letter topic. Run the redrive with the CLI.
