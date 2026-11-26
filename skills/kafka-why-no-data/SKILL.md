---
name: kafka-why-no-data
description: Diagnose why a Kafka topic or consumer shows no data, using the ferrika CLI read-only. Use when someone says a topic is empty, "my consumer receives nothing", "messages are missing", "the producer says it sent but I see nothing", or records stopped arriving.
---

# Why is there no data?

Walks from the cluster to the topic, its partitions, the records and the
consumer group until the gap is found. Every command here only reads.

## Before you start

- `ferrika --version` works (else ask the user to install Ferrika:
  <https://github.com/Jangmyun/ferrika#install>).
- Global options go before the subcommand: `ferrika -p PROFILE -f json ...`.
- `TOPIC`, `GROUP`, `PROFILE` and `CONNECTOR` below are placeholders.

## Steps

Stop at the first step that explains the problem and report it.

### 1. Right cluster?

```bash
ferrika -f json profile show
ferrika -f json profile list
```

`profile show` gives the active profile's `name` and `bootstrap_servers`. A
very common cause is looking at staging while the producer writes to
production (or the reverse). Ask the user which cluster the producer and the
consumer use, and pass `-p PROFILE` from then on.

### 2. Does the topic exist, and does it hold records?

```bash
ferrika -f json topics describe TOPIC
```

- An `unknown topic` error: the name is wrong (Ferrika never creates a topic
  by looking it up). List near matches with `ferrika -f json topics list`
  and compare names (case, `-` versus `_`, prefixes).
- `total_messages` and, per partition, `high_watermark` and `low_watermark`:
  - `high_watermark` 0 everywhere: nothing was ever written. The problem is
    on the producer side (step 6).
  - `high_watermark` > 0 but equal to `low_watermark`: records were written
    and are already deleted, by retention or a purge. Check step 3.
  - `high_watermark` > `low_watermark`: records are there; continue with
    step 4.
- A partition without a live leader, or an `isr` shorter than `replicas`: run
  `ferrika -f json health --fail-on never` (leaderless partitions cannot be
  read or written).

### 3. Retention and recent changes

```bash
ferrika -f json topics config TOPIC
ferrika -f json history --resource topic:TOPIC
```

A short `retention.ms` (or `retention.bytes`) deletes records before a slow
consumer reads them. `history` lists changes made with Ferrika (purges,
config changes) with who and when; `ferrika history show ID` gives the
before and after values.

### 4. When did records arrive?

```bash
ferrika topics histogram TOPIC --since 24h
ferrika -f json topics histogram TOPIC --since 7d --bucket 1h
```

Counts per time bucket from offsets, without reading records. It shows when
records stopped arriving, gaps and bursts. In JSON, `buckets[].start_ms`
(epoch ms) and `buckets[].count`; `after` counts records with timestamps in
the future (producer clock skew: a time-based consumer or query skips them),
`before` records older than the range. The table ends with a `consume`
command for the busiest bucket.

### 5. Look at the newest records

Read without a consumer group (nothing is committed, the application is not
affected), bounded, with PII masked:

```bash
ferrika -f json consume TOPIC --offset -5 -n 5 -t 5000 --mask-pii --show-headers
```

`--offset -5` starts five records before the end of every partition. Each
line has `partition`, `offset`, `timestamp_ms`, `key`, `payload` and
`headers`.

- Records come back here but not in the application: the application
  filters them out or cannot decode them. Try `-D` (Schema Registry wire
  format) or `--hex` to see the real encoding; a producer that switched to
  Avro breaks a JSON consumer.
- If the user filters (in the application or with `--where`), check the field
  names. `ferrika schema fields --topic TOPIC --plain` lists the paths a
  `--where` expression can use when the topic has a registry schema.

A filtered read over the whole topic:

```bash
ferrika -f json consume TOPIC -b -n 20 -t 5000 --mask-pii -W '$.order_id == "A-1001"'
```

### 6. The consumer group's view

```bash
ferrika -f json monitor lag
ferrika -f json monitor lag GROUP
```

- The group is missing from the list (`monitor lag GROUP` exits 1): the
  application uses another group id, or never committed.
- `active_members` 0: the application is not connected.
- Every partition `is_caught_up` with lag 0 while the user expects old
  records: the group's committed offsets are at the end. A new group starts
  at the end too when the consumer uses `auto.offset.reset=latest`. Reading
  older records needs an offset reset; use the `kafka-safe-offset-reset`
  skill.
- `current_offset` below `low_watermark`: the group fell behind retention.

### 7. The producer side

Ferrika cannot see a producer that fails before it reaches the broker. Ask
for the producer's errors (authorization, `RecordTooLarge`, timeouts). Useful
reads:

```bash
ferrika acl list -t TOPIC
ferrika connect status CONNECTOR
```

`acl list -t` shows every binding that applies to the topic (needs
permission to describe ACLs). `connect status` shows a Kafka Connect
connector's tasks and failure traces when the topic is fed by Connect.

## Report

Say which step found the cause, quote the numbers (watermarks, counts,
times converted from epoch ms to UTC), and name the fix and who owns it.

## Safety rules

- Everything here reads. Do not produce test records to "see if it works"
  unless the user asks; `produce`, `probe` and `generate` write to the
  cluster.
- Never use `consume -g` with the application's group id: it joins the
  group and commits offsets.
- Always bound `consume` with `-n` and `-t`; keep `--mask-pii` on and show
  only what the user needs.
- An error that starts with `Refused` comes from the company policy or a
  read-only profile (an export ban can refuse `consume` with a redirect or a
  pipe). Stop and tell the user.
- Never print or ask for passwords, tokens or SASL secrets.

## With the Ferrika MCP server

`describe_topic`, `message_histogram`, `peek_messages` (with `where`),
`schema_fields`, `list_consumer_groups` and `consumer_group_lag` cover steps
2 to 6.
