---
name: kafka-lag-triage
description: Find out why a Kafka consumer group is lagging and what to do about it, using the ferrika CLI read-only. Use when someone reports consumer lag, a backlog, "the consumer is behind", a lag alert, or slow processing of a topic.
---

# Kafka consumer lag triage

Works out whether lag comes from stopped consumers, slow consumers, one hot
or stuck partition, or a burst of input, and proposes the next step. Every
command here only reads the cluster.

## Before you start

- `ferrika --version` works. If not, ask the user to install Ferrika
  (<https://github.com/Jangmyun/ferrika#install>).
- Confirm which cluster you are on. `ferrika -f json profile show` names the
  active profile and its brokers; pass `-p PROFILE` on every command if the
  user names another one. Never guess a production profile.
- Put global options before the subcommand: `ferrika -p PROFILE -f json ...`.
- In the examples, `GROUP`, `TOPIC` and `PROFILE` are placeholders.

## Steps

### 1. Overview of all groups

```bash
ferrika -f json monitor lag
```

One object per group: `group_id`, `state` (`Stable`, `Rebalancing`,
`Empty`, `Dead`), `active_members`, `target_topics`, `total_lag`.
Pick the group the user means; if they did not name one, list the groups
with the largest `total_lag` and ask.

### 2. Per-partition lag of the group

```bash
ferrika -f json monitor lag GROUP
```

One object per partition: `topic`, `partition`, `current_offset` (committed),
`high_watermark`, `low_watermark`, `lag`, `is_caught_up`, and `behind_ms`
(how old the oldest unconsumed record is, in milliseconds; absent when the
partition is caught up or the time is unknown). Exit status 1 means the group
does not exist: check the group id with the user (a typo or a renamed
application is common).

Add `-t TOPIC` to look at one topic of a group that reads several.

### 3. Classify

| What you see | Likely cause | Next check |
| --- | --- | --- |
| `state` `Empty`, `active_members` 0, lag > 0 | No consumer is running: crashed, scaled to zero, deploy in progress | Ask the user about the application's deployment; step 5 |
| `state` `Rebalancing`, or it flips between runs | Members join and leave (crash loop, `max.poll.interval.ms` exceeded, long GC) | Run step 1 again a few times; check application logs |
| `Stable`, lag spread over all partitions and growing | Consumers too slow for the input rate | Step 4: did input grow? |
| `Stable`, lag on one or a few partitions only | Hot key or a stuck consumer (a poison record that it retries forever) | Step 4 with `--by-partition`; step 6 |
| `Stable`, lag high but `behind_ms` small | A short burst being worked off | Sample again in a minute |
| `current_offset` below `low_watermark` | The group fell behind retention; records were deleted before it read them | Report data loss for that group; see `kafka-safe-offset-reset` |

To see whether lag is growing, sample it:

```bash
ferrika report --samples 6 --interval 10 --format json -o lag-trend.json
```

`lag_trend.groups[]` has each group's `first`, `last`, `min`, `max` and
`values` (one per sample, 10 s apart here): `last` well above `first` means
the lag is growing. It takes about a minute. With one sample there is no
trend (`lag_trend` is null), only the `lag` snapshot.

### 4. Did the input change?

```bash
ferrika -f json topics histogram TOPIC --since 6h
ferrika topics histogram TOPIC --since 6h --by-partition
```

Records per time bucket, from offsets (no record is read). `buckets[].count`
per `start_ms` (epoch ms), `buckets[].by_partition` per partition. A recent
jump in `count` means a burst of input; one partition with most of the
records means key skew. `after` > 0 means records with future timestamps
(producer clock skew).

### 5. Cluster problems that look like lag

```bash
ferrika -f json health --fail-on never
```

Findings with `severity` (`Critical`, `Warning`, `Info`), `message`, `target`.
Offline brokers and leaderless or under-replicated partitions stall
consumers. A finding starting with `Incomplete check:` means part of the
cluster could not be read. `ferrika topology --under-replicated-only` shows
the affected partitions.

### 6. A stuck partition

Look at the records right after the committed offset of the lagging
partition (no consumer group is used, nothing is committed):

```bash
ferrika -f json consume TOPIC -p 3 --offset 120500 -n 5 -t 5000 --mask-pii
```

Use the partition's `current_offset` from step 2 as `--offset`. A record
that is much larger than the others, not valid JSON, or in an unexpected
schema is a candidate poison record. Report its partition and offset; do not
paste record contents beyond what the user needs.

## What to propose

- Consumers down or crash-looping: the application team restarts or fixes
  them; lag drains by itself. No offset change is needed.
- Too slow: more consumer instances (up to the partition count; more members
  than partitions stay idle, lint rule G003), faster processing, or more
  partitions (changes key placement; a decision for the team).
- Poison record: fix the consumer's error handling or send the record to a
  dead letter topic. Skipping it with an offset reset is a last resort: use
  the `kafka-safe-offset-reset` skill and get the user's approval.
- Fell behind retention: data is already gone for this group; say so plainly.

## Safety rules

- This skill does not change anything. Do not run `offset reset --execute`,
  `topics config --set` or other writes from here.
- Do not use `consume -g GROUP` with the application's group: it would join
  the group and commit offsets. Read without `-g`.
- Always bound `consume` with `-n` and `-t` (idle timeout in milliseconds),
  and keep `--mask-pii` on unless the user asks for raw values.
- An error that starts with `Refused` comes from the company policy or a
  read-only profile. Stop and tell the user; do not look for a way around it.
- Never print or ask for passwords, tokens or SASL secrets.

## With the Ferrika MCP server

If the `ferrika mcp` server is connected, the tools `list_consumer_groups`,
`consumer_group_lag`, `message_histogram`, `cluster_health` and
`peek_messages` return the same data as steps 1 to 6.
