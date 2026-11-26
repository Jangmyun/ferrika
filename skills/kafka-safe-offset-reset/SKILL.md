---
name: kafka-safe-offset-reset
description: Move a Kafka consumer group's committed offsets safely with the ferrika CLI (rewind to replay, skip ahead, jump to a time or offset), always as a dry run first, with the user's approval, and with undo. Use when someone asks to reset, rewind, replay, skip or seek a consumer group's offsets, or to reprocess records from a point in time.
---

# Safe consumer group offset reset

An offset reset changes what an application reads next: rewinding makes it
process records again (duplicates downstream), skipping ahead makes it never
process them. Treat it as a production change: plan, show, get approval,
execute, verify, keep the undo path.

## Before you start

- `ferrika --version` works; global options go before the subcommand
  (`ferrika -p PROFILE -f json offset reset ...`).
- Confirm the cluster: `ferrika -f json profile show`. Use `-p PROFILE` when
  the user names a cluster.
- `GROUP`, `TOPIC` and `PROFILE` below are placeholders; numbers are examples.
- You need from the user: the group, the topic, and what the application
  should read next (from the beginning, from the end, from a time, from an
  offset, or N records back).

## Steps

### 1. The group must have no members

```bash
ferrika -f json monitor lag
```

Find the group: its `state` must be `Empty` (`active_members` 0). Kafka
refuses to change the offsets of a group with running consumers, and the
consumers would overwrite them anyway. If it is `Stable` or `Rebalancing`,
ask the user to stop the application's consumers first. Do not stop them
yourself.

### 2. Current position

```bash
ferrika -f json monitor lag GROUP -t TOPIC
```

Note `current_offset`, `low_watermark`, `high_watermark` and `lag` per
partition. This is what undo restores.

### 3. Choose the target

| User wants | Option |
| --- | --- |
| Reprocess everything still retained | `--to-earliest` |
| Skip the backlog, read only new records | `--to-latest` |
| Reprocess from a point in time | `--to-timestamp 1767225600000` (epoch ms, UTC) |
| Go back or forward N records per partition | `--shift-by -1000` / `--shift-by 500` |
| One partition to one offset (skip a poison record) | `-p 3 --to-offset 120501` |

To find a time, look at when records arrived (offsets only, nothing read):

```bash
ferrika -f json topics histogram TOPIC --since 24h --bucket 15m
```

Convert the time the user gives to epoch milliseconds and say the UTC time
back to them.

### 4. Dry run (the default)

Without `--execute` nothing is committed:

```bash
ferrika -f json offset reset GROUP -t TOPIC --to-timestamp 1767225600000
```

The plan: `executed` false and `plans[]` with `partition`,
`current_offset`, `target_offset`, `low_watermark`, `high_watermark` and
`delta`. A negative `delta` is the number of records the group will read
again; a positive one is the number it will skip and never process. Sum them
and show the user a short table: partition, from, to, delta, plus the
totals.

Point out:

- Skipped records (positive delta) are not processed by this group at all.
- Replayed records (negative delta) reach downstream systems again; the
  application must tolerate duplicates.
- A `target_offset` equal to `low_watermark` while the user wanted an older
  time: those records are already deleted by retention.

### 5. Approval

Ask the user to approve this exact plan (group, topic, partitions, deltas).
Do not continue without an explicit yes. If the profile is production, say
so in the question.

### 6. Execute

```bash
ferrika -f json offset reset GROUP -t TOPIC --to-timestamp 1767225600000 --execute
```

Run the same command as the dry run with `--execute` added and nothing else
changed. If the group gained members since step 1, Kafka refuses: go back to
step 1.

### 7. Verify and keep the undo path

```bash
ferrika -f json monitor lag GROUP -t TOPIC
ferrika -f json history -n 5
```

`current_offset` should equal the planned `target_offset`. The reset is in
the history with the offsets it replaced; give the user its `id`. To revert
(again only while the group is empty):

```bash
ferrika undo ID --dry-run
ferrika undo ID
```

`undo --dry-run` shows what it would restore and refuses when the offsets
changed since; `undo` asks for confirmation (`-y` skips it; use that only
when the user approved the undo). Then the user restarts the consumers.

## Safety rules

- Never pass `--execute` before the user approved the dry-run plan.
- Never reset a group that has members, and never change the plan between
  the dry run and the execute.
- An error that starts with `Refused` comes from the company policy or a
  read-only profile. Stop and tell the user; do not retry with another
  profile or `-b` to get around it.
- Do not delete the group (`offset delete-group`) as a shortcut: it drops
  the group's offsets on every topic, and the application then starts
  wherever its `auto.offset.reset` says.
- Never print or ask for passwords, tokens or SASL secrets.

## With the Ferrika MCP server

With `ferrika mcp --allow-write`, `reset_consumer_group_offsets` takes the
same plan-then-execute path (`mode: "plan"`, then `mode: "execute"`), and
the change lands in `ferrika history` for `ferrika undo`. Resets that move
more records than `--max-offset-delta` need a person's confirmation.
