---
name: kafka-config-lint
description: Review Kafka topic, broker and consumer group configuration for risky or inconsistent settings with ferrika lint, explain each finding, and apply fixes only through a validated dry run, the user's approval and undo. Use when someone asks to audit or review Kafka configs, check replication, min.insync.replicas, retention or unclean leader election, prepare a cluster for production, or gate configs in CI.
---

# Kafka config lint

`ferrika lint` checks configs against a fixed rule catalog (T001-T014
topics, B001-B007 brokers, G001-G003 groups), each finding with a severity
and a fix. This skill runs it, explains the results, and applies fixes
safely when the user wants them.

## Before you start

- `ferrika --version` works; global options go before the subcommand.
- Confirm the cluster: `ferrika -f json profile show`; use `-p PROFILE`.
- `TOPIC` and `PROFILE` below are placeholders.

## Steps

### 1. Lint

```bash
ferrika -f json lint
ferrika -f json lint --topic 'orders-*' --min-severity warning
```

The first stderr line says how findings are graded: production (`env:
prod`, when the profile is read-only, a policy read-only rule matches, or
its name has `prod`, `prd` or `live`) or not. Override with `--env prod` or
`--env dev` when the guess is wrong; the same finding is more severe for
production.

Each finding: `rule` (such as `T002`), `name`, `severity` (`Critical`,
`Warning`, `Info`), `target`, `message`, `current` (the config keys and
values involved), `fix` (`kind` and `steps` or the change), `fix_class` and
`command` (the command a person can run, when there is one).

Resources Ferrika could not read (no permission to describe configs) are
always listed; mention them, as the check is incomplete there.

### 2. Explain

```bash
ferrika lint rules
```

The catalog: id, name, severity for production and other clusters, fix
class and a one-line summary. Group the findings by severity and explain
each in terms of risk: data loss (replication 1, unclean leader election,
short retention), unavailability (`min.insync.replicas` at or above the
replication factor), or cost and hygiene.

Fix classes:

- `auto`: a reversible dynamic config change or leader election.
- `guided`: moves data, changes client behaviour or cannot be undone
  (replication, raising `min.insync.replicas`, partitions, deleting a
  group). Runs only when its rule is named with `--rule`, one by one.
- `manual`: static broker settings or business decisions (retention). Only
  explained; tell the user what to change and where.

### 3. Dry run of the fixes

```bash
ferrika lint --fix --dry-run
ferrika lint --fix --rule T002 --dry-run
```

Nothing changes: the brokers validate the config changes and replication
changes are only planned. Show the user the plan. To hand it over as a
file:

```bash
ferrika lint --fix --save-plan lint-plan.json
```

`--save-plan` only writes the plan.

### 4. Apply, with approval

Only after the user approved the plan. The command asks once before it
applies (a guided fix asks per fix, and deleting a group asks for its name);
let the user answer, or pass `--yes` only when they approved this exact
plan:

```bash
ferrika lint --fix
ferrika lint --fix --rule T002
```

Before each fix the cluster is read again, and a finding that changed since
is skipped. Every applied fix is recorded with the value it replaced.

### 5. Verify and keep the undo path

```bash
ferrika -f json lint --min-severity warning
ferrika -f json history -n 10
ferrika undo ID --dry-run
```

The findings should be gone. `history` lists each fix with its `id`;
`ferrika undo ID` reverts one (it asks first and refuses when the value
changed since).

## In CI

```bash
ferrika -p PROFILE lint --fail-on warning --skip T007
```

Exit status 1 when a finding is at or above `--fail-on` (the default
`never` only reports). `--skip RULE` or `--skip RULE:GLOB` leaves out
accepted exceptions; keep the list short and explained.

## Safety rules

- Never run `lint --fix` without `--dry-run` before the user approved the
  plan, and never add `--yes` on your own.
- Guided fixes copy data or cannot be undone: name the rule, say what it
  costs (replication traffic, client errors with `acks=all`), and get
  approval for each.
- An error that starts with `Refused` comes from the company policy or a
  read-only profile. Stop and tell the user.
- Never print passwords or secret config values.

## With the Ferrika MCP server

`config_lint` returns the same findings (with `fix.command`). Fixes are
not MCP tools; a person runs them with the CLI.
