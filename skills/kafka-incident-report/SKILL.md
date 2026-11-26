---
name: kafka-incident-report
description: Collect the facts of a Kafka incident with the ferrika CLI read-only and write them up - cluster health, under-replicated partitions, consumer lag and its trend, and the recent changes from the audit log and change history. Use during or after an outage, for a postmortem, or when someone asks "what is wrong with the cluster" or "what changed".
---

# Kafka incident report

Gathers evidence quickly and without side effects, then produces one
document a team can read. Ferrika's `report` command does most of it; the
other commands answer follow-up questions.

## Before you start

- `ferrika --version` works; global options go before the subcommand.
- Confirm the cluster: `ferrika -f json profile show`; use `-p PROFILE`
  for the affected cluster. Ask when unsure; reports of the wrong cluster
  waste the most time in an incident.
- Ask for the time window: when did it start? `--since` takes `30m`, `2h`,
  `1d`.

## Steps

### 1. The report

```bash
ferrika report --since 2h --samples 6 --interval 10 -o incident.md
```

A Markdown document with the profile and brokers (no credentials), the
policy in force, a cluster summary, the health findings, consumer lag with
its trend over the six samples (one minute), and the audit timeline of the
window (changes made with Ferrika, PII-masked). It contains no record data.
Ferrika keeps no lag history: the trend covers only this run.

For the same content as data:

```bash
ferrika report --since 2h --format json -o incident.json
```

Fields: `summary` (`brokers_online`, `brokers_total`, `controller`,
`under_replicated_partitions`, `consumer_groups`, `total_lag`), `health[]`,
`lag[]`, `lag_trend`, `audit.entries[]`, `cluster_error` (set when the
cluster could not be read; then say so first).

### 2. Health in detail

```bash
ferrika -f json health --fail-on never
ferrika topology --under-replicated-only
ferrika topology --imbalanced-only
ferrika -f json monitor brokers
```

`health` findings have `severity` (`Critical`, `Warning`, `Info`),
`message` and `target`. Findings starting with `Incomplete check:` mean a
part of the cluster could not be read: report that as a finding, not as
"healthy". Exit status without `--fail-on never`: 1 for a finding at or
above `--fail-on`, 3 when the check was incomplete, 0 otherwise.

### 3. What changed?

```bash
ferrika -f json history -n 30
ferrika -f json audit -n 100
ferrika -f json audit --failed -n 50
ferrika -f json audit --source mcp -n 50
```

`history` lists changes recorded for undo with `id`, `time_ms`, `user`,
`source` (`cli`, `gui`, `mcp`), `action`, `target` and `reversibility`.
`ferrika history show ID` gives the before and after values. `audit` is the
tamper-evident log of every change and refusal made through Ferrika on this
machine; `--source mcp` shows what AI assistants did. Changes made with
other tools are not in either.

### 4. Follow the symptom

- Consumer lag: the `kafka-lag-triage` skill.
- Missing records: the `kafka-why-no-data` skill.
- Risky configs behind the incident: the `kafka-config-lint` skill.
- A change to revert: `ferrika undo ID --dry-run` shows what undo would do;
  reverting is the user's decision.

## Writing it up

Use this order, in plain language, with times in UTC (convert `*_ms`
fields from epoch milliseconds):

1. Summary: what is broken, since when, who is affected.
2. Evidence: findings and numbers from the commands above, with the command
   that produced each.
3. Changes in the window (history and audit).
4. Cause, if known, labelled as confirmed or suspected.
5. Next steps and owners. Mark every step that changes the cluster and say
   that it needs approval.

## Safety rules

- This skill only reads. Do not fix anything during the collection; propose
  fixes and let the user decide.
- Do not attach record data to the report. If a sample is needed, use
  `consume -n 3 -t 5000 --mask-pii` without `-g`.
- An error that starts with `Refused` comes from the company policy or a
  read-only profile; note it in the report and continue with what works.
- Never include passwords, tokens or connection secrets; `report` leaves
  them out, keep it that way when you add text.

## With the Ferrika MCP server

`cluster_health`, `list_brokers`, `config_lint`, `list_consumer_groups`
and `consumer_group_lag` give the same live data; the audit log, history
and `report` are CLI only.
