# Ferrika with Redpanda

[Redpanda](https://redpanda.com) speaks the Kafka API, so Ferrika connects to
it like to any Kafka cluster: give its Kafka address (`-b host:9092`) and, for
schemas, its built-in Schema Registry (`-S http://host:8081`).

This page lists what has been checked and what differs. It is updated as more
setups are tested.

## Checked setup

- Redpanda **v26.3.1**, one node, `redpanda start --mode dev-container`,
  with the built-in Schema Registry. No SASL or TLS.
- `ferrika` CLI from `main` (2026-11-22) on Windows and Linux (CI).
- CI runs the end-to-end smoke test (`tools/e2e/smoke.py`) against this
  Redpanda version on every pull request, next to Apache Kafka 3.9.

Not checked yet: several brokers (reassignment between brokers, offline
brokers), SASL/SCRAM and TLS listeners, Redpanda Cloud.

## Results

| Area | Status | Notes |
| --- | --- | --- |
| Topics: list, describe, create, delete, clone, purge, config, analyze, histogram | Works | |
| Produce, consume (filters, groups, time ranges, `-D`), dump/restore, mirror, generate, bench, probe | Works | Smoke test 45/45 |
| Consumer groups: lag, offset reset/delete, group delete | Works | Deleting all of a group's offsets removes the group, as on Kafka |
| Health, monitor, Prometheus exporter, report, topology, top | Works | Redpanda has no `DescribeQuorum`; Ferrika falls back to Metadata |
| Lint | Works | Redpanda's defaults (segment.ms 14 d over retention.ms 7 d) give one cluster-wide T008; broker hints name the Redpanda cluster setting |
| Preferred leader election (`topics elect-leaders`, desktop "Rebalance leaders") | Not supported by Redpanda | Redpanda has no ElectLeaders API and balances leadership itself (`rpk cluster partitions balance`); Ferrika says so |
| Reassignment: plan, status | Works (one node) | Moves between brokers not checked yet |
| Schema Registry: list, get, register, check, compatibility, delete | Works | |
| ACLs: create, list, delete | Works | Without SASL the ACLs are stored but not enforced |
| SCRAM users, client quotas | Works | |
| Operation history and undo (topic configs, ACLs, quotas, offsets) | Works | |
| MCP server (read tools) | Works | |
| Desktop: Overview, Brokers, Topics, Message Studio, Schemas | Works | |

## Kafka APIs

Redpanda v26.3.1 lists every Kafka API and version the CLI uses, except
ElectLeaders (43) and DescribeQuorum (55), as above.
