# Ferrika with Redpanda

[Redpanda](https://redpanda.com) speaks the Kafka API, so Ferrika connects to
it like to any Kafka cluster: give its Kafka address (`-b host:9092`) and, for
schemas, its built-in Schema Registry (`-S http://host:8081`).

This page lists what has been checked and what differs. It is updated as more
setups are tested.

## Checked setup

- Redpanda **v26.3.1**, one node, `redpanda start --mode dev-container`,
  with the built-in Schema Registry.
- Redpanda v26.3.1, three nodes ([`docs/e2e/redpanda-compose.yml`](e2e/redpanda-compose.yml)),
  first without and then with SASL (SCRAM-SHA-256 and PLAIN users, ACLs
  enforced).
- `ferrika` CLI from `main` (2026-11-22) on Windows and Linux (CI).
- CI runs the end-to-end smoke test (`tools/e2e/smoke.py`) against this
  Redpanda version on every pull request, next to Apache Kafka 3.9.

Not checked yet: TLS listeners, Redpanda Cloud. SCRAM through librdkafka
needs the `ssl` build (as with Kafka); the default build was checked with
PLAIN, and its own wire client (users, ACLs) with SCRAM.

## Results

| Area | Status | Notes |
| --- | --- | --- |
| Topics: list, describe, create, delete, clone, purge, config, analyze, histogram | Works | |
| Produce, consume (filters, groups, time ranges, `-D`), dump/restore, mirror, generate, bench, probe | Works | Smoke test 45/45 |
| Consumer groups: lag, offset reset/delete, group delete | Works | Deleting all of a group's offsets removes the group, as on Kafka |
| Health, monitor, Prometheus exporter, report, topology, top | Works | Redpanda has no `DescribeQuorum`; Ferrika falls back to Metadata |
| Lint | Works | Redpanda's defaults (segment.ms 14 d over retention.ms 7 d) give one cluster-wide T008; broker hints name the Redpanda cluster setting |
| Health: uneven leadership | Differs | `health` may suggest rebalancing leaders; Redpanda does that itself (`rpk cluster partitions balance`) |
| Preferred leader election (`topics elect-leaders`, desktop "Rebalance leaders") | Not supported by Redpanda | Redpanda has no ElectLeaders API and balances leadership itself (`rpk cluster partitions balance`); Ferrika says so |
| Reassignment: plan, execute, verify, status, undo | Works | Checked moving partitions between three nodes |
| Replication factor changes (`topics set-replication`, lint fix, undo) | Works | Redpanda refuses replica-count changes through reassignment; Ferrika then sets the topic's `replication.factor` and Redpanda places the replicas |
| Offline broker | Works | `health` reports it, `monitor brokers` shows it offline, `reassign plan` refuses it as a target |
| Schema Registry: list, get, register, check, compatibility, delete | Works | |
| ACLs: create, list, delete | Works | Enforced with SASL: reads allowed by an ACL work, writes without one get TopicAuthorizationFailed |
| Consumer group permissions | Differs | Redpanda needs Describe on a group as well as Read (Kafka lets Read imply Describe); `consume` says which grants are missing |
| Topics a user may not describe | Same as Kafka | They are left out of the topic list, so commands report "not found" |
| SCRAM users, client quotas | Works | |
| Operation history and undo (topic configs, ACLs, quotas, offsets) | Works | |
| MCP server (read tools) | Works | |
| Desktop: Overview, Brokers, Topics, Message Studio, Schemas | Works | |

## Kafka APIs

Redpanda v26.3.1 lists every Kafka API and version the CLI uses, except
ElectLeaders (43) and DescribeQuorum (55), as above.
