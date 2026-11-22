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
| Replication factor changes (`topics set-replication`, lint fix, undo) | Works | Redpanda refuses replica-count changes through reassignment; Ferrika then sets the topic's `replication.factor` and Redpanda places the replicas. Undo of a change made by `lint --fix` may report a drift (the record holds the planned replicas): use `undo --force` |
| Offline broker | Works | `health` reports it, `monitor brokers` shows it offline, `reassign plan` refuses it as a target |
| Schema Registry: list, get, register, check, compatibility, delete | Works | |
| ACLs: create, list, delete | Works | Enforced with SASL: reads allowed by an ACL work, writes without one get TopicAuthorizationFailed |
| Consumer group permissions | Differs | Redpanda needs Describe on a group as well as Read (Kafka lets Read imply Describe); `consume` says which grants are missing |
| Topics a user may not describe | Same as Kafka | They are left out of the topic list, so commands report "not found" |
| SCRAM users, client quotas | Works | |
| Operation history and undo (topic configs, ACLs, quotas, offsets) | Works | |
| MCP server (read tools) | Works | |
| Desktop: Overview, Brokers, Topics, Message Studio, Schemas | Works | |

## Trying SASL on the test cluster

Start [`docs/e2e/redpanda-compose.yml`](e2e/redpanda-compose.yml), then
create a superuser and turn SASL on (test values only):

```bash
docker exec ferrika-redpanda-redpanda-0-1 rpk security user create admin -p admin-secret --mechanism SCRAM-SHA-256
docker exec ferrika-redpanda-redpanda-0-1 rpk cluster config set superusers '["admin"]'
docker exec ferrika-redpanda-redpanda-0-1 rpk cluster config set enable_sasl true
# PLAIN as well, for the default build of ferrika (SCRAM needs --features ssl):
docker exec ferrika-redpanda-redpanda-0-1 rpk cluster config set sasl_mechanisms '["SCRAM","PLAIN"]'   -X user=admin -X pass=admin-secret -X sasl.mechanism=SCRAM-SHA-256
ferrika profile add rp-admin -b localhost:39092 --security-protocol sasl_plaintext   --sasl-mechanism PLAIN --sasl-username admin --sasl-password-stdin
```

## Kafka APIs

Redpanda v26.3.1 lists every Kafka API and version the CLI uses, except
ElectLeaders (43) and DescribeQuorum (55), as above.
