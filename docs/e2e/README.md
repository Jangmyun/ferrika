# End-to-end test fixtures

Throw-away Kafka clusters and helpers for testing the `ferrika` CLI against
real brokers. Everything here is for local tests only: the credentials are
fixed, publicly known test values and must never be used anywhere else.

The quickest check is the smoke test, which needs one plain broker
(`apache/kafka:3.9.0`) and no files from this folder; see
[`tools/e2e/smoke.py`](../../tools/e2e/smoke.py) and the README.

The same smoke test runs against Redpanda, a Kafka API compatible broker
(CI runs it against Kafka, Redpanda and AutoMQ):

```bash
docker run -d --name redpanda -p 9092:9092 docker.redpanda.com/redpandadata/redpanda:v26.3.1 \
  redpanda start --mode dev-container --smp 1 --memory 1G --overprovisioned \
  --kafka-addr 0.0.0.0:9092 --advertise-kafka-addr localhost:9092
python3 tools/e2e/smoke.py target/debug/ferrika --broker localhost:9092
```

Two more smoke tests cover the MCP server and the RabbitMQ commands; CI runs
the MCP one in the Kafka job, the RabbitMQ one against a
`rabbitmq:4-management` service container, and the MCP one again with the
RabbitMQ tools in that job (which gets a Kafka service for it):

```bash
# MCP: `ferrika mcp --allow-write --topics "<run>*"` over stdio JSON-RPC
# (tools/list, read tools, transform_preview, transform_execute with its
# confirmation token, a destination outside --topics, the audit entries).
python3 tools/e2e/mcp_smoke.py target/debug/ferrika --broker localhost:9092
# Optional: also call the RabbitMQ tools through a temporary profile: the
# read tools, and rabbit_publish, rabbit_peek, rabbit_dlq_inspect and
# rabbit_dlq_redrive (plan, token, execute) under --queues/--exchanges,
# plus refusals outside them.
python3 tools/e2e/mcp_smoke.py target/debug/ferrika --broker localhost:9092 \
  --rabbit-url http://localhost:15672 --rabbit-user zz --rabbit-password zz-pw \
  --rabbit-amqp-url amqp://localhost:5672

# RabbitMQ: profiles, queues/exchanges/bindings, publish, peek, dead-lettering,
# dlq inspect and redrive (preview and --execute), read-only profiles, audit.
docker run -d --name ferrika-rabbit-test -p 15672:15672 -p 5672:5672 \
  -e RABBITMQ_DEFAULT_USER=zz -e RABBITMQ_DEFAULT_PASS=zz-pw rabbitmq:4-management
python3 tools/e2e/rabbit_smoke.py target/debug/ferrika --url http://localhost:15672 \
  --user zz --password zz-pw --amqp-url amqp://localhost:5672
```

A third one covers the signed company policy ([docs/POLICY.md](../POLICY.md))
with two clusters, `--broker` as the `prod` environment and `--broker2` as
`dev`: keygen, sign, `policy show`/`verify`, `forbid`, `read_only_brokers`,
per-environment `forbid`, `copy_down: masked` and `forbid` for `mirror` and
`transform --execute` (refused without `--mask`, masked values arrive),
`(environment …)` in audit entries, `export: forbidden` (consume to a pipe,
`--save`, `dump`), enforced masking, `mcp_write: forbidden` and
`confirm_human` over stdio, a tampered policy (strictest mode), history and
undo with an edited journal entry (EDITED, undo refused), and `audit verify`
on an edited audit line. It needs a debug build (or `--features
dev-policy-dir`), which reads the policy from `FERRIKA_POLICY_DIR`; the
script signs its own policies with a throw-away key into a temporary
directory and never sets `audit: required` (which would move the audit log
to the account's real `~/.ferrika`). The environments match the exact
`host:port` given, so the two must differ. It runs in about 30 seconds:

```bash
docker run -d --name ferrika-redpanda-test -p 29092:29092 docker.redpanda.com/redpandadata/redpanda:v26.3.1 \
  redpanda start --mode dev-container --smp 1 --memory 1G --overprovisioned \
  --kafka-addr 0.0.0.0:29092 --advertise-kafka-addr localhost:29092
python3 tools/e2e/policy_smoke.py target/debug/ferrika --broker localhost:9092 --broker2 localhost:29092
```

CI runs it in the Kafka job against a second `apache/kafka:3.9.0` service
container on `localhost:29092` (KRaft settings and its own `CLUSTER_ID` in
`.github/workflows/ci.yml`) as `dev`. A service container cannot take
Redpanda's start arguments, so a second Kafka service is the simplest
second cluster: it starts next to the first one with no extra step.

All the scripts need only the Python 3 standard library, run the CLI with their
own `FERRIKA_CONFIG_DIR` (and `HOME`/`USERPROFILE`), name everything they
create `<prefix><run id>-...` (`--prefix`, default `zz-`) and delete only
that at the end, so runs can share a broker. `rabbit_smoke.py` builds its
own exchanges and queues through the management API and checks message
counts with the API's `get` (requeueing), not with the queue counters, which
refresh only every few seconds. By default it expects the profile password
in the OS credential store; with `FERRIKA_SECRET_STORE=file` (as in CI) it
checks that the password went to `rabbit-profiles.json` and that the file is
private (mode 0600 on POSIX). If the AMQP port is not reachable on
`localhost` (for example Docker in WSL with 5672 in a reserved Windows port
range), pass the container host's address in `--amqp-url`.

And against AutoMQ (one node on an S3-compatible store; it starts in under a
minute once the images are pulled, and needs about 2 GB of memory):

```bash
docker compose -f docs/e2e/automq-compose.yml up -d
python3 tools/e2e/smoke.py target/debug/ferrika --broker localhost:49092
BOOTSTRAP=localhost:49092 KAFKA_CONTAINER=ferrika-automq-automq-1 \
  KAFKA_BIN=/opt/automq/kafka/bin FERRIKA=./target/debug/ferrika bash docs/e2e/undo.sh
docker compose -f docs/e2e/automq-compose.yml down -v
```

| File | What it starts or does |
| --- | --- |
| `kafka-sasl-compose.yml` | Three-node KRaft cluster with SASL/PLAIN on `localhost:19092` |
| `kafka-sasl-ssl-compose.yml` | One broker with SASL over TLS on `localhost:19192` |
| `kafka-scram-compose.yml` | One broker with SASL SCRAM-SHA-256/512 on `localhost:19392`; the file lists how to create the users |
| `kafka-oauth-compose.yml` | One broker that accepts unsecured JWTs on `localhost:19096`, plus a token endpoint on `localhost:18080` served by `oauth-idp.py` |
| `oauth-idp.py` | Test-only OAuth 2.0 client-credentials endpoint (standard library only) |
| `redpanda-compose.yml` | Three-node Redpanda (Kafka API on `localhost:39092`-`39094`, Schema Registry on `localhost:38081`) |
| `automq-compose.yml` | One-node AutoMQ 1.7.5 on an S3-compatible store (Kafka API on `localhost:49092`); CI runs the smoke test against it |
| `undo.sh` | Checks `ferrika history` and `ferrika undo` against a broker; the header lists its variables |

Usage, for example (undo check against a plain single-node broker):

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
cargo build -p ferrika-cli
FERRIKA=./target/debug/ferrika BOOTSTRAP=localhost:9092 KAFKA_CONTAINER=kafka bash docs/e2e/undo.sh
docker rm -f kafka
```

For the SASL, SCRAM and OAuth clusters, start the compose file, then add a
profile with the credentials named in the file's header
(`docker compose -f docs/e2e/kafka-sasl-compose.yml up -d`) and pass it as
`PROFILE=...` to `undo.sh`. Each compose file defines its ports, users and
passwords. Use test clusters only: the scripts create and delete topics, ACLs
and users.
