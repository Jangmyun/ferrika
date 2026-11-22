# End-to-end test fixtures

Throw-away Kafka clusters and helpers for testing the `ferrika` CLI against
real brokers. Everything here is for local tests only: the credentials are
fixed, publicly known test values and must never be used anywhere else.

The quickest check is the smoke test, which needs one plain broker
(`apache/kafka:3.9.0`) and no files from this folder; see
[`tools/e2e/smoke.py`](../../tools/e2e/smoke.py) and the README.

The same smoke test runs against Redpanda, a Kafka API compatible broker
(CI does both):

```bash
docker run -d --name redpanda -p 9092:9092 docker.redpanda.com/redpandadata/redpanda:v26.3.1 \
  redpanda start --mode dev-container --smp 1 --memory 1G --overprovisioned \
  --kafka-addr 0.0.0.0:9092 --advertise-kafka-addr localhost:9092
python3 tools/e2e/smoke.py target/debug/ferrika --broker localhost:9092
```

| File | What it starts or does |
| --- | --- |
| `kafka-sasl-compose.yml` | Three-node KRaft cluster with SASL/PLAIN on `localhost:19092` |
| `kafka-sasl-ssl-compose.yml` | One broker with SASL over TLS on `localhost:19192` |
| `kafka-scram-compose.yml` | One broker with SASL SCRAM-SHA-256/512 on `localhost:19392`; the file lists how to create the users |
| `kafka-oauth-compose.yml` | One broker that accepts unsecured JWTs on `localhost:19096`, plus a token endpoint on `localhost:18080` served by `oauth-idp.py` |
| `oauth-idp.py` | Test-only OAuth 2.0 client-credentials endpoint (standard library only) |
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
