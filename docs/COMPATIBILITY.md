# Kafka-compatible brokers

Ferrika talks the Kafka protocol, so it works with brokers and managed
services that implement it. This page says what has been checked against
each one, how to connect (`ferrika profile add --preset`), and the
limitations that are known or expected.

Names of other products are used only to say what Ferrika works with. None
of these vendors tests, endorses or supports Ferrika.

## Status

| Broker | Status | Preset |
| --- | --- | --- |
| Apache Kafka 3.9 | CI-tested (smoke test on every pull request) | none needed |
| Redpanda v26.3.1 | CI-tested (smoke test on every pull request); details in [REDPANDA.md](REDPANDA.md) | none needed (`redpanda-cloud` for Redpanda Cloud) |
| Redpanda Cloud | Preset available, not CI-tested | `redpanda-cloud` |
| Confluent Cloud | Preset available, not CI-tested | `confluent-cloud` |
| Amazon MSK (IAM) | Preset available, not CI-tested | `msk-iam` |
| Amazon MSK (SASL/SCRAM) | Preset available, not CI-tested | `msk-scram` |
| WarpStream | Preset available, not CI-tested | `warpstream` |
| AutoMQ 1.7.5 | CI-tested (smoke test on every pull request, one node on [`e2e/automq-compose.yml`](e2e/automq-compose.yml)); last checked by hand 2026-11-23, results below | `automq` (plaintext; the same settings as Apache Kafka) |

"Not CI-tested" means no Ferrika test runs against that service. The
limitations listed for it come from the Kafka protocol and the vendor's
public documentation, not from a test run; reports are welcome.

## Presets

A preset fills in the security protocol and SASL mechanism a service
expects. Flags given on the command line always win, and credentials are
never part of a preset: passwords and secrets go to the OS credential store
like any other profile secret. `profile add` prints the settings the preset
filled in and notes for the service.

| Preset | Security protocol | SASL mechanism | Credentials |
| --- | --- | --- | --- |
| `confluent-cloud` | `sasl_ssl` | `PLAIN` | API key as `--sasl-username`, its secret as the password |
| `msk-iam` | `sasl_ssl` | `AWS-MSK-IAM` | AWS credential chain (`--aws-profile`, `--aws-region` optional) |
| `msk-scram` | `sasl_ssl` | `SCRAM-SHA-512` | The user and password stored in AWS Secrets Manager |
| `redpanda-cloud` | `sasl_ssl` | `SCRAM-SHA-256` | A Redpanda Cloud SCRAM user |
| `warpstream` | `plaintext` | none | Add `--security-protocol` and `--sasl-mechanism` when the agents require TLS or SASL |
| `automq` | `plaintext` | none | As for Apache Kafka |

Examples (secrets from stdin; with two `-stdin` flags the SASL password is
the first line and the Schema Registry password the second):

```bash
# Confluent Cloud, with Schema Registry
printf '%s\n%s\n' "$CC_API_SECRET" "$SR_API_SECRET" | ferrika profile add cc \
  -b pkc-xxxxx.us-east-1.aws.confluent.cloud:9092 --preset confluent-cloud \
  --sasl-username "$CC_API_KEY" --sasl-password-stdin \
  --schema-registry https://psrc-xxxxx.us-east-1.aws.confluent.cloud \
  --schema-registry-user "$SR_API_KEY" --schema-registry-password-stdin

# Amazon MSK with IAM
ferrika profile add msk -b b-1.demo.abc123.c2.kafka.us-east-1.amazonaws.com:9098 \
  --preset msk-iam --aws-profile prod

# Amazon MSK with SASL/SCRAM
echo "$MSK_PASSWORD" | ferrika profile add msk-scram \
  -b b-1.demo.abc123.c2.kafka.us-east-1.amazonaws.com:9096 \
  --preset msk-scram --sasl-username alice --sasl-password-stdin

# Redpanda Cloud
echo "$RP_PASSWORD" | ferrika profile add rpc -b seed-xxxx.cloud.redpanda.com:9092 \
  --preset redpanda-cloud --sasl-username alice --sasl-password-stdin

# WarpStream and AutoMQ (plaintext listeners)
ferrika profile add ws -b warpstream-agent:9092 --preset warpstream
ferrika profile add automq -b automq:9092 --preset automq
```

SASL over TLS, SCRAM and OAUTHBEARER (which MSK IAM uses) through
librdkafka need a Ferrika build with the `ssl` feature; the release binaries
have it.

## Unsupported admin APIs

Before partition reassignment, SCRAM user management and client quota
requests, Ferrika asks the broker which APIs it offers (ApiVersions). When
the API is missing, the command fails with "this cluster does not support
..." instead of a dropped connection. Preferred leader election works the
same way. A service that offers an API but refuses the request (for example
for lack of permission) returns the broker's own error.

## Known or expected limitations

### Redpanda and Redpanda Cloud

See [REDPANDA.md](REDPANDA.md) for the checked results: no ElectLeaders
(Redpanda balances leadership itself), no DescribeQuorum (Ferrika falls back
to Metadata), replication factor changes through the topic config. Redpanda
Cloud manages the cluster configuration itself, so broker-level changes are
expected to be refused.

### Confluent Cloud

- Broker settings are managed by the service; broker config changes (such
  as `lint --fix` on broker configs) are refused.
- Partition placement is managed by the service: `reassign` and
  `topics set-replication` are not expected to work.
- SCRAM users (`ferrika user`) and client quotas (`ferrika quota`) are not
  managed through the Kafka API there.
- ACLs use service-account principals (`User:sa-...`).
- Schema Registry has its own API key: `--schema-registry-user` and
  `--schema-registry-password-stdin`.

### Amazon MSK

- IAM access control: permissions come from IAM policies, so Kafka ACLs do
  not apply to IAM clients. IAM listeners are on port 9098 (9198 for public
  access).
- SASL/SCRAM: users live in AWS Secrets Manager, so `ferrika user` cannot
  create or change them. SCRAM listeners are on port 9096 (9196 public).
- Broker configs are set through MSK configurations; changing them through
  the Kafka API may be refused.
- MSK Serverless restricts more admin operations (for example partition
  reassignment and many topic configs) and only offers IAM.

### WarpStream

- Agents are stateless and data lives in object storage, so partition
  replicas, ISR and leadership work differently: reassignment, preferred
  leader election and replication-factor changes do not apply, and broker
  health findings about replicas may not be meaningful.
- WarpStream documents client settings for zone-aware routing (set through
  the client id); Ferrika does not set them. See WarpStream's client
  configuration guide.
- Admin APIs it does not offer are reported as unsupported (see above).

### AutoMQ

Checked on 2026-11-23 against AutoMQ 1.7.5 (`automqinc/automq:1.7.5`), one
node with broker and controller roles on an S3-compatible store
([`e2e/automq-compose.yml`](e2e/automq-compose.yml)):

- The smoke test (`tools/e2e/smoke.py`, 45 checks) and the undo check
  (`e2e/undo.sh`, with `KAFKA_BIN=/opt/automq/kafka/bin`) pass.
- Also checked by hand: `topics describe`, `topics config`, `topics purge`,
  `topics elect-leaders`, `health`, `lint`, `topology`, `top -n 1`,
  `quota list` and `quota set`, `user create`/`list`/`delete` (SCRAM),
  `reassign plan`. They behave as on Apache Kafka.
- AutoMQ opens a new topic's partitions in object storage asynchronously:
  for about a second after `topics create`, offset lookups answer
  `NotLeaderForPartition`. Ferrika retries such lookups for a few seconds
  (before 0.2.0 `topics describe` showed `Messages: unknown` and `undo` of a
  topic creation failed in that second).
- New topics get a `min.insync.replicas=1` override from AutoMQ, so
  `topics describe` and `topics config` list it.
- AutoMQ adds internal topics (`__automq_cluster_events`,
  `__auto_balancer_metrics`); `topics list -a` shows them.
- `acl list` fails with `SECURITY_DISABLED` because the test node has no
  authorizer, as on Apache Kafka without one; ACLs were not checked.
- Not checked (one node): reassignment and replication factor changes
  between brokers, and the `T001` lint rule (replication factor 1 with two
  or more brokers). Durability comes from shared storage, so topics commonly
  have a replication factor of 1; on a multi-node AutoMQ cluster `T001`
  findings are expected and may not indicate a risk there. AutoMQ's own
  self-balancing can also move partitions while a `reassign` runs.
- Not checked: SASL or TLS listeners, AutoMQ's table topics and other
  AutoMQ-only features.
