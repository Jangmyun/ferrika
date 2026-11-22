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
| AutoMQ | Preset available, not CI-tested; a compose file for a future E2E run is in [`e2e/automq-compose.yml`](e2e/automq-compose.yml) | `automq` |

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

- AutoMQ is a Kafka fork that keeps data in object storage; the Kafka admin
  APIs are expected to behave as in Apache Kafka.
- Durability comes from shared storage, so topics commonly have a
  replication factor of 1; lint findings about low replication can be
  expected on AutoMQ and may not indicate a risk there.
