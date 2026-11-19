# ferrika-core

The engine behind the [Ferrika](https://github.com/Jangmyun/ferrika) Kafka
command line: cluster, Schema Registry and Kafka Connect clients, record
formats and the domain model.

- `model`, `error`: plain domain data (ACL bindings, cluster snapshots, filter
  expressions) and the error type. No I/O.
- `codec`: Base64, Avro, Protobuf, JSON Schema, the Confluent wire format and
  PII masking. No I/O.
- `files`, `notice`: owner-only file writes and user-facing warnings.
- `kafka`: everything that talks to a cluster, a Schema Registry or Kafka
  Connect.

Producing, consuming and metadata go through librdkafka (the
[`rdkafka`](https://crates.io/crates/rdkafka) crate, re-exported as
`ferrika_core::rdkafka`); admin requests, Schema Registry and Connect are
native Rust over rustls.

## Building

librdkafka is built from source, so a C compiler and CMake are needed. The
`ssl` feature adds TLS and SASL SCRAM to the librdkafka paths by building
OpenSSL from source and linking it statically; that needs Perl.

## Stability

Pre-1.0: public items can change between minor versions.

## License

Apache-2.0
