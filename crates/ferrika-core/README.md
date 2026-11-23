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
[`rdkafka`](https://crates.io/crates/rdkafka) crate, not re-exported; the API
uses Ferrika's own offset, commit mode and metadata types, and committed
offsets as plain maps); admin requests, Schema Registry and Connect are native Rust over rustls.

## Example

```rust
use ferrika_core::kafka::admin::KafkaAdminClient;
use ferrika_core::kafka::config::KafkaClientConfig;
use ferrika_core::kafka::producer::{KafkaProducerClient, OwnedProducerRecord};

async fn run() -> ferrika_core::error::KafkaResult<()> {
    let config = KafkaClientConfig::new("localhost:9092");

    let admin = KafkaAdminClient::new(&config)?;
    for topic in admin.list_topics(None)? {
        println!("{topic}");
    }

    let producer = KafkaProducerClient::new(&config)?;
    let mut record = OwnedProducerRecord::new("orders");
    record.key = Some(b"order-1".to_vec());
    record.payload = Some(br#"{"amount": 42}"#.to_vec());
    let receipt = producer.send(&record, None).await?;
    println!("partition {} offset {}", receipt.partition, receipt.offset);
    Ok(())
}
```

The crate documentation on [docs.rs](https://docs.rs/ferrika-core) has the
same example, compiled with the crate.

## Building

librdkafka is built from source, so a C compiler and CMake are needed. The
`ssl` feature adds TLS and SASL SCRAM to the librdkafka paths by building
OpenSSL from source and linking it statically; that needs Perl.

## Stability

Pre-1.0: public items can change between minor versions.

## License

Apache-2.0
