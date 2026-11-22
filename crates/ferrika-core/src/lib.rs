//! Ferrika's engine: Kafka clients, record formats and the domain model
//! behind the `ferrika` command line.
//!
//! Layers, each using only the ones before it:
//!
//! - [`error`], [`model`]: the error type and plain domain data (ACL
//!   bindings, cluster snapshots, filter expressions, formatting). No I/O.
//! - [`codec`]: Base64, Avro, Protobuf, JSON Schema, the Confluent wire
//!   format and PII masking. No I/O.
//! - [`files`], [`notice`]: owner-only file writes, and warnings for the
//!   person running the tool.
//! - [`kafka`]: everything that talks to a cluster, a Schema Registry or
//!   Kafka Connect.
//!
//! The clients are built on librdkafka through the `rdkafka` crate, which is
//! not re-exported: start offsets are [`kafka::consumer::StartOffset`],
//! commit modes [`kafka::consumer::CommitMode`] and cluster metadata
//! [`kafka::admin::ClusterMetadata`].
//!
//! # Semver notes
//!
//! - A few consumer-group offset functions still take or return
//!   `rdkafka::TopicPartitionList` (rdkafka 0.39): the committed-offset reads
//!   and `commit` of [`kafka::consumer::KafkaConsumerClient`], and
//!   `group_lag` in `ferrika-ops`. They change with rdkafka; plain maps are
//!   planned for 0.3.
//! - `jiff` 1 [`jiff::Timestamp`] and `serde_json` 1 [`serde_json::Value`]
//!   appear in public signatures, so a major version change of either is a
//!   breaking change here too.
//!
//! # Example
//!
//! List the topics of a cluster and send one record:
//!
//! ```no_run
//! use ferrika_core::kafka::admin::KafkaAdminClient;
//! use ferrika_core::kafka::config::KafkaClientConfig;
//! use ferrika_core::kafka::producer::{KafkaProducerClient, OwnedProducerRecord};
//!
//! # async fn run() -> ferrika_core::error::KafkaResult<()> {
//! let config = KafkaClientConfig::new("localhost:9092");
//!
//! let admin = KafkaAdminClient::new(&config)?;
//! for topic in admin.list_topics(None)? {
//!     println!("{topic}");
//! }
//!
//! let producer = KafkaProducerClient::new(&config)?;
//! let mut record = OwnedProducerRecord::new("orders");
//! record.key = Some(b"order-1".to_vec());
//! record.payload = Some(br#"{"amount": 42}"#.to_vec());
//! let receipt = producer.send(&record, None).await?;
//! println!("partition {} offset {}", receipt.partition, receipt.offset);
//! # Ok(())
//! # }
//! ```
//!
//! Mask personal data in text without a cluster:
//!
//! ```
//! use ferrika_core::codec::pii::{mask_text, MaskStrategy};
//!
//! let masked = mask_text("mail kim@example.com", MaskStrategy::Redact);
//! assert!(!masked.contains("kim@example.com"));
//! ```

pub mod codec;
pub mod error;
pub mod files;
pub mod kafka;
pub mod model;
pub mod notice;
