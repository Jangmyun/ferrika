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
//! The clients are built on librdkafka through the [`rdkafka`] crate, which
//! some signatures (offsets, commit modes) still name. It is re-exported so
//! that callers use the same version.

pub mod codec;
pub mod error;
pub mod files;
pub mod kafka;
pub mod model;
pub mod notice;

pub use rdkafka;
