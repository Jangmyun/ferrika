use std::time::{Duration, Instant};

use rdkafka::client::DefaultClientContext;
use rdkafka::error::{KafkaError as RdKafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use tokio::task::{JoinError, JoinSet};

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::config::KafkaClientConfig;
use crate::kafka::detached::DetachedDrop;

/// Record to be sent to a Kafka topic.
#[derive(Debug, Clone)]
pub struct OwnedProducerRecord {
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub partition: Option<i32>,
    pub timestamp: Option<i64>,
    /// Headers in send order; a key may repeat.
    pub headers: Vec<(String, Vec<u8>)>,
}

impl OwnedProducerRecord {
    /// Creates a new record for the specified topic.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            key: None,
            payload: None,
            partition: None,
            timestamp: None,
            headers: Vec::new(),
        }
    }

    /// Sets the message key as bytes.
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Sets the message payload as bytes.
    pub fn with_payload(mut self, payload: impl Into<Vec<u8>>) -> Self {
        self.payload = Some(payload.into());
        self
    }

    /// Sets the target partition.
    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Sets the message timestamp (ms since UNIX epoch).
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Appends a header; an earlier header of the same key stays.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }
}

/// Delivery confirmation returned upon successful message production.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryReceipt {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// Builds a borrowed rdkafka record from an owned record.
fn build_future_record(record: &OwnedProducerRecord) -> FutureRecord<'_, [u8], [u8]> {
    let mut future_record = FutureRecord::to(&record.topic);

    if let Some(ref payload) = record.payload {
        future_record = future_record.payload(payload.as_slice());
    }

    if let Some(ref key) = record.key {
        future_record = future_record.key(key.as_slice());
    }

    if let Some(partition) = record.partition {
        future_record = future_record.partition(partition);
    }

    if let Some(ts) = record.timestamp {
        future_record = future_record.timestamp(ts);
    }

    let mut owned_headers = OwnedHeaders::new();
    if !record.headers.is_empty() {
        for (k, v) in &record.headers {
            owned_headers = owned_headers.insert(Header {
                key: k.as_str(),
                value: Some(v.as_slice()),
            });
        }
        future_record = future_record.headers(owned_headers);
    }

    future_record
}

/// Delivery report of a pipelined message, with the time from enqueue to broker acknowledgement.
#[derive(Debug)]
pub struct DeliveryOutcome {
    pub result: KafkaResult<DeliveryReceipt>,
    pub latency: Duration,
    /// Payload size of the delivered record in bytes.
    pub payload_bytes: usize,
}

/// Keeps up to `max_in_flight` messages in flight so librdkafka can batch them,
/// instead of awaiting each delivery before sending the next message.
pub struct DeliveryPipeline {
    inner: TaggedDeliveryPipeline<()>,
}

impl DeliveryPipeline {
    /// Creates a pipeline that allows at most `max_in_flight` unacknowledged messages.
    pub fn new(max_in_flight: usize) -> Self {
        Self {
            inner: TaggedDeliveryPipeline::new(max_in_flight),
        }
    }

    /// Number of messages enqueued but not yet acknowledged.
    pub fn in_flight(&self) -> usize {
        self.inner.in_flight()
    }

    /// Enqueues a record. When the window is full, first waits for one earlier
    /// delivery to complete and returns its outcome.
    pub async fn submit(
        &mut self,
        producer: &KafkaProducerClient,
        record: &OwnedProducerRecord,
    ) -> KafkaResult<Option<DeliveryOutcome>> {
        let completed = if self.inner.is_full() {
            self.next_outcome().await
        } else {
            None
        };
        self.inner.push(producer, record, ()).await?;
        Ok(completed)
    }

    /// Waits for the next delivery to complete. Returns `None` when nothing is
    /// in flight. A delivery task that panicked is reported as a failed delivery.
    pub async fn next_outcome(&mut self) -> Option<DeliveryOutcome> {
        let joined = self.inner.next_joined().await?;
        Some(match joined {
            Ok(((), outcome)) => outcome,
            Err(err) => DeliveryOutcome {
                result: Err(KafkaError::DeliveryFailed {
                    topic: String::new(),
                    partition: None,
                    reason: format!("delivery task failed: {err}"),
                }),
                latency: Duration::ZERO,
                payload_bytes: 0,
            },
        })
    }
}

/// A [`DeliveryPipeline`] whose outcomes carry a caller-chosen tag, for
/// callers that must know which record a delivery report belongs to (for
/// example to commit a source offset only after its copy was delivered).
///
/// A delivery task that panics loses its tag: its outcome is never returned,
/// so a caller tracking tags must treat a tag without an outcome as pending.
pub struct TaggedDeliveryPipeline<T> {
    in_flight: JoinSet<(T, DeliveryOutcome)>,
    max_in_flight: usize,
}

impl<T: Send + 'static> TaggedDeliveryPipeline<T> {
    /// Creates a pipeline that allows at most `max_in_flight` unacknowledged messages.
    pub fn new(max_in_flight: usize) -> Self {
        Self {
            in_flight: JoinSet::new(),
            max_in_flight: max_in_flight.max(1),
        }
    }

    /// Number of messages enqueued but not yet acknowledged.
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    fn is_full(&self) -> bool {
        self.in_flight.len() >= self.max_in_flight
    }

    /// When the window is full, waits for one delivery to complete and
    /// returns it; otherwise returns `None` at once. Call before [`Self::push`].
    pub async fn make_room(&mut self) -> Option<(T, DeliveryOutcome)> {
        if self.is_full() {
            self.next_outcome().await
        } else {
            None
        }
    }

    /// Hands a record to the producer. On an error the record was not
    /// enqueued and `tag` comes back with no outcome.
    pub async fn push(
        &mut self,
        producer: &KafkaProducerClient,
        record: &OwnedProducerRecord,
        tag: T,
    ) -> KafkaResult<()> {
        let enqueued_at = Instant::now();
        let delivery = producer.enqueue(record).await?;
        let payload_bytes = record.payload.as_ref().map_or(0, Vec::len);
        self.in_flight.spawn(async move {
            let result = delivery.await;
            let outcome = DeliveryOutcome {
                result,
                latency: enqueued_at.elapsed(),
                payload_bytes,
            };
            (tag, outcome)
        });
        Ok(())
    }

    /// Waits for the next delivery to complete. Returns `None` when nothing is
    /// in flight. A delivery task that panicked is skipped (see the type docs).
    pub async fn next_outcome(&mut self) -> Option<(T, DeliveryOutcome)> {
        loop {
            match self.next_joined().await? {
                Ok(done) => return Some(done),
                Err(err) => tracing::warn!("delivery task failed: {err}"),
            }
        }
    }

    async fn next_joined(&mut self) -> Option<Result<(T, DeliveryOutcome), JoinError>> {
        self.in_flight.join_next().await
    }
}

fn map_delivery(
    delivery: <DeliveryFuture as std::future::Future>::Output,
    topic: String,
    partition: Option<i32>,
) -> KafkaResult<DeliveryReceipt> {
    match delivery {
        Ok(Ok(delivery)) => Ok(DeliveryReceipt {
            topic,
            partition: delivery.partition,
            offset: delivery.offset,
        }),
        Ok(Err((kafka_err, _))) => Err(KafkaError::DeliveryFailed {
            topic,
            partition,
            reason: kafka_err.to_string(),
        }),
        Err(_) => Err(KafkaError::DeliveryFailed {
            topic,
            partition,
            reason: "delivery report was dropped before completion".to_string(),
        }),
    }
}

/// A record handed to the producer with [`KafkaProducerClient::enqueue`].
/// Awaiting it gives the broker's acknowledgement (or the delivery error).
#[must_use = "a pending delivery does nothing unless awaited"]
pub struct PendingDelivery {
    future: DeliveryFuture,
    topic: String,
    partition: Option<i32>,
}

impl std::future::Future for PendingDelivery {
    type Output = KafkaResult<DeliveryReceipt>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        std::pin::Pin::new(&mut this.future)
            .poll(cx)
            .map(|delivery| map_delivery(delivery, std::mem::take(&mut this.topic), this.partition))
    }
}

/// The librdkafka config of a producer: the shared client settings plus the
/// producer-only ones. A key in `extra_properties` wins over both.
fn producer_rdkafka_config(config: &KafkaClientConfig) -> KafkaResult<rdkafka::ClientConfig> {
    let mut rd_config = config.to_rdkafka_config()?;
    let explicit = |key: &str| config.extra_properties.contains_key(key);
    // Writing to a topic that does not exist yet follows the broker's
    // `auto.create.topics.enable`, as a Kafka producer normally does;
    // read and admin clients never create topics.
    if !explicit("allow.auto.create.topics") {
        rd_config.set("allow.auto.create.topics", "true");
    }
    // Unset, librdkafka uses `consistent_random`.
    if let Some(partitioner) = config.partitioner {
        if !explicit("partitioner") {
            rd_config.set("partitioner", partitioner.as_str());
        }
    }
    Ok(rd_config)
}

/// High-performance asynchronous Kafka producer client.
pub struct KafkaProducerClient {
    // Declared before `producer` so it stops before the client is destroyed.
    _oauth: Option<crate::kafka::oauth_refresh::OAuthRefresher>,
    producer: DetachedDrop<FutureProducer<DefaultClientContext>>,
    config: KafkaClientConfig,
}

impl KafkaProducerClient {
    /// Creates a new `KafkaProducerClient` from the configuration.
    pub fn new(config: &KafkaClientConfig) -> KafkaResult<Self> {
        let rd_config = producer_rdkafka_config(config)?;
        let (oauth, producer) = crate::kafka::oauth_refresh::with_oauth(
            config,
            || Ok(rd_config.create::<FutureProducer<DefaultClientContext>>()?),
            |producer| producer.client(),
        )?;
        Ok(Self {
            _oauth: oauth,
            producer: DetachedDrop::new(producer),
            config: config.clone(),
        })
    }

    /// Returns the client configuration.
    pub fn config(&self) -> &KafkaClientConfig {
        &self.config
    }

    /// Produces a single message asynchronously and awaits broker delivery acknowledgement.
    pub async fn send(
        &self,
        record: &OwnedProducerRecord,
        queue_timeout: Option<Duration>,
    ) -> KafkaResult<DeliveryReceipt> {
        let timeout = queue_timeout.unwrap_or(self.config.message_timeout);
        let future_record = build_future_record(record);
        let delivery_result = self.producer.send(future_record, timeout).await;

        match delivery_result {
            Ok(delivery) => Ok(DeliveryReceipt {
                topic: record.topic.clone(),
                partition: delivery.partition,
                offset: delivery.offset,
            }),
            Err((kafka_err, _)) => Err(KafkaError::DeliveryFailed {
                topic: record.topic.clone(),
                partition: record.partition,
                reason: kafka_err.to_string(),
            }),
        }
    }

    /// Produces a simple text payload with an optional string key.
    pub async fn send_str(
        &self,
        topic: &str,
        key: Option<&str>,
        payload: &str,
    ) -> KafkaResult<DeliveryReceipt> {
        let mut record = OwnedProducerRecord::new(topic).with_payload(payload.as_bytes().to_vec());
        if let Some(k) = key {
            record = record.with_key(k.as_bytes().to_vec());
        }
        self.send(&record, None).await
    }

    /// Produces multiple messages concurrently in batches.
    pub async fn send_batch(
        &self,
        records: &[OwnedProducerRecord],
        queue_timeout: Option<Duration>,
    ) -> KafkaResult<Vec<DeliveryReceipt>> {
        let mut receipts = Vec::with_capacity(records.len());
        for record in records {
            let receipt = self.send(record, queue_timeout).await?;
            receipts.push(receipt);
        }
        Ok(receipts)
    }

    /// Hands a record to librdkafka without waiting for the broker acknowledgement;
    /// await the returned [`PendingDelivery`] for it. Retries while the local
    /// queue is full, up to the configured message timeout.
    pub async fn enqueue(&self, record: &OwnedProducerRecord) -> KafkaResult<PendingDelivery> {
        let deadline = Instant::now() + self.config.message_timeout;
        loop {
            match self.producer.send_result(build_future_record(record)) {
                Ok(future) => {
                    return Ok(PendingDelivery {
                        future,
                        topic: record.topic.clone(),
                        partition: record.partition,
                    })
                }
                Err((RdKafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _))
                    if Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Err((kafka_err, _)) => {
                    return Err(KafkaError::DeliveryFailed {
                        topic: record.topic.clone(),
                        partition: record.partition,
                        reason: kafka_err.to_string(),
                    })
                }
            }
        }
    }

    /// Flushes any pending message deliveries within the given timeout.
    pub fn flush(&self, timeout: Option<Duration>) -> KafkaResult<()> {
        let to = timeout.unwrap_or(self.config.message_timeout);
        self.producer.flush(to)?;
        Ok(())
    }
}

impl Drop for KafkaProducerClient {
    fn drop(&mut self) {
        // The client is torn down on another thread (see `DetachedDrop`),
        // which can lose to process exit. Records still queued are handed to
        // the broker first; with none queued this returns at once.
        let _ = self.producer.flush(Duration::from_millis(500));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_builder() {
        let record = OwnedProducerRecord::new("test-topic")
            .with_key("key-1".as_bytes())
            .with_payload("payload-1".as_bytes())
            .with_partition(2)
            .with_timestamp(1726900000000)
            .with_header("trace_id", "xyz123".as_bytes());

        assert_eq!(record.topic, "test-topic");
        assert_eq!(record.key, Some(b"key-1".to_vec()));
        assert_eq!(record.payload, Some(b"payload-1".to_vec()));
        assert_eq!(record.partition, Some(2));
        assert_eq!(record.timestamp, Some(1726900000000));
        assert_eq!(
            record.headers,
            [("trace_id".to_string(), b"xyz123".to_vec())]
        );
    }

    #[test]
    fn repeated_header_keys_are_all_sent_in_order() {
        let record = OwnedProducerRecord::new("t")
            .with_header("a", "1")
            .with_header("a", "2")
            .with_header("b", "3");
        use rdkafka::message::Headers;
        let built = build_future_record(&record);
        let headers = built.headers.expect("headers");
        let got: Vec<(String, Vec<u8>)> = (0..headers.count())
            .map(|i| {
                let h = headers.get(i);
                (h.key.to_string(), h.value.unwrap().to_vec())
            })
            .collect();
        assert_eq!(
            got,
            [
                ("a".to_string(), b"1".to_vec()),
                ("a".to_string(), b"2".to_vec()),
                ("b".to_string(), b"3".to_vec()),
            ]
        );
    }

    #[test]
    fn test_producer_client_creation() {
        let config = KafkaClientConfig::new("localhost:9092").with_client_id("producer-test");
        let client = KafkaProducerClient::new(&config);
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.config().client_id.as_deref(), Some("producer-test"));
    }

    #[test]
    fn partitioner_is_left_to_librdkafka_unless_set() {
        use crate::kafka::config::Partitioner;
        let plain = KafkaClientConfig::new("localhost:9092");
        assert_eq!(
            producer_rdkafka_config(&plain).unwrap().get("partitioner"),
            None
        );
        // Shared clients never get it, even when set.
        let set = plain.clone().with_partitioner(Partitioner::Murmur2Random);
        assert_eq!(set.to_rdkafka_config().unwrap().get("partitioner"), None);
        assert_eq!(
            producer_rdkafka_config(&set).unwrap().get("partitioner"),
            Some("murmur2_random")
        );
        // A raw librdkafka property wins.
        let raw = set.with_property("partitioner", "fnv1a");
        assert_eq!(
            producer_rdkafka_config(&raw).unwrap().get("partitioner"),
            Some("fnv1a")
        );
        for p in Partitioner::ALL {
            let config = KafkaClientConfig::new("localhost:9092").with_partitioner(p);
            assert!(KafkaProducerClient::new(&config).is_ok(), "{p}");
        }
    }
}
