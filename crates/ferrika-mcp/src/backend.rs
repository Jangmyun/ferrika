//! What the write tools do to the cluster, behind a trait so the whole
//! plan → confirm → execute flow is tested without a broker (as the GUI's
//! `Backend` is).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_core::kafka::admin::{ConfigEntryView, KafkaAdminClient};
use ferrika_core::kafka::config::KafkaClientConfig;
use ferrika_core::kafka::offset_reset::{OffsetResetPlan, ResetStrategy};
use ferrika_core::kafka::producer::{DeliveryReceipt, KafkaProducerClient, OwnedProducerRecord};
use ferrika_core::kafka::wire::ConfigChange;
use ferrika_ops::transform::TransformSummary;

use super::transform_tools::{Start, StepSpecs};

pub(super) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = KafkaResult<T>> + 'a>>;

/// A topic's partition count and replication factor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TopicShape {
    pub partitions: i32,
    pub replication_factor: i32,
}

/// A transform `transform_execute` runs: records of `src_topic` from
/// `start` up to the end each partition has when it starts, through
/// `steps`, into `dst_topic` on the same cluster, at most `max_records`.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct TransformRun {
    pub src_topic: String,
    pub dst_topic: String,
    pub steps: StepSpecs,
    pub start: Start,
    pub partition: Option<i32>,
    pub max_records: u64,
    /// Decode Schema Registry wire-format values to JSON first.
    pub decode: bool,
}

/// The cluster operations behind the write tools.
pub(super) trait WriteBackend {
    /// `topic`'s shape, `None` when it does not exist.
    fn topic<'a>(&'a self, topic: &'a str) -> BoxFuture<'a, Option<TopicShape>>;
    /// Every config of `topic` with its value and origin.
    fn topic_configs<'a>(&'a self, topic: &'a str) -> BoxFuture<'a, Vec<ConfigEntryView>>;
    fn broker_count(&self) -> BoxFuture<'_, usize>;
    /// The brokers the cluster reports in its metadata, as `host:port`, for
    /// the policy's broker-address rules. Fakes know none.
    fn known_brokers(&self) -> BoxFuture<'_, Vec<String>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    /// The cluster id, for the operation history's scope; empty when it
    /// cannot be read. Fakes have none.
    fn cluster_id(&self) -> BoxFuture<'_, String> {
        Box::pin(async { Ok(String::new()) })
    }
    /// CreateTopics; a replication factor of -1 is the broker default.
    fn create_topic<'a>(
        &'a self,
        name: &'a str,
        partitions: i32,
        replication_factor: i32,
        configs: &'a HashMap<String, String>,
        validate_only: bool,
    ) -> BoxFuture<'a, ()>;
    /// IncrementalAlterConfigs on a topic.
    fn alter_config<'a>(
        &'a self,
        topic: &'a str,
        changes: &'a [ConfigChange],
        validate_only: bool,
    ) -> BoxFuture<'a, ()>;
    fn plan_reset<'a>(
        &'a self,
        group: &'a str,
        topic: &'a str,
        partition: Option<i32>,
        strategy: &'a ResetStrategy,
    ) -> BoxFuture<'a, Vec<OffsetResetPlan>>;
    /// The group's state and member count, `None` when the broker does not
    /// know it.
    fn group_state<'a>(&'a self, group: &'a str) -> BoxFuture<'a, Option<(String, usize)>>;
    fn commit_reset<'a>(
        &'a self,
        group: &'a str,
        plans: &'a [OffsetResetPlan],
    ) -> BoxFuture<'a, ()>;
    /// `json_text` serialized with the latest schema of `subject`.
    fn encode_value<'a>(&'a self, subject: &'a str, json_text: &'a str) -> BoxFuture<'a, Vec<u8>>;
    /// Sends each record, in order; one result per record.
    fn produce(
        &self,
        records: Vec<OwnedProducerRecord>,
    ) -> BoxFuture<'_, Vec<KafkaResult<DeliveryReceipt>>>;
    /// Runs `run` (the copy pipeline of `ferrika transform --execute`) and
    /// returns its counts.
    fn transform_copy<'a>(&'a self, run: &'a TransformRun) -> BoxFuture<'a, TransformSummary>;
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_secs(10))
}

/// The real cluster, through the profile's client config.
pub(super) struct KafkaBackend {
    pub config: KafkaClientConfig,
}

impl KafkaBackend {
    fn admin(&self) -> KafkaResult<KafkaAdminClient> {
        KafkaAdminClient::new(&self.config)
    }
}

impl WriteBackend for KafkaBackend {
    fn cluster_id(&self) -> BoxFuture<'_, String> {
        Box::pin(async move { Ok(self.admin()?.cluster_id(timeout()).unwrap_or_default()) })
    }

    fn topic<'a>(&'a self, topic: &'a str) -> BoxFuture<'a, Option<TopicShape>> {
        Box::pin(async move {
            match self.admin()?.describe_topic(topic, timeout()) {
                Ok(summary) => Ok(Some(TopicShape {
                    partitions: summary.partition_count as i32,
                    replication_factor: summary.replication_factor as i32,
                })),
                Err(KafkaError::TopicNotFound(_)) => Ok(None),
                Err(e) => Err(e),
            }
        })
    }

    fn topic_configs<'a>(&'a self, topic: &'a str) -> BoxFuture<'a, Vec<ConfigEntryView>> {
        Box::pin(async move { self.admin()?.describe_topic_configs(topic, timeout()).await })
    }

    fn broker_count(&self) -> BoxFuture<'_, usize> {
        Box::pin(async move { Ok(self.admin()?.list_brokers(timeout())?.len()) })
    }

    fn known_brokers(&self) -> BoxFuture<'_, Vec<String>> {
        Box::pin(async move {
            ferrika_core::kafka::admin::known_brokers(&self.config).map_err(KafkaError::config)
        })
    }

    fn create_topic<'a>(
        &'a self,
        name: &'a str,
        partitions: i32,
        replication_factor: i32,
        configs: &'a HashMap<String, String>,
        validate_only: bool,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.admin()?
                .create_topic_with(
                    name,
                    partitions,
                    replication_factor,
                    Some(configs),
                    timeout(),
                    validate_only,
                )
                .await
        })
    }

    fn alter_config<'a>(
        &'a self,
        topic: &'a str,
        changes: &'a [ConfigChange],
        validate_only: bool,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            ferrika_core::kafka::wire::incremental_alter_topic_config(
                &self.config,
                topic,
                changes,
                validate_only,
            )
        })
    }

    fn plan_reset<'a>(
        &'a self,
        group: &'a str,
        topic: &'a str,
        partition: Option<i32>,
        strategy: &'a ResetStrategy,
    ) -> BoxFuture<'a, Vec<OffsetResetPlan>> {
        Box::pin(ferrika_core::kafka::offset_reset::prepare_offset_reset(
            &self.config,
            group,
            topic,
            partition,
            strategy,
        ))
    }

    fn group_state<'a>(&'a self, group: &'a str) -> BoxFuture<'a, Option<(String, usize)>> {
        Box::pin(
            async move { ferrika_core::kafka::offset_reset::group_state(&self.admin()?, group) },
        )
    }

    fn commit_reset<'a>(
        &'a self,
        group: &'a str,
        plans: &'a [OffsetResetPlan],
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            ferrika_core::kafka::offset_reset::commit_offset_reset(&self.config, group, plans)
        })
    }

    fn encode_value<'a>(&'a self, subject: &'a str, json_text: &'a str) -> BoxFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let url = self.config.schema_registry_url.as_deref().ok_or_else(|| {
                KafkaError::config(
                    "value_subject and key_subject need a Schema Registry URL on the profile"
                        .to_string(),
                )
            })?;
            let client = ferrika_core::kafka::schema::SchemaRegistryClient::new(url)
                .with_timeout(self.config.request_timeout);
            Ok(client.encode_latest(subject, json_text).await?.0)
        })
    }

    fn produce(
        &self,
        records: Vec<OwnedProducerRecord>,
    ) -> BoxFuture<'_, Vec<KafkaResult<DeliveryReceipt>>> {
        Box::pin(async move {
            let producer = KafkaProducerClient::new(&self.config)?;
            let mut results = Vec::with_capacity(records.len());
            for record in &records {
                results.push(producer.send(record, None).await);
            }
            Ok(results)
        })
    }

    fn transform_copy<'a>(&'a self, run: &'a TransformRun) -> BoxFuture<'a, TransformSummary> {
        Box::pin(async move {
            use ferrika_ops::mirror::{
                run_copy_pipeline, CopyDestination, MirrorConfig, MirrorStats,
            };
            use ferrika_ops::transform::{DecodingTransform, Pipeline};
            let pipeline =
                Pipeline::parse(run.steps.iter().map(|(kind, text)| (*kind, text.as_str())))
                    .map_err(KafkaError::config)?;
            let digest = pipeline.digest();
            let decoder = run.decode.then(|| {
                ferrika_core::kafka::schema::PayloadDecoder::new(
                    self.config.schema_registry_url.as_deref(),
                )
                .with_timeout(self.config.request_timeout)
            });
            let mut transform =
                DecodingTransform::new(pipeline, run.dst_topic.clone()).with_decoder(decoder);
            let (from_beginning, from_timestamp) = match run.start {
                Start::Beginning => (true, None),
                Start::Timestamp(ms) => (false, Some(ms)),
            };
            let config = MirrorConfig::new(&run.src_topic, &run.dst_topic)
                .with_from_beginning(from_beginning)
                .with_from_timestamp(from_timestamp)
                .with_until_end(true)
                .with_partition(run.partition)
                .with_max_messages(Some(usize::try_from(run.max_records).unwrap_or(usize::MAX)));
            // No consumer group: nothing is joined or committed, and the
            // run reads only the range it was planned for.
            let mut source = self.config.clone();
            source.group_id = None;
            let stats = run_copy_pipeline(
                &source,
                CopyDestination::Cluster(&self.config),
                config,
                &mut transform,
                |_: &MirrorStats| {},
            )
            .await?;
            Ok(TransformSummary::new(
                &run.src_topic,
                &run.dst_topic,
                false,
                &stats,
                transform.counters(),
                digest,
            ))
        })
    }
}

/// A shared backend, so a test can keep looking at the fake it handed over.
#[cfg(test)]
impl<T: WriteBackend + ?Sized> WriteBackend for &T {
    fn topic<'a>(&'a self, topic: &'a str) -> BoxFuture<'a, Option<TopicShape>> {
        (**self).topic(topic)
    }
    fn topic_configs<'a>(&'a self, topic: &'a str) -> BoxFuture<'a, Vec<ConfigEntryView>> {
        (**self).topic_configs(topic)
    }
    fn broker_count(&self) -> BoxFuture<'_, usize> {
        (**self).broker_count()
    }
    fn known_brokers(&self) -> BoxFuture<'_, Vec<String>> {
        (**self).known_brokers()
    }
    fn create_topic<'a>(
        &'a self,
        name: &'a str,
        partitions: i32,
        replication_factor: i32,
        configs: &'a HashMap<String, String>,
        validate_only: bool,
    ) -> BoxFuture<'a, ()> {
        (**self).create_topic(name, partitions, replication_factor, configs, validate_only)
    }
    fn alter_config<'a>(
        &'a self,
        topic: &'a str,
        changes: &'a [ConfigChange],
        validate_only: bool,
    ) -> BoxFuture<'a, ()> {
        (**self).alter_config(topic, changes, validate_only)
    }
    fn plan_reset<'a>(
        &'a self,
        group: &'a str,
        topic: &'a str,
        partition: Option<i32>,
        strategy: &'a ResetStrategy,
    ) -> BoxFuture<'a, Vec<OffsetResetPlan>> {
        (**self).plan_reset(group, topic, partition, strategy)
    }
    fn group_state<'a>(&'a self, group: &'a str) -> BoxFuture<'a, Option<(String, usize)>> {
        (**self).group_state(group)
    }
    fn commit_reset<'a>(
        &'a self,
        group: &'a str,
        plans: &'a [OffsetResetPlan],
    ) -> BoxFuture<'a, ()> {
        (**self).commit_reset(group, plans)
    }
    fn encode_value<'a>(&'a self, subject: &'a str, json_text: &'a str) -> BoxFuture<'a, Vec<u8>> {
        (**self).encode_value(subject, json_text)
    }
    fn produce(
        &self,
        records: Vec<OwnedProducerRecord>,
    ) -> BoxFuture<'_, Vec<KafkaResult<DeliveryReceipt>>> {
        (**self).produce(records)
    }
    fn transform_copy<'a>(&'a self, run: &'a TransformRun) -> BoxFuture<'a, TransformSummary> {
        (**self).transform_copy(run)
    }
}

#[cfg(test)]
pub(super) mod fake {
    //! A cluster in memory that records every call.

    use std::cell::RefCell;

    use super::*;
    use ferrika_core::kafka::admin::ConfigOrigin;
    use ferrika_core::kafka::offset_reset::PartitionPosition;

    /// (key, value, set on the topic itself).
    pub type ConfigRow = (String, String, bool);
    /// (group, topic).
    pub type GroupTopic = (String, String);

    #[derive(Default)]
    pub struct FakeCluster {
        /// Topic → (partitions, replication factor).
        pub topics: RefCell<HashMap<String, (i32, i32)>>,
        /// Topic → (key, value, from a topic override).
        pub configs: RefCell<HashMap<String, Vec<ConfigRow>>>,
        pub brokers: usize,
        /// (group, topic) → committed offset per partition, and log end.
        pub offsets: RefCell<HashMap<GroupTopic, Vec<(i64, i64)>>>,
        pub group_states: RefCell<HashMap<String, (String, usize)>>,
        /// Every call that would change the cluster, e.g. `create orders`.
        pub changes: RefCell<Vec<String>>,
        /// Next offset per (topic, partition).
        pub produced: RefCell<HashMap<(String, i32), i64>>,
        /// Fail produce for records whose value contains this text.
        pub fail_value: Option<String>,
        /// A broker error for validate-only requests.
        pub reject_validation: Option<String>,
        /// What the metadata lookup of broker addresses answers (`Err` = it
        /// fails); `None` = no brokers.
        pub known: Option<Result<Vec<String>, String>>,
        /// How many times the lookup ran.
        pub known_calls: std::cell::Cell<u32>,
        /// How many records a transform finds to copy (it writes at most
        /// its `max_records` of them).
        pub transform_records: u64,
    }

    impl FakeCluster {
        pub fn with_topic(self, name: &str, partitions: i32) -> Self {
            self.topics
                .borrow_mut()
                .insert(name.into(), (partitions, 1));
            self
        }

        pub fn set_config(&self, topic: &str, key: &str, value: &str, overridden: bool) {
            let mut configs = self.configs.borrow_mut();
            let list = configs.entry(topic.into()).or_default();
            list.retain(|(k, _, _)| k != key);
            list.push((key.into(), value.into(), overridden));
        }

        fn change(&self, text: String) {
            self.changes.borrow_mut().push(text);
        }
    }

    fn ready<'a, T: 'a>(value: KafkaResult<T>) -> BoxFuture<'a, T> {
        Box::pin(async move { value })
    }

    impl WriteBackend for FakeCluster {
        fn known_brokers(&self) -> BoxFuture<'_, Vec<String>> {
            self.known_calls.set(self.known_calls.get() + 1);
            match &self.known {
                None => ready(Ok(Vec::new())),
                Some(Ok(list)) => ready(Ok(list.clone())),
                Some(Err(e)) => ready(Err(KafkaError::config(e.clone()))),
            }
        }

        fn topic<'a>(&'a self, topic: &'a str) -> BoxFuture<'a, Option<TopicShape>> {
            ready(Ok(self.topics.borrow().get(topic).map(|&(p, rf)| {
                TopicShape {
                    partitions: p,
                    replication_factor: rf,
                }
            })))
        }

        fn topic_configs<'a>(&'a self, topic: &'a str) -> BoxFuture<'a, Vec<ConfigEntryView>> {
            let entries = self
                .configs
                .borrow()
                .get(topic)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|(name, value, overridden)| ConfigEntryView {
                    sensitive: name.contains("password"),
                    name,
                    value,
                    source: if overridden {
                        ConfigOrigin::TopicOverride
                    } else {
                        ConfigOrigin::Default
                    },
                    read_only: false,
                })
                .collect();
            ready(Ok(entries))
        }

        fn broker_count(&self) -> BoxFuture<'_, usize> {
            ready(Ok(self.brokers))
        }

        fn create_topic<'a>(
            &'a self,
            name: &'a str,
            partitions: i32,
            replication_factor: i32,
            _configs: &'a HashMap<String, String>,
            validate_only: bool,
        ) -> BoxFuture<'a, ()> {
            if validate_only {
                return ready(match &self.reject_validation {
                    Some(e) => Err(KafkaError::admin(e.clone())),
                    None => Ok(()),
                });
            }
            self.topics
                .borrow_mut()
                .insert(name.into(), (partitions, replication_factor));
            self.change(format!("create {name} {partitions} {replication_factor}"));
            ready(Ok(()))
        }

        fn alter_config<'a>(
            &'a self,
            topic: &'a str,
            changes: &'a [ConfigChange],
            validate_only: bool,
        ) -> BoxFuture<'a, ()> {
            if validate_only {
                return ready(match &self.reject_validation {
                    Some(e) => Err(KafkaError::admin(e.clone())),
                    None => Ok(()),
                });
            }
            for change in changes {
                match change {
                    ConfigChange::Set(k, v) => {
                        self.set_config(topic, k, v, true);
                        self.change(format!("set {topic} {k}={v}"));
                    }
                    ConfigChange::Delete(k) => {
                        if let Some(list) = self.configs.borrow_mut().get_mut(topic) {
                            list.retain(|(key, _, _)| key != k);
                        }
                        self.change(format!("delete {topic} {k}"));
                    }
                }
            }
            ready(Ok(()))
        }

        fn plan_reset<'a>(
            &'a self,
            group: &'a str,
            topic: &'a str,
            partition: Option<i32>,
            strategy: &'a ResetStrategy,
        ) -> BoxFuture<'a, Vec<OffsetResetPlan>> {
            let offsets = self.offsets.borrow();
            let Some(parts) = offsets.get(&(group.to_string(), topic.to_string())) else {
                return ready(Err(KafkaError::TopicNotFound(topic.into())));
            };
            let positions: Vec<PartitionPosition> = parts
                .iter()
                .enumerate()
                .filter(|(p, _)| partition.is_none_or(|only| only as usize == *p))
                .map(|(p, &(current, high))| PartitionPosition {
                    partition: p as i32,
                    committed: current,
                    low_watermark: 0,
                    high_watermark: high,
                })
                .collect();
            let plans = match ferrika_core::kafka::offset_reset::plan_offset_reset(
                group, topic, strategy, &positions,
            ) {
                Ok(plans) => plans,
                Err(e) => return ready(Err(e)),
            };
            ready(Ok(plans))
        }

        fn group_state<'a>(&'a self, group: &'a str) -> BoxFuture<'a, Option<(String, usize)>> {
            ready(Ok(self.group_states.borrow().get(group).cloned()))
        }

        fn commit_reset<'a>(
            &'a self,
            group: &'a str,
            plans: &'a [OffsetResetPlan],
        ) -> BoxFuture<'a, ()> {
            let mut offsets = self.offsets.borrow_mut();
            for plan in plans {
                if let Some(parts) = offsets.get_mut(&(group.to_string(), plan.topic.clone())) {
                    if let Some(slot) = parts.get_mut(plan.partition as usize) {
                        slot.0 = plan.target_offset;
                    }
                }
                self.change(format!(
                    "commit {group} {}/{}={}",
                    plan.topic, plan.partition, plan.target_offset
                ));
            }
            ready(Ok(()))
        }

        fn encode_value<'a>(
            &'a self,
            subject: &'a str,
            json_text: &'a str,
        ) -> BoxFuture<'a, Vec<u8>> {
            if subject == "missing-value" {
                return ready(Err(KafkaError::schema("subject not found".to_string())));
            }
            let mut bytes = vec![0u8, 0, 0, 0, 1];
            bytes.extend_from_slice(json_text.as_bytes());
            ready(Ok(bytes))
        }

        fn produce(
            &self,
            records: Vec<OwnedProducerRecord>,
        ) -> BoxFuture<'_, Vec<KafkaResult<DeliveryReceipt>>> {
            let mut results = Vec::new();
            for record in records {
                let value = record.payload.clone().unwrap_or_default();
                if self
                    .fail_value
                    .as_ref()
                    .is_some_and(|f| String::from_utf8_lossy(&value).contains(f.as_str()))
                {
                    results.push(Err(KafkaError::DeliveryFailed {
                        topic: record.topic.clone(),
                        partition: record.partition,
                        reason: "Message size too large".into(),
                    }));
                    continue;
                }
                let partition = record.partition.unwrap_or(0);
                let mut produced = self.produced.borrow_mut();
                let next = produced
                    .entry((record.topic.clone(), partition))
                    .or_default();
                results.push(Ok(DeliveryReceipt {
                    topic: record.topic.clone(),
                    partition,
                    offset: *next,
                }));
                *next += 1;
                self.change(format!("produce {}/{partition}", record.topic));
            }
            ready(Ok(results))
        }

        fn transform_copy<'a>(&'a self, run: &'a TransformRun) -> BoxFuture<'a, TransformSummary> {
            use ferrika_ops::mirror::MirrorStats;
            use ferrika_ops::transform::{Pipeline, TransformCounters};
            let digest = match Pipeline::parse(
                run.steps.iter().map(|(kind, text)| (*kind, text.as_str())),
            ) {
                Ok(pipeline) => pipeline.digest(),
                Err(e) => return ready(Err(KafkaError::config(e))),
            };
            let written = self.transform_records.min(run.max_records);
            let mut stats = MirrorStats::new();
            for _ in 0..written {
                stats.record_read();
                stats.record_mirrored(10);
            }
            self.change(format!(
                "transform {} -> {} ({written} records)",
                run.src_topic, run.dst_topic
            ));
            ready(Ok(TransformSummary::new(
                &run.src_topic,
                &run.dst_topic,
                false,
                &stats,
                &TransformCounters::default(),
                digest,
            )))
        }
    }
}
