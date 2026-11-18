use std::collections::HashMap;
use std::time::Duration;

use rdkafka::admin::{
    AdminClient, AdminOptions, ConfigSource, NewPartitions, NewTopic, OwnedResourceSpecifier,
    ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::metadata::{Metadata, MetadataTopic};

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::config::KafkaClientConfig;
use crate::kafka::detached::DetachedDrop;
use crate::model::cluster::{BrokerInfo, PartitionDetail, TopicSummary};

/// A consumer group as the broker's group listing reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupListing {
    pub name: String,
    pub state: String,
    pub members: Vec<GroupMemberListing>,
}

/// One member of a consumer group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMemberListing {
    /// Broker-assigned member id, usually `<client.id>-<uuid>`.
    pub member_id: String,
    pub client_id: String,
    pub host: String,
    /// `(topic, partition)` pairs assigned to this member.
    pub assignment: Vec<(String, i32)>,
}

impl GroupMemberListing {
    /// `client-id 1a2b3c @ host`: the short id tells apart members that share
    /// a client id, as consumers started by the same tool usually do.
    pub fn label(&self) -> String {
        let suffix = self
            .member_id
            .strip_prefix(&self.client_id)
            .unwrap_or(&self.member_id)
            .trim_start_matches('-');
        let short: String = suffix.chars().take(6).collect();
        if short.is_empty() {
            format!("{} @ {}", self.client_id, self.host)
        } else {
            format!("{} {short} @ {}", self.client_id, self.host)
        }
    }
}

/// Decodes the consumer protocol `Assignment` a member carries: version
/// (int16), then an array of topics (int16-length name + int32 partitions),
/// then user data we ignore. Returns `None` on a truncated or malformed blob.
pub(crate) fn parse_consumer_assignment(bytes: &[u8]) -> Option<Vec<(String, i32)>> {
    fn take<'a>(buf: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
        let (head, rest) = (buf.get(..n)?, buf.get(n..)?);
        *buf = rest;
        Some(head)
    }
    fn int16(buf: &mut &[u8]) -> Option<i16> {
        Some(i16::from_be_bytes(take(buf, 2)?.try_into().ok()?))
    }
    fn int32(buf: &mut &[u8]) -> Option<i32> {
        Some(i32::from_be_bytes(take(buf, 4)?.try_into().ok()?))
    }

    let mut buf = bytes;
    if buf.is_empty() {
        return Some(Vec::new());
    }
    int16(&mut buf)?;
    let topic_count = usize::try_from(int32(&mut buf)?).ok()?;
    let mut assigned = Vec::new();
    for _ in 0..topic_count {
        let name_len = usize::try_from(int16(&mut buf)?).ok()?;
        let topic = std::str::from_utf8(take(&mut buf, name_len)?).ok()?;
        let partition_count = usize::try_from(int32(&mut buf)?).ok()?;
        for _ in 0..partition_count {
            assigned.push((topic.to_string(), int32(&mut buf)?));
        }
    }
    Some(assigned)
}

/// Adds brokers that partitions still list as replicas but metadata no longer
/// returns. Metadata only lists live brokers, so without this a dead broker
/// would simply disappear instead of showing as offline.
fn with_offline_replicas(
    mut brokers: Vec<BrokerInfo>,
    leader_counts: &HashMap<i32, usize>,
    replica_counts: &HashMap<i32, usize>,
) -> Vec<BrokerInfo> {
    for (&id, &replicas) in replica_counts {
        if id < 0 || brokers.iter().any(|b| b.id == id) {
            continue;
        }
        brokers.push(BrokerInfo {
            id,
            host: String::new(),
            port: 0,
            is_controller: false,
            partition_leaders: *leader_counts.get(&id).unwrap_or(&0),
            replicas_hosted: replicas,
            is_online: false,
            log_dir_bytes: None,
            log_dirs: Vec::new(),
        });
    }
    brokers.sort_by_key(|b| b.id);
    brokers
}

/// Where a config value comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum ConfigOrigin {
    /// Set on the topic itself.
    TopicOverride,
    /// Set dynamically on this broker only (`DYNAMIC_BROKER_CONFIG`).
    BrokerDynamic,
    /// Set dynamically as the default for every broker (`DYNAMIC_DEFAULT_BROKER_CONFIG`).
    /// Removing a broker's own value does not touch it.
    BrokerDynamicDefault,
    /// From the broker's server.properties.
    BrokerStatic,
    /// Kafka's built-in default.
    Default,
    Unknown,
}

impl From<ConfigSource> for ConfigOrigin {
    fn from(source: ConfigSource) -> Self {
        match source {
            ConfigSource::DynamicTopic => Self::TopicOverride,
            ConfigSource::DynamicBroker => Self::BrokerDynamic,
            ConfigSource::DynamicDefaultBroker => Self::BrokerDynamicDefault,
            ConfigSource::StaticBroker => Self::BrokerStatic,
            ConfigSource::Default => Self::Default,
            _ => Self::Unknown,
        }
    }
}

impl ConfigOrigin {
    /// Short label for tables.
    pub fn label(self) -> &'static str {
        match self {
            Self::TopicOverride => "override",
            Self::BrokerDynamic => "broker (dynamic)",
            Self::BrokerDynamicDefault => "cluster default",
            Self::BrokerStatic => "broker",
            Self::Default => "default",
            Self::Unknown => "unknown",
        }
    }
}

/// One config entry as shown to users; sensitive values are masked.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ConfigEntryView {
    pub name: String,
    pub value: String,
    pub source: ConfigOrigin,
    pub read_only: bool,
    pub sensitive: bool,
}

/// Kafka administrative client for cluster metadata and topic management.
pub struct KafkaAdminClient {
    // Declared before `admin` so it stops before the client is destroyed.
    _oauth: Option<crate::kafka::oauth_refresh::OAuthRefresher>,
    admin: DetachedDrop<AdminClient<DefaultClientContext>>,
    config: KafkaClientConfig,
}

impl KafkaAdminClient {
    /// Creates a new `KafkaAdminClient` from the given configuration.
    pub fn new(config: &KafkaClientConfig) -> KafkaResult<Self> {
        let rd_config = config.to_rdkafka_config()?;
        let (oauth, admin) = crate::kafka::oauth_refresh::with_oauth(
            config,
            || Ok(rd_config.create::<AdminClient<DefaultClientContext>>()?),
            |admin| admin.inner(),
        )?;
        Ok(Self {
            _oauth: oauth,
            admin: DetachedDrop::new(admin),
            config: config.clone(),
        })
    }

    /// Returns the configuration associated with this admin client.
    pub fn config(&self) -> &KafkaClientConfig {
        &self.config
    }

    /// Fetches raw metadata for the entire cluster or a specific topic.
    pub fn fetch_raw_metadata(
        &self,
        topic: Option<&str>,
        timeout: Option<Duration>,
    ) -> KafkaResult<Metadata> {
        let to = timeout.unwrap_or(self.config.request_timeout);
        let metadata = self.admin.inner().fetch_metadata(topic, to)?;
        Ok(metadata)
    }

    /// The cluster id from Metadata, or `None` when the brokers do not answer
    /// or report none (very old brokers). librdkafka keeps it from the last
    /// metadata answer, so right after a metadata fetch this does not wait.
    pub fn cluster_id(&self, timeout: Option<Duration>) -> Option<String> {
        let to = timeout.unwrap_or(self.config.request_timeout);
        self.admin
            .inner()
            .fetch_cluster_id(to)
            .filter(|id| !id.is_empty())
    }

    /// Lists all consumer group IDs found in the cluster.
    pub fn list_consumer_groups(&self, timeout: Option<Duration>) -> KafkaResult<Vec<String>> {
        let to = timeout.unwrap_or(self.config.request_timeout);
        let group_list = self.admin.inner().fetch_group_list(None, to)?;
        let mut groups: Vec<String> = group_list
            .groups()
            .iter()
            .map(|g| g.name().to_string())
            .collect();
        groups.sort();
        Ok(groups)
    }

    /// Lists consumer groups with their state and member count.
    pub fn list_consumer_group_states(
        &self,
        timeout: Option<Duration>,
    ) -> KafkaResult<Vec<(String, String, usize)>> {
        Ok(self
            .list_consumer_group_listings(timeout)?
            .into_iter()
            .map(|g| (g.name, g.state, g.members.len()))
            .collect())
    }

    /// Lists consumer groups with their state and members, including the
    /// partitions each member is assigned. Sorted by group name.
    pub fn list_consumer_group_listings(
        &self,
        timeout: Option<Duration>,
    ) -> KafkaResult<Vec<GroupListing>> {
        let to = timeout.unwrap_or(self.config.request_timeout);
        let group_list = self.admin.inner().fetch_group_list(None, to)?;
        let mut groups: Vec<GroupListing> = group_list
            .groups()
            .iter()
            .map(|g| {
                let consumer_protocol = g.protocol_type() == "consumer";
                GroupListing {
                    name: g.name().to_string(),
                    state: g.state().to_string(),
                    members: g
                        .members()
                        .iter()
                        .map(|m| GroupMemberListing {
                            member_id: m.id().to_string(),
                            client_id: m.client_id().to_string(),
                            host: m.client_host().trim_start_matches('/').to_string(),
                            assignment: m
                                .assignment()
                                .filter(|_| consumer_protocol)
                                .and_then(parse_consumer_assignment)
                                .unwrap_or_default(),
                        })
                        .collect(),
                }
            })
            .collect();
        groups.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(groups)
    }

    /// Lists all topic names present in the cluster.
    pub fn list_topics(&self, timeout: Option<Duration>) -> KafkaResult<Vec<String>> {
        let metadata = self.fetch_raw_metadata(None, timeout)?;
        let mut names: Vec<String> = metadata
            .topics()
            .iter()
            .map(|t| t.name().to_string())
            .collect();
        names.sort();
        Ok(names)
    }

    /// The active controller's node id, or `None` when it cannot be determined.
    ///
    /// Metadata's `orig_broker_id` is only the broker that answered (and KRaft
    /// brokers report a random live broker as controller), so it is never used
    /// for this: no answer is better than a wrong one.
    pub fn active_controller_id(&self) -> Option<i32> {
        match crate::kafka::wire::active_controller_id(&self.config) {
            Ok(id) => id,
            Err(e) => {
                tracing::debug!(%e, "Could not determine the active controller");
                None
            }
        }
    }

    /// Fetches all broker details from the cluster metadata.
    pub fn list_brokers(&self, timeout: Option<Duration>) -> KafkaResult<Vec<BrokerInfo>> {
        let metadata = self.fetch_raw_metadata(None, timeout)?;
        let controller_id = self.active_controller_id();

        // Count leader / replica distribution per broker
        let mut leader_counts: HashMap<i32, usize> = HashMap::new();
        let mut replica_counts: HashMap<i32, usize> = HashMap::new();

        for topic in metadata.topics() {
            for partition in topic.partitions() {
                *leader_counts.entry(partition.leader()).or_insert(0) += 1;
                for replica in partition.replicas() {
                    *replica_counts.entry(*replica).or_insert(0) += 1;
                }
            }
        }

        let brokers = metadata
            .brokers()
            .iter()
            .map(|b| {
                let id = b.id();
                BrokerInfo {
                    id,
                    host: b.host().to_string(),
                    port: b.port() as u16,
                    is_controller: Some(id) == controller_id,
                    partition_leaders: *leader_counts.get(&id).unwrap_or(&0),
                    replicas_hosted: *replica_counts.get(&id).unwrap_or(&0),
                    is_online: true,
                    log_dir_bytes: None,
                    log_dirs: Vec::new(),
                }
            })
            .collect();

        Ok(with_offline_replicas(
            brokers,
            &leader_counts,
            &replica_counts,
        ))
    }

    /// Fetches detailed summaries for all topics in the cluster.
    pub fn list_topic_summaries(
        &self,
        timeout: Option<Duration>,
    ) -> KafkaResult<Vec<TopicSummary>> {
        let metadata = self.fetch_raw_metadata(None, timeout)?;
        let mut summaries = Vec::new();

        for topic in metadata.topics() {
            let summary = self.map_topic_metadata(topic);
            summaries.push(summary);
        }

        summaries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(summaries)
    }

    /// Describes a single topic in detail.
    pub fn describe_topic(
        &self,
        topic_name: &str,
        timeout: Option<Duration>,
    ) -> KafkaResult<TopicSummary> {
        let metadata = self.fetch_raw_metadata(Some(topic_name), timeout)?;
        let topic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic_name)
            .ok_or_else(|| KafkaError::TopicNotFound(topic_name.to_string()))?;
        // A missing or hidden topic still comes back as an entry, with an error
        // code and no partitions.
        if let Some(err) = topic.error() {
            use rdkafka::types::RDKafkaErrorCode as Code;
            return Err(match Code::from(err) {
                Code::TopicAuthorizationFailed => {
                    KafkaError::acl(format!("Not authorized to describe topic '{topic_name}'"))
                }
                Code::UnknownTopicOrPartition | Code::UnknownTopic => {
                    KafkaError::TopicNotFound(topic_name.to_string())
                }
                other => {
                    KafkaError::admin(format!("Cannot describe topic '{topic_name}': {other}"))
                }
            });
        }

        Ok(self.map_topic_metadata(topic))
    }

    /// Helper to convert rdkafka `MetadataTopic` to `TopicSummary`.
    fn map_topic_metadata(&self, topic: &MetadataTopic) -> TopicSummary {
        let name = topic.name().to_string();
        let is_internal = crate::kafka::internal_topic::is_internal_topic(&name);

        let mut partitions: Vec<PartitionDetail> = topic
            .partitions()
            .iter()
            .map(|p| PartitionDetail {
                partition_id: p.id(),
                leader: p.leader(),
                replicas: p.replicas().to_vec(),
                isr: p.isr().to_vec(),
                high_watermark: 0, // Will be updated if watermarks queried
                low_watermark: 0,
            })
            .collect();

        partitions.sort_by_key(|p| p.partition_id);

        let replication_factor = partitions.first().map(|p| p.replicas.len()).unwrap_or(0);

        TopicSummary {
            name,
            partition_count: partitions.len(),
            replication_factor,
            total_messages: 0,
            cleanup_policy: "delete".to_string(),
            is_internal,
            size_bytes: None,
            msg_rate: None,
            partitions,
        }
    }

    /// Creates a new topic with the specified partitions and replication factor.
    /// Returns the value of config `key` for each of `topics`, in one DescribeConfigs request.
    ///
    /// Topics whose config could not be read are left out of the map.
    pub async fn describe_topic_config(
        &self,
        topics: &[String],
        key: &str,
        timeout: Option<Duration>,
    ) -> KafkaResult<HashMap<String, String>> {
        if topics.is_empty() {
            return Ok(HashMap::new());
        }
        let specifiers: Vec<ResourceSpecifier> = topics
            .iter()
            .map(|t| ResourceSpecifier::Topic(t.as_str()))
            .collect();
        let options = AdminOptions::new()
            .request_timeout(Some(timeout.unwrap_or(self.config.request_timeout)));
        let results = self
            .admin
            .describe_configs(&specifiers, &options)
            .await
            .map_err(|e| KafkaError::Admin(format!("describe_configs request failed: {e}")))?;

        let mut values = HashMap::with_capacity(topics.len());
        for resource in results.into_iter().flatten() {
            let OwnedResourceSpecifier::Topic(topic) = &resource.specifier else {
                continue;
            };
            if let Some(value) = resource.get(key).and_then(|e| e.value.clone()) {
                values.insert(topic.clone(), value);
            }
        }
        Ok(values)
    }

    /// Returns the configs set on `topic` itself (not inherited defaults), sorted by name.
    /// Sensitive values are masked.
    pub async fn describe_topic_overrides(
        &self,
        topic: &str,
        timeout: Option<Duration>,
    ) -> KafkaResult<Vec<(String, String)>> {
        Ok(self
            .describe_topic_configs(topic, timeout)
            .await?
            .into_iter()
            .filter(|e| e.source == ConfigOrigin::TopicOverride)
            .map(|e| (e.name, e.value))
            .collect())
    }

    /// Every config of `topic` with its effective value and where it comes from.
    pub async fn describe_topic_configs(
        &self,
        topic: &str,
        timeout: Option<Duration>,
    ) -> KafkaResult<Vec<ConfigEntryView>> {
        self.describe_resource_configs(ResourceSpecifier::Topic(topic), timeout)
            .await
    }

    /// Every config of broker `id` with its effective value and where it comes from.
    pub async fn describe_broker_configs(
        &self,
        id: i32,
        timeout: Option<Duration>,
    ) -> KafkaResult<Vec<ConfigEntryView>> {
        self.describe_resource_configs(ResourceSpecifier::Broker(id), timeout)
            .await
    }

    async fn describe_resource_configs(
        &self,
        resource: ResourceSpecifier<'_>,
        timeout: Option<Duration>,
    ) -> KafkaResult<Vec<ConfigEntryView>> {
        let options = AdminOptions::new()
            .request_timeout(Some(timeout.unwrap_or(self.config.request_timeout)));
        let results = self
            .admin
            .describe_configs(&[resource], &options)
            .await
            .map_err(|e| KafkaError::Admin(format!("describe_configs request failed: {e}")))?;
        let resource = results
            .into_iter()
            .next()
            .ok_or_else(|| KafkaError::Admin("describe_configs returned no result".to_string()))?
            .map_err(|code| KafkaError::Admin(format!("describe_configs failed: {code}")))?;
        Ok(entry_views(resource.entries))
    }

    /// Every config of each of `topics`, read [`TOPIC_CONFIG_BATCH`] topics
    /// per DescribeConfigs request. Topics whose configs could not be read
    /// (no `DESCRIBE_CONFIGS` permission, deleted meanwhile, a failed batch)
    /// are returned in the second list instead.
    pub async fn describe_topics_configs(
        &self,
        topics: &[String],
        timeout: Option<Duration>,
    ) -> (HashMap<String, Vec<ConfigEntryView>>, Vec<String>) {
        let options = AdminOptions::new()
            .request_timeout(Some(timeout.unwrap_or(self.config.request_timeout)));
        let mut configs = HashMap::with_capacity(topics.len());
        for batch in topics.chunks(TOPIC_CONFIG_BATCH) {
            let specifiers: Vec<ResourceSpecifier> = batch
                .iter()
                .map(|t| ResourceSpecifier::Topic(t.as_str()))
                .collect();
            let Ok(results) = self.admin.describe_configs(&specifiers, &options).await else {
                continue;
            };
            for resource in results.into_iter().flatten() {
                if let OwnedResourceSpecifier::Topic(topic) = resource.specifier {
                    configs.insert(topic, entry_views(resource.entries));
                }
            }
        }
        let missing = topics
            .iter()
            .filter(|t| !configs.contains_key(*t))
            .cloned()
            .collect();
        (configs, missing)
    }
}

/// Topics per DescribeConfigs request in [`KafkaAdminClient::describe_topics_configs`].
pub const TOPIC_CONFIG_BATCH: usize = 200;

/// Config entries as shown to users, sorted by name; sensitive values are masked.
fn entry_views(entries: Vec<rdkafka::admin::ConfigEntry>) -> Vec<ConfigEntryView> {
    let mut views: Vec<ConfigEntryView> = entries
        .into_iter()
        .map(|e| ConfigEntryView {
            value: if e.is_sensitive {
                "******".to_string()
            } else {
                e.value.unwrap_or_default()
            },
            source: ConfigOrigin::from(e.source),
            read_only: e.is_read_only,
            sensitive: e.is_sensitive,
            name: e.name,
        })
        .collect();
    views.sort_by(|a, b| a.name.cmp(&b.name));
    views
}

impl KafkaAdminClient {
    /// Raises the partition count of `topic` to `total`. Kafka cannot remove partitions,
    /// and keyed records may map to different partitions afterwards.
    pub async fn add_partitions(
        &self,
        topic: &str,
        total: usize,
        timeout: Option<Duration>,
    ) -> KafkaResult<()> {
        let mut options = AdminOptions::new();
        if let Some(to) = timeout {
            options = options.operation_timeout(Some(to));
        }
        let results = self
            .admin
            .create_partitions(&[NewPartitions::new(topic, total)], &options)
            .await
            .map_err(|e| KafkaError::Admin(format!("create_partitions request failed: {e}")))?;
        for result in results {
            if let Err((name, code)) = result {
                return Err(KafkaError::Admin(format!(
                    "Failed to add partitions to '{name}': {code:?} ({code})"
                )));
            }
        }
        Ok(())
    }

    /// Deletes a consumer group and its committed offsets. Kafka refuses while the
    /// group has members.
    pub async fn delete_consumer_group(
        &self,
        group: &str,
        timeout: Option<Duration>,
    ) -> KafkaResult<()> {
        let mut options = AdminOptions::new();
        if let Some(to) = timeout {
            options = options.operation_timeout(Some(to));
        }
        let results = self
            .admin
            .delete_groups(&[group], &options)
            .await
            .map_err(|e| KafkaError::Admin(format!("delete_groups request failed: {e}")))?;
        for result in results {
            if let Err((name, code)) = result {
                return Err(KafkaError::Admin(format!(
                    "Failed to delete group '{name}': {code}"
                )));
            }
        }
        Ok(())
    }

    /// Creates `target` with the partition count, replication factor and
    /// config overrides of `source` (each overridable). Records are not copied.
    /// Returns the partition count, replication factor and number of overrides used.
    pub async fn clone_topic(
        &self,
        source: &str,
        target: &str,
        partitions: Option<i32>,
        replication: Option<i32>,
        timeout: Option<Duration>,
    ) -> KafkaResult<(i32, i32, usize)> {
        let detail = self.describe_topic(source, timeout)?;
        let partitions = partitions.unwrap_or(detail.partitions.len() as i32);
        let replication = replication.unwrap_or_else(|| {
            detail
                .partitions
                .first()
                .map_or(1, |p| p.replicas.len() as i32)
        });
        let overrides: HashMap<String, String> = self
            .describe_topic_configs(source, timeout)
            .await?
            .into_iter()
            // A masked secret would be copied as the literal mask.
            .filter(|e| e.source == ConfigOrigin::TopicOverride && !e.sensitive)
            .map(|e| (e.name, e.value))
            .collect();
        self.create_topic(target, partitions, replication, Some(&overrides), timeout)
            .await?;
        Ok((partitions, replication, overrides.len()))
    }

    pub async fn create_topic(
        &self,
        name: &str,
        num_partitions: i32,
        replication_factor: i32,
        configs: Option<&HashMap<String, String>>,
        timeout: Option<Duration>,
    ) -> KafkaResult<()> {
        self.create_topic_with(
            name,
            num_partitions,
            replication_factor,
            configs,
            timeout,
            false,
        )
        .await
    }

    /// [`Self::create_topic`]; with `validate_only` the broker checks the
    /// request (name, partitions, replication factor, config names and
    /// values) without creating the topic. A replication factor of -1 means
    /// the broker's `default.replication.factor`.
    pub async fn create_topic_with(
        &self,
        name: &str,
        num_partitions: i32,
        replication_factor: i32,
        configs: Option<&HashMap<String, String>>,
        timeout: Option<Duration>,
        validate_only: bool,
    ) -> KafkaResult<()> {
        let mut new_topic = NewTopic::new(
            name,
            num_partitions,
            TopicReplication::Fixed(replication_factor),
        );

        if let Some(configs) = configs {
            for (key, val) in configs {
                new_topic = new_topic.set(key, val);
            }
        }

        let mut options = AdminOptions::new().validate_only(validate_only);
        if let Some(to) = timeout {
            options = options.operation_timeout(Some(to));
        }

        let results = self
            .admin
            .create_topics(&[new_topic], &options)
            .await
            .map_err(|e| KafkaError::Admin(format!("create_topics request failed: {e}")))?;

        for res in results {
            match res {
                Ok(topic_name) if validate_only => {
                    tracing::info!(topic = %topic_name, "Topic creation validated");
                }
                Ok(topic_name) => {
                    tracing::info!(topic = %topic_name, "Topic created successfully");
                }
                Err((topic_name, err)) => {
                    return Err(KafkaError::Admin(format!(
                        "Failed to create topic '{topic_name}': {err}"
                    )));
                }
            }
        }

        Ok(())
    }

    /// The partitions to act on: all of the topic's when `requested` is empty,
    /// otherwise `requested` after checking each exists. A missing topic is
    /// [`KafkaError::TopicNotFound`]; a partition outside the topic is a
    /// readable out-of-range error instead of librdkafka's `NoEnt`.
    pub fn resolve_partitions(
        &self,
        topic: &str,
        requested: &[i32],
        timeout: Option<Duration>,
    ) -> KafkaResult<Vec<i32>> {
        let detail = self.describe_topic(topic, timeout)?;
        let existing: Vec<i32> = detail.partitions.iter().map(|p| p.partition_id).collect();
        if requested.is_empty() {
            return Ok(existing);
        }
        if let Some(bad) = requested.iter().find(|p| !existing.contains(p)) {
            return Err(KafkaError::Admin(format!(
                "Partition {bad} is out of range (topic '{topic}' has {} partition(s))",
                existing.len()
            )));
        }
        Ok(requested.to_vec())
    }

    /// Deletes every record of `partitions` of `topic` (all partitions when
    /// empty) up to the current end, keeping the topic. Returns the number of
    /// partitions emptied. Compacted topics reject this (policy violation).
    pub async fn purge_topic(
        &self,
        topic: &str,
        partitions: &[i32],
        timeout: Option<Duration>,
    ) -> KafkaResult<usize> {
        let timeout = timeout.unwrap_or(self.config.request_timeout);
        let partitions = self.resolve_partitions(topic, partitions, Some(timeout))?;
        let mut list = rdkafka::TopicPartitionList::new();
        for &p in &partitions {
            list.add_partition_offset(topic, p, rdkafka::Offset::End)
                .map_err(|e| KafkaError::Admin(format!("invalid partition {p}: {e}")))?;
        }
        let options = AdminOptions::new().operation_timeout(Some(timeout));
        let result = self
            .admin
            .delete_records(&list, &options)
            .await
            .map_err(|e| KafkaError::Admin(format!("delete_records request failed: {e}")))?;
        let mut failures: Vec<(i32, String)> = Vec::new();
        for e in result.elements() {
            if let Err(err) = e.error() {
                let reason = match err.rdkafka_error_code() {
                    Some(rdkafka::types::RDKafkaErrorCode::PolicyViolation) => {
                        "compacted topics cannot be purged (cleanup.policy=compact)".to_string()
                    }
                    Some(code) => code.to_string(),
                    None => err.to_string(),
                };
                failures.push((e.partition(), reason));
            }
        }
        if let Some((_, first)) = failures.first() {
            let detail = if failures.iter().all(|(_, r)| r == first) {
                first.clone()
            } else {
                failures
                    .iter()
                    .map(|(p, r)| format!("partition {p}: {r}"))
                    .collect::<Vec<_>>()
                    .join("; ")
            };
            return Err(KafkaError::Admin(format!(
                "Could not purge '{topic}': {detail}"
            )));
        }
        Ok(partitions.len())
    }

    pub async fn delete_topic(&self, name: &str, timeout: Option<Duration>) -> KafkaResult<()> {
        let mut options = AdminOptions::new();
        if let Some(to) = timeout {
            options = options.operation_timeout(Some(to));
        }

        let results = self
            .admin
            .delete_topics(&[name], &options)
            .await
            .map_err(|e| KafkaError::Admin(format!("delete_topics request failed: {e}")))?;

        for res in results {
            match res {
                Ok(topic_name) => {
                    tracing::info!(topic = %topic_name, "Topic deleted successfully");
                }
                Err((topic_name, err)) => {
                    return Err(KafkaError::Admin(format!(
                        "Failed to delete topic '{topic_name}': {err}"
                    )));
                }
            }
        }

        Ok(())
    }
}

/// `host:port` for a broker learned from cluster metadata (IPv6 in brackets).
pub fn broker_address(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// The brokers `config`'s cluster reports in its metadata, as `host:port`.
pub fn known_brokers(config: &KafkaClientConfig) -> Result<Vec<String>, String> {
    let admin = KafkaAdminClient::new(config).map_err(|e| e.to_string())?;
    let metadata = admin
        .fetch_raw_metadata(None, Some(Duration::from_secs(10)))
        .map_err(|e| e.to_string())?;
    Ok(metadata
        .brokers()
        .iter()
        .map(|b| broker_address(b.host(), b.port() as u16))
        .collect())
}

#[cfg(test)]
mod tests {
    #[test]
    fn per_broker_and_cluster_default_dynamic_configs_stay_apart() {
        use super::{ConfigOrigin, ConfigSource};
        let per_broker = ConfigOrigin::from(ConfigSource::DynamicBroker);
        let cluster = ConfigOrigin::from(ConfigSource::DynamicDefaultBroker);
        assert_eq!(per_broker, ConfigOrigin::BrokerDynamic);
        assert_eq!(cluster, ConfigOrigin::BrokerDynamicDefault);
        assert_ne!(per_broker.label(), cluster.label());
        assert_eq!(
            ConfigOrigin::from(ConfigSource::DynamicTopic),
            ConfigOrigin::TopicOverride
        );
        assert_eq!(
            ConfigOrigin::from(ConfigSource::StaticBroker),
            ConfigOrigin::BrokerStatic
        );
    }

    #[test]
    fn cluster_id_is_none_without_a_broker() {
        // Nothing listens on port 1, so no Metadata answer carries an id.
        let config = crate::kafka::config::KafkaClientConfig::new("127.0.0.1:1");
        let admin = super::KafkaAdminClient::new(&config).unwrap();
        let timeout = std::time::Duration::from_millis(300);
        assert_eq!(admin.cluster_id(Some(timeout)), None);
    }

    #[test]
    fn member_label_shortens_the_member_id() {
        let member = super::GroupMemberListing {
            member_id: "ferrika-client-3f2a1c9e-77aa".into(),
            client_id: "ferrika-client".into(),
            host: "172.20.0.1".into(),
            assignment: Vec::new(),
        };
        assert_eq!(member.label(), "ferrika-client 3f2a1c @ 172.20.0.1");
    }

    #[test]
    fn parses_consumer_assignment() {
        // version 3, two topics: "a" -> [0, 2], "bc" -> [1], then user data.
        let mut blob = vec![0, 3, 0, 0, 0, 2];
        blob.extend([0, 1, b'a', 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2]);
        blob.extend([0, 2, b'b', b'c', 0, 0, 0, 1, 0, 0, 0, 1]);
        blob.extend([0, 0, 0, 0]);
        assert_eq!(
            super::parse_consumer_assignment(&blob),
            Some(vec![
                ("a".to_string(), 0),
                ("a".to_string(), 2),
                ("bc".to_string(), 1)
            ])
        );
        assert_eq!(super::parse_consumer_assignment(&[]), Some(Vec::new()));
        // Truncated inside the partition array.
        assert_eq!(super::parse_consumer_assignment(&blob[..12]), None);
    }

    use super::*;

    #[test]
    fn test_with_offline_replicas_marks_missing_brokers() {
        let live = |id| BrokerInfo {
            id,
            host: "h".into(),
            port: 9092,
            is_controller: false,
            partition_leaders: 1,
            replicas_hosted: 2,
            is_online: true,
            log_dir_bytes: None,
            log_dirs: Vec::new(),
        };
        let leaders = HashMap::from([(1, 1), (2, 1), (-1, 2)]);
        let replicas = HashMap::from([(1, 2), (2, 2), (3, 4)]);
        let brokers = with_offline_replicas(vec![live(2), live(1)], &leaders, &replicas);
        let ids: Vec<i32> = brokers.iter().map(|b| b.id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
        assert!(!brokers[2].is_online);
        assert_eq!(brokers[2].replicas_hosted, 4);
        assert_eq!(brokers[2].partition_leaders, 0);
    }

    #[test]
    fn test_admin_client_creation() {
        let config = KafkaClientConfig::new("localhost:9092").with_client_id("admin-test");
        let client = KafkaAdminClient::new(&config);
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.config().client_id.as_deref(), Some("admin-test"));
    }
}
