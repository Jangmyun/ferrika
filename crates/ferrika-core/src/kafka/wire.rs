//! Minimal Kafka wire-protocol client for admin APIs that librdkafka does not
//! expose (`AlterPartitionReassignments`, `DescribeQuorum`,
//! `IncrementalAlterConfigs`, and batched committed-offset lookups for many
//! groups).
//!
//! Supports `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` and `SASL_SSL` with the
//! `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` and `OAUTHBEARER` (OIDC or AWS
//! MSK IAM tokens) mechanisms (TLS through rustls, trusting the profile's CA
//! file or the OS store). GSSAPI is rejected with a configuration error
//! instead of silently connecting without it.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::config::{KafkaClientConfig, SaslMechanism, SecurityProtocol};
use crate::kafka::reassign::PartitionAssignment;
use crate::model::acl::{
    AclBinding, AclFilter, AclOperation, AclPermissionType, AclResourcePatternType, AclResourceType,
};
use crate::model::quota::{quota_entity_label, QuotaEntity};

const API_OFFSET_FETCH: i16 = 9;
const API_FIND_COORDINATOR: i16 = 10;
const API_METADATA: i16 = 3;
const API_SASL_HANDSHAKE: i16 = 17;
const API_SASL_AUTHENTICATE: i16 = 36;
const API_ALTER_PARTITION_REASSIGNMENTS: i16 = 45;
const API_INCREMENTAL_ALTER_CONFIGS: i16 = 44;
const API_DESCRIBE_QUORUM: i16 = 55;
const API_DESCRIBE_LOG_DIRS: i16 = 35;
const API_DESCRIBE_ACLS: i16 = 29;
const API_ELECT_LEADERS: i16 = 43;
const API_CREATE_ACLS: i16 = 30;
const API_DELETE_ACLS: i16 = 31;
const API_OFFSET_DELETE: i16 = 47;
const API_LIST_PARTITION_REASSIGNMENTS: i16 = 46;
const API_DESCRIBE_CLIENT_QUOTAS: i16 = 48;
const API_ALTER_CLIENT_QUOTAS: i16 = 49;
const API_DESCRIBE_USER_SCRAM_CREDENTIALS: i16 = 50;
const API_ALTER_USER_SCRAM_CREDENTIALS: i16 = 51;

/// `ResourceType` of a topic in config requests.
const RESOURCE_TOPIC: u8 = 2;
const RESOURCE_BROKER: u8 = 4;

/// The KRaft metadata log, whose quorum leader is the active controller.
const METADATA_TOPIC: &str = "__cluster_metadata";

/// Upper bound for a single response frame; guards against garbage length prefixes.
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// Per-partition outcome of an `AlterPartitionReassignments` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignmentResult {
    pub topic: String,
    pub partition: i32,
    /// `None` when the broker accepted the reassignment.
    pub error: Option<String>,
}

/// Submits the given replica assignments to the cluster controller.
///
/// Returns one result per partition. A request-level failure (authorization,
/// unsupported version, connection loss) is returned as an error.
pub fn alter_partition_reassignments(
    config: &KafkaClientConfig,
    assignments: &[PartitionAssignment],
) -> KafkaResult<Vec<PartitionReassignmentResult>> {
    let timeout_ms = i32::try_from(config.request_timeout.as_millis()).unwrap_or(i32::MAX);
    let body = encode_alter_partition_reassignments(assignments, timeout_ms);

    let mut conn = connect_to_controller(config)?;
    let response = conn.request(API_ALTER_PARTITION_REASSIGNMENTS, 0, true, &body)?;
    let mut results = decode_alter_partition_reassignments(&response)?;
    add_missing_reassignment_results(assignments, &mut results);
    Ok(results)
}

/// A partition the broker's response does not mention was not confirmed:
/// callers read "no error" as "reassignment started", so it gets an error.
fn add_missing_reassignment_results(
    assignments: &[PartitionAssignment],
    results: &mut Vec<PartitionReassignmentResult>,
) {
    for a in assignments {
        if !results
            .iter()
            .any(|r| r.topic == a.topic && r.partition == a.partition)
        {
            results.push(PartitionReassignmentResult {
                topic: a.topic.clone(),
                partition: a.partition,
                error: Some("the broker's response had no result for this partition".to_string()),
            });
        }
    }
}

/// Returns the node id of the active controller.
///
/// On KRaft clusters this is the leader of the metadata quorum, from
/// DescribeQuorum (brokers forward it to the controller). Metadata's
/// `controller_id` is not used there because KRaft brokers report a random
/// live broker in it. Clusters without DescribeQuorum (ZooKeeper mode) fall
/// back to the Metadata v1 controller.
pub fn active_controller_id(config: &KafkaClientConfig) -> KafkaResult<Option<i32>> {
    check_supported_security(config)?;
    let mut conn = connect_bootstrap(config)?;
    match conn.request(API_DESCRIBE_QUORUM, 0, true, &encode_describe_quorum()) {
        Ok(response) => decode_describe_quorum_leader(&response),
        // Brokers close the connection on an unsupported API; retry with Metadata.
        Err(KafkaError::Io(_)) => {
            let mut conn = connect_bootstrap(config)?;
            let metadata = conn.request(API_METADATA, 1, false, &0i32.to_be_bytes())?;
            Ok(decode_metadata_controller(&metadata)?.map(|(id, _)| id))
        }
        Err(e) => Err(e),
    }
}

/// The KRaft metadata quorum: leader, epoch, and each voter's and
/// observer's position in the metadata log. `None` on clusters without
/// DescribeQuorum (ZooKeeper mode).
pub fn describe_quorum(
    config: &KafkaClientConfig,
) -> KafkaResult<Option<crate::model::cluster::QuorumInfo>> {
    check_supported_security(config)?;
    let mut conn = connect_bootstrap(config)?;
    match conn.request(API_DESCRIBE_QUORUM, 0, true, &encode_describe_quorum()) {
        Ok(response) => decode_describe_quorum(&response),
        // Brokers close the connection on an unsupported API.
        Err(KafkaError::Io(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Partition moves in progress (ListPartitionReassignments v0, Kafka 2.4+),
/// asked of the controller.
pub fn list_partition_reassignments(
    config: &KafkaClientConfig,
) -> KafkaResult<Vec<crate::model::cluster::OngoingReassignment>> {
    check_supported_security(config)?;
    let mut conn = connect_to_controller(config)?;
    let mut w = Writer::default();
    w.i32(10_000); // timeout_ms
    w.uvarint(0); // topics: null = every reassignment
    w.uvarint(0); // tagged fields
    let response = conn.request(API_LIST_PARTITION_REASSIGNMENTS, 0, true, &w.0)?;
    decode_list_partition_reassignments(&response)
}

fn decode_list_partition_reassignments(
    body: &[u8],
) -> KafkaResult<Vec<crate::model::cluster::OngoingReassignment>> {
    fn ids(r: &mut Reader<'_>) -> KafkaResult<Vec<i32>> {
        (0..r.compact_array_len()?).map(|_| r.i32()).collect()
    }
    let mut r = Reader::new(body);
    let _throttle_ms = r.i32()?;
    let code = r.i16()?;
    let message = r.compact_nullable_string()?;
    if code != 0 {
        return Err(error_for_code(code, message.as_deref()));
    }
    let mut out = Vec::new();
    for _ in 0..r.compact_array_len()? {
        let topic = r.compact_nullable_string()?.unwrap_or_default();
        for _ in 0..r.compact_array_len()? {
            let partition = r.i32()?;
            let replicas = ids(&mut r)?;
            let adding = ids(&mut r)?;
            let removing = ids(&mut r)?;
            r.skip_tagged_fields()?;
            out.push(crate::model::cluster::OngoingReassignment {
                topic: topic.clone(),
                partition,
                replicas,
                adding,
                removing,
            });
        }
        r.skip_tagged_fields()?;
    }
    out.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
    Ok(out)
}

/// A user's SCRAM credentials: `(mechanism, iterations)` per credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramUser {
    pub name: String,
    pub credentials: Vec<(crate::kafka::scram::ScramHash, i32)>,
}

/// Every user with SCRAM credentials (DescribeUserScramCredentials v0, Kafka 2.7+).
pub fn describe_scram_users(config: &KafkaClientConfig) -> KafkaResult<Vec<ScramUser>> {
    check_supported_security(config)?;
    let mut conn = connect_bootstrap(config)?;
    let mut w = Writer::default();
    w.uvarint(0); // users: null = all
    w.uvarint(0); // tagged fields
    let response = conn.request(API_DESCRIBE_USER_SCRAM_CREDENTIALS, 0, true, &w.0)?;
    decode_describe_scram_users(&response)
}

fn decode_describe_scram_users(body: &[u8]) -> KafkaResult<Vec<ScramUser>> {
    let mut r = Reader::new(body);
    let _throttle_ms = r.i32()?;
    let code = r.i16()?;
    let message = r.compact_nullable_string()?;
    if code != 0 {
        return Err(error_for_code(code, message.as_deref()));
    }
    let mut users = Vec::new();
    for _ in 0..r.compact_array_len()? {
        let name = r.compact_nullable_string()?.unwrap_or_default();
        let _code = r.i16()?;
        let _message = r.compact_nullable_string()?;
        let mut credentials = Vec::new();
        for _ in 0..r.compact_array_len()? {
            let mechanism = r.i8()?;
            let iterations = r.i32()?;
            r.skip_tagged_fields()?;
            if let Some(hash) = crate::kafka::scram::ScramHash::from_id(mechanism) {
                credentials.push((hash, iterations));
            }
        }
        r.skip_tagged_fields()?;
        users.push(ScramUser { name, credentials });
    }
    users.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(users)
}

/// Creates or replaces (`Some(password)`) or deletes (`None`) one SCRAM
/// credential of `user` (AlterUserScramCredentials v0). The password is
/// salted here; only the salted form is sent.
pub fn alter_scram_user(
    config: &KafkaClientConfig,
    user: &str,
    hash: crate::kafka::scram::ScramHash,
    password: Option<&str>,
    iterations: u32,
) -> KafkaResult<()> {
    check_supported_security(config)?;
    let mut w = Writer::default();
    match password {
        None => {
            w.compact_len(1);
            w.compact_string(user);
            w.i8(hash.id());
            w.uvarint(0);
            w.compact_len(0); // upsertions
        }
        Some(password) => {
            let wire_iterations = i32::try_from(iterations).map_err(|_| {
                KafkaError::config(format!(
                    "SCRAM iteration count {iterations} exceeds i32::MAX"
                ))
            })?;
            let (salt, salted) = hash.salt_password(password, iterations)?;
            w.compact_len(0); // deletions
            w.compact_len(1);
            w.compact_string(user);
            w.i8(hash.id());
            w.i32(wire_iterations);
            w.compact_len(salt.len());
            w.0.extend_from_slice(&salt);
            w.compact_len(salted.len());
            w.0.extend_from_slice(&salted);
            w.uvarint(0);
        }
    }
    w.uvarint(0); // tagged fields
                  // ZooKeeper-mode brokers only accept this on the controller; KRaft forwards.
    let mut conn = connect_to_controller(config)?;
    let response = conn.request(API_ALTER_USER_SCRAM_CREDENTIALS, 0, true, &w.0)?;
    let mut r = Reader::new(&response);
    let _throttle_ms = r.i32()?;
    let mut results = 0usize;
    for _ in 0..r.compact_array_len()? {
        let _user = r.compact_nullable_string()?;
        let code = r.i16()?;
        let message = r.compact_nullable_string()?;
        r.skip_tagged_fields()?;
        if code != 0 {
            return Err(error_for_code(code, message.as_deref()));
        }
        results += 1;
    }
    require_result("AlterUserScramCredentials", results)
}

/// One quota value of one entity.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuota {
    pub entity: QuotaEntity,
    pub key: String,
    pub value: f64,
}

/// Every client quota (DescribeClientQuotas v0, Kafka 2.6+), one entry per
/// entity and key, sorted.
pub fn describe_client_quotas(config: &KafkaClientConfig) -> KafkaResult<Vec<ClientQuota>> {
    check_supported_security(config)?;
    let mut conn = connect_bootstrap(config)?;
    let mut w = Writer::default();
    w.i32(0); // no filter components: every quota
    w.i8(0); // strict = false
    let response = conn.request(API_DESCRIBE_CLIENT_QUOTAS, 0, false, &w.0)?;
    decode_describe_client_quotas(&response)
}

fn read_quota_entity(r: &mut Reader<'_>) -> KafkaResult<QuotaEntity> {
    let mut entity = Vec::new();
    for _ in 0..r.array_len()? {
        let kind = r.nullable_string()?.unwrap_or_default();
        let name = r.nullable_string()?;
        entity.push((kind, name));
    }
    Ok(crate::model::quota::canonical_quota_entity(entity))
}

fn decode_describe_client_quotas(body: &[u8]) -> KafkaResult<Vec<ClientQuota>> {
    let mut r = Reader::new(body);
    let _throttle_ms = r.i32()?;
    let code = r.i16()?;
    let message = r.nullable_string()?;
    if code != 0 {
        return Err(error_for_code(code, message.as_deref()));
    }
    let mut out = Vec::new();
    // A null entries array (-1) means none.
    let count = r.i32()?;
    for _ in 0..count.max(0) {
        let entity = read_quota_entity(&mut r)?;
        for _ in 0..r.array_len()? {
            let key = r.nullable_string()?.unwrap_or_default();
            let value = f64::from_bits(r.i64()? as u64);
            out.push(ClientQuota {
                entity: entity.clone(),
                key,
                value,
            });
        }
    }
    out.sort_by(|a, b| {
        quota_entity_label(&a.entity)
            .cmp(&quota_entity_label(&b.entity))
            .then_with(|| a.key.cmp(&b.key))
    });
    Ok(out)
}

/// Sets (`Some`) or removes (`None`) one quota of `entity` (AlterClientQuotas v0).
pub fn alter_client_quota(
    config: &KafkaClientConfig,
    entity: &QuotaEntity,
    key: &str,
    value: Option<f64>,
) -> KafkaResult<()> {
    check_supported_security(config)?;
    let mut conn = connect_bootstrap(config)?;
    let response = conn.request(
        API_ALTER_CLIENT_QUOTAS,
        0,
        false,
        &encode_alter_client_quota(entity, key, value),
    )?;
    decode_alter_client_quotas(&response)
}

fn encode_alter_client_quota(entity: &QuotaEntity, key: &str, value: Option<f64>) -> Vec<u8> {
    let mut w = Writer::default();
    w.i32(1); // one entry
    w.i32(entity.len() as i32);
    for (kind, name) in entity {
        w.string(kind);
        w.nullable_string(name.as_deref());
    }
    w.i32(1); // one op
    w.string(key);
    w.0.extend_from_slice(&value.unwrap_or(0.0).to_bits().to_be_bytes());
    w.i8(i8::from(value.is_none())); // remove
    w.i8(0); // validate_only
    w.0
}

fn decode_alter_client_quotas(body: &[u8]) -> KafkaResult<()> {
    let mut r = Reader::new(body);
    let _throttle_ms = r.i32()?;
    let mut results = 0usize;
    for _ in 0..r.array_len()? {
        let code = r.i16()?;
        let message = r.nullable_string()?;
        let _entity = read_quota_entity(&mut r)?;
        if code != 0 {
            return Err(error_for_code(code, message.as_deref()));
        }
        results += 1;
    }
    require_result("AlterClientQuotas", results)
}

/// One change in an `IncrementalAlterConfigs` request.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigChange {
    /// Sets the key to the value as a topic override.
    Set(String, String),
    /// Removes the override, so the broker default applies again.
    Delete(String),
}

/// "retention.ms (set), cleanup.policy (reset)" for the audit log: keys
/// only, values may be secrets.
pub fn change_keys(changes: &[ConfigChange]) -> String {
    changes
        .iter()
        .map(|c| match c {
            ConfigChange::Set(k, _) => format!("{k} (set)"),
            ConfigChange::Delete(k) => format!("{k} (reset)"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Applies `changes` to the overrides of `topic`, leaving every other override
/// as it is. (The legacy `AlterConfigs` API that librdkafka exposes replaces
/// the whole set, resetting anything not listed.)
///
/// With `validate_only` the broker checks the changes without applying them.
pub fn incremental_alter_topic_config(
    config: &KafkaClientConfig,
    topic: &str,
    changes: &[ConfigChange],
    validate_only: bool,
) -> KafkaResult<()> {
    check_supported_security(config)?;
    let mut conn = connect_bootstrap(config)?;
    let body = encode_incremental_alter_configs(RESOURCE_TOPIC, topic, changes, validate_only);
    let response = conn.request(API_INCREMENTAL_ALTER_CONFIGS, 1, true, &body)?;
    decode_incremental_alter_configs(&response)
}

/// Changes only the listed dynamic configs of broker `broker_id`, sending
/// the request to that broker at `broker_addr` (`host:port`), as Kafka
/// requires for per-broker configs. Static (server.properties-only) keys are
/// rejected by the broker.
///
/// With `validate_only` the broker checks the changes without applying them.
pub fn incremental_alter_broker_config(
    config: &KafkaClientConfig,
    broker_id: i32,
    broker_addr: &str,
    changes: &[ConfigChange],
    validate_only: bool,
) -> KafkaResult<()> {
    check_supported_security(config)?;
    let mut conn = WireConnection::connect(broker_addr, config)?;
    let body = encode_incremental_alter_configs(
        RESOURCE_BROKER,
        &broker_id.to_string(),
        changes,
        validate_only,
    );
    let response = conn.request(API_INCREMENTAL_ALTER_CONFIGS, 1, true, &body)?;
    decode_incremental_alter_configs(&response)
}

/// Changes the cluster-wide dynamic broker defaults
/// (`DYNAMIC_DEFAULT_BROKER_CONFIG`, the broker resource named `""`), which
/// every broker uses unless it has its own dynamic value. Any broker accepts
/// this request.
///
/// With `validate_only` the broker checks the changes without applying them.
pub fn incremental_alter_cluster_broker_default(
    config: &KafkaClientConfig,
    changes: &[ConfigChange],
    validate_only: bool,
) -> KafkaResult<()> {
    check_supported_security(config)?;
    let mut conn = connect_bootstrap(config)?;
    let body = encode_incremental_alter_configs(
        RESOURCE_BROKER,
        CLUSTER_DEFAULT_BROKER,
        changes,
        validate_only,
    );
    let response = conn.request(API_INCREMENTAL_ALTER_CONFIGS, 1, true, &body)?;
    decode_incremental_alter_configs(&response)
}

/// The broker resource name of the cluster-wide dynamic defaults.
const CLUSTER_DEFAULT_BROKER: &str = "";

/// IncrementalAlterConfigs v1 for one resource (`RESOURCE_TOPIC` or `RESOURCE_BROKER`).
fn encode_incremental_alter_configs(
    resource_type: u8,
    resource_name: &str,
    changes: &[ConfigChange],
    validate_only: bool,
) -> Vec<u8> {
    let mut w = Writer::default();
    w.compact_len(1);
    w.0.push(resource_type);
    w.compact_string(resource_name);
    w.compact_len(changes.len());
    for change in changes {
        match change {
            ConfigChange::Set(name, value) => {
                w.compact_string(name);
                w.0.push(0); // SET
                w.compact_string(value);
            }
            ConfigChange::Delete(name) => {
                w.compact_string(name);
                w.0.push(1); // DELETE
                w.uvarint(0); // null value
            }
        }
        w.uvarint(0); // config tagged fields
    }
    w.uvarint(0); // resource tagged fields
    w.0.push(u8::from(validate_only));
    w.uvarint(0); // request tagged fields
    w.0
}

fn decode_incremental_alter_configs(body: &[u8]) -> KafkaResult<()> {
    let mut r = Reader::new(body);
    let _throttle_ms = r.i32()?;
    let mut results = 0usize;
    for _ in 0..r.compact_array_len()? {
        let error_code = r.i16()?;
        let message = r.compact_nullable_string()?;
        let _resource_type = r.take(1)?;
        let _resource_name = r.compact_nullable_string()?;
        r.skip_tagged_fields()?;
        if error_code != 0 {
            return Err(error_for_code(error_code, message.as_deref()));
        }
        results += 1;
    }
    require_result("IncrementalAlterConfigs", results)
}

/// Each of these requests carries exactly one entry (one resource, user or
/// quota), so a response with no result for it must not read as success: the
/// broker did not say the change was made.
fn require_result(api: &str, results: usize) -> KafkaResult<()> {
    if results == 0 {
        return Err(KafkaError::admin(format!(
            "{api}: the broker's response had no result for the request, so the change is not confirmed"
        )));
    }
    Ok(())
}

/// Committed offsets per group: `(topic, partition) -> offset`. Partitions
/// without a committed offset are omitted.
pub type GroupOffsets = HashMap<String, HashMap<(String, i32), i64>>;

/// Fetches the committed offsets of many consumer groups with one
/// FindCoordinator v4 request and one OffsetFetch v8 request per coordinator
/// (both batch groups; Kafka 3.0+).
///
/// librdkafka can only read the offsets of the group a consumer belongs to,
/// which costs a client (threads and connections) per group. Groups whose
/// coordinator or offsets could not be resolved are left out of the result;
/// a request-level failure is an error so callers can fall back.
pub fn fetch_group_offsets(
    config: &KafkaClientConfig,
    groups: &[String],
) -> KafkaResult<GroupOffsets> {
    check_supported_security(config)?;
    let mut offsets = GroupOffsets::new();
    if groups.is_empty() {
        return Ok(offsets);
    }
    let mut bootstrap = connect_bootstrap(config)?;
    let response = bootstrap.request(
        API_FIND_COORDINATOR,
        4,
        true,
        &encode_find_coordinators(groups),
    )?;

    let mut by_coordinator: HashMap<String, Vec<String>> = HashMap::new();
    for (group, coordinator) in decode_find_coordinators(&response)? {
        match coordinator {
            Ok(addr) => by_coordinator.entry(addr).or_default().push(group),
            Err(reason) => tracing::debug!(group, reason, "No coordinator for group"),
        }
    }

    // Coordinators are often the bootstrap broker itself; reuse that connection.
    let mut connections = HashMap::new();
    connections.insert(bootstrap.addr.clone(), bootstrap);
    for (addr, groups) in by_coordinator {
        let conn = match connections.entry(addr) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                let conn = WireConnection::connect(e.key(), config)?;
                e.insert(conn)
            }
        };
        let response = conn.request(API_OFFSET_FETCH, 8, true, &encode_offset_fetch(&groups))?;
        offsets.extend(decode_offset_fetch(&response)?);
    }
    Ok(offsets)
}

/// Deletes `group`'s committed offsets for `topic` (OffsetDelete v0, Kafka
/// 2.4+), for `partitions` or, when empty, every partition it committed.
/// The group must not be consuming the topic. Returns the partitions whose
/// offsets were deleted.
pub fn delete_group_offsets(
    config: &KafkaClientConfig,
    group: &str,
    topic: &str,
    partitions: &[i32],
) -> KafkaResult<Vec<i32>> {
    check_supported_security(config)?;
    let mut partitions = partitions.to_vec();
    if partitions.is_empty() {
        let committed = fetch_group_offsets(config, &[group.to_string()])?;
        partitions = committed
            .get(group)
            .map(|offsets| {
                offsets
                    .keys()
                    .filter(|(t, _)| t == topic)
                    .map(|(_, p)| *p)
                    .collect()
            })
            .unwrap_or_default();
        partitions.sort_unstable();
        if partitions.is_empty() {
            return Err(KafkaError::Offset(format!(
                "Group '{group}' has no committed offsets for '{topic}'"
            )));
        }
    }

    let mut bootstrap = connect_bootstrap(config)?;
    let response = bootstrap.request(
        API_FIND_COORDINATOR,
        4,
        true,
        &encode_find_coordinators(&[group.to_string()]),
    )?;
    let coordinator = decode_find_coordinators(&response)?
        .into_iter()
        .next()
        .map(|(_, c)| c)
        .ok_or_else(|| KafkaError::admin(format!("No coordinator found for group '{group}'")))?
        .map_err(|reason| KafkaError::admin(format!("No coordinator for '{group}': {reason}")))?;
    let mut conn = if coordinator == bootstrap.addr {
        bootstrap
    } else {
        WireConnection::connect(&coordinator, config)?
    };
    let response = conn.request(
        API_OFFSET_DELETE,
        0,
        false,
        &encode_offset_delete(group, topic, &partitions),
    )?;
    decode_offset_delete(&response, group, topic)
}

fn encode_offset_delete(group: &str, topic: &str, partitions: &[i32]) -> Vec<u8> {
    let mut w = Writer::default();
    w.string(group);
    w.i32(1);
    w.string(topic);
    w.i32(partitions.len() as i32);
    for p in partitions {
        w.i32(*p);
    }
    w.0
}

fn decode_offset_delete(body: &[u8], group: &str, topic: &str) -> KafkaResult<Vec<i32>> {
    let mut r = Reader::new(body);
    let code = r.i16()?;
    let _throttle_ms = r.i32()?;
    if code != 0 {
        return Err(offset_delete_error(code, group, topic));
    }
    let mut deleted = Vec::new();
    let mut failures = Vec::new();
    for _ in 0..r.array_len()? {
        let _name = r.nullable_string()?;
        for _ in 0..r.array_len()? {
            let partition = r.i32()?;
            match r.i16()? {
                0 => deleted.push(partition),
                code => failures.push((partition, code)),
            }
        }
    }
    if let Some(&(_, code)) = failures.first() {
        let list: Vec<String> = failures.iter().map(|(p, _)| p.to_string()).collect();
        let reason = match offset_delete_error(code, group, topic) {
            KafkaError::Offset(m) | KafkaError::Acl(m) => m,
            other => other.to_string(),
        };
        return Err(KafkaError::Offset(format!(
            "Partition(s) {} not deleted: {reason}",
            list.join(", ")
        )));
    }
    Ok(deleted)
}

fn offset_delete_error(code: i16, group: &str, topic: &str) -> KafkaError {
    let reason = describe_error(code, None);
    match code {
        29 | 30 => KafkaError::acl(reason),
        86 => KafkaError::Offset(format!(
            "'{group}' is still consuming '{topic}'; stop its consumers first ({reason})"
        )),
        _ => KafkaError::Offset(reason),
    }
}

/// Bytes each partition replica occupies on one broker, from DescribeLogDirs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BrokerLogDirs {
    /// `(topic, partition) -> bytes`, summed over the broker's log dirs.
    pub partitions: HashMap<(String, i32), u64>,
    /// Each log dir with its size or error.
    pub dirs: Vec<crate::model::cluster::LogDirSummary>,
}

impl BrokerLogDirs {
    /// Total bytes stored on the broker.
    pub fn total_bytes(&self) -> u64 {
        self.partitions
            .values()
            .fold(0u64, |a, b| a.saturating_add(*b))
    }
}

/// Asks every broker in `brokers` (`(id, "host:port")`) for the size of the
/// replicas it stores (DescribeLogDirs v2, Kafka 2.6+). Unreachable brokers
/// are left out; the call fails only if no broker answered.
pub fn describe_log_dirs(
    config: &KafkaClientConfig,
    brokers: &[(i32, String)],
) -> KafkaResult<HashMap<i32, BrokerLogDirs>> {
    check_supported_security(config)?;
    let mut result = HashMap::new();
    let mut last_error = None;
    for (id, addr) in brokers {
        let answer = WireConnection::connect(addr, config).and_then(|mut conn| {
            let body = conn.request(API_DESCRIBE_LOG_DIRS, 2, true, &encode_describe_log_dirs())?;
            decode_describe_log_dirs(&body)
        });
        match answer {
            Ok(dirs) => {
                result.insert(*id, dirs);
            }
            Err(e) => {
                tracing::debug!(broker = id, %e, "DescribeLogDirs failed");
                last_error = Some(e);
            }
        }
    }
    match last_error {
        Some(e) if result.is_empty() => Err(e),
        _ => Ok(result),
    }
}

/// DescribeLogDirs v2 for every topic (null topic list).
fn encode_describe_log_dirs() -> Vec<u8> {
    let mut w = Writer::default();
    w.uvarint(0); // null topics: describe all
    w.uvarint(0); // tagged fields
    w.0
}

fn decode_describe_log_dirs(body: &[u8]) -> KafkaResult<BrokerLogDirs> {
    let mut r = Reader::new(body);
    let _throttle_ms = r.i32()?;
    let mut dirs = BrokerLogDirs::default();
    for _ in 0..r.compact_array_len()? {
        let error_code = r.i16()?;
        let log_dir = r.compact_nullable_string()?.unwrap_or_default();
        let mut dir_bytes = 0u64;
        let error = (error_code != 0).then(|| describe_error(error_code, None));
        if let Some(reason) = &error {
            tracing::debug!(log_dir, reason, "Log dir error");
        }
        for _ in 0..r.compact_array_len()? {
            let topic = r.compact_nullable_string()?.unwrap_or_default();
            for _ in 0..r.compact_array_len()? {
                let partition = r.i32()?;
                let size = r.i64()?;
                let _offset_lag = r.i64()?;
                let is_future = r.take(1)?[0] != 0;
                r.skip_tagged_fields()?;
                // A future replica is a copy being moved between dirs; count the current one.
                if !is_future && size > 0 {
                    // Saturating: a corrupt size must not overflow (a panic in debug builds).
                    dir_bytes = dir_bytes.saturating_add(size as u64);
                    let total = dirs
                        .partitions
                        .entry((topic.clone(), partition))
                        .or_default();
                    *total = total.saturating_add(size as u64);
                }
            }
            r.skip_tagged_fields()?;
        }
        r.skip_tagged_fields()?;
        dirs.dirs.push(crate::model::cluster::LogDirSummary {
            path: log_dir,
            bytes: dir_bytes,
            error,
        });
    }
    Ok(dirs)
}

// ---------------------------------------------------------------------------
// ACLs: DescribeAcls / CreateAcls / DeleteAcls v1 (Kafka 2.0+, non-flexible).
// ---------------------------------------------------------------------------

fn acl_resource_code(t: &AclResourceType) -> i8 {
    match t {
        AclResourceType::Topic => 2,
        AclResourceType::Group => 3,
        AclResourceType::Cluster => 4,
        AclResourceType::TransactionalId => 5,
        AclResourceType::DelegationToken => 6,
    }
}

fn acl_resource_from(code: i8) -> Option<AclResourceType> {
    Some(match code {
        2 => AclResourceType::Topic,
        3 => AclResourceType::Group,
        4 => AclResourceType::Cluster,
        5 => AclResourceType::TransactionalId,
        6 => AclResourceType::DelegationToken,
        _ => return None,
    })
}

fn acl_pattern_code(p: &AclResourcePatternType) -> i8 {
    match p {
        AclResourcePatternType::Match => 2,
        AclResourcePatternType::Literal => 3,
        AclResourcePatternType::Prefixed => 4,
    }
}

fn acl_pattern_from(code: i8) -> Option<AclResourcePatternType> {
    Some(match code {
        2 => AclResourcePatternType::Match,
        3 => AclResourcePatternType::Literal,
        4 => AclResourcePatternType::Prefixed,
        _ => return None,
    })
}

const ACL_OPERATIONS: [(i8, AclOperation); 11] = [
    (2, AclOperation::All),
    (3, AclOperation::Read),
    (4, AclOperation::Write),
    (5, AclOperation::Create),
    (6, AclOperation::Delete),
    (7, AclOperation::Alter),
    (8, AclOperation::Describe),
    (9, AclOperation::ClusterAction),
    (10, AclOperation::DescribeConfigs),
    (11, AclOperation::AlterConfigs),
    (12, AclOperation::IdempotentWrite),
];

fn acl_operation_code(op: &AclOperation) -> i8 {
    ACL_OPERATIONS
        .iter()
        .find(|(_, o)| o == op)
        .map_or(1, |(c, _)| *c)
}

fn acl_operation_from(code: i8) -> Option<AclOperation> {
    ACL_OPERATIONS
        .iter()
        .find(|(c, _)| *c == code)
        .map(|(_, o)| *o)
}

fn acl_permission_code(p: &AclPermissionType) -> i8 {
    match p {
        AclPermissionType::Deny => 2,
        AclPermissionType::Allow => 3,
    }
}

fn acl_permission_from(code: i8) -> Option<AclPermissionType> {
    match code {
        2 => Some(AclPermissionType::Deny),
        3 => Some(AclPermissionType::Allow),
        _ => None,
    }
}

/// Principal as Kafka stores it: `User:alice` for a bare `alice`.
fn acl_principal(p: &str) -> String {
    if p.contains(':') {
        p.to_string()
    } else {
        format!("User:{p}")
    }
}

/// Writes an ACL binding filter. `default_pattern` applies when the filter
/// names a resource without a pattern type: MATCH (2) finds every binding that
/// affects the resource, ANY (1) the bindings for exactly that name, literal
/// or prefixed.
fn write_acl_filter(w: &mut Writer, filter: &AclFilter, default_pattern: i8) {
    w.i8(filter.resource_type.as_ref().map_or(1, acl_resource_code));
    w.nullable_string(filter.resource_name.as_deref());
    let pattern = match (&filter.pattern_type, &filter.resource_name) {
        (Some(p), _) => acl_pattern_code(p),
        (None, Some(_)) => default_pattern,
        (None, None) => 1,
    };
    w.i8(pattern);
    w.nullable_string(filter.principal.as_deref().map(acl_principal).as_deref());
    w.nullable_string(filter.host.as_deref());
    w.i8(filter.operation.as_ref().map_or(1, acl_operation_code));
    w.i8(filter
        .permission_type
        .as_ref()
        .map_or(1, acl_permission_code));
}

/// Reads principal, host, operation and permission of one ACL entry.
fn read_acl_entry(
    r: &mut Reader<'_>,
    resource: Option<(AclResourceType, String, AclResourcePatternType)>,
) -> KafkaResult<Option<AclBinding>> {
    let principal = r.nullable_string()?.unwrap_or_default();
    let host = r.nullable_string()?.unwrap_or_default();
    let operation = acl_operation_from(r.i8()?);
    let permission = acl_permission_from(r.i8()?);
    Ok(match (resource, operation, permission) {
        (Some((resource_type, resource_name, pattern_type)), Some(operation), Some(permission)) => {
            Some(AclBinding {
                resource_type,
                resource_name,
                pattern_type,
                principal,
                host,
                operation,
                permission_type: permission,
            })
        }
        // Resource or operation kinds this client does not model (e.g. User, CreateTokens).
        _ => None,
    })
}

/// Lists the cluster's ACL bindings matching `filter` (DescribeAcls v1). A
/// resource name without a pattern type matches every binding that applies
/// to it (wildcard and prefixed ones too), or with `exact` only the bindings
/// for exactly that name, which is what `delete_acls` removes.
pub fn describe_acls(
    config: &KafkaClientConfig,
    filter: &AclFilter,
    exact: bool,
) -> KafkaResult<Vec<AclBinding>> {
    check_supported_security(config)?;
    let mut conn = connect_bootstrap(config)?;
    let mut w = Writer::default();
    write_acl_filter(&mut w, filter, if exact { 1 } else { 2 });
    let body = conn.request(API_DESCRIBE_ACLS, 1, false, &w.0)?;
    decode_describe_acls(&body)
}

fn decode_describe_acls(body: &[u8]) -> KafkaResult<Vec<AclBinding>> {
    let mut r = Reader::new(body);
    let _throttle_ms = r.i32()?;
    let error_code = r.i16()?;
    let message = r.nullable_string()?;
    if error_code != 0 {
        return Err(error_for_code(error_code, message.as_deref()));
    }
    let mut bindings = Vec::new();
    for _ in 0..r.array_len()? {
        let resource_type = acl_resource_from(r.i8()?);
        let name = r.nullable_string()?.unwrap_or_default();
        let pattern = acl_pattern_from(r.i8()?);
        let resource = match (resource_type, pattern) {
            (Some(t), Some(p)) => Some((t, name, p)),
            _ => None,
        };
        for _ in 0..r.array_len()? {
            bindings.extend(read_acl_entry(&mut r, resource.clone())?);
        }
    }
    Ok(bindings)
}

/// Creates ACL bindings (CreateAcls v1). The broker creates the accepted
/// bindings even when others are rejected; the error then says how many were
/// created and lists every rejection by binding index.
pub fn create_acls(config: &KafkaClientConfig, bindings: &[AclBinding]) -> KafkaResult<()> {
    check_supported_security(config)?;
    let mut conn = connect_bootstrap(config)?;
    let mut w = Writer::default();
    w.i32(bindings.len() as i32);
    for b in bindings {
        w.i8(acl_resource_code(&b.resource_type));
        w.string(&b.resource_name);
        w.i8(acl_pattern_code(&b.pattern_type));
        w.string(&acl_principal(&b.principal));
        w.string(&b.host);
        w.i8(acl_operation_code(&b.operation));
        w.i8(acl_permission_code(&b.permission_type));
    }
    let body = conn.request(API_CREATE_ACLS, 1, false, &w.0)?;
    let mut r = Reader::new(&body);
    let _throttle_ms = r.i32()?;
    let mut results = Vec::new();
    for _ in 0..r.array_len()? {
        let code = r.i16()?;
        let message = r.nullable_string()?;
        results.push((code, message));
    }
    summarize_create_acls(&results, bindings.len())
}

/// Turns the per-binding CreateAcls results into one outcome. Any rejection
/// reports how many bindings were created and every rejected one, since the
/// accepted bindings exist on the broker regardless.
fn summarize_create_acls(results: &[(i16, Option<String>)], requested: usize) -> KafkaResult<()> {
    let rejected: Vec<(usize, i16, &Option<String>)> = results
        .iter()
        .enumerate()
        .filter(|(_, (code, _))| *code != 0)
        .map(|(i, (code, msg))| (i, *code, msg))
        .collect();
    if rejected.is_empty() {
        if results.len() < requested {
            return Err(KafkaError::admin(format!(
                "CreateAcls: the broker confirmed {} of {requested} bindings",
                results.len()
            )));
        }
        return Ok(());
    }
    if requested == 1 {
        let (_, code, msg) = rejected[0];
        return Err(error_for_code(code, msg.as_deref()));
    }
    let created = results.len() - rejected.len();
    let detail = rejected
        .iter()
        .map(|(i, code, msg)| format!("#{} {}", i + 1, describe_error(*code, msg.as_deref())))
        .collect::<Vec<_>>()
        .join("; ");
    let reason = format!(
        "CreateAcls: {created} of {requested} bindings were created, {} rejected: {detail}",
        rejected.len()
    );
    if rejected.iter().all(|(_, code, _)| matches!(code, 29..=31)) {
        Err(KafkaError::acl(reason))
    } else {
        Err(KafkaError::admin(reason))
    }
}

/// Deletes the ACL bindings matching `filter` (DeleteAcls v1) and returns
/// them. A resource name without a pattern type deletes the bindings for
/// exactly that name (literal or prefixed), not wildcard or broader ones.
pub fn delete_acls(config: &KafkaClientConfig, filter: &AclFilter) -> KafkaResult<Vec<AclBinding>> {
    check_supported_security(config)?;
    let mut conn = connect_bootstrap(config)?;
    let mut w = Writer::default();
    w.i32(1);
    write_acl_filter(&mut w, filter, 1);
    let body = conn.request(API_DELETE_ACLS, 1, false, &w.0)?;
    decode_delete_acls(&body)
}

fn decode_delete_acls(body: &[u8]) -> KafkaResult<Vec<AclBinding>> {
    let mut r = Reader::new(body);
    let _throttle_ms = r.i32()?;
    let mut deleted = Vec::new();
    for _ in 0..r.array_len()? {
        let code = r.i16()?;
        let message = r.nullable_string()?;
        if code != 0 {
            return Err(error_for_code(code, message.as_deref()));
        }
        for _ in 0..r.array_len()? {
            let code = r.i16()?;
            let message = r.nullable_string()?;
            let resource_type = acl_resource_from(r.i8()?);
            let name = r.nullable_string()?.unwrap_or_default();
            let pattern = acl_pattern_from(r.i8()?);
            let resource = match (resource_type, pattern) {
                (Some(t), Some(p)) => Some((t, name, p)),
                _ => None,
            };
            let entry = read_acl_entry(&mut r, resource)?;
            if code != 0 {
                return Err(error_for_code(code, message.as_deref()));
            }
            deleted.extend(entry);
        }
    }
    Ok(deleted)
}

/// Outcome of a preferred-leader election.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ElectionOutcome {
    /// Partitions whose leadership moved to the preferred replica.
    pub elected: Vec<(String, i32)>,
    /// Partitions already led by their preferred replica.
    pub already_preferred: usize,
    /// Partitions that could not be moved, with the reason.
    pub failed: Vec<(String, i32, String)>,
}

/// Moves leadership of `partitions` (every partition when `None`) back to
/// their preferred (first) replica — ElectLeaders v1, Kafka 2.4+.
pub fn elect_preferred_leaders(
    config: &KafkaClientConfig,
    partitions: Option<&[(String, i32)]>,
) -> KafkaResult<ElectionOutcome> {
    let timeout_ms = i32::try_from(config.request_timeout.as_millis()).unwrap_or(i32::MAX);
    let body = encode_elect_leaders(partitions, timeout_ms);
    let mut conn = connect_to_controller(config)?;
    let response = conn.request(API_ELECT_LEADERS, 1, false, &body)?;
    let (mut outcome, answered) = decode_elect_leaders_with_seen(&response)?;
    // A requested partition the response omits was not elected.
    for (topic, partition) in partitions.unwrap_or_default() {
        if !answered.contains(&(topic.clone(), *partition)) {
            outcome.failed.push((
                topic.clone(),
                *partition,
                "the broker's response had no result for this partition".to_string(),
            ));
        }
    }
    Ok(outcome)
}

fn encode_elect_leaders(partitions: Option<&[(String, i32)]>, timeout_ms: i32) -> Vec<u8> {
    let mut w = Writer::default();
    w.i8(0); // PREFERRED
    match partitions {
        None => w.i32(-1),
        Some(list) => {
            let mut by_topic: Vec<(&str, Vec<i32>)> = Vec::new();
            for (topic, partition) in list {
                match by_topic.iter_mut().find(|(t, _)| t == topic) {
                    Some((_, ps)) => ps.push(*partition),
                    None => by_topic.push((topic, vec![*partition])),
                }
            }
            w.i32(by_topic.len() as i32);
            for (topic, ps) in by_topic {
                w.string(topic);
                w.i32(ps.len() as i32);
                for p in ps {
                    w.i32(p);
                }
            }
        }
    }
    w.i32(timeout_ms);
    w.0
}

/// ELECTION_NOT_NEEDED: the preferred replica already leads.
const ELECTION_NOT_NEEDED: i16 = 84;

#[cfg(test)]
fn decode_elect_leaders(body: &[u8]) -> KafkaResult<ElectionOutcome> {
    decode_elect_leaders_with_seen(body).map(|(outcome, _)| outcome)
}

/// The outcome plus every partition the response mentions.
fn decode_elect_leaders_with_seen(
    body: &[u8],
) -> KafkaResult<(ElectionOutcome, std::collections::HashSet<(String, i32)>)> {
    let mut seen = std::collections::HashSet::new();
    let mut r = Reader::new(body);
    let _throttle_ms = r.i32()?;
    let code = r.i16()?;
    if code != 0 {
        return Err(error_for_code(code, None));
    }
    let mut outcome = ElectionOutcome::default();
    for _ in 0..r.array_len()? {
        let topic = r.nullable_string()?.unwrap_or_default();
        for _ in 0..r.array_len()? {
            let partition = r.i32()?;
            let code = r.i16()?;
            let message = r.nullable_string()?;
            seen.insert((topic.clone(), partition));
            match code {
                0 => outcome.elected.push((topic.clone(), partition)),
                ELECTION_NOT_NEEDED => outcome.already_preferred += 1,
                _ => outcome.failed.push((
                    topic.clone(),
                    partition,
                    describe_error(code, message.as_deref()),
                )),
            }
        }
    }
    Ok((outcome, seen))
}

/// FindCoordinator v4 for consumer groups (key type 0).
fn encode_find_coordinators(groups: &[String]) -> Vec<u8> {
    let mut w = Writer::default();
    w.0.push(0); // key_type: group
    w.compact_len(groups.len());
    for group in groups {
        w.compact_string(group);
    }
    w.uvarint(0); // request tagged fields
    w.0
}

/// Returns `(group, Ok("host:port") | Err(reason))` per requested group.
fn decode_find_coordinators(body: &[u8]) -> KafkaResult<Vec<(String, Result<String, String>)>> {
    let mut r = Reader::new(body);
    let _throttle_ms = r.i32()?;
    let mut coordinators = Vec::new();
    for _ in 0..r.compact_array_len()? {
        let key = r.compact_nullable_string()?.unwrap_or_default();
        let _node_id = r.i32()?;
        let host = r.compact_nullable_string()?.unwrap_or_default();
        let port = r.i32()?;
        let error_code = r.i16()?;
        let message = r.compact_nullable_string()?;
        r.skip_tagged_fields()?;
        let coordinator = if error_code == 0 {
            Ok(format!("{host}:{port}"))
        } else {
            Err(describe_error(error_code, message.as_deref()))
        };
        coordinators.push((key, coordinator));
    }
    Ok(coordinators)
}

/// OffsetFetch v8 for every committed partition (null topic list) of each group.
fn encode_offset_fetch(groups: &[String]) -> Vec<u8> {
    let mut w = Writer::default();
    w.compact_len(groups.len());
    for group in groups {
        w.compact_string(group);
        w.uvarint(0); // topics: null = all committed partitions
        w.uvarint(0); // group tagged fields
    }
    w.0.push(0); // require_stable: false
    w.uvarint(0); // request tagged fields
    w.0
}

fn decode_offset_fetch(body: &[u8]) -> KafkaResult<GroupOffsets> {
    let mut r = Reader::new(body);
    let _throttle_ms = r.i32()?;
    let mut offsets = GroupOffsets::new();
    for _ in 0..r.compact_array_len()? {
        let group = r.compact_nullable_string()?.unwrap_or_default();
        let mut committed = HashMap::new();
        for _ in 0..r.compact_array_len()? {
            let topic = r.compact_nullable_string()?.unwrap_or_default();
            for _ in 0..r.compact_array_len()? {
                let partition = r.i32()?;
                let offset = r.i64()?;
                let _leader_epoch = r.i32()?;
                let _metadata = r.compact_nullable_string()?;
                let error_code = r.i16()?;
                r.skip_tagged_fields()?;
                if error_code == 0 && offset >= 0 {
                    committed.insert((topic.clone(), partition), offset);
                }
            }
            r.skip_tagged_fields()?;
        }
        let error_code = r.i16()?;
        r.skip_tagged_fields()?;
        if error_code == 0 {
            offsets.insert(group, committed);
        } else {
            tracing::debug!(
                group,
                reason = describe_error(error_code, None),
                "OffsetFetch failed"
            );
        }
    }
    Ok(offsets)
}

/// Wraps `tcp` in TLS for broker `addr` (`host:port`) and completes the
/// handshake, so certificate problems surface here with the broker's name.
fn tls_handshake(
    addr: &str,
    tcp: TcpStream,
    config: &KafkaClientConfig,
) -> KafkaResult<rustls::StreamOwned<rustls::ClientConnection, TcpStream>> {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| KafkaError::config(format!("Invalid TLS host '{host}': {e}")))?;
    let tls_config = crate::kafka::http::broker_tls_config(config)?;
    let conn = rustls::ClientConnection::new(tls_config, name)
        .map_err(|e| KafkaError::config(format!("TLS setup for '{addr}' failed: {e}")))?;
    let mut stream = rustls::StreamOwned::new(conn, tcp);
    while stream.conn.is_handshaking() {
        stream
            .conn
            .complete_io(&mut stream.sock)
            .map_err(|e| KafkaError::io(format!("TLS handshake with '{addr}' failed: {e}")))?;
    }
    Ok(stream)
}

/// Connects to the first reachable bootstrap broker.
fn connect_bootstrap(config: &KafkaClientConfig) -> KafkaResult<WireConnection> {
    let mut last_error = None;
    for server in config
        .bootstrap_servers
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        match WireConnection::connect(server, config) {
            Ok(conn) => return Ok(conn),
            Err(e) => last_error = Some(e),
        }
    }
    Err(last_error.unwrap_or_else(|| KafkaError::config("No bootstrap servers configured")))
}

/// Connects to a bootstrap broker, then to the controller it reports.
///
/// In KRaft clusters brokers forward the request to the active controller, so
/// any broker is acceptable when the controller is unknown.
fn connect_to_controller(config: &KafkaClientConfig) -> KafkaResult<WireConnection> {
    check_supported_security(config)?;
    let mut conn = connect_bootstrap(config)?;
    let metadata = conn.request(API_METADATA, 1, false, &0i32.to_be_bytes())?;
    match decode_metadata_controller(&metadata)? {
        Some((_, addr)) if addr != conn.addr => WireConnection::connect(&addr, config),
        _ => Ok(conn),
    }
}

fn check_supported_security(config: &KafkaClientConfig) -> KafkaResult<()> {
    match config.security_protocol {
        SecurityProtocol::Plaintext | SecurityProtocol::Ssl => Ok(()),
        SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl => {
            match config.sasl_mechanism {
                Some(
                    SaslMechanism::Plain
                    | SaslMechanism::ScramSha256
                    | SaslMechanism::ScramSha512
                    | SaslMechanism::Oauthbearer,
                )
                | None => Ok(()),
                Some(other) => Err(KafkaError::config(format!(
                    "Native admin requests support SASL PLAIN, SCRAM and OAUTHBEARER, not {}",
                    other.as_str()
                ))),
            }
        }
    }
}

/// A broker socket: plain TCP or TLS over it.
trait BrokerStream: Read + Write + Send {}
impl<T: Read + Write + Send> BrokerStream for T {}

/// A blocking connection to one broker.
struct WireConnection {
    stream: Box<dyn BrokerStream>,
    addr: String,
    client_id: String,
    correlation_id: i32,
}

impl WireConnection {
    fn connect(addr: &str, config: &KafkaClientConfig) -> KafkaResult<Self> {
        let timeout = config.request_timeout.max(Duration::from_millis(100));
        let socket_addrs = addr
            .to_socket_addrs()
            .map_err(|e| KafkaError::io(format!("Cannot resolve broker '{addr}': {e}")))?;

        let mut last_error = None;
        let mut stream = None;
        for socket_addr in socket_addrs {
            match TcpStream::connect_timeout(&socket_addr, timeout) {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(e) => last_error = Some(e),
            }
        }
        let stream = stream.ok_or_else(|| {
            let reason = last_error.map_or_else(|| "no addresses".to_string(), |e| e.to_string());
            KafkaError::io(format!("Cannot connect to broker '{addr}': {reason}"))
        })?;
        stream
            .set_read_timeout(Some(timeout))
            .and_then(|_| stream.set_write_timeout(Some(timeout)))
            .map_err(|e| KafkaError::io(format!("Cannot configure socket for '{addr}': {e}")))?;
        let stream: Box<dyn BrokerStream> = match config.security_protocol {
            SecurityProtocol::Ssl | SecurityProtocol::SaslSsl => {
                Box::new(tls_handshake(addr, stream, config)?)
            }
            _ => Box::new(stream),
        };

        let mut conn = Self {
            stream,
            addr: addr.to_string(),
            client_id: config
                .client_id
                .clone()
                .unwrap_or_else(|| "ferrika".to_string()),
            correlation_id: 0,
        };

        if matches!(
            config.security_protocol,
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
        ) {
            match config.sasl_mechanism {
                Some(SaslMechanism::ScramSha256) => {
                    conn.authenticate_scram(config, crate::kafka::scram::ScramHash::Sha256)?
                }
                Some(SaslMechanism::ScramSha512) => {
                    conn.authenticate_scram(config, crate::kafka::scram::ScramHash::Sha512)?
                }
                Some(SaslMechanism::Oauthbearer) => conn.authenticate_oauthbearer(config)?,
                _ => conn.authenticate_plain(config)?,
            }
        }
        Ok(conn)
    }

    fn sasl_handshake(&mut self, mechanism: &str) -> KafkaResult<()> {
        let mut handshake = Writer::default();
        handshake.string(mechanism);
        let response = self.request(API_SASL_HANDSHAKE, 1, false, &handshake.0)?;
        let code = Reader::new(&response).i16()?;
        if code != 0 {
            return Err(KafkaError::acl(format!(
                "SASL handshake ({mechanism}) rejected by {}: {}",
                self.addr,
                describe_error(code, None)
            )));
        }
        Ok(())
    }

    /// One SaslAuthenticate v0 round; returns the broker's auth bytes.
    fn sasl_authenticate(&mut self, token: &[u8]) -> KafkaResult<Vec<u8>> {
        let mut auth = Writer::default();
        auth.bytes(token);
        let response = self.request(API_SASL_AUTHENTICATE, 0, false, &auth.0)?;
        let mut reader = Reader::new(&response);
        let code = reader.i16()?;
        let message = reader.nullable_string()?;
        if code != 0 {
            return Err(KafkaError::acl(format!(
                "SASL authentication failed on {}: {}",
                self.addr,
                describe_error(code, message.as_deref())
            )));
        }
        let len = usize::try_from(reader.i32()?).unwrap_or(0);
        let rest = reader.remaining();
        Ok(rest.get(..len).unwrap_or(rest).to_vec())
    }

    fn authenticate_scram(
        &mut self,
        config: &KafkaClientConfig,
        hash: crate::kafka::scram::ScramHash,
    ) -> KafkaResult<()> {
        let (Some(user), Some(password)) = (&config.sasl_username, &config.sasl_password) else {
            return Err(KafkaError::config(
                "SASL SCRAM requires both a username and a password",
            ));
        };
        self.sasl_handshake(hash.mechanism())?;
        let mut scram = crate::kafka::scram::ScramClient::new(hash, user, password)?;
        let server_first = self.sasl_authenticate(scram.client_first().as_bytes())?;
        let client_final = scram.client_final(&String::from_utf8_lossy(&server_first))?;
        let server_final = self.sasl_authenticate(client_final.as_bytes())?;
        scram.verify_server_final(&String::from_utf8_lossy(&server_final))
    }

    /// SASL OAUTHBEARER with a token from the profile's source (cached for
    /// the process, so one token serves many connections).
    fn authenticate_oauthbearer(&mut self, config: &KafkaClientConfig) -> KafkaResult<()> {
        let source = crate::kafka::oauth::source_for(config)?;
        let token = crate::kafka::oauth::blocking_token(source.as_ref())?;
        let result = self.authenticate_oauthbearer_with(&token);
        if result.is_err() {
            // A rejected token is fetched again next time.
            crate::kafka::oauth::forget(source.as_ref());
        }
        result
    }

    /// Sends `token` as the OAUTHBEARER initial response (RFC 7628). A broker
    /// that rejects it answers with a JSON error challenge, which the client
    /// acknowledges with a single `\x01` before the broker fails the
    /// authentication.
    fn authenticate_oauthbearer_with(
        &mut self,
        token: &crate::kafka::oauth::OAuthToken,
    ) -> KafkaResult<()> {
        self.sasl_handshake("OAUTHBEARER")?;
        let challenge = self.sasl_authenticate(&token.sasl_initial_response())?;
        if challenge.is_empty() {
            return Ok(());
        }
        let detail = serde_json::from_slice::<serde_json::Value>(&challenge)
            .ok()
            .map(|v| {
                let field = |k: &str| v.get(k).and_then(|s| s.as_str()).map(str::to_string);
                let mut text = field("status").unwrap_or_else(|| "invalid_token".to_string());
                if let Some(scope) = field("scope") {
                    text.push_str(&format!(" (scope: {scope})"));
                }
                text
            })
            .unwrap_or_else(|| String::from_utf8_lossy(&challenge).into_owned());
        // The broker's reply to the acknowledgement is the failure itself.
        let ack = self.sasl_authenticate(b"\x01").err();
        Err(KafkaError::acl(format!(
            "SASL OAUTHBEARER authentication failed on {}: the broker rejected the token: \
             {detail}{}",
            self.addr,
            ack.map(|e| format!(" ({e})")).unwrap_or_default()
        )))
    }

    fn authenticate_plain(&mut self, config: &KafkaClientConfig) -> KafkaResult<()> {
        let (Some(user), Some(password)) = (&config.sasl_username, &config.sasl_password) else {
            return Err(KafkaError::config(
                "SASL PLAIN requires both a username and a password",
            ));
        };

        let mut handshake = Writer::default();
        handshake.string("PLAIN");
        let response = self.request(API_SASL_HANDSHAKE, 1, false, &handshake.0)?;
        let code = Reader::new(&response).i16()?;
        if code != 0 {
            return Err(KafkaError::acl(format!(
                "SASL handshake rejected by {}: {}",
                self.addr,
                describe_error(code, None)
            )));
        }

        let mut token = Vec::with_capacity(user.len() + password.len() + 2);
        token.push(0);
        token.extend_from_slice(user.as_bytes());
        token.push(0);
        token.extend_from_slice(password.as_bytes());

        let mut auth = Writer::default();
        auth.bytes(&token);
        let response = self.request(API_SASL_AUTHENTICATE, 0, false, &auth.0)?;
        let mut reader = Reader::new(&response);
        let code = reader.i16()?;
        let message = reader.nullable_string()?;
        if code != 0 {
            return Err(KafkaError::acl(format!(
                "SASL authentication failed on {}: {}",
                self.addr,
                describe_error(code, message.as_deref())
            )));
        }
        Ok(())
    }

    /// Sends one request and returns the response body (after the response header).
    ///
    /// `flexible` selects request header v2 / response header v1, which carry tagged fields.
    fn request(
        &mut self,
        api_key: i16,
        api_version: i16,
        flexible: bool,
        body: &[u8],
    ) -> KafkaResult<Vec<u8>> {
        self.correlation_id = self.correlation_id.wrapping_add(1);

        let mut header = Writer::default();
        header.i16(api_key);
        header.i16(api_version);
        header.i32(self.correlation_id);
        header.string(&self.client_id);
        if flexible {
            header.uvarint(0);
        }

        let frame_len = i32::try_from(header.0.len() + body.len())
            .map_err(|_| KafkaError::Serialization("Request too large".to_string()))?;
        let mut frame = Vec::with_capacity(4 + header.0.len() + body.len());
        frame.extend_from_slice(&frame_len.to_be_bytes());
        frame.extend_from_slice(&header.0);
        frame.extend_from_slice(body);

        let io_error = |e: std::io::Error| {
            KafkaError::io(format!(
                "Broker {} closed or failed the API {api_key} v{api_version} request: {e}",
                self.addr
            ))
        };
        self.stream.write_all(&frame).map_err(io_error)?;

        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).map_err(io_error)?;
        let len = usize::try_from(i32::from_be_bytes(len_buf))
            .ok()
            .filter(|len| (4..=MAX_RESPONSE_BYTES).contains(len))
            .ok_or_else(|| {
                KafkaError::Serialization(format!("Invalid response length from {}", self.addr))
            })?;
        let mut response = vec![0u8; len];
        self.stream.read_exact(&mut response).map_err(io_error)?;

        let mut reader = Reader::new(&response);
        let correlation_id = reader.i32()?;
        if correlation_id != self.correlation_id {
            return Err(KafkaError::Serialization(format!(
                "Correlation id mismatch from {}: expected {}, got {correlation_id}",
                self.addr, self.correlation_id
            )));
        }
        if flexible {
            reader.skip_tagged_fields()?;
        }
        Ok(reader.remaining().to_vec())
    }
}

fn encode_alter_partition_reassignments(
    assignments: &[PartitionAssignment],
    timeout_ms: i32,
) -> Vec<u8> {
    // Group partitions by topic, preserving plan order.
    let mut topics: Vec<(&str, Vec<&PartitionAssignment>)> = Vec::new();
    for a in assignments {
        match topics.iter_mut().find(|(name, _)| *name == a.topic) {
            Some((_, parts)) => parts.push(a),
            None => topics.push((&a.topic, vec![a])),
        }
    }

    let mut w = Writer::default();
    w.i32(timeout_ms);
    w.compact_len(topics.len());
    for (name, parts) in &topics {
        w.compact_string(name);
        w.compact_len(parts.len());
        for p in parts {
            w.i32(p.partition);
            w.compact_len(p.replicas.len());
            for replica in &p.replicas {
                w.i32(*replica);
            }
            w.uvarint(0);
        }
        w.uvarint(0);
    }
    w.uvarint(0);
    w.0
}

fn decode_alter_partition_reassignments(
    body: &[u8],
) -> KafkaResult<Vec<PartitionReassignmentResult>> {
    let mut r = Reader::new(body);
    let _throttle_time_ms = r.i32()?;
    let code = r.i16()?;
    let message = r.compact_nullable_string()?;
    if code != 0 {
        let reason = describe_error(code, message.as_deref());
        return Err(if matches!(code, 29 | 31) {
            KafkaError::acl(reason)
        } else {
            KafkaError::admin(format!("Partition reassignment rejected: {reason}"))
        });
    }

    let mut results = Vec::new();
    for _ in 0..r.compact_array_len()? {
        let topic = r.compact_nullable_string()?.unwrap_or_default();
        for _ in 0..r.compact_array_len()? {
            let partition = r.i32()?;
            let code = r.i16()?;
            let message = r.compact_nullable_string()?;
            r.skip_tagged_fields()?;
            results.push(PartitionReassignmentResult {
                topic: topic.clone(),
                partition,
                error: (code != 0).then(|| describe_error(code, message.as_deref())),
            });
        }
        r.skip_tagged_fields()?;
    }
    Ok(results)
}

/// Returns the controller's id and `host:port` from a Metadata v1 response, if known.
fn decode_metadata_controller(body: &[u8]) -> KafkaResult<Option<(i32, String)>> {
    let mut r = Reader::new(body);
    let mut brokers = Vec::new();
    for _ in 0..r.array_len()? {
        let node_id = r.i32()?;
        let host = r.nullable_string()?.unwrap_or_default();
        let port = r.i32()?;
        let _rack = r.nullable_string()?;
        brokers.push((node_id, format!("{host}:{port}")));
    }
    let controller_id = r.i32()?;
    Ok(brokers.into_iter().find(|(id, _)| *id == controller_id))
}

/// DescribeQuorum v0 request for partition 0 of the metadata log.
fn encode_describe_quorum() -> Vec<u8> {
    let mut w = Writer::default();
    w.compact_len(1);
    w.compact_string(METADATA_TOPIC);
    w.compact_len(1);
    w.i32(0);
    w.uvarint(0); // partition tagged fields
    w.uvarint(0); // topic tagged fields
    w.uvarint(0); // request tagged fields
    w.0
}

/// Returns the quorum leader id from a DescribeQuorum v0 response.
fn decode_describe_quorum_leader(body: &[u8]) -> KafkaResult<Option<i32>> {
    let mut r = Reader::new(body);
    let error_code = r.i16()?;
    if error_code != 0 {
        return Err(error_for_code(error_code, None));
    }
    for _ in 0..r.compact_array_len()? {
        let _topic = r.compact_nullable_string()?;
        // Only partition 0 is requested, so the first entry is the answer.
        if r.compact_array_len()? > 0 {
            let _partition = r.i32()?;
            let error_code = r.i16()?;
            let leader_id = r.i32()?;
            if error_code != 0 {
                return Err(error_for_code(error_code, None));
            }
            return Ok((leader_id >= 0).then_some(leader_id));
        }
    }
    Ok(None)
}

/// The whole DescribeQuorum v0 answer for the metadata partition.
fn decode_describe_quorum(body: &[u8]) -> KafkaResult<Option<crate::model::cluster::QuorumInfo>> {
    use crate::model::cluster::{QuorumInfo, QuorumReplica};
    fn replicas(r: &mut Reader<'_>) -> KafkaResult<Vec<QuorumReplica>> {
        let mut out = Vec::new();
        for _ in 0..r.compact_array_len()? {
            let id = r.i32()?;
            let log_end_offset = r.i64()?;
            r.skip_tagged_fields()?;
            out.push(QuorumReplica { id, log_end_offset });
        }
        Ok(out)
    }
    let mut r = Reader::new(body);
    let error_code = r.i16()?;
    if error_code != 0 {
        return Err(error_for_code(error_code, None));
    }
    for _ in 0..r.compact_array_len()? {
        let _topic = r.compact_nullable_string()?;
        if r.compact_array_len()? > 0 {
            let _partition = r.i32()?;
            let error_code = r.i16()?;
            if error_code != 0 {
                return Err(error_for_code(error_code, None));
            }
            let leader_id = r.i32()?;
            let leader_epoch = r.i32()?;
            let high_watermark = r.i64()?;
            let voters = replicas(&mut r)?;
            let observers = replicas(&mut r)?;
            return Ok(Some(QuorumInfo {
                leader_id,
                leader_epoch,
                high_watermark,
                voters,
                observers,
            }));
        }
    }
    Ok(None)
}

fn error_for_code(code: i16, message: Option<&str>) -> KafkaError {
    let reason = describe_error(code, message);
    if matches!(code, 29..=31) {
        KafkaError::acl(reason)
    } else {
        KafkaError::admin(reason)
    }
}

fn describe_error(code: i16, message: Option<&str>) -> String {
    let name = match code {
        3 => "UNKNOWN_TOPIC_OR_PARTITION",
        7 => "REQUEST_TIMED_OUT",
        15 => "COORDINATOR_NOT_AVAILABLE",
        16 => "NOT_COORDINATOR",
        29 => "TOPIC_AUTHORIZATION_FAILED",
        30 => "GROUP_AUTHORIZATION_FAILED",
        31 => "CLUSTER_AUTHORIZATION_FAILED",
        33 => "UNSUPPORTED_SASL_MECHANISM",
        35 => "UNSUPPORTED_VERSION",
        38 => "INVALID_REPLICATION_FACTOR",
        39 => "INVALID_REPLICA_ASSIGNMENT",
        40 => "INVALID_CONFIG",
        41 => "NOT_CONTROLLER",
        42 => "INVALID_REQUEST",
        44 => "POLICY_VIOLATION",
        54 => "SECURITY_DISABLED",
        58 => "SASL_AUTHENTICATION_FAILED",
        60 => "REASSIGNMENT_IN_PROGRESS",
        69 => "GROUP_ID_NOT_FOUND",
        91 => "RESOURCE_NOT_FOUND",
        92 => "DUPLICATE_RESOURCE",
        93 => "UNACCEPTABLE_CREDENTIAL",
        84 => "ELECTION_NOT_NEEDED",
        86 => "GROUP_SUBSCRIBED_TO_TOPIC",
        85 => "NO_REASSIGNMENT_IN_PROGRESS",
        83 => "PREFERRED_LEADER_NOT_AVAILABLE",
        _ => "KAFKA_ERROR",
    };
    // Authorization errors quote the broker's internal request object
    // ("Request Request(processor=…, …) needs DESCRIBE permission"); keep the reason.
    let message = message.map(|m| match (m.starts_with("Request "), m.find(" needs ")) {
        (true, Some(i)) => &m[i + 1..],
        _ => m,
    });
    match message.filter(|m| !m.is_empty()) {
        Some(m) => format!("{name} (code {code}): {m}"),
        None => format!("{name} (code {code})"),
    }
}

#[derive(Default)]
struct Writer(Vec<u8>);

impl Writer {
    fn i16(&mut self, v: i16) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }

    fn i32(&mut self, v: i32) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }

    fn i8(&mut self, v: i8) {
        self.0.push(v as u8);
    }

    /// Classic nullable string: i16 length, -1 for null.
    fn nullable_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.string(s),
            None => self.i16(-1),
        }
    }

    /// Classic string: i16 length. Kafka caps these at 32767 bytes; longer
    /// input is cut at the last UTF-8 character boundary that fits, so the
    /// broker never receives a split code point.
    fn string(&mut self, s: &str) {
        let mut end = s.len().min(i16::MAX as usize);
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        let len = i16::try_from(end).unwrap_or(i16::MAX);
        self.i16(len);
        self.0.extend_from_slice(&s.as_bytes()[..end]);
    }

    fn bytes(&mut self, b: &[u8]) {
        self.i32(i32::try_from(b.len()).unwrap_or(i32::MAX));
        self.0.extend_from_slice(b);
    }

    fn uvarint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.0.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.0.push(v as u8);
    }

    /// Compact (flexible-version) length prefix: N + 1.
    fn compact_len(&mut self, len: usize) {
        self.uvarint(len as u64 + 1);
    }

    fn compact_string(&mut self, s: &str) {
        self.compact_len(s.len());
        self.0.extend_from_slice(s.as_bytes());
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> KafkaResult<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|end| *end <= self.buf.len())
            .ok_or_else(|| KafkaError::Serialization("Truncated Kafka response".to_string()))?;
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn remaining(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    fn i8(&mut self) -> KafkaResult<i8> {
        Ok(self.take(1)?[0] as i8)
    }

    fn i16(&mut self) -> KafkaResult<i16> {
        let b = self.take(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    fn i32(&mut self) -> KafkaResult<i32> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn i64(&mut self) -> KafkaResult<i64> {
        let mut b = [0u8; 8];
        b.copy_from_slice(self.take(8)?);
        Ok(i64::from_be_bytes(b))
    }

    fn uvarint(&mut self) -> KafkaResult<u64> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.take(1)?[0];
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(KafkaError::Serialization("Malformed varint".to_string()))
    }

    fn utf8(bytes: &[u8]) -> KafkaResult<String> {
        String::from_utf8(bytes.to_vec())
            .map_err(|_| KafkaError::Serialization("Invalid UTF-8 string".to_string()))
    }

    fn nullable_string(&mut self) -> KafkaResult<Option<String>> {
        match usize::try_from(self.i16()?) {
            Ok(len) => {
                let bytes = self.take(len)?;
                Self::utf8(bytes).map(Some)
            }
            Err(_) => Ok(None),
        }
    }

    fn array_len(&mut self) -> KafkaResult<usize> {
        Ok(usize::try_from(self.i32()?).unwrap_or(0))
    }

    fn compact_nullable_string(&mut self) -> KafkaResult<Option<String>> {
        match self.uvarint()? {
            0 => Ok(None),
            n => {
                let len = usize::try_from(n - 1)
                    .map_err(|_| KafkaError::Serialization("String too long".to_string()))?;
                let bytes = self.take(len)?;
                Self::utf8(bytes).map(Some)
            }
        }
    }

    fn compact_array_len(&mut self) -> KafkaResult<usize> {
        let n = self.uvarint()?;
        usize::try_from(n.saturating_sub(1))
            .map_err(|_| KafkaError::Serialization("Array too long".to_string()))
    }

    fn skip_tagged_fields(&mut self) -> KafkaResult<()> {
        for _ in 0..self.uvarint()? {
            let _tag = self.uvarint()?;
            let size = usize::try_from(self.uvarint()?)
                .map_err(|_| KafkaError::Serialization("Tagged field too long".to_string()))?;
            self.take(size)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_acls_summary_covers_all_outcomes() {
        assert!(summarize_create_acls(&[(0, None), (0, None)], 2).is_ok());
        assert!(summarize_create_acls(&[], 0).is_ok());
        let short = summarize_create_acls(&[(0, None)], 2)
            .unwrap_err()
            .to_string();
        assert!(short.contains("confirmed 1 of 2"), "{short}");
        // A single binding keeps the plain broker error (ACL class for 29..=31).
        assert!(matches!(
            summarize_create_acls(&[(31, None)], 1),
            Err(KafkaError::Acl(_))
        ));
        let partial = summarize_create_acls(
            &[(0, None), (42, Some("bad".into())), (0, None), (31, None)],
            4,
        )
        .unwrap_err()
        .to_string();
        assert!(
            partial.contains("2 of 4 bindings were created, 2 rejected"),
            "{partial}"
        );
        assert!(
            partial.contains("#2") && partial.contains("#4"),
            "{partial}"
        );
    }
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn test_string_truncates_on_char_boundary() {
        // 32766 ASCII bytes + a 3-byte char: the char straddles the cap.
        let s = format!("{}{}", "a".repeat(32_766), "\u{AC00}");
        let mut w = Writer::default();
        w.string(&s);
        let len = i16::from_be_bytes([w.0[0], w.0[1]]) as usize;
        assert_eq!(len, 32_766);
        assert_eq!(w.0.len(), 2 + len);
        assert!(std::str::from_utf8(&w.0[2..]).is_ok());

        let mut short = Writer::default();
        short.string("t\u{AC00}");
        assert_eq!(short.0, [0, 4, b't', 0xEA, 0xB0, 0x80]);
    }

    #[test]
    fn test_alter_scram_user_rejects_iterations_beyond_i32() {
        let config = KafkaClientConfig::default();
        let err = alter_scram_user(
            &config,
            "alice",
            crate::kafka::scram::ScramHash::Sha256,
            Some("pw"),
            u32::MAX,
        )
        .unwrap_err();
        assert!(err.to_string().contains("exceeds i32::MAX"), "{err}");
    }

    #[test]
    fn test_uvarint_roundtrip() {
        for v in [0u64, 1, 127, 128, 300, 16_384, u32::MAX as u64] {
            let mut w = Writer::default();
            w.uvarint(v);
            assert_eq!(Reader::new(&w.0).uvarint().unwrap(), v);
        }
    }

    #[test]
    fn test_encode_groups_partitions_by_topic() {
        let plan = vec![
            PartitionAssignment::new("a", 0, vec![1, 2]),
            PartitionAssignment::new("b", 0, vec![3]),
            PartitionAssignment::new("a", 1, vec![2, 3]),
        ];
        let body = encode_alter_partition_reassignments(&plan, 30_000);

        let mut r = Reader::new(&body);
        assert_eq!(r.i32().unwrap(), 30_000);
        assert_eq!(r.compact_array_len().unwrap(), 2);
        assert_eq!(r.compact_nullable_string().unwrap().as_deref(), Some("a"));
        assert_eq!(r.compact_array_len().unwrap(), 2);
        assert_eq!(r.i32().unwrap(), 0);
        assert_eq!(r.compact_array_len().unwrap(), 2);
        assert_eq!((r.i32().unwrap(), r.i32().unwrap()), (1, 2));
        r.skip_tagged_fields().unwrap();
        assert_eq!(r.i32().unwrap(), 1);
        assert_eq!(r.compact_array_len().unwrap(), 2);
        assert_eq!((r.i32().unwrap(), r.i32().unwrap()), (2, 3));
    }

    fn alter_response(top_code: i16, partitions: &[(i32, i16, Option<&str>)]) -> Vec<u8> {
        let mut w = Writer::default();
        w.i32(0);
        w.i16(top_code);
        w.uvarint(0);
        w.compact_len(1);
        w.compact_string("orders");
        w.compact_len(partitions.len());
        for (partition, code, message) in partitions {
            w.i32(*partition);
            w.i16(*code);
            match message {
                Some(m) => w.compact_string(m),
                None => w.uvarint(0),
            }
            w.uvarint(0);
        }
        w.uvarint(0);
        w.uvarint(0);
        w.0
    }

    #[test]
    fn test_decode_partition_level_errors() {
        let body = alter_response(0, &[(0, 0, None), (1, 39, Some("broker 9 is not alive"))]);
        let results = decode_alter_partition_reassignments(&body).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].error, None);
        assert_eq!(
            results[1].error.as_deref(),
            Some("INVALID_REPLICA_ASSIGNMENT (code 39): broker 9 is not alive")
        );
    }

    #[test]
    fn test_decode_top_level_authorization_error() {
        let body = alter_response(31, &[]);
        let err = decode_alter_partition_reassignments(&body).unwrap_err();
        assert!(matches!(err, KafkaError::Acl(_)));
    }

    #[test]
    fn test_encode_incremental_alter_configs() {
        let body = encode_incremental_alter_configs(
            RESOURCE_TOPIC,
            "t",
            &[
                ConfigChange::Set("a".into(), "1".into()),
                ConfigChange::Delete("b".into()),
            ],
            true,
        );
        #[rustfmt::skip]
        let expected = vec![
            2, RESOURCE_TOPIC, 2, b't',
            3,
            2, b'a', 0, 2, b'1', 0,
            2, b'b', 1, 0, 0,
            0,
            1,
            0,
        ];
        assert_eq!(body, expected);
    }

    #[test]
    fn test_encode_cluster_default_broker_config() {
        let body = encode_incremental_alter_configs(
            RESOURCE_BROKER,
            CLUSTER_DEFAULT_BROKER,
            &[ConfigChange::Set(
                "unclean.leader.election.enable".into(),
                "false".into(),
            )],
            false,
        );
        // An empty (not null) resource name: compact length 0 + 1.
        assert_eq!(&body[..3], &[2, RESOURCE_BROKER, 1]);
        // validate_only, then the request's tagged fields.
        assert_eq!(&body[body.len() - 2..], &[0, 0]);

        let per_broker = encode_incremental_alter_configs(
            RESOURCE_BROKER,
            "3",
            &[ConfigChange::Delete("min.insync.replicas".into())],
            true,
        );
        assert_eq!(&per_broker[..4], &[2, RESOURCE_BROKER, 2, b'3']);
        assert_eq!(&per_broker[per_broker.len() - 2..], &[1, 0]);
    }

    #[test]
    fn test_decode_incremental_alter_configs_reports_invalid_config() {
        let mut w = Writer::default();
        w.i32(0);
        w.compact_len(1);
        w.i16(40);
        w.compact_string("Invalid value abc for configuration retention.ms");
        w.0.push(RESOURCE_TOPIC);
        w.compact_string("t");
        w.uvarint(0);
        w.uvarint(0);
        let err = decode_incremental_alter_configs(&w.0).unwrap_err();
        assert!(err.to_string().contains("INVALID_CONFIG"), "{err}");

        let mut ok = Writer::default();
        ok.i32(0);
        ok.compact_len(1);
        ok.i16(0);
        ok.uvarint(0);
        ok.0.push(RESOURCE_TOPIC);
        ok.compact_string("t");
        ok.uvarint(0);
        ok.uvarint(0);
        assert!(decode_incremental_alter_configs(&ok.0).is_ok());
    }

    #[test]
    fn test_encode_find_coordinators_batches_groups() {
        let body = encode_find_coordinators(&["a".to_string(), "bc".to_string()]);
        assert_eq!(body, vec![0, 3, 2, b'a', 3, b'b', b'c', 0]);
    }

    #[test]
    fn test_decode_find_coordinators() {
        let mut w = Writer::default();
        w.i32(0); // throttle
        w.compact_len(2);
        w.compact_string("orders");
        w.i32(2);
        w.compact_string("broker-2");
        w.i32(9092);
        w.i16(0);
        w.uvarint(0); // null message
        w.uvarint(0);
        w.compact_string("pending");
        w.i32(-1);
        w.compact_string("");
        w.i32(-1);
        w.i16(15); // COORDINATOR_NOT_AVAILABLE
        w.uvarint(0);
        w.uvarint(0);
        w.uvarint(0);
        let decoded = decode_find_coordinators(&w.0).unwrap();
        assert_eq!(
            decoded[0],
            ("orders".to_string(), Ok("broker-2:9092".to_string()))
        );
        assert_eq!(decoded[1].0, "pending");
        assert!(decoded[1].1.is_err());
    }

    #[test]
    fn test_offset_fetch_roundtrip_skips_uncommitted_and_failed_groups() {
        assert_eq!(
            encode_offset_fetch(&["g".to_string()]),
            vec![2, 2, b'g', 0, 0, 0, 0]
        );

        let mut w = Writer::default();
        w.i32(0); // throttle
        w.compact_len(2);
        // Group "g": topic "t" with partition 0 at 42 and partition 1 uncommitted.
        w.compact_string("g");
        w.compact_len(1);
        w.compact_string("t");
        w.compact_len(2);
        for (partition, offset) in [(0, 42i64), (1, -1)] {
            w.i32(partition);
            w.0.extend_from_slice(&offset.to_be_bytes());
            w.i32(-1); // leader epoch
            w.uvarint(0); // null metadata
            w.i16(0);
            w.uvarint(0);
        }
        w.uvarint(0); // topic tagged fields
        w.i16(0);
        w.uvarint(0);
        // Group "denied": GROUP_AUTHORIZATION_FAILED.
        w.compact_string("denied");
        w.compact_len(0);
        w.i16(30);
        w.uvarint(0);
        w.uvarint(0);

        let offsets = decode_offset_fetch(&w.0).unwrap();
        assert_eq!(offsets.len(), 1);
        let g = &offsets["g"];
        assert_eq!(g.get(&("t".to_string(), 0)), Some(&42));
        assert_eq!(g.get(&("t".to_string(), 1)), None);
    }

    #[test]
    fn test_decode_describe_log_dirs_skips_future_replicas() {
        assert_eq!(encode_describe_log_dirs(), vec![0, 0]);

        let mut w = Writer::default();
        w.i32(0); // throttle
        w.compact_len(2);
        for (dir, future) in [("/data/a", false), ("/data/b", true)] {
            w.i16(0);
            w.compact_string(dir);
            w.compact_len(1);
            w.compact_string("t");
            w.compact_len(2);
            for (partition, size) in [(0, 100i64), (1, 50)] {
                w.i32(partition);
                w.0.extend_from_slice(&size.to_be_bytes());
                w.0.extend_from_slice(&0i64.to_be_bytes());
                w.0.push(u8::from(future));
                w.uvarint(0);
            }
            w.uvarint(0); // topic
            w.uvarint(0); // result
        }
        w.uvarint(0);

        let dirs = decode_describe_log_dirs(&w.0).unwrap();
        assert_eq!(dirs.partitions.get(&("t".to_string(), 0)), Some(&100));
        assert_eq!(dirs.partitions.get(&("t".to_string(), 1)), Some(&50));
        assert_eq!(dirs.total_bytes(), 150);
    }

    #[test]
    fn test_acl_filter_encoding_uses_match_for_named_resources() {
        let filter = AclFilter {
            resource_type: Some(AclResourceType::Topic),
            resource_name: Some("orders".into()),
            principal: Some("alice".into()),
            ..AclFilter::default()
        };
        let mut w = Writer::default();
        write_acl_filter(&mut w, &filter, 2);
        let mut expected = vec![2u8, 0, 6];
        expected.extend_from_slice(b"orders");
        expected.push(2); // MATCH
        expected.extend_from_slice(&[0, 10]);
        expected.extend_from_slice(b"User:alice");
        expected.extend_from_slice(&[0xff, 0xff, 1, 1]); // null host, any op, any permission
        assert_eq!(w.0, expected);
    }

    #[test]
    fn test_decode_describe_acls() {
        let mut w = Writer::default();
        w.i32(0);
        w.i16(0);
        w.nullable_string(None);
        w.i32(1);
        w.i8(2); // topic
        w.string("orders");
        w.i8(4); // prefixed
        w.i32(2);
        for (op, perm) in [(3i8, 3i8), (13, 3)] {
            w.string("User:alice");
            w.string("*");
            w.i8(op);
            w.i8(perm);
        }
        let acls = decode_describe_acls(&w.0).unwrap();
        // The CreateTokens (13) entry is not modelled and is skipped.
        assert_eq!(acls.len(), 1);
        assert_eq!(acls[0].resource_name, "orders");
        assert_eq!(acls[0].pattern_type, AclResourcePatternType::Prefixed);
        assert_eq!(acls[0].operation, AclOperation::Read);
        assert_eq!(acls[0].permission_type, AclPermissionType::Allow);

        let mut w = Writer::default();
        w.i32(0);
        w.i16(54);
        w.nullable_string(Some("No Authorizer is configured"));
        assert!(decode_describe_acls(&w.0).is_err());
    }

    #[test]
    fn test_decode_delete_acls_returns_deleted_bindings() {
        let mut w = Writer::default();
        w.i32(0);
        w.i32(1);
        w.i16(0);
        w.nullable_string(None);
        w.i32(1);
        w.i16(0);
        w.nullable_string(None);
        w.i8(3); // group
        w.string("billing");
        w.i8(3); // literal
        w.string("User:svc");
        w.string("*");
        w.i8(3);
        w.i8(2); // deny
        let deleted = decode_delete_acls(&w.0).unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0].resource_type, AclResourceType::Group);
        assert_eq!(deleted[0].permission_type, AclPermissionType::Deny);
    }

    #[test]
    fn test_elect_leaders_roundtrip() {
        let all = encode_elect_leaders(None, 5000);
        assert_eq!(all, vec![0, 0xff, 0xff, 0xff, 0xff, 0, 0, 0x13, 0x88]);
        let some = encode_elect_leaders(Some(&[("t".to_string(), 0), ("t".to_string(), 2)]), 1);
        assert_eq!(
            some,
            vec![0, 0, 0, 0, 1, 0, 1, b't', 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1]
        );

        let mut w = Writer::default();
        w.i32(0);
        w.i16(0);
        w.i32(1);
        w.string("t");
        w.i32(3);
        for (p, code) in [(0, 0i16), (1, 84), (2, 83)] {
            w.i32(p);
            w.i16(code);
            w.nullable_string(None);
        }
        let outcome = decode_elect_leaders(&w.0).unwrap();
        assert_eq!(outcome.elected, vec![("t".to_string(), 0)]);
        assert_eq!(outcome.already_preferred, 1);
        assert_eq!(outcome.failed.len(), 1);
    }

    #[test]
    fn test_encode_describe_quorum_targets_metadata_log() {
        let mut expected = vec![2, 19];
        expected.extend_from_slice(METADATA_TOPIC.as_bytes());
        expected.extend_from_slice(&[2, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(encode_describe_quorum(), expected);
    }

    fn quorum_response(top_code: i16, partition_code: i16, leader: i32) -> Vec<u8> {
        let mut w = Writer::default();
        w.i16(top_code);
        w.compact_len(1);
        w.compact_string(METADATA_TOPIC);
        w.compact_len(1);
        w.i32(0);
        w.i16(partition_code);
        w.i32(leader);
        w.i32(7); // leader epoch
        w.0.extend_from_slice(&42i64.to_be_bytes()); // high watermark
        w.compact_len(0); // voters
        w.compact_len(0); // observers
        w.uvarint(0);
        w.uvarint(0);
        w.uvarint(0);
        w.0
    }

    #[test]
    fn test_decode_describe_quorum_replicas() {
        let mut w = Writer::default();
        w.i16(0);
        w.compact_len(1);
        w.compact_string(METADATA_TOPIC);
        w.compact_len(1);
        w.i32(0);
        w.i16(0);
        w.i32(2); // leader
        w.i32(9); // epoch
        w.0.extend_from_slice(&100i64.to_be_bytes());
        w.compact_len(2);
        for (id, leo) in [(1, 100i64), (2, 90)] {
            w.i32(id);
            w.0.extend_from_slice(&leo.to_be_bytes());
            w.uvarint(0);
        }
        w.compact_len(1);
        w.i32(4);
        w.0.extend_from_slice(&(-1i64).to_be_bytes());
        w.uvarint(0);
        w.uvarint(0);
        w.uvarint(0);
        w.uvarint(0);
        let q = decode_describe_quorum(&w.0).unwrap().unwrap();
        assert_eq!((q.leader_id, q.leader_epoch, q.high_watermark), (2, 9, 100));
        assert_eq!(q.voters.len(), 2);
        assert_eq!(q.lag(&q.voters[1]), Some(10));
        assert_eq!(q.lag(&q.observers[0]), None);
        assert!(decode_describe_quorum(&quorum_response(0, 0, 1))
            .unwrap()
            .unwrap()
            .voters
            .is_empty());
    }

    #[test]
    fn test_decode_describe_quorum_leader() {
        assert_eq!(
            decode_describe_quorum_leader(&quorum_response(0, 0, 2)).unwrap(),
            Some(2)
        );
        assert_eq!(
            decode_describe_quorum_leader(&quorum_response(0, 0, -1)).unwrap(),
            None
        );
        let err = decode_describe_quorum_leader(&quorum_response(31, 0, 2)).unwrap_err();
        assert!(matches!(err, KafkaError::Acl(_)));
        let err = decode_describe_quorum_leader(&quorum_response(0, 6, 2)).unwrap_err();
        assert!(matches!(err, KafkaError::Admin(_)));
    }

    #[test]
    fn test_decode_list_partition_reassignments() {
        let mut w = Writer::default();
        w.i32(0);
        w.i16(0);
        w.uvarint(0); // no error message
        w.compact_len(1);
        w.compact_string("orders");
        w.compact_len(1);
        w.i32(2);
        for list in [&[1, 2, 3][..], &[3], &[1]] {
            w.compact_len(list.len());
            for id in list {
                w.i32(*id);
            }
        }
        w.uvarint(0);
        w.uvarint(0);
        w.uvarint(0);
        let moves = decode_list_partition_reassignments(&w.0).unwrap();
        assert_eq!(moves.len(), 1);
        assert_eq!((moves[0].topic.as_str(), moves[0].partition), ("orders", 2));
        assert_eq!(
            (moves[0].adding.clone(), moves[0].removing.clone()),
            (vec![3], vec![1])
        );
    }

    #[test]
    fn test_decode_describe_scram_users() {
        let mut w = Writer::default();
        w.i32(0);
        w.i16(0);
        w.uvarint(0);
        w.compact_len(1);
        w.compact_string("alice");
        w.i16(0);
        w.uvarint(0);
        w.compact_len(2);
        for (mechanism, iterations) in [(1i8, 4096), (2, 8192)] {
            w.i8(mechanism);
            w.i32(iterations);
            w.uvarint(0);
        }
        w.uvarint(0);
        w.uvarint(0);
        let users = decode_describe_scram_users(&w.0).unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].name, "alice");
        assert_eq!(
            users[0].credentials,
            vec![
                (crate::kafka::scram::ScramHash::Sha256, 4096),
                (crate::kafka::scram::ScramHash::Sha512, 8192)
            ]
        );
    }

    #[test]
    fn test_client_quota_codecs() {
        let entity = vec![("user".to_string(), Some("alice".to_string()))];
        let body = encode_alter_client_quota(&entity, "producer_byte_rate", Some(1024.0));
        // entries(1) entity(1) "user" "alice" ops(1) key value remove=0 validate=0
        assert_eq!(&body[..4], &[0, 0, 0, 1]);
        assert_eq!(body[body.len() - 2..], [0, 0]);
        let removal = encode_alter_client_quota(&entity, "producer_byte_rate", None);
        assert_eq!(removal[removal.len() - 2..], [1, 0]);

        let mut w = Writer::default();
        w.i32(0);
        w.i16(0);
        w.nullable_string(None);
        w.i32(1); // entries
        w.i32(2); // entity parts
        w.string("user");
        w.nullable_string(Some("alice"));
        w.string("client-id");
        w.nullable_string(None);
        w.i32(1); // values
        w.string("consumer_byte_rate");
        w.0.extend_from_slice(&2048f64.to_bits().to_be_bytes());
        let quotas = decode_describe_client_quotas(&w.0).unwrap();
        assert_eq!(quotas.len(), 1);
        assert_eq!(quotas[0].value, 2048.0);
        assert_eq!(
            quota_entity_label(&quotas[0].entity),
            "user=alice, client-id=<default>"
        );
    }

    #[test]
    fn test_offset_delete_codec() {
        let body = encode_offset_delete("g", "t", &[0, 2]);
        let mut expected = Vec::new();
        expected.extend_from_slice(&[0, 1, b'g', 0, 0, 0, 1, 0, 1, b't']);
        expected.extend_from_slice(&[0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2]);
        assert_eq!(body, expected);

        let response = |p2_code: i16| {
            let mut w = Writer::default();
            w.i16(0);
            w.i32(0);
            w.i32(1);
            w.string("t");
            w.i32(2);
            w.i32(0);
            w.i16(0);
            w.i32(2);
            w.i16(p2_code);
            w.0
        };
        assert_eq!(
            decode_offset_delete(&response(0), "g", "t").unwrap(),
            vec![0, 2]
        );
        let err = decode_offset_delete(&response(86), "g", "t").unwrap_err();
        assert!(err.to_string().contains("still consuming"), "{err}");
        let mut top = Writer::default();
        top.i16(69);
        top.i32(0);
        let err = decode_offset_delete(&top.0, "g", "t").unwrap_err();
        assert!(err.to_string().contains("GROUP_ID_NOT_FOUND"), "{err}");
    }

    /// Reads one request frame and returns (api_key, correlation_id, body).
    fn read_request_body(stream: &mut TcpStream) -> (i16, i32, Vec<u8>) {
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).unwrap();
        let mut frame = vec![0u8; i32::from_be_bytes(len) as usize];
        stream.read_exact(&mut frame).unwrap();
        let mut r = Reader::new(&frame);
        let api = r.i16().unwrap();
        let _version = r.i16().unwrap();
        let corr = r.i32().unwrap();
        let _client_id = r.nullable_string().unwrap();
        (api, corr, r.remaining().to_vec())
    }

    /// The mechanism a fake broker saw and the auth bytes it received.
    type SeenAuth = (String, Vec<Vec<u8>>);

    /// A broker that accepts the OAUTHBEARER handshake, records the auth
    /// bytes it receives, and answers each SaslAuthenticate with the next
    /// `(error_code, auth_bytes)` of `replies`.
    fn fake_oauth_broker(replies: Vec<(i16, Vec<u8>)>) -> (String, thread::JoinHandle<SeenAuth>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let (api, corr, body) = read_request_body(&mut stream);
            assert_eq!(api, API_SASL_HANDSHAKE);
            let mechanism = Reader::new(&body).nullable_string().unwrap().unwrap();
            let mut w = Writer::default();
            w.i16(0);
            w.i32(1);
            w.string("OAUTHBEARER");
            write_response(&mut stream, corr, false, &w.0);
            let mut seen = Vec::new();
            for (code, bytes) in replies {
                let (api, corr, body) = read_request_body(&mut stream);
                assert_eq!(api, API_SASL_AUTHENTICATE);
                let mut r = Reader::new(&body);
                let len = r.i32().unwrap() as usize;
                seen.push(r.take(len).unwrap().to_vec());
                let mut w = Writer::default();
                w.i16(code);
                w.nullable_string(if code == 0 { None } else { Some("bad token") });
                w.bytes(&bytes);
                write_response(&mut stream, corr, false, &w.0);
            }
            (mechanism, seen)
        });
        (addr, handle)
    }

    fn oauth_token(value: &str) -> crate::kafka::oauth::OAuthToken {
        let mut extensions = std::collections::BTreeMap::new();
        extensions.insert("logicalCluster".to_string(), "lkc-1".to_string());
        crate::kafka::oauth::OAuthToken {
            value: value.to_string(),
            principal: "svc".to_string(),
            issued_at_ms: 0,
            expires_at_ms: i64::MAX,
            extensions,
        }
    }

    #[test]
    fn test_oauthbearer_sends_the_rfc7628_initial_response() {
        let (addr, broker) = fake_oauth_broker(vec![(0, Vec::new())]);
        let config = KafkaClientConfig::new(addr.clone());
        let mut conn = WireConnection::connect(&addr, &config).unwrap();
        conn.authenticate_oauthbearer_with(&oauth_token("eyJ.abc.sig"))
            .unwrap();
        let (mechanism, seen) = broker.join().unwrap();
        assert_eq!(mechanism, "OAUTHBEARER");
        assert_eq!(
            seen,
            vec![b"n,,\x01auth=Bearer eyJ.abc.sig\x01logicalCluster=lkc-1\x01\x01".to_vec()]
        );
    }

    #[test]
    fn test_oauthbearer_acknowledges_the_error_challenge() {
        let challenge = br#"{"status":"invalid_token","scope":"kafka"}"#.to_vec();
        let (addr, broker) = fake_oauth_broker(vec![(0, challenge), (58, Vec::new())]);
        let config = KafkaClientConfig::new(addr.clone());
        let mut conn = WireConnection::connect(&addr, &config).unwrap();
        let err = conn
            .authenticate_oauthbearer_with(&oauth_token("expired"))
            .unwrap_err();
        let (_, seen) = broker.join().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[1], b"\x01");
        let text = err.to_string();
        assert!(text.contains("invalid_token (scope: kafka)"), "{text}");
        assert!(!text.contains("expired"), "the token leaked: {text}");
    }

    #[test]
    fn test_oauthbearer_is_allowed_and_needs_its_settings() {
        let mut config = KafkaClientConfig::new("127.0.0.1:1");
        config.security_protocol = SecurityProtocol::SaslPlaintext;
        config.sasl_mechanism = Some(SaslMechanism::Oauthbearer);
        assert!(check_supported_security(&config).is_ok());
        // OIDC without its settings fails before any connection.
        let Err(err) = crate::kafka::oauth::source_for(&config) else {
            panic!("OIDC without settings must fail");
        };
        assert!(err.to_string().contains("oauth_token_endpoint"), "{err}");
    }

    /// The whole path: an OIDC token from a local token endpoint, then the
    /// SASL exchange with a fake broker through `WireConnection::connect`.
    #[test]
    fn test_connect_authenticates_with_an_oidc_token() {
        let idp = TcpListener::bind("127.0.0.1:0").unwrap();
        let idp_port = idp.local_addr().unwrap().port();
        thread::spawn(move || {
            let (mut s, _) = idp.accept().unwrap();
            let mut buf = [0u8; 4096];
            let _ = s.read(&mut buf);
            let _ = s.write_all(
                b"HTTP/1.1 200 OK\r\n\r\n{\"access_token\":\"oidc-wire-token\",\"expires_in\":3600}",
            );
        });
        let (addr, broker) = fake_oauth_broker(vec![(0, Vec::new())]);
        let mut config = KafkaClientConfig::new(addr.clone());
        config.security_protocol = SecurityProtocol::SaslPlaintext;
        config.sasl_mechanism = Some(SaslMechanism::Oauthbearer);
        config.oauth.token_endpoint = Some(format!("http://127.0.0.1:{idp_port}/token"));
        config.oauth.client_id = Some(format!("wire-test-{idp_port}"));
        config.oauth.client_secret = Some("s".to_string());
        WireConnection::connect(&addr, &config).unwrap();
        let (_, seen) = broker.join().unwrap();
        assert_eq!(
            seen,
            vec![b"n,,\x01auth=Bearer oidc-wire-token\x01\x01".to_vec()]
        );
    }

    #[test]
    fn test_gssapi_is_rejected_instead_of_downgraded() {
        let mut config = KafkaClientConfig::new("localhost:9093");
        config.security_protocol = SecurityProtocol::SaslSsl;
        config.sasl_mechanism = Some(SaslMechanism::Gssapi);
        let err = alter_partition_reassignments(&config, &[]).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
    }

    #[test]
    fn test_tls_to_a_plaintext_listener_fails_cleanly() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            // Answer the ClientHello with bytes that are not TLS.
            if let Ok((mut s, _)) = listener.accept() {
                let mut buf = [0u8; 512];
                let _ = s.read(&mut buf);
                let _ = s.write_all(&[0, 0, 0, 4, 0, 0, 0, 1]);
            }
        });
        let mut config = KafkaClientConfig::new(format!("localhost:{}", addr.port()));
        config.security_protocol = SecurityProtocol::Ssl;
        let Err(err) = WireConnection::connect(&config.bootstrap_servers, &config) else {
            panic!("a TLS handshake with a plaintext listener must fail");
        };
        assert!(err.to_string().contains("TLS handshake"), "{err}");
    }

    /// Reads one request frame and returns (api_key, api_version, correlation_id).
    fn read_request(stream: &mut TcpStream) -> (i16, i16, i32) {
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).unwrap();
        let mut frame = vec![0u8; i32::from_be_bytes(len) as usize];
        stream.read_exact(&mut frame).unwrap();
        let mut r = Reader::new(&frame);
        (r.i16().unwrap(), r.i16().unwrap(), r.i32().unwrap())
    }

    fn write_response(stream: &mut TcpStream, correlation_id: i32, flexible: bool, body: &[u8]) {
        let mut w = Writer::default();
        w.i32(correlation_id);
        if flexible {
            w.uvarint(0);
        }
        w.0.extend_from_slice(body);
        stream.write_all(&(w.0.len() as i32).to_be_bytes()).unwrap();
        stream.write_all(&w.0).unwrap();
    }

    #[test]
    fn test_submit_against_fake_broker() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let broker = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();

            let (api, version, corr) = read_request(&mut stream);
            assert_eq!((api, version), (API_METADATA, 1));
            let mut metadata = Writer::default();
            metadata.i32(1);
            metadata.i32(1);
            metadata.string("127.0.0.1");
            metadata.i32(i32::from(port));
            metadata.i16(-1);
            metadata.i32(1);
            metadata.i32(0);
            write_response(&mut stream, corr, false, &metadata.0);

            let (api, version, corr) = read_request(&mut stream);
            assert_eq!((api, version), (API_ALTER_PARTITION_REASSIGNMENTS, 0));
            write_response(&mut stream, corr, true, &alter_response(0, &[(0, 0, None)]));
        });

        let config = KafkaClientConfig::new(format!("127.0.0.1:{port}"));
        let plan = vec![PartitionAssignment::new("orders", 0, vec![1])];
        let results = alter_partition_reassignments(&config, &plan).unwrap();
        broker.join().unwrap();

        assert_eq!(
            results,
            vec![PartitionReassignmentResult {
                topic: "orders".to_string(),
                partition: 0,
                error: None,
            }]
        );
    }

    #[test]
    fn test_empty_alter_responses_are_not_confirmations() {
        // Throttle only, then an empty results array.
        let mut compact = Writer::default();
        compact.i32(0);
        compact.compact_len(0);
        compact.uvarint(0);
        assert!(decode_incremental_alter_configs(&compact.0).is_err());
        let mut classic = Writer::default();
        classic.i32(0);
        classic.i32(0);
        assert!(decode_alter_client_quotas(&classic.0).is_err());
        classic.0.clear();
        classic.i32(-1);
        assert!(decode_alter_client_quotas(&classic.0).is_err());
    }

    #[test]
    fn test_unanswered_partitions_are_not_reported_as_started() {
        let plan = vec![
            PartitionAssignment::new("orders", 0, vec![1]),
            PartitionAssignment::new("orders", 1, vec![1]),
        ];
        let mut results =
            decode_alter_partition_reassignments(&alter_response(0, &[(0, 0, None)])).unwrap();
        add_missing_reassignment_results(&plan, &mut results);
        assert_eq!(results.len(), 2);
        assert!(results[0].error.is_none());
        assert_eq!(results[1].partition, 1);
        assert!(results[1].error.is_some());
    }

    #[test]
    fn test_elect_leaders_response_lists_answered_partitions() {
        let mut w = Writer::default();
        w.i32(0);
        w.i16(0);
        w.i32(1); // one topic
        w.string("t");
        w.i32(1);
        w.i32(0);
        w.i16(0);
        w.nullable_string(None);
        let (outcome, seen) = decode_elect_leaders_with_seen(&w.0).unwrap();
        assert_eq!(outcome.elected, vec![("t".to_string(), 0)]);
        assert!(seen.contains(&("t".to_string(), 0)));
        assert!(!seen.contains(&("t".to_string(), 1)));
    }

    #[test]
    fn test_log_dir_sizes_saturate_instead_of_overflowing() {
        let mut w = Writer::default();
        w.i32(0);
        w.compact_len(1);
        w.i16(0);
        w.compact_string("/d");
        w.compact_len(1);
        w.compact_string("t");
        w.compact_len(3);
        for p in 0..3 {
            w.i32(p);
            w.0.extend_from_slice(&i64::MAX.to_be_bytes());
            w.0.extend_from_slice(&0i64.to_be_bytes());
            w.0.push(0);
            w.uvarint(0);
        }
        w.uvarint(0);
        w.uvarint(0);
        w.uvarint(0);
        let dirs = decode_describe_log_dirs(&w.0).unwrap();
        assert_eq!(dirs.dirs[0].bytes, u64::MAX);
        assert_eq!(dirs.total_bytes(), u64::MAX);
    }
}
