//! The read-only tools: cluster, topic and group inspection, peeking, the
//! transform preview and the RabbitMQ listings. Under a policy with
//! `export: forbidden`, `peek_messages` and `transform_preview` are not
//! offered: they would hand records to an outside AI service.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use ferrika_core::codec::pii::{mask_bytes, mask_payload, MaskStrategy};
use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_core::kafka::admin::KafkaAdminClient;
use ferrika_core::kafka::consumer::KafkaConsumerClient;
use ferrika_core::kafka::consumer::StartOffset;
use ferrika_core::kafka::context::ResolvedContext;
use ferrika_core::model::cluster::TopicSummary;
use ferrika_governance::policy::{self, AuditSink, EffectivePolicy, ExportChannel, ExportRequest};

/// Most records `peek_messages` returns.
const PEEK_MAX: u64 = 50;
/// Records `peek_messages` examines for a `where` filter, by default and at most.
const SCAN_DEFAULT: u64 = 5_000;
const SCAN_MAX: u64 = 100_000;
/// Most buckets `message_histogram` returns.
const HISTOGRAM_MAX_BUCKETS: usize = 200;
/// How long `peek_messages` waits for the next record before stopping.
const PEEK_IDLE: Duration = Duration::from_secs(2);
/// Longest key or value text returned per record.
const PEEK_TEXT_MAX: usize = 2000;

/// The read tools that return record data, which pass the export gate and
/// are hidden under `export: forbidden`.
const RECORD_TOOLS: [&str; 2] = ["peek_messages", "transform_preview"];

/// Whether `peek_messages` and `transform_preview` are offered: not under
/// `export: forbidden`, since they hand records to an outside AI service.
pub(super) fn peek_listed(policy: &EffectivePolicy, context: &ResolvedContext) -> bool {
    // The cluster's own brokers count too (`export_clusters`); a lookup that
    // fails hides the tool, since the rule cannot be checked.
    !ferrika_governance::policy::export_forbidden_with(
        policy,
        &context.config.clone(),
        ferrika_core::kafka::admin::known_brokers,
    )
}

pub(super) fn tool_definitions(with_peek: bool) -> Value {
    let read_only = json!({ "readOnlyHint": true, "openWorldHint": false });
    let mut tools = json!([
        {
            "name": "cluster_health",
            "description": "Health findings for the cluster: offline brokers, leaderless or under-replicated partitions, leader skew, stalled consumer groups, quorum lag. Empty when healthy.",
            "inputSchema": { "type": "object", "properties": {} },
            "annotations": read_only
        },
        {
            "name": "config_lint",
            "description": "Risky or inconsistent topic, broker and consumer group configs (ferrika lint), each with a rule id, severity, the values involved and a fix; fix.command is the command a person can run. Nothing is changed.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "topic": { "type": "string", "description": "Only topics matching this glob, e.g. orders-*" },
                    "min_severity": { "type": "string", "enum": ["critical", "warning", "info"], "default": "info" }
                }
            },
            "annotations": read_only
        },
        {
            "name": "list_brokers",
            "description": "Brokers with host, controller flag, partition leaders and replicas hosted.",
            "inputSchema": { "type": "object", "properties": {} },
            "annotations": read_only
        },
        {
            "name": "list_topics",
            "description": "Topics with partition count, replication factor, message count and cleanup policy.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "filter": { "type": "string", "description": "Only topics whose name contains this text" },
                    "include_internal": { "type": "boolean", "description": "Include internal topics such as __consumer_offsets", "default": false }
                }
            },
            "annotations": read_only
        },
        {
            "name": "describe_topic",
            "description": "One topic's partitions (leader, replicas, ISR, low/high watermark) and the configs set on it.",
            "inputSchema": {
                "type": "object",
                "properties": { "topic": { "type": "string" } },
                "required": ["topic"]
            },
            "annotations": read_only
        },
        {
            "name": "list_consumer_groups",
            "description": "Consumer groups with state, member count, topics and total lag.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "filter": { "type": "string", "description": "Only groups whose id contains this text" }
                }
            },
            "annotations": read_only
        },
        {
            "name": "consumer_group_lag",
            "description": "Per-partition committed offset, end offset and lag of one consumer group, with how old the first unconsumed record is (seconds) where known.",
            "inputSchema": {
                "type": "object",
                "properties": { "group": { "type": "string" } },
                "required": ["group"]
            },
            "annotations": read_only
        },
        {
            "name": "message_histogram",
            "description": "How many records arrived in each time bucket of a topic, from offsets (no record is read): bursts, gaps and records with future timestamps (after). Counts are offset ranges, so transaction markers and records compaction removed between kept records are included; offsets removed before the oldest kept record are reported as removed_by_compaction. peak.start_ms is the busiest bucket's start in epoch ms (e.g. for ferrika consume --from-timestamp).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "topic": { "type": "string" },
                    "partition": { "type": "integer", "description": "Only this partition" },
                    "since": { "type": "string", "description": "How far back from now, e.g. 30m, 24h, 7d", "default": "24h" },
                    "from": { "type": "string", "description": "Start instead of since: ISO 8601 (2026-11-02T09:00:00Z) or epoch ms" },
                    "to": { "type": "string", "description": "End with from (default now)" },
                    "bucket": { "type": "string", "description": "Bucket width, e.g. 5m or 1h (default: about 48 buckets)" }
                },
                "required": ["topic"]
            },
            "annotations": read_only
        },
        {
            "name": "schema_fields",
            "description": "The fields of a topic's Schema Registry schema (Avro, Protobuf or JSON Schema): path, type, optional, enum values. Each path works as-is in peek_messages' where (e.g. $.amount > 100). Needs a Schema Registry URL on the profile.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "topic": { "type": "string", "description": "Read the topic's <topic>-value subject (<topic>-key with key)" },
                    "subject": { "type": "string", "description": "A subject instead of topic" },
                    "key": { "type": "boolean", "default": false, "description": "With topic: the key subject" },
                    "version": { "type": "integer", "minimum": 1, "description": "Subject version (default: latest)" },
                    "message": { "type": "string", "description": "Protobuf: the message type (default: the first)" }
                }
            },
            "annotations": read_only
        },
        {
            "name": "peek_messages",
            "description": "Reads a few records without joining a consumer group. By default the newest records across all partitions.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "topic": { "type": "string" },
                    "partition": { "type": "integer", "description": "Only this partition" },
                    "from": { "type": "string", "description": "\"latest\" (default: the newest records), \"earliest\", or an offset number (needs partition)" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": PEEK_MAX, "default": 10 },
                    "where": { "type": "string", "description": "Keep only records matching an expression, e.g. $.amount > 100 && $.status == \"PAID\". Fields: $.path (JSON value), key, partition, offset, timestamp, headers.<name>; operators == != > >= < <= =~ contains exists && || !" },
                    "scan_limit": { "type": "integer", "minimum": 1, "maximum": SCAN_MAX, "default": SCAN_DEFAULT, "description": "With where: most records to examine" },
                    "mask_pii": { "type": "boolean", "default": true, "description": "Mask emails, card numbers, SSNs and tokens" }
                },
                "required": ["topic"]
            },
            "annotations": read_only
        },
        {
            "name": "transform_preview",
            "description": "Shows what ferrika transform would write: reads a topic from the beginning (or from_timestamp) up to its current end, applies the steps in order and returns up to max_records transformed records with counts (read, kept, filtered, sampled out, not JSON). Writes nothing and joins no consumer group. Record text is PII-masked unless mask_pii is false; when the company policy enforces masking a mask step is put first.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "topic": { "type": "string", "description": "Topic to read" },
                    "steps": crate::transform_tools::steps_schema(),
                    "from_beginning": { "type": "boolean", "default": true, "description": "Start at the earliest offset (the default)" },
                    "from_timestamp": { "type": ["string", "integer"], "description": "Start at the first record at or after this time instead: ISO 8601, epoch ms or a time ago (30m, 2h)" },
                    "partition": { "type": "integer", "minimum": 0, "description": "Only this partition" },
                    "max_records": { "type": "integer", "minimum": 1, "maximum": crate::transform_tools::PREVIEW_MAX, "default": 10, "description": "Most transformed records to return" },
                    "scan_limit": { "type": "integer", "minimum": 1, "maximum": SCAN_MAX, "default": SCAN_DEFAULT, "description": "Most records to read" },
                    "decode_wire_format": { "type": "boolean", "default": false, "description": "Decode Schema Registry wire-format values (Avro, Protobuf, JSON Schema) to JSON first" },
                    "mask_pii": { "type": "boolean", "default": true, "description": "Mask emails, card numbers, SSNs and tokens in the returned text" }
                },
                "required": ["topic", "steps"]
            },
            "annotations": read_only
        }
    ]);
    if !with_peek {
        if let Some(list) = tools.as_array_mut() {
            list.retain(|tool| {
                !RECORD_TOOLS
                    .iter()
                    .any(|name| tool["name"].as_str() == Some(name))
            });
        }
    }
    if let Some(list) = tools.as_array_mut() {
        list.extend(crate::rabbit_tools::definitions());
    }
    tools
}

/// Runs a tool; failures are reported as tool results with `isError`.
/// `actor` names the MCP client in the audit entries the tool writes.
pub(super) async fn call_tool(
    params: &Value,
    context: &ResolvedContext,
    policy: &EffectivePolicy,
    sink: &impl AuditSink,
    actor: Option<&str>,
) -> Value {
    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
    let args = params.get("arguments").cloned().unwrap_or(json!({}));
    let result = match name {
        "cluster_health" => cluster_health(context).await,
        "config_lint" => {
            config_lint(
                context,
                str_arg(&args, "topic"),
                str_arg(&args, "min_severity"),
            )
            .await
        }
        "list_brokers" => list_brokers(context),
        "list_topics" => list_topics(context, &args).await,
        "describe_topic" => describe_topic(context, &args).await,
        "list_consumer_groups" => list_consumer_groups(context, &args).await,
        "consumer_group_lag" => consumer_group_lag(context, &args).await,
        "message_histogram" => message_histogram(context, &args).await,
        "schema_fields" => schema_fields(context, &args).await,
        "peek_messages" => match authorize_export(policy, sink, context, name, &args, actor) {
            Ok(()) => peek_messages(context, &args).await,
            Err(e) => Err(e),
        },
        "transform_preview" => match authorize_export(policy, sink, context, name, &args, actor) {
            Ok(()) => crate::transform_tools::preview(context, policy, &args).await,
            Err(e) => Err(e),
        },
        rabbit if crate::rabbit_tools::RABBIT_TOOLS.contains(&rabbit) => {
            crate::rabbit_tools::call(name, &args).await
        }
        _ => Err(KafkaError::config(format!("Unknown tool '{name}'"))),
    };
    tool_reply(result)
}

/// A tool result: the value as text and, when it is an object, as
/// structured content; an error as text with `isError`.
fn tool_reply(result: KafkaResult<Value>) -> Value {
    match result {
        Ok(value) => {
            let mut reply = json!({
                "content": [{ "type": "text", "text": serde_json::to_string_pretty(&value).unwrap_or_default() }],
                "isError": false
            });
            // The same result as structured content (MCP 2025-06-18), which
            // must be an object; the text stays for older clients.
            if value.is_object() {
                reply["structuredContent"] = value;
            }
            reply
        }
        Err(e) => json!({
            "content": [{ "type": "text", "text": e.to_string() }],
            "isError": true
        }),
    }
}

/// The export gate for a tool that returns records (`peek_messages`,
/// `transform_preview`): refused (and recorded) under `export: forbidden`
/// even when a client calls it without listing it, recorded under
/// `export: audited`.
fn authorize_export(
    policy: &EffectivePolicy,
    sink: &impl AuditSink,
    context: &ResolvedContext,
    label: &str,
    args: &Value,
    actor: Option<&str>,
) -> KafkaResult<()> {
    let profile = context.profile_name.clone().unwrap_or_default();
    let bootstrap = ferrika_governance::policy::export_brokers(policy, &context.config.clone())
        .map_err(KafkaError::Check)?;
    let request = ExportRequest {
        source: "mcp",
        channel: ExportChannel::Mcp,
        label,
        target: str_arg(args, "topic").unwrap_or_default(),
        records: None,
        profile: &profile,
        bootstrap: &bootstrap,
        actor,
    };
    policy::authorize_export_with(policy, sink, &request)
        .map_err(|refusal| KafkaError::Check(refusal.to_string()))
}

/// The `message_histogram` request from tool arguments (times in UTC).
fn histogram_request(
    args: &Value,
    now_ms: i64,
) -> KafkaResult<ferrika_ops::time_histogram::HistogramRequest> {
    use ferrika_ops::time_histogram::{BucketSpec, CostPolicy, HistogramRequest, TimeRange};
    let topic = required(args, "topic")?.to_string();
    let time = |name: &str| -> KafkaResult<Option<i64>> {
        str_arg(args, name)
            .map(|text| {
                ferrika_core::model::time::parse_time_ms_in(text.trim(), now_ms, 0)
                    .ok_or_else(|| KafkaError::config(format!("'{name}' is not a time: {text}")))
            })
            .transpose()
    };
    let range = match time("from")? {
        Some(from) => TimeRange::Between(from, time("to")?.unwrap_or(now_ms)),
        None => TimeRange::Since(
            ferrika_ops::report::parse_window(str_arg(args, "since").unwrap_or("24h"))
                .map_err(KafkaError::config)?,
        ),
    };
    let bucket = match str_arg(args, "bucket") {
        Some(text) => {
            BucketSpec::Fixed(ferrika_ops::report::parse_window(text).map_err(KafkaError::config)?)
        }
        None => BucketSpec::Auto { target: 48 },
    };
    // Checked here so a too-fine bucket fails before connecting.
    let (from, to) = range.resolve(now_ms, None).map_err(KafkaError::config)?;
    let width =
        ferrika_ops::time_histogram::bucket_ms(&bucket, to - from).map_err(KafkaError::config)?;
    let buckets = (to - from) / width.max(1);
    if buckets > HISTOGRAM_MAX_BUCKETS as i64 {
        return Err(KafkaError::config(format!(
            "{buckets} buckets is more than {HISTOGRAM_MAX_BUCKETS}: use a wider bucket or a shorter range"
        )));
    }
    let partition = args
        .get("partition")
        .and_then(Value::as_i64)
        .map(|p| vec![p as i32]);
    Ok(HistogramRequest {
        topic,
        partitions: partition,
        range,
        bucket,
        tz_offset_ms: 0,
        cost: CostPolicy::Coarsen,
        exact: None,
    })
}

/// Bucket counts without per-partition numbers (fewer tokens).
async fn message_histogram(context: &ResolvedContext, args: &Value) -> KafkaResult<Value> {
    let now = ferrika_ops::time_histogram::now_ms();
    let request = histogram_request(args, now)?;
    let hist =
        ferrika_ops::time_histogram::run(&context.config.clone(), request, |_, _| {}).await?;
    Ok(histogram_json(&hist))
}

fn histogram_json(hist: &ferrika_ops::time_histogram::TimeHistogram) -> Value {
    let iso = |ms: i64| {
        jiff::Timestamp::from_millisecond(ms)
            .ok()
            .map(|t| t.strftime("%Y-%m-%dT%H:%M:%SZ").to_string())
            .unwrap_or_default()
    };
    json!({
        "topic": hist.topic,
        "from": iso(hist.from_ms),
        "to": iso(hist.to_ms),
        "bucket_seconds": hist.bucket_ms / 1000,
        "timestamp_type": hist.timestamp_type,
        "approximate": hist.approximate,
        "notes": hist.notes(),
        "buckets": hist.buckets.iter().map(|b| json!({
            "start": iso(b.start_ms),
            "count": b.count,
            "state": b.state,
        })).collect::<Vec<_>>(),
        "before": hist.before,
        "removed_by_compaction": hist.removed_by_compaction,
        "after": hist.after,
        "unknown": hist.unknown,
        "peak": hist.peak().map(|b| json!({ "start": iso(b.start_ms), "start_ms": b.start_ms, "count": b.count })),
        "gaps": hist.gaps(1).iter().map(|(a, b)| json!({ "from": iso(*a), "to": iso(*b) })).collect::<Vec<_>>(),
        "unavailable_partitions": hist.unavailable,
        "lookup_error": hist.lookup_error,
    })
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_secs(10))
}

fn str_arg<'a>(args: &'a Value, name: &str) -> Option<&'a str> {
    args.get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

fn required<'a>(args: &'a Value, name: &str) -> KafkaResult<&'a str> {
    str_arg(args, name).ok_or_else(|| KafkaError::config(format!("'{name}' is required")))
}

fn admin(context: &ResolvedContext) -> KafkaResult<KafkaAdminClient> {
    KafkaAdminClient::new(&context.config.clone())
}

async fn cluster_health(context: &ResolvedContext) -> KafkaResult<Value> {
    let issues = health_issues(context).await?;
    Ok(json!({ "findings": issues }))
}

/// The health checks of `ferrika health` against the cluster.
async fn health_issues(
    context: &ResolvedContext,
) -> KafkaResult<Vec<ferrika_ops::health::HealthIssue>> {
    let admin = KafkaAdminClient::new(&context.config)?;
    let snapshot = ferrika_ops::health::snapshot(
        &admin,
        &context.config,
        context.profile_name.as_deref().unwrap_or_default(),
    )
    .await?;
    Ok(ferrika_ops::health::health_checks(&snapshot))
}

/// The `config_lint` tool: findings graded like `ferrika lint --env auto`,
/// with each fix's command for a person to run.
async fn config_lint(
    context: &ResolvedContext,
    topic: Option<&str>,
    min_severity: Option<&str>,
) -> KafkaResult<Value> {
    use ferrika_ops::health::Severity;
    use ferrika_ops::lint::{self, LintFinding, LintOptions};
    let min = match min_severity.unwrap_or("info") {
        "critical" => Severity::Critical,
        "warning" => Severity::Warning,
        "info" => Severity::Info,
        other => {
            return Err(KafkaError::config(format!(
                "min_severity must be critical, warning or info, not '{other}'"
            )))
        }
    };
    let admin = KafkaAdminClient::new(&context.config)?;
    let profile = context.profile_name.clone().unwrap_or_default();
    let (env, why) = lint::auto_env(context);
    let opts = LintOptions {
        env,
        topics: topic.map(|t| vec![t.to_string()]).unwrap_or_default(),
        ..LintOptions::default()
    };
    let input = lint::collect::gather(&admin, &context.config, &profile, None, &opts).await?;
    let findings: Vec<LintFinding> = lint::lint(&input, &opts)
        .into_iter()
        .filter(|f| f.severity <= min)
        .collect();
    Ok(json!({
        "env": env,
        "env_reason": why,
        "findings": findings,
        "not_checked": input.skipped,
    }))
}

fn list_brokers(context: &ResolvedContext) -> KafkaResult<Value> {
    let brokers = admin(context)?.list_brokers(timeout())?;
    Ok(json!({ "brokers": brokers }))
}

/// Metadata carries no offsets; fills in each partition's watermarks and
/// the topic's message count (best effort).
async fn add_watermarks(context: &ResolvedContext, topics: &mut [TopicSummary]) {
    let Ok(lookup) = KafkaConsumerClient::lookup(&context.config.clone()) else {
        return;
    };
    let wanted: Vec<(&str, i32)> = topics
        .iter()
        .flat_map(|t| {
            t.partitions
                .iter()
                .map(|p| (t.name.as_str(), p.partition_id))
        })
        .collect();
    let Ok(marks) = lookup
        .fetch_watermarks_batch_async(&wanted, timeout())
        .await
    else {
        return;
    };
    for topic in topics.iter_mut() {
        for p in &mut topic.partitions {
            if let Some(&(low, high)) = marks.get(&(topic.name.clone(), p.partition_id)) {
                p.low_watermark = low;
                p.high_watermark = high;
            }
        }
        topic.total_messages = ferrika_core::kafka::metrics::topic_depth(
            topic
                .partitions
                .iter()
                .map(|p| (p.low_watermark, p.high_watermark)),
        );
    }
}

async fn list_topics(context: &ResolvedContext, args: &Value) -> KafkaResult<Value> {
    let filter = str_arg(args, "filter");
    let internal = args
        .get("include_internal")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut topics: Vec<TopicSummary> = admin(context)?
        .list_topic_summaries(timeout())?
        .into_iter()
        .filter(|t| internal || !t.is_internal)
        .filter(|t| filter.is_none_or(|f| t.name.contains(f)))
        .collect();
    add_watermarks(context, &mut topics).await;
    let topics: Vec<Value> = topics
        .into_iter()
        .map(|t| {
            json!({
                "name": t.name,
                "partitions": t.partition_count,
                "replication_factor": t.replication_factor,
                "messages": t.total_messages,
                "cleanup_policy": t.cleanup_policy,
                "internal": t.is_internal,
            })
        })
        .collect();
    Ok(json!({ "count": topics.len(), "topics": topics }))
}

/// The subject `schema_fields` reads: `subject`, or the topic's value (or
/// key) subject.
fn fields_subject(args: &Value) -> KafkaResult<String> {
    let key = args.get("key").and_then(Value::as_bool).unwrap_or(false);
    match (str_arg(args, "subject"), str_arg(args, "topic")) {
        (Some(_), Some(_)) => Err(KafkaError::config("Give topic or subject, not both")),
        (Some(_), None) if key => Err(KafkaError::config("key goes with topic")),
        (Some(subject), None) => Ok(subject.to_string()),
        (None, Some(topic)) => Ok(format!("{topic}-{}", if key { "key" } else { "value" })),
        (None, None) => Err(KafkaError::config("Give topic or subject")),
    }
}

async fn schema_fields(context: &ResolvedContext, args: &Value) -> KafkaResult<Value> {
    use ferrika_core::codec::schema_fields::{from_definition, Limits};
    let subject = fields_subject(args)?;
    let url = context
        .config
        .schema_registry_url
        .as_deref()
        .filter(|u| !u.trim().is_empty())
        .ok_or_else(|| KafkaError::config("This profile has no Schema Registry URL"))?;
    let version = args
        .get("version")
        .and_then(Value::as_u64)
        .map(|v| u32::try_from(v).map_err(|_| KafkaError::config("version is too large")))
        .transpose()?;
    let message = str_arg(args, "message");
    let client = ferrika_core::kafka::schema::SchemaRegistryClient::new(url)
        .with_timeout(context.config.request_timeout);
    let def = client.subject_schema_resolved(&subject, version).await?;
    let catalog = from_definition(&def, message, &Limits::default())?;
    let fields: Vec<Value> = catalog
        .entries
        .iter()
        .map(|e| {
            let mut field = json!({
                "path": e.path(),
                "type": e.kind.label(),
                "optional": e.optional,
            });
            if e.recursive {
                field["recursive"] = json!(true);
            }
            if let Some(group) = &e.group {
                field["oneof"] = json!(group);
            }
            if !e.enum_values.is_empty() {
                field["values"] = json!(e.enum_values);
            }
            field
        })
        .collect();
    Ok(json!({
        "subject": subject,
        "id": def.id,
        "version": def.version,
        "schema_type": def.schema_type,
        "truncated": catalog.truncated,
        "notes": catalog.notes,
        "fields": fields,
    }))
}

async fn describe_topic(context: &ResolvedContext, args: &Value) -> KafkaResult<Value> {
    let topic = required(args, "topic")?;
    let admin = admin(context)?;
    let mut summary = [admin.describe_topic(topic, timeout())?];
    add_watermarks(context, &mut summary).await;
    let [summary] = summary;
    let configs: BTreeMap<String, String> = admin
        .describe_topic_overrides(topic, timeout())
        .await
        .unwrap_or_default()
        .into_iter()
        .collect();
    let mut value =
        serde_json::to_value(&summary).map_err(|e| KafkaError::Serialization(e.to_string()))?;
    value["config_overrides"] = json!(configs);
    Ok(value)
}

async fn list_consumer_groups(context: &ResolvedContext, args: &Value) -> KafkaResult<Value> {
    let config = context.config.clone();
    let admin = KafkaAdminClient::new(&config)?;
    let filter = str_arg(args, "filter");
    let listings = admin.list_consumer_group_listings(timeout())?;
    let lags = ferrika_ops::exporter::scrape_cluster_metrics(&admin, &config).await;
    let groups: Vec<Value> = listings
        .into_iter()
        .filter(|g| filter.is_none_or(|f| g.name.contains(f)))
        .map(|g| {
            let mine: Vec<_> = lags
                .consumer_lags
                .iter()
                .filter(|l| l.group == g.name)
                .collect();
            let mut topics: Vec<&str> = mine.iter().map(|l| l.topic.as_str()).collect();
            topics.sort();
            topics.dedup();
            json!({
                "group": g.name,
                "state": g.state,
                "members": g.members.len(),
                "topics": topics,
                "total_lag": mine.iter().map(|l| l.lag).sum::<i64>(),
            })
        })
        .collect();
    Ok(json!({ "count": groups.len(), "groups": groups }))
}

async fn consumer_group_lag(context: &ResolvedContext, args: &Value) -> KafkaResult<Value> {
    let group = required(args, "group")?;
    let config = context.config.clone();
    let admin = KafkaAdminClient::new(&config)?;
    if !admin
        .list_consumer_groups(timeout())?
        .iter()
        .any(|g| g == group)
    {
        return Err(KafkaError::config(format!("No consumer group '{group}'")));
    }
    let metrics = ferrika_ops::exporter::scrape_cluster_metrics(&admin, &config).await;
    let partitions: Vec<Value> = metrics
        .consumer_lags
        .iter()
        .filter(|l| l.group == group)
        .map(|l| {
            let behind = metrics
                .lag_seconds
                .iter()
                .find(|(g, t, p, _)| g == group && *t == l.topic && *p == l.partition)
                .map(|(_, _, _, s)| *s);
            json!({
                "topic": l.topic,
                "partition": l.partition,
                "committed_offset": l.current_offset,
                "end_offset": l.high_watermark,
                "lag": l.lag,
                "behind_seconds": behind,
            })
        })
        .collect();
    let total: i64 = metrics
        .consumer_lags
        .iter()
        .filter(|l| l.group == group)
        .map(|l| l.lag)
        .sum();
    Ok(json!({ "group": group, "total_lag": total, "partitions": partitions }))
}

/// Text of a key or value: UTF-8 (masked if asked), else hex; shortened.
pub(super) fn record_text(bytes: Option<&[u8]>, mask: bool) -> Value {
    let Some(bytes) = bytes else {
        return Value::Null;
    };
    let mut text = match std::str::from_utf8(bytes) {
        // `mask_bytes` hides undecoded schema records whole, which are often
        // valid UTF-8 but would pass a free-text scan.
        _ if mask => mask_bytes(bytes, MaskStrategy::Partial),
        Ok(s) => s.to_string(),
        Err(_) => {
            let hex =
                ferrika_core::model::format::to_hex(&bytes[..bytes.len().min(PEEK_TEXT_MAX / 2)]);
            format!("hex:{hex}")
        }
    };
    if text.chars().count() > PEEK_TEXT_MAX {
        text = text.chars().take(PEEK_TEXT_MAX).collect::<String>() + "…";
    }
    json!(text)
}

/// `where` evaluated on the record, or on its masked rendering when `masked`.
fn where_matches(
    expr: &ferrika_core::model::expr::Expr,
    m: &ferrika_core::kafka::consumer::ConsumedMessage,
    value: Option<&ferrika_core::codec::schema::DecodedPayload>,
    key: Option<&ferrika_core::codec::schema::DecodedPayload>,
    masked: bool,
) -> bool {
    use ferrika_core::kafka::filter::expression_matches;
    if !masked {
        return expression_matches(expr, m, value, key);
    }
    let mask = |d: &ferrika_core::codec::schema::DecodedPayload| {
        ferrika_core::codec::schema::DecodedPayload {
            text: mask_payload(&d.text, MaskStrategy::Partial),
            ..d.clone()
        }
    };
    let (value, key) = (value.map(mask), key.map(mask));
    expression_matches(expr, &m.masked(), value.as_ref(), key.as_ref())
}

async fn peek_messages(context: &ResolvedContext, args: &Value) -> KafkaResult<Value> {
    let topic = required(args, "topic")?;
    let limit = args
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(10)
        .clamp(1, PEEK_MAX) as i64;
    // A policy that enforces masking ignores mask_pii: false, and `where`
    // then sees only masked text, so hit counts cannot recover masked values.
    let enforced = ferrika_governance::policy::current().masking_enforced();
    let mask = args
        .get("mask_pii")
        .and_then(Value::as_bool)
        .unwrap_or(true)
        || enforced;
    let only = args
        .get("partition")
        .and_then(Value::as_i64)
        .map(|p| p as i32);
    let from = match args.get("from") {
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::String(s)) => s.to_ascii_lowercase(),
        _ => "latest".to_string(),
    };
    let filter = match str_arg(args, "where") {
        Some(text) => Some(
            ferrika_core::model::expr::Expr::parse(text)
                .map_err(|e| KafkaError::config(format!("where: {e}")))?,
        ),
        None => None,
    };
    // Without a filter every record read is returned; with one, up to
    // `scan` records are examined for matches.
    let scan = match filter {
        Some(_) => args
            .get("scan_limit")
            .and_then(Value::as_u64)
            .unwrap_or(SCAN_DEFAULT)
            .clamp(1, SCAN_MAX) as i64,
        None => limit,
    };

    let config = context.config.clone();
    let consumer = KafkaConsumerClient::scanner(&config)?;
    // Schema-encoded records are decoded through the profile's registry, for
    // both `where` and the returned text.
    let mut decoder = config
        .schema_registry_url
        .as_deref()
        .map(|url| ferrika_core::kafka::schema::PayloadDecoder::new(Some(url)));
    let mut partitions = consumer.topic_partitions(topic, timeout())?;
    if let Some(p) = only {
        if !partitions.contains(&p) {
            return Err(KafkaError::config(format!(
                "'{topic}' has no partition {p}"
            )));
        }
        partitions = vec![p];
    }
    // Start positions, and where each partition ends now.
    let mut starts = Vec::new();
    let mut ends = BTreeMap::new();
    for &p in &partitions {
        let (low, high) = consumer.fetch_watermarks_async(topic, p, timeout()).await?;
        let start = match from.as_str() {
            "latest" => (high - (scan / partitions.len() as i64).max(limit)).max(low),
            "earliest" => low,
            offset => {
                if only.is_none() {
                    return Err(KafkaError::config(
                        "An offset needs a partition".to_string(),
                    ));
                }
                offset
                    .parse::<i64>()
                    .map_err(|_| {
                        KafkaError::config(format!(
                            "'from' must be latest, earliest or an offset, not '{offset}'"
                        ))
                    })?
                    .clamp(low, high)
            }
        };
        if start < high {
            starts.push((topic, p, StartOffset::At(start)));
            ends.insert(p, high);
        }
    }
    let mut records = Vec::new();
    let mut examined = 0i64;
    if !starts.is_empty() {
        consumer.assign_with_offsets(&starts)?;
        let budget = if filter.is_some() { 20 } else { 8 };
        let deadline = Instant::now() + Duration::from_secs(budget);
        // From "latest" every partition contributes its newest records; keep
        // reading until each has reached its end, then keep the newest overall.
        let latest = from == "latest";
        let read_cap = if latest {
            (scan / starts.len() as i64).max(limit) * starts.len() as i64
        } else {
            scan
        };
        while examined < read_cap && !ends.is_empty() && (latest || (records.len() as i64) < limit)
        {
            // A transaction marker at the end of a partition is never
            // delivered, so a quiet spell also ends the read.
            let left = deadline
                .saturating_duration_since(Instant::now())
                .min(PEEK_IDLE);
            let Ok(Ok(m)) = tokio::time::timeout(left, consumer.recv()).await else {
                break;
            };
            examined += 1;
            if ends
                .get(&m.partition)
                .is_some_and(|&end| m.offset + 1 >= end)
            {
                ends.remove(&m.partition);
            }
            let (mut value, mut key) = (None, None);
            if let Some(decoder) = decoder.as_mut() {
                if let Some(payload) = m.payload.as_deref() {
                    value = decoder.decode(payload).await;
                }
                if let Some(k) = m.key.as_deref() {
                    key = decoder.decode(k).await;
                }
            }
            if filter
                .as_ref()
                .is_none_or(|f| where_matches(f, &m, value.as_ref(), key.as_ref(), enforced))
            {
                records.push((m, value, key));
            }
        }
    }
    if from == "latest" {
        records.sort_by_key(|(m, _, _)| (m.timestamp_ms.unwrap_or(0), m.partition, m.offset));
        let skip = records.len().saturating_sub(limit as usize);
        records.drain(..skip);
    } else {
        records.truncate(limit as usize);
    }
    let out: Vec<Value> = records
        .iter()
        .map(|(m, value, key)| {
            let mut headers: Vec<&str> = m.headers.iter().map(|(k, _)| k.as_str()).collect();
            headers.sort();
            headers.dedup();
            let key_text = key.as_ref().map(|k| ferrika_core::codec::schema::key_text(&k.text));
            let mut record = json!({
                "partition": m.partition,
                "offset": m.offset,
                "timestamp": m.timestamp_ms.and_then(|ms| jiff::Timestamp::from_millisecond(ms).ok()).map(ferrika_core::model::time::rfc3339),
                "key": match &key_text {
                    Some(k) => record_text(Some(k.as_bytes()), mask),
                    None => record_text(m.key.as_deref(), mask),
                },
                "value": match value {
                    Some(v) => record_text(Some(v.text.as_bytes()), mask),
                    None => record_text(m.payload.as_deref(), mask),
                },
                "header_keys": headers,
            });
            if let Some(v) = value {
                record["decoded"] = json!(v.label);
            }
            record
        })
        .collect();
    let mut result = json!({ "topic": topic, "count": out.len(), "masked": mask, "records": out });
    if filter.is_some() {
        result["examined"] = json!(examined);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn undecoded_schema_records_are_hidden_when_masking() {
        let mut avro = vec![0u8, 0, 0, 0, 1, 0x0e];
        avro.extend_from_slice(b"hunter2");
        let shown = super::record_text(Some(&avro), true);
        assert!(!shown.to_string().contains("hunter2"), "{shown}");
        // Unmasked, the text is passed through as before.
        assert!(super::record_text(Some(b"plain"), false)
            .to_string()
            .contains("plain"));
    }

    use crate::protocol::tests::context;

    #[test]
    fn every_listed_read_tool_only_reads() {
        let tools = tool_definitions(true);
        let tools = tools.as_array().unwrap();
        assert!(tools.iter().any(|t| t["name"] == "config_lint"));
        // Lint fixes are not offered as a tool.
        for tool in tools {
            assert_eq!(
                tool["annotations"]["readOnlyHint"], true,
                "{}",
                tool["name"]
            );
        }
    }

    #[tokio::test]
    async fn export_forbidden_hides_and_refuses_peek_messages() {
        use ferrika_governance::test_policy::{self, MemoryLog};
        let g = context();
        let call = json!({ "name": "peek_messages", "arguments": { "topic": "orders" } });
        let names = |tools: Value| -> Vec<String> {
            tools
                .as_array()
                .map(|tools| {
                    tools
                        .iter()
                        .filter_map(|t| t["name"].as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default()
        };

        let forbidden = test_policy::export_forbidden();
        let log = MemoryLog::default();
        let tools = names(tool_definitions(peek_listed(&forbidden, &g)));
        assert!(!tools.iter().any(|n| n == "peek_messages"), "{tools:?}");
        assert!(tools.iter().any(|n| n == "list_topics"), "{tools:?}");

        let refused = call_tool(&call, &g, &forbidden, &log, Some("mcp:t/1#00")).await;
        assert_eq!(refused["isError"], true);
        let text = refused["content"][0]["text"].as_str().unwrap_or("");
        assert!(text.contains("export: forbidden"), "{text}");
        let entries = log.entries.take();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "policy.blocked");
        assert_eq!(entries[0].source, "mcp");
        assert_eq!(entries[0].target, "export:mcp peek_messages orders");
        assert_eq!(entries[0].actor.as_deref(), Some("mcp:t/1#00"));

        // Audited: listed, and each call recorded before it reads.
        let audited = test_policy::export_audited();
        let log = MemoryLog::default();
        let tools = names(tool_definitions(peek_listed(&audited, &g)));
        assert!(tools.iter().any(|n| n == "peek_messages"), "{tools:?}");
        // No topic: recorded, then refused by the tool itself without
        // connecting.
        let no_topic = json!({ "name": "peek_messages", "arguments": {} });
        let result = call_tool(&no_topic, &g, &audited, &log, Some("mcp:t/1#00")).await;
        assert_eq!(result["isError"], true);
        assert_eq!(log.entries.borrow()[0].action, "record.export");
        assert_eq!(log.entries.borrow()[0].actor.as_deref(), Some("mcp:t/1#00"));
    }

    #[tokio::test]
    async fn export_forbidden_hides_and_refuses_transform_preview() {
        use ferrika_governance::test_policy::{self, MemoryLog};
        let g = context();
        let forbidden = test_policy::export_forbidden();
        let listed = tool_definitions(peek_listed(&forbidden, &g));
        assert!(!listed
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["name"] == "transform_preview"));
        // RabbitMQ listings carry no record data and stay.
        assert!(listed
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["name"] == "rabbit_queues"));

        let log = MemoryLog::default();
        let call = json!({
            "name": "transform_preview",
            "arguments": { "topic": "orders", "steps": [{ "drop": "user.email" }] }
        });
        let refused = call_tool(&call, &g, &forbidden, &log, Some("mcp:t/1#00")).await;
        assert_eq!(refused["isError"], true);
        let entries = log.entries.take();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "policy.blocked");
        assert_eq!(entries[0].target, "export:mcp transform_preview orders");

        // Audited: recorded before reading; bad steps then fail without
        // connecting.
        let audited = test_policy::export_audited();
        let log = MemoryLog::default();
        let bad =
            json!({ "name": "transform_preview", "arguments": { "topic": "orders", "steps": [] } });
        let result = call_tool(&bad, &g, &audited, &log, None).await;
        assert_eq!(result["isError"], true);
        assert_eq!(log.entries.borrow()[0].action, "record.export");
    }

    #[test]
    fn message_histogram_arguments() {
        let now = 1_730_538_000_000;
        let req = histogram_request(&json!({ "topic": "orders" }), now).unwrap();
        assert_eq!(
            req.range,
            ferrika_ops::time_histogram::TimeRange::Since(Duration::from_secs(86_400))
        );
        let req = histogram_request(
            &json!({ "topic": "orders", "from": "2024-11-02T08:00:00Z", "bucket": "5m", "partition": 2 }),
            now,
        )
        .unwrap();
        assert_eq!(
            req.range,
            ferrika_ops::time_histogram::TimeRange::Between(now - 3_600_000, now)
        );
        assert_eq!(req.partitions, Some(vec![2]));
        assert!(histogram_request(&json!({}), now).is_err());
        // 7 days of 1-minute buckets is far more than 200.
        let err = histogram_request(&json!({ "topic": "t", "since": "7d", "bucket": "1m" }), now)
            .unwrap_err();
        assert!(err.to_string().contains("200"), "{err}");
    }

    #[tokio::test]
    async fn schema_fields_is_metadata_and_stays_outside_the_export_gate() {
        use ferrika_governance::test_policy::{self, MemoryLog};
        let g = context();
        // No record data: offered and not recorded as an export, also under
        // `export: forbidden`.
        let forbidden = test_policy::export_forbidden();
        let listed = tool_definitions(peek_listed(&forbidden, &g));
        assert!(listed
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["name"] == "schema_fields"));
        let log = MemoryLog::default();
        let call = json!({ "name": "schema_fields", "arguments": { "topic": "orders" } });
        let reply = call_tool(&call, &g, &forbidden, &log, Some("mcp:t/1#00")).await;
        // The test profile has no registry: the tool says so, nothing is audited.
        assert_eq!(reply["isError"], true);
        let text = reply["content"][0]["text"].as_str().unwrap_or("");
        assert!(text.contains("Schema Registry URL"), "{text}");
        assert!(log.entries.take().is_empty());
    }

    #[test]
    fn schema_fields_reads_the_topic_value_or_key_subject() {
        assert_eq!(
            fields_subject(&json!({ "topic": "orders" })).unwrap(),
            "orders-value"
        );
        assert_eq!(
            fields_subject(&json!({ "topic": "orders", "key": true })).unwrap(),
            "orders-key"
        );
        assert_eq!(fields_subject(&json!({ "subject": "s" })).unwrap(), "s");
        assert!(fields_subject(&json!({})).is_err());
        assert!(fields_subject(&json!({ "topic": "t", "subject": "s" })).is_err());
        assert!(fields_subject(&json!({ "subject": "s", "key": true })).is_err());
    }

    #[test]
    fn results_carry_structured_content_when_they_are_objects() {
        let reply = tool_reply(Ok(json!({ "count": 2, "topics": ["a", "b"] })));
        assert_eq!(reply["isError"], false);
        assert_eq!(reply["structuredContent"]["count"], 2);
        let text: Value =
            serde_json::from_str(reply["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(text, reply["structuredContent"]);
        // Structured content must be an object: an array stays text only.
        assert!(tool_reply(Ok(json!([1, 2])))
            .get("structuredContent")
            .is_none());
        let failed = tool_reply(Err(KafkaError::config("nope")));
        assert_eq!(failed["isError"], true);
        assert!(failed.get("structuredContent").is_none());
    }

    #[tokio::test]
    async fn message_histogram_without_topic_is_an_error() {
        use ferrika_governance::test_policy::MemoryLog;
        let g = context();
        let call = json!({ "name": "message_histogram", "arguments": {} });
        let result = call_tool(
            &call,
            &g,
            &ferrika_governance::test_policy::none(),
            &MemoryLog::default(),
            None,
        )
        .await;
        assert_eq!(result["isError"], true);
        assert!(tool_definitions(false)
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["name"] == "message_histogram"));
    }

    #[test]
    fn histogram_json_has_counts_but_no_partition_detail() {
        use ferrika_core::kafka::consumer::Lookup;
        let slots = vec![vec![
            Some(Lookup::At(0)),
            Some(Lookup::At(2)),
            Some(Lookup::End),
        ]];
        let from = 1_730_538_000_000;
        let hist = ferrika_ops::time_histogram::assemble(
            "orders",
            &[from, from + 60_000, from + 120_000],
            &[0],
            &slots,
            &[(0, 5)],
            i64::MAX,
        );
        let v = histogram_json(&hist);
        assert_eq!(v["buckets"][1]["count"], 3);
        assert_eq!(v["peak"]["start"], "2024-11-02T09:01:00Z");
        assert!(v["buckets"][0].get("by_partition").is_none());
    }

    #[test]
    fn record_text_masks_and_shortens() {
        let masked = record_text(Some(b"mail jane.doe@example.com now"), true);
        assert!(!masked.as_str().unwrap().contains("jane.doe@example.com"));
        assert_eq!(record_text(Some(&[0xff, 0x00]), false), json!("hex:ff00"));
        let long = "x".repeat(PEEK_TEXT_MAX + 10);
        let text = record_text(Some(long.as_bytes()), false);
        assert_eq!(text.as_str().unwrap().chars().count(), PEEK_TEXT_MAX + 1);
    }

    #[test]
    fn record_text_hides_binary_when_masked() {
        let out = record_text(Some(&[0xff, 0xfe, 0x01]), true);
        let text = out.as_str().unwrap();
        assert!(!text.contains("hex:") && !text.contains("fffe"), "{text}");
        assert!(text.contains("binary 3 bytes"), "{text}");
    }

    #[test]
    fn masked_where_cannot_recover_masked_values() {
        let m = ferrika_core::kafka::consumer::ConsumedMessage {
            topic: "users".into(),
            partition: 0,
            offset: 1,
            key: None,
            payload: Some(br#"{"rrn":"900101-1234567"}"#.to_vec()),
            timestamp_ms: None,
            headers: Vec::from([("auth_token".to_string(), b"abc123".to_vec())]),
        };
        let rrn = ferrika_core::model::expr::Expr::parse(r#"$.rrn =~ "^900101-1234""#).unwrap();
        let header =
            ferrika_core::model::expr::Expr::parse(r#"headers.auth_token == "abc123""#).unwrap();
        assert!(where_matches(&rrn, &m, None, None, false));
        assert!(!where_matches(&rrn, &m, None, None, true));
        // The masked rendering is still searchable.
        let shown = ferrika_core::model::expr::Expr::parse(r#"$.rrn == "****""#).unwrap();
        assert!(where_matches(&shown, &m, None, None, true));
        let decoded = ferrika_core::codec::schema::DecodedPayload {
            text: r#"{"rrn":"900101-1234567"}"#.into(),
            label: "AVRO schema 1".into(),
            schema_ref: Some(ferrika_core::model::schema::SchemaRef::id(1)),
        };
        assert!(where_matches(&rrn, &m, Some(&decoded), None, false));
        assert!(!where_matches(&rrn, &m, Some(&decoded), None, true));
        assert!(where_matches(&header, &m, None, None, false));
        assert!(!where_matches(&header, &m, None, None, true));
    }
}
