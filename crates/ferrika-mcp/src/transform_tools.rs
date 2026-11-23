//! Record transforms over MCP: the step list `transform_preview` (a read
//! tool) and `transform_execute` (a write tool) share, and the preview.
//!
//! The steps are those of `ferrika transform` ([`ferrika_ops::transform`]),
//! given as a list of one-key objects in the order they apply:
//! `[{"filter": "$.amount > 100"}, {"drop": "user.email"}, {"mask": "partial"}]`.
//!
//! A preview reads a bounded range (from the beginning or a timestamp up to
//! the end each partition had when it started), never joins a consumer
//! group and never writes. Its records are PII-masked unless the caller
//! turns masking off; under a policy that enforces masking a mask step is
//! put first, so the other steps (a filter in particular) see only masked
//! data, as the CLI requires `--mask` first.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_core::kafka::consumer::StartOffset;
use ferrika_core::kafka::consumer::{ConsumedMessage, KafkaConsumerClient};
use ferrika_core::kafka::context::ResolvedContext;
use ferrika_core::kafka::producer::OwnedProducerRecord;
use ferrika_governance::policy::EffectivePolicy;
use ferrika_ops::transform::{Pipeline, StepKind, TransformCounters, MAX_STEPS};

use super::read_tools::record_text;

/// Most records `transform_preview` returns, and its default.
pub(super) const PREVIEW_MAX: u64 = 50;
const PREVIEW_DEFAULT: u64 = 10;
/// Records a preview reads at most, by default and at most.
const SCAN_DEFAULT: u64 = 5_000;
const SCAN_MAX: u64 = 100_000;
/// Most records one `transform_execute` call writes.
pub(super) const EXECUTE_MAX: u64 = 10_000;
/// How long a preview waits for the next record, and reads in all.
const PREVIEW_IDLE: Duration = Duration::from_secs(2);
const PREVIEW_BUDGET: Duration = Duration::from_secs(20);

/// The steps as given: kind and argument text, in order.
pub(super) type StepSpecs = Vec<(StepKind, String)>;

/// Where a transform starts reading. Both kinds stop at the end each
/// partition had when the run started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Start {
    /// The earliest offset of each partition.
    Beginning,
    /// Each partition's first record at or after this epoch millisecond.
    Timestamp(i64),
}

impl Start {
    /// The start as plans and results show it.
    pub(super) fn label(self) -> String {
        match self {
            Start::Beginning => "beginning".to_string(),
            Start::Timestamp(ms) => jiff::Timestamp::from_millisecond(ms)
                .map(ferrika_core::model::time::rfc3339)
                .unwrap_or_else(|_| ms.to_string()),
        }
    }
}

/// The JSON schema of `steps`, shared by both tools' definitions.
pub(super) fn steps_schema() -> Value {
    json!({
        "type": "array",
        "minItems": 1,
        "maxItems": MAX_STEPS,
        "description": "Steps in the order they apply, each an object with one key: \
            {\"filter\": \"$.amount > 100\"} (keep matching records; the syntax of peek_messages' where), \
            {\"drop\": \"user.email\"} (remove a value field), \
            {\"rename\": \"old=new\"} (move a field), \
            {\"set\": \"env=\\\"staging\\\"\"} (set a field to a JSON value), \
            {\"rekey\": \"user.id\"} (key from a field), \
            {\"mask\": \"partial\"} or {\"mask\": \"redact\"} (mask PII in key, value and headers), \
            {\"sample\": 10} (keep one record in 10). Field steps work on JSON object values; others pass through.",
        "items": {
            "type": "object",
            "minProperties": 1,
            "maxProperties": 1,
            "properties": {
                "filter": { "type": "string" },
                "drop": { "type": "string" },
                "rename": { "type": "string" },
                "set": { "type": "string" },
                "rekey": { "type": "string" },
                "mask": { "enum": ["partial", "redact"] },
                "sample": { "type": "integer", "minimum": 1 }
            },
            "additionalProperties": false
        }
    })
}

fn step_kind(name: &str) -> Option<StepKind> {
    StepKind::ALL.into_iter().find(|k| k.name() == name)
}

/// `steps` from tool arguments, checked for shape (the step texts are
/// checked by [`build_pipeline`]).
pub(super) fn parse_steps(value: Option<&Value>) -> Result<StepSpecs, String> {
    let Some(Value::Array(items)) = value else {
        return Err(
            "'steps' must be an array of steps, e.g. [{\"filter\": \"$.amount > 100\"}, \
             {\"drop\": \"user.email\"}]"
                .to_string(),
        );
    };
    if items.is_empty() {
        return Err("'steps' is empty: give at least one step".to_string());
    }
    if items.len() > MAX_STEPS {
        return Err(format!("Too many steps (max {MAX_STEPS})"));
    }
    let names: Vec<&str> = StepKind::ALL.iter().map(|k| k.name()).collect();
    let mut specs = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        let one = item.as_object().filter(|m| m.len() == 1);
        let Some((name, arg)) = one.and_then(|m| m.iter().next()) else {
            return Err(format!(
                "steps[{i}] must be an object with one key: {}",
                names.join(", ")
            ));
        };
        let Some(kind) = step_kind(name) else {
            return Err(format!(
                "steps[{i}]: unknown step '{name}' (expected: {})",
                names.join(", ")
            ));
        };
        let text = match (kind, arg) {
            (_, Value::String(text)) => text.clone(),
            (StepKind::Sample, Value::Number(n)) => n.to_string(),
            (StepKind::Mask, Value::Null) => "partial".to_string(),
            _ => return Err(format!("steps[{i}].{name} must be a string")),
        };
        specs.push((kind, text));
    }
    Ok(specs)
}

/// Whether the steps start with a mask step.
pub(super) fn masks_first(specs: &[(StepKind, String)]) -> bool {
    specs
        .first()
        .is_some_and(|(kind, _)| *kind == StepKind::Mask)
}

/// The pipeline of `specs`. With `prepend_mask` a partial mask step goes
/// first unless the steps already start with one; the flag returned says
/// whether it was added.
pub(super) fn build_pipeline(
    specs: &[(StepKind, String)],
    prepend_mask: bool,
) -> Result<(Pipeline, bool), String> {
    let added = prepend_mask && !masks_first(specs);
    let mask = (StepKind::Mask, "partial".to_string());
    let all = added.then_some(&mask).into_iter().chain(specs.iter());
    let pipeline = Pipeline::parse(all.map(|(kind, text)| (*kind, text.as_str())))?;
    Ok((pipeline, added))
}

/// `from_beginning` / `from_timestamp`. `now_ms` resolves relative times
/// (`30m`, `2h`); without it only absolute ones are accepted (a write's
/// plan and execute must name the same range).
pub(super) fn parse_start(args: &Value, now_ms: Option<i64>) -> Result<Start, String> {
    let beginning = match args.get("from_beginning") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(b)) => Some(*b),
        Some(_) => return Err("'from_beginning' must be true or false".to_string()),
    };
    let bad_time = |text: &str| {
        let kinds = if now_ms.is_some() {
            "ISO 8601 (2026-11-02T09:00:00Z), epoch ms or a time ago (30m, 2h, 1d)"
        } else {
            "ISO 8601 (2026-11-02T09:00:00Z) or epoch ms"
        };
        format!("'from_timestamp' must be {kinds}, not '{text}'")
    };
    let timestamp = match args.get("from_timestamp") {
        None | Some(Value::Null) => None,
        Some(Value::Number(n)) => Some(
            n.as_i64()
                .filter(|ms| *ms > 0)
                .ok_or_else(|| bad_time(&n.to_string()))?,
        ),
        Some(Value::String(text)) => {
            let text = text.trim();
            let parsed = ferrika_core::model::time::parse_time_ms_in(text, now_ms.unwrap_or(0), 0)
                .filter(|ms| *ms > 0);
            // Without a clock a relative time would resolve against 0.
            let relative = text.chars().last().is_some_and(|c| "smhd".contains(c));
            match parsed {
                Some(ms) if now_ms.is_some() || !relative => Some(ms),
                _ => return Err(bad_time(text)),
            }
        }
        Some(_) => return Err("'from_timestamp' must be a string or a number".to_string()),
    };
    match (beginning, timestamp) {
        (Some(true), Some(_)) => Err("Give from_beginning or from_timestamp, not both".to_string()),
        (_, Some(ms)) => Ok(Start::Timestamp(ms)),
        (Some(false), None) => Err(
            "from_beginning false needs from_timestamp: a transform reads up to the end each \
             partition had when it started, so from the current end there is nothing to read"
                .to_string(),
        ),
        (Some(true) | None, None) => Ok(Start::Beginning),
    }
}

/// `partition`, when given.
pub(super) fn parse_partition(args: &Value) -> Result<Option<i32>, String> {
    match args.get("partition") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_i64()
            .filter(|p| (0..=i32::MAX as i64).contains(p))
            .map(|p| Some(p as i32))
            .ok_or_else(|| "'partition' must be a partition number (0 or more)".to_string()),
    }
}

/// A boolean argument, `default` when absent.
pub(super) fn parse_flag(args: &Value, name: &str, default: bool) -> Result<bool, String> {
    match args.get(name) {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Bool(b)) => Ok(*b),
        Some(_) => Err(format!("'{name}' must be true or false")),
    }
}

/// A `transform_preview` call, checked.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct PreviewRequest {
    pub topic: String,
    pub specs: StepSpecs,
    pub start: Start,
    pub partition: Option<i32>,
    pub max_records: u64,
    pub scan_limit: u64,
    pub decode: bool,
    /// Mask the returned text (`mask_pii`, forced on by the policy).
    pub mask: bool,
    /// Put a mask step first (the policy enforces masking).
    pub prepend_mask: bool,
}

/// The preview request from tool arguments. `enforced`: the policy
/// enforces PII masking, which overrides `mask_pii: false` and puts a mask
/// step first.
pub(super) fn preview_request(
    args: &Value,
    now_ms: i64,
    enforced: bool,
) -> Result<PreviewRequest, String> {
    let topic = args
        .get("topic")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| "'topic' is required".to_string())?
        .to_string();
    let specs = parse_steps(args.get("steps"))?;
    let count = |name: &str, default: u64, max: u64| -> Result<u64, String> {
        match args.get(name) {
            None | Some(Value::Null) => Ok(default),
            Some(v) => v
                .as_u64()
                .filter(|n| *n >= 1)
                .map(|n| n.min(max))
                .ok_or_else(|| format!("'{name}' must be a whole number from 1 to {max}")),
        }
    };
    Ok(PreviewRequest {
        topic,
        specs,
        start: parse_start(args, Some(now_ms))?,
        partition: parse_partition(args)?,
        max_records: count("max_records", PREVIEW_DEFAULT, PREVIEW_MAX)?,
        scan_limit: count("scan_limit", SCAN_DEFAULT, SCAN_MAX)?,
        decode: parse_flag(args, "decode_wire_format", false)?,
        mask: parse_flag(args, "mask_pii", true)? || enforced,
        prepend_mask: enforced,
    })
}

/// One transformed record as the preview returns it: where it came from,
/// the key and value text (masked when asked, shortened) and the header
/// names (header values are not returned).
pub(super) fn preview_record(
    source: &ConsumedMessage,
    record: &OwnedProducerRecord,
    mask: bool,
) -> Value {
    let mut headers: Vec<&str> = record.headers.iter().map(|(k, _)| k.as_str()).collect();
    headers.sort();
    headers.dedup();
    json!({
        "source_partition": source.partition,
        "source_offset": source.offset,
        "timestamp": record
            .timestamp
            .and_then(|ms| jiff::Timestamp::from_millisecond(ms).ok())
            .map(ferrika_core::model::time::rfc3339),
        "key": record_text(record.key.as_deref(), mask),
        "value": record_text(record.payload.as_deref(), mask),
        "header_keys": headers,
    })
}

/// What a preview read and what the steps did with it.
#[derive(Debug, Default)]
struct PreviewTally {
    read: u64,
    kept: u64,
    counters: TransformCounters,
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_secs(10))
}

/// `transform_preview`: reads the range, applies the steps and returns up
/// to `max_records` transformed records with the counts. Writes nothing.
pub(super) async fn preview(
    context: &ResolvedContext,
    policy: &EffectivePolicy,
    args: &Value,
) -> KafkaResult<Value> {
    let now = ferrika_core::kafka::clock::now_ms();
    let request =
        preview_request(args, now, policy.masking_enforced()).map_err(KafkaError::config)?;
    let (pipeline, mask_added) =
        build_pipeline(&request.specs, request.prepend_mask).map_err(KafkaError::config)?;
    let topic = request.topic.as_str();

    let config = context.config.clone();
    let consumer = KafkaConsumerClient::scanner(&config)?;
    let mut decoder = request.decode.then(|| {
        ferrika_core::kafka::schema::PayloadDecoder::new(config.schema_registry_url.as_deref())
            .with_timeout(config.request_timeout)
    });
    let mut partitions = consumer.topic_partitions(topic, timeout())?;
    if let Some(p) = request.partition {
        if !partitions.contains(&p) {
            return Err(KafkaError::config(format!(
                "'{topic}' has no partition {p}"
            )));
        }
        partitions = vec![p];
    }
    let starts: Vec<(i32, Option<i64>)> = match request.start {
        Start::Beginning => partitions.iter().map(|&p| (p, None)).collect(),
        Start::Timestamp(ms) => {
            let keys: Vec<(&str, i32)> = partitions.iter().map(|&p| (topic, p)).collect();
            let lookup = consumer.offsets_for_timestamp(&keys, ms, timeout())?;
            ferrika_core::kafka::consumer::timestamp_start_offsets(topic, &partitions, &lookup)
                .into_iter()
                .map(|(p, offset)| match offset {
                    StartOffset::At(o) => (p, Some(o)),
                    // Nothing at or after the timestamp: start at the end.
                    _ => (p, Some(i64::MAX)),
                })
                .collect()
        }
    };
    // Where each partition starts, and the end it has now.
    let mut assign = Vec::new();
    let mut ends = BTreeMap::new();
    for (p, from) in starts {
        let (low, high) = consumer.fetch_watermarks_async(topic, p, timeout()).await?;
        let first = from.unwrap_or(low).clamp(low, high);
        if first < high {
            assign.push((topic, p, StartOffset::At(first)));
            ends.insert(p, high);
        }
    }

    let mut tally = PreviewTally::default();
    let mut records = Vec::new();
    let mut timed_out = false;
    if !assign.is_empty() {
        consumer.assign_with_offsets(&assign)?;
        let deadline = Instant::now() + PREVIEW_BUDGET;
        while tally.read < request.scan_limit
            && !ends.is_empty()
            && (records.len() as u64) < request.max_records
        {
            let left = deadline
                .saturating_duration_since(Instant::now())
                .min(PREVIEW_IDLE);
            if left.is_zero() {
                timed_out = true;
                break;
            }
            // A transaction marker at the end of a partition is never
            // delivered, so a quiet spell also ends the read.
            let Ok(Ok(m)) = tokio::time::timeout(left, consumer.recv()).await else {
                break;
            };
            let Some(&end) = ends.get(&m.partition) else {
                continue;
            };
            if m.offset + 1 >= end {
                ends.remove(&m.partition);
            }
            if m.offset >= end {
                continue;
            }
            tally.read += 1;
            let decoded = match (decoder.as_mut(), m.payload.as_deref()) {
                (Some(decoder), Some(payload)) => decoder
                    .decode(payload)
                    .await
                    .and_then(|d| serde_json::from_str::<Value>(&d.text).ok()),
                _ => None,
            };
            if let Some(record) = pipeline.apply(&m, decoded, topic, &mut tally.counters) {
                tally.kept += 1;
                records.push(preview_record(&m, &record, request.mask));
            }
        }
    }
    Ok(preview_result(
        &request,
        &pipeline,
        mask_added,
        &tally,
        records,
        ends.is_empty(),
        timed_out,
    ))
}

/// The preview's result object.
fn preview_result(
    request: &PreviewRequest,
    pipeline: &Pipeline,
    mask_added: bool,
    tally: &PreviewTally,
    records: Vec<Value>,
    complete: bool,
    timed_out: bool,
) -> Value {
    let mut notes = Vec::new();
    if mask_added {
        notes.push(
            "The company policy enforces PII masking, so a mask step was put first: the other \
             steps saw masked data. transform_execute needs {\"mask\": ...} as its first step."
                .to_string(),
        );
    }
    if !complete {
        notes.push(format!(
            "Stopped before the end of the range ({}); the counts cover the records read.",
            if timed_out {
                "time limit".to_string()
            } else if tally.read >= request.scan_limit {
                format!("scan_limit {}", request.scan_limit)
            } else if records.len() as u64 >= request.max_records {
                format!("max_records {}", request.max_records)
            } else {
                "no more records arrived".to_string()
            }
        ));
    }
    json!({
        "topic": request.topic,
        "start": request.start.label(),
        "partition": request.partition,
        "written": 0,
        "masked": request.mask,
        "mask_step_added": mask_added,
        "steps": pipeline.steps().len(),
        "steps_sha256": pipeline.digest(),
        "summary": {
            "read": tally.read,
            "kept": tally.kept,
            "returned": records.len(),
            "filtered": tally.counters.filtered,
            "sampled_out": tally.counters.sampled_out,
            "not_json": tally.counters.not_json,
            "rekey_missing": tally.counters.rekey_missing,
        },
        "complete": complete,
        "notes": notes,
        "records": records,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_730_538_000_000;

    #[test]
    fn steps_parse_in_order_and_refuse_bad_shapes() {
        let specs = parse_steps(Some(&json!([
            { "filter": "$.amount > 100" },
            { "drop": "user.email" },
            { "sample": 10 },
            { "mask": null }
        ])))
        .unwrap();
        assert_eq!(
            specs,
            vec![
                (StepKind::Filter, "$.amount > 100".to_string()),
                (StepKind::Drop, "user.email".to_string()),
                (StepKind::Sample, "10".to_string()),
                (StepKind::Mask, "partial".to_string()),
            ]
        );
        assert!(parse_steps(None).is_err());
        assert!(parse_steps(Some(&json!([]))).is_err());
        assert!(parse_steps(Some(&json!([{ "filter": "a", "drop": "b" }]))).is_err());
        assert!(parse_steps(Some(&json!([{ "explode": "x" }])))
            .unwrap_err()
            .contains("unknown step"));
        assert!(parse_steps(Some(&json!([{ "drop": 3 }]))).is_err());
        let too_many: Vec<Value> = (0..=MAX_STEPS).map(|_| json!({ "drop": "a" })).collect();
        assert!(parse_steps(Some(&Value::Array(too_many))).is_err());
    }

    #[test]
    fn enforced_masking_puts_a_mask_step_first_once() {
        let specs = [(StepKind::Filter, "$.rrn == \"x\"".to_string())];
        let (pipeline, added) = build_pipeline(&specs, true).unwrap();
        assert!(added);
        assert!(pipeline.masks_first());
        assert_eq!(pipeline.steps().len(), 2);
        // Already masking first: nothing added.
        let masked = [
            (StepKind::Mask, "redact".to_string()),
            (StepKind::Drop, "a".to_string()),
        ];
        let (pipeline, added) = build_pipeline(&masked, true).unwrap();
        assert!(!added);
        assert_eq!(pipeline.steps().len(), 2);
        // Not enforced: the steps as given.
        let (pipeline, added) = build_pipeline(&specs, false).unwrap();
        assert!(!added && !pipeline.masks_first());
        // A bad step text is refused.
        assert!(build_pipeline(&[(StepKind::Sample, "0".to_string())], false).is_err());
    }

    #[test]
    fn start_defaults_to_the_beginning_and_takes_timestamps() {
        assert_eq!(parse_start(&json!({}), Some(NOW)), Ok(Start::Beginning));
        assert_eq!(
            parse_start(&json!({ "from_beginning": true }), Some(NOW)),
            Ok(Start::Beginning)
        );
        assert_eq!(
            parse_start(&json!({ "from_timestamp": 1_700_000_000_000i64 }), None),
            Ok(Start::Timestamp(1_700_000_000_000))
        );
        assert_eq!(
            parse_start(&json!({ "from_timestamp": "1h" }), Some(NOW)),
            Ok(Start::Timestamp(NOW - 3_600_000))
        );
        assert!(parse_start(&json!({ "from_timestamp": "2024-11-02T08:00:00Z" }), None).is_ok());
        // A write cannot use a relative time.
        assert!(parse_start(&json!({ "from_timestamp": "1h" }), None).is_err());
        assert!(parse_start(
            &json!({ "from_beginning": true, "from_timestamp": 5 }),
            Some(NOW)
        )
        .is_err());
        assert!(parse_start(&json!({ "from_beginning": false }), Some(NOW)).is_err());
        assert!(parse_start(&json!({ "from_timestamp": "soon" }), Some(NOW)).is_err());
        assert!(parse_start(&json!({ "from_timestamp": -5 }), Some(NOW)).is_err());
    }

    #[test]
    fn preview_request_caps_counts_and_forces_masking() {
        let args = json!({
            "topic": "orders",
            "steps": [{ "drop": "user.email" }],
            "max_records": 500,
            "scan_limit": 1_000_000,
            "mask_pii": false
        });
        let request = preview_request(&args, NOW, false).unwrap();
        assert_eq!(request.max_records, PREVIEW_MAX);
        assert_eq!(request.scan_limit, SCAN_MAX);
        assert!(!request.mask && !request.prepend_mask);
        let enforced = preview_request(&args, NOW, true).unwrap();
        assert!(enforced.mask && enforced.prepend_mask);
        let defaults = preview_request(
            &json!({ "topic": "t", "steps": [{ "sample": 2 }] }),
            NOW,
            false,
        )
        .unwrap();
        assert_eq!(defaults.max_records, PREVIEW_DEFAULT);
        assert!(defaults.mask);
        assert!(preview_request(&json!({ "steps": [{ "sample": 2 }] }), NOW, false).is_err());
        assert!(preview_request(
            &json!({ "topic": "t", "steps": [{ "sample": 2 }], "max_records": 0 }),
            NOW,
            false
        )
        .is_err());
    }

    fn message(value: &str) -> ConsumedMessage {
        ConsumedMessage {
            topic: "users".into(),
            partition: 2,
            offset: 7,
            key: Some(b"k1".to_vec()),
            payload: Some(value.as_bytes().to_vec()),
            timestamp_ms: Some(1_700_000_000_000),
            headers: vec![("auth_token".to_string(), b"abc123".to_vec())],
        }
    }

    #[test]
    fn previewed_records_are_transformed_and_masked() {
        let m = message(r#"{"email":"jane.doe@example.com","amount":120,"secret":"x"}"#);
        let specs = [
            (StepKind::Filter, "$.amount > 100".to_string()),
            (StepKind::Drop, "secret".to_string()),
        ];
        let (pipeline, _) = build_pipeline(&specs, false).unwrap();
        let mut counters = TransformCounters::default();
        let record = pipeline.apply(&m, None, "users", &mut counters).unwrap();
        let shown = preview_record(&m, &record, true);
        let value = shown["value"].as_str().unwrap();
        assert!(!value.contains("jane.doe@example.com"), "{value}");
        assert!(!value.contains("secret"), "{value}");
        assert_eq!(shown["source_offset"], 7);
        assert_eq!(shown["header_keys"], json!(["auth_token"]));
        // Header values are never returned.
        assert!(!shown.to_string().contains("abc123"));
        // Unmasked on request (no enforcing policy).
        let plain = preview_record(&m, &record, false);
        assert!(plain["value"]
            .as_str()
            .unwrap()
            .contains("jane.doe@example.com"));
        // A filtered record is counted, not returned.
        let low = message(r#"{"amount":5}"#);
        assert!(pipeline.apply(&low, None, "users", &mut counters).is_none());
        assert_eq!(counters.filtered, 1);
    }

    #[test]
    fn the_result_carries_counts_and_notes_but_no_steps() {
        let request = preview_request(
            &json!({ "topic": "t", "steps": [{ "filter": "$.card == \"4111\"" }] }),
            NOW,
            true,
        )
        .unwrap();
        let (pipeline, added) = build_pipeline(&request.specs, request.prepend_mask).unwrap();
        let tally = PreviewTally {
            read: 3,
            kept: 1,
            counters: TransformCounters {
                filtered: 2,
                ..Default::default()
            },
        };
        let result = preview_result(&request, &pipeline, added, &tally, Vec::new(), true, false);
        assert_eq!(result["summary"]["read"], 3);
        assert_eq!(result["summary"]["filtered"], 2);
        assert_eq!(result["written"], 0);
        assert_eq!(result["mask_step_added"], true);
        assert_eq!(result["steps_sha256"].as_str().unwrap().len(), 64);
        assert!(!result.to_string().contains("4111"));
    }
}
