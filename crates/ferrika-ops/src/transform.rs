//! Record transforms for `ferrika transform`: a short pipeline of steps
//! (filter, drop, rename, set, rekey, mask, sample) applied in order to each
//! record copied from one topic to another.
//!
//! Field steps work on record values that are JSON objects. Any other value
//! (text, binary, a JSON scalar or array) passes through them untouched and
//! is counted in [`TransformCounters::not_json`]; a filter on a `$.` field
//! of such a value is false. A value no step changed is written byte for
//! byte as it was read.
//!
//! Filters use the record expression syntax of `consume --where`
//! ([`ferrika_core::model::expr`]): `$.user.country == "KR"`,
//! `$.amount > 100`, `key =~ "^order-"`, `headers.source == "web"`.
//!
//! Headers with a null value are written as null in their original
//! positions. No step changes them: `mask` leaves them null (there is
//! nothing to mask) and a filter reads them as missing, as `consume --where`
//! does (`headers.<name> exists` is false for them).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use ferrika_core::codec::pii::{mask_bytes, mask_header_value, mask_json_value, MaskStrategy};
use ferrika_core::error::KafkaError;
use ferrika_core::kafka::consumer::ConsumedMessage;
use ferrika_core::kafka::producer::OwnedProducerRecord;
use ferrika_core::kafka::schema::PayloadDecoder;

use crate::mirror::{MirrorStats, RecordTransform};
use ferrika_core::model::expr::{expression_text, Expr, RecordView};

/// The most steps one pipeline may hold.
pub const MAX_STEPS: usize = 64;
/// The most segments a field path may have.
const MAX_PATH_DEPTH: usize = 64;

/// The kind of a step, as its command-line flag names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StepKind {
    Filter,
    Drop,
    Rename,
    Set,
    Rekey,
    Mask,
    Sample,
}

impl StepKind {
    /// Every kind, in the order the flags are documented.
    pub const ALL: [StepKind; 7] = [
        StepKind::Filter,
        StepKind::Drop,
        StepKind::Rename,
        StepKind::Set,
        StepKind::Rekey,
        StepKind::Mask,
        StepKind::Sample,
    ];

    /// The step's name, which is also its long flag without `--`.
    pub fn name(self) -> &'static str {
        match self {
            StepKind::Filter => "filter",
            StepKind::Drop => "drop",
            StepKind::Rename => "rename",
            StepKind::Set => "set",
            StepKind::Rekey => "rekey",
            StepKind::Mask => "mask",
            StepKind::Sample => "sample",
        }
    }
}

/// A dotted path to a field of a JSON value: `user.address.city`, or
/// `items.0` for an array element. A leading `$.` (or `.`) is accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldPath(Vec<String>);

impl FieldPath {
    pub fn parse(text: &str) -> Result<Self, String> {
        let trimmed = text.trim();
        let body = trimmed
            .strip_prefix("$.")
            .or_else(|| trimmed.strip_prefix('.'))
            .unwrap_or(trimmed);
        if body.is_empty() {
            return Err("A field path is empty (e.g. user.email)".to_string());
        }
        let segments: Vec<String> = body.split('.').map(str::to_string).collect();
        if segments.iter().any(String::is_empty) {
            return Err(format!("Field path '{trimmed}' has an empty segment"));
        }
        if segments.len() > MAX_PATH_DEPTH {
            return Err(format!(
                "Field path '{trimmed}' is too deep (max {MAX_PATH_DEPTH} segments)"
            ));
        }
        Ok(Self(segments))
    }

    /// The value at this path, if there is one.
    pub fn get<'v>(&self, root: &'v Value) -> Option<&'v Value> {
        self.0
            .iter()
            .try_fold(root, |current, segment| child(current, segment))
    }

    /// Removes and returns the value at this path. Object fields keep the
    /// order of the others.
    pub fn remove(&self, root: &mut Value) -> Option<Value> {
        let (last, parents) = self.0.split_last()?;
        let mut current = root;
        for segment in parents {
            current = child_mut(current, segment)?;
        }
        match current {
            Value::Object(map) => map.shift_remove(last.as_str()),
            Value::Array(items) => {
                let index = last.parse::<usize>().ok()?;
                (index < items.len()).then(|| items.remove(index))
            }
            _ => None,
        }
    }

    /// Sets the value at this path, creating missing parent objects.
    /// Returns `false` (and changes nothing) when a parent is neither an
    /// object nor an array, or an array index is out of range.
    pub fn set(&self, root: &mut Value, value: Value) -> bool {
        let Some((last, parents)) = self.0.split_last() else {
            return false;
        };
        // Check the whole path first so that a failure changes nothing.
        let mut probe: Option<&Value> = Some(root);
        for segment in parents {
            match probe {
                Some(Value::Object(_)) | None => {}
                Some(Value::Array(items)) => {
                    let in_range = segment.parse::<usize>().is_ok_and(|i| i < items.len());
                    if !in_range {
                        return false;
                    }
                }
                Some(_) => return false,
            }
            probe = probe.and_then(|current| child(current, segment));
        }
        match probe {
            Some(Value::Object(_)) | None => {}
            Some(Value::Array(items)) => {
                if !last.parse::<usize>().is_ok_and(|i| i < items.len()) {
                    return false;
                }
            }
            Some(_) => return false,
        }

        let mut current = root;
        for segment in parents {
            current = match current {
                Value::Object(map) => map
                    .entry(segment.clone())
                    .or_insert_with(|| Value::Object(serde_json::Map::new())),
                Value::Array(items) => match segment.parse::<usize>().ok() {
                    Some(index) => match items.get_mut(index) {
                        Some(item) => item,
                        None => return false,
                    },
                    None => return false,
                },
                _ => return false,
            };
        }
        match current {
            Value::Object(map) => {
                map.insert(last.clone(), value);
                true
            }
            Value::Array(items) => match last.parse::<usize>().ok() {
                Some(index) if index < items.len() => {
                    items[index] = value;
                    true
                }
                _ => false,
            },
            _ => false,
        }
    }
}

fn child<'v>(value: &'v Value, segment: &str) -> Option<&'v Value> {
    match value {
        Value::Object(map) => map.get(segment),
        Value::Array(items) => items.get(segment.parse::<usize>().ok()?),
        _ => None,
    }
}

fn child_mut<'v>(value: &'v mut Value, segment: &str) -> Option<&'v mut Value> {
    match value {
        Value::Object(map) => map.get_mut(segment),
        Value::Array(items) => items.get_mut(segment.parse::<usize>().ok()?),
        _ => None,
    }
}

/// One step of a [`Pipeline`].
#[derive(Debug, Clone)]
pub enum Step {
    /// Keep only records the expression matches.
    Filter(Expr),
    /// Remove a field.
    Drop(FieldPath),
    /// Move a field to another path.
    Rename { from: FieldPath, to: FieldPath },
    /// Set a field to a JSON value.
    Set { field: FieldPath, value: Value },
    /// Take the record key from a field.
    Rekey(FieldPath),
    /// Mask PII in the key, the value and the header values.
    Mask(MaskStrategy),
    /// Keep one record in N: those whose offset is a multiple of N.
    Sample(u64),
}

impl Step {
    /// Parses the argument of the flag `kind`.
    pub fn parse(kind: StepKind, text: &str) -> Result<Self, String> {
        let flag = kind.name();
        let fail = |e: String| format!("--{flag} {text}: {e}");
        match kind {
            StepKind::Filter => Expr::parse(expression_text(text))
                .map(Step::Filter)
                .map_err(fail),
            StepKind::Drop => FieldPath::parse(text).map(Step::Drop).map_err(fail),
            StepKind::Rename => {
                let (from, to) = text
                    .split_once('=')
                    .ok_or_else(|| fail("expected OLD=NEW".to_string()))?;
                let from = FieldPath::parse(from).map_err(fail)?;
                let to = FieldPath::parse(to).map_err(fail)?;
                if from == to {
                    return Err(fail("the old and new field are the same".to_string()));
                }
                Ok(Step::Rename { from, to })
            }
            StepKind::Set => {
                let (field, value) = text
                    .split_once('=')
                    .ok_or_else(|| fail("expected FIELD=JSONVALUE".to_string()))?;
                let field = FieldPath::parse(field).map_err(fail)?;
                let value = serde_json::from_str::<Value>(value.trim()).map_err(|_| {
                    fail(
                        "the value after '=' must be JSON; quote text, e.g. env=\"staging\""
                            .to_string(),
                    )
                })?;
                Ok(Step::Set { field, value })
            }
            StepKind::Rekey => FieldPath::parse(text).map(Step::Rekey).map_err(fail),
            StepKind::Mask => match text.trim() {
                "" | "partial" => Ok(Step::Mask(MaskStrategy::Partial)),
                "redact" => Ok(Step::Mask(MaskStrategy::Redact)),
                other => Err(format!(
                    "--{flag}={other}: expected 'partial' (the default) or 'redact'"
                )),
            },
            StepKind::Sample => match text.trim().parse::<u64>() {
                Ok(n) if n >= 1 => Ok(Step::Sample(n)),
                _ => Err(fail("expected a whole number of at least 1".to_string())),
            },
        }
    }

    /// Whether this step reads or changes fields of the value.
    fn uses_fields(&self) -> bool {
        matches!(
            self,
            Step::Filter(_)
                | Step::Drop(_)
                | Step::Rename { .. }
                | Step::Set { .. }
                | Step::Rekey(_)
        )
    }
}

/// What a pipeline did besides keeping records, for the run's summary.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TransformCounters {
    /// Records a `--filter` dropped.
    pub filtered: u64,
    /// Records `--sample` left out.
    pub sampled_out: u64,
    /// Records whose value was not a JSON object (and not empty), passed
    /// through the field steps untouched.
    pub not_json: u64,
    /// Records whose `--rekey` field was missing; their key was kept.
    pub rekey_missing: u64,
}

/// The ordered steps given on the command line.
#[derive(Debug, Clone)]
pub struct Pipeline {
    steps: Vec<Step>,
    /// The steps as given, for [`Self::digest`].
    spec: Vec<(StepKind, String)>,
}

impl Pipeline {
    /// Parses the steps, in the order they apply.
    pub fn parse<'a>(specs: impl IntoIterator<Item = (StepKind, &'a str)>) -> Result<Self, String> {
        let mut steps = Vec::new();
        let mut spec = Vec::new();
        for (kind, text) in specs {
            if steps.len() >= MAX_STEPS {
                return Err(format!("Too many steps (max {MAX_STEPS})"));
            }
            steps.push(Step::parse(kind, text)?);
            spec.push((kind, text.to_string()));
        }
        Ok(Self { steps, spec })
    }

    pub fn steps(&self) -> &[Step] {
        &self.steps
    }

    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// Whether the first step masks PII, so every later step and the
    /// written record see only masked data.
    pub fn masks_first(&self) -> bool {
        matches!(self.steps.first(), Some(Step::Mask(_)))
    }

    /// Whether any step reads or changes value fields.
    pub(crate) fn uses_fields(&self) -> bool {
        self.steps.iter().any(Step::uses_fields)
    }

    /// SHA-256 (hex) of the steps as given. The audit log records this
    /// instead of the steps: a filter or `--set` can quote a record value.
    pub fn digest(&self) -> String {
        let mut text = String::new();
        for (kind, arg) in &self.spec {
            text.push_str(kind.name());
            text.push('\t');
            text.push_str(arg);
            text.push('\n');
        }
        let digest = ring::digest::digest(&ring::digest::SHA256, text.as_bytes());
        ferrika_core::model::format::to_hex(digest.as_ref())
    }

    /// Applies the steps to `msg` and returns the record to write to
    /// `dst_topic`, or `None` when a filter or the sample dropped it.
    ///
    /// `decoded` is the value already decoded to JSON (Schema Registry
    /// wire format); it is then written as JSON. The source timestamp and
    /// header order are kept; the partition is left to the producer, so a
    /// new key picks its partition the usual way.
    pub fn apply(
        &self,
        msg: &ConsumedMessage,
        decoded: Option<Value>,
        dst_topic: &str,
        counters: &mut TransformCounters,
    ) -> Option<OwnedProducerRecord> {
        let mut work = Working::new(msg, decoded);
        if self.uses_fields() && matches!(work.body, Body::Raw(Some(_))) {
            counters.not_json += 1;
        }
        for step in &self.steps {
            match step {
                Step::Filter(expr) => {
                    if !work.matches(expr, msg) {
                        counters.filtered += 1;
                        return None;
                    }
                }
                Step::Sample(n) => {
                    // `n` is at least 1 (see `Step::parse`).
                    let n = i64::try_from(*n).unwrap_or(i64::MAX);
                    if msg.offset.rem_euclid(n) != 0 {
                        counters.sampled_out += 1;
                        return None;
                    }
                }
                Step::Drop(path) => {
                    if let Body::Json(value) = &mut work.body {
                        if path.remove(value).is_some() {
                            work.value_changed = true;
                        }
                    }
                }
                Step::Rename { from, to } => {
                    if let Body::Json(value) = &mut work.body {
                        // On a copy, so a new place that cannot hold the
                        // field leaves the value as it was.
                        let mut renamed = value.clone();
                        if let Some(moved) = from.remove(&mut renamed) {
                            if to.set(&mut renamed, moved) {
                                *value = renamed;
                                work.value_changed = true;
                            }
                        }
                    }
                }
                Step::Set { field, value: new } => {
                    if let Body::Json(value) = &mut work.body {
                        if field.set(value, new.clone()) {
                            work.value_changed = true;
                        }
                    }
                }
                Step::Rekey(path) => {
                    if let Body::Json(value) = &work.body {
                        match path.get(value) {
                            Some(Value::Null) => work.key = None,
                            Some(Value::String(text)) => work.key = Some(text.clone().into_bytes()),
                            Some(other) => work.key = Some(other.to_string().into_bytes()),
                            None => counters.rekey_missing += 1,
                        }
                    }
                }
                Step::Mask(strategy) => work.mask(*strategy),
            }
        }
        Some(work.into_record(msg, dst_topic))
    }
}

/// A record value on its way through the steps.
enum Body {
    /// A JSON object the field steps work on.
    Json(Value),
    /// Anything else, as read (or as masked).
    Raw(Option<Vec<u8>>),
}

struct Working {
    key: Option<Vec<u8>>,
    body: Body,
    headers: Vec<(String, Vec<u8>)>,
    /// Null-valued headers (position, key), copied as they are.
    null_headers: Vec<(usize, String)>,
    /// Whether the value must be written from `body` rather than copied.
    value_changed: bool,
}

impl Working {
    fn new(msg: &ConsumedMessage, decoded: Option<Value>) -> Self {
        let (body, value_changed) = match decoded {
            Some(value @ Value::Object(_)) => (Body::Json(value), true),
            Some(other) => (Body::Raw(Some(other.to_string().into_bytes())), true),
            None => (parse_body(msg.payload.as_deref()), false),
        };
        Self {
            key: msg.key.clone(),
            body,
            headers: msg.headers.clone(),
            null_headers: msg.null_headers.clone(),
            value_changed,
        }
    }

    fn matches(&self, expr: &Expr, msg: &ConsumedMessage) -> bool {
        let key = self
            .key
            .as_deref()
            .map(|k| String::from_utf8_lossy(k).into_owned());
        let headers: Vec<(String, String)> = self
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).into_owned()))
            .collect();
        let (text, json) = match &self.body {
            Body::Json(value) => (None, Some(value)),
            Body::Raw(bytes) => (
                bytes
                    .as_deref()
                    .map(|b| String::from_utf8_lossy(b).into_owned()),
                None,
            ),
        };
        let view = RecordView {
            key: key.as_deref(),
            value: text.as_deref(),
            headers: &headers,
            partition: msg.partition,
            offset: msg.offset,
            timestamp_ms: msg.timestamp_ms.unwrap_or(0),
        };
        expr.matches_parsed(&view, json)
    }

    fn mask(&mut self, strategy: MaskStrategy) {
        self.key = self
            .key
            .as_deref()
            .map(|k| mask_bytes(k, strategy).into_bytes());
        for (name, value) in &mut self.headers {
            *value = mask_header_value(name, value, strategy).into_bytes();
        }
        match &mut self.body {
            Body::Json(value) => mask_json_value(value, strategy),
            Body::Raw(Some(bytes)) => *bytes = mask_bytes(bytes, strategy).into_bytes(),
            // A tombstone stays a tombstone.
            Body::Raw(None) => return,
        }
        self.value_changed = true;
    }

    fn into_record(self, msg: &ConsumedMessage, dst_topic: &str) -> OwnedProducerRecord {
        let payload = match (self.value_changed, self.body) {
            (false, _) => msg.payload.clone(),
            (true, Body::Json(value)) => Some(value.to_string().into_bytes()),
            (true, Body::Raw(bytes)) => bytes,
        };
        OwnedProducerRecord {
            topic: dst_topic.to_string(),
            key: self.key,
            payload,
            partition: None,
            timestamp: msg.timestamp_ms,
            headers: self.headers,
            null_headers: self.null_headers,
        }
    }
}

/// A value field steps can work on (a JSON object, a leading byte order
/// mark allowed), or the bytes as they are.
fn parse_body(payload: Option<&[u8]>) -> Body {
    let Some(bytes) = payload else {
        return Body::Raw(None);
    };
    let text = bytes.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(bytes);
    match serde_json::from_slice::<Value>(text) {
        Ok(value @ Value::Object(_)) => Body::Json(value),
        _ => Body::Raw(Some(bytes.to_vec())),
    }
}

/// How a transform run reads and where it starts, as plain data: what
/// [`check_options`] judges before connecting. Front ends fill it from
/// their own inputs (command-line flags, a form).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunOptions {
    /// Start at the earliest offset.
    pub from_beginning: bool,
    /// Start each partition at its first record at or after this epoch ms.
    pub from_timestamp: Option<i64>,
    /// Stop at the end each partition had when the run started.
    pub until_end: bool,
    /// A consumer group is given (a writing run resumes from its offsets).
    pub group: bool,
    /// The run writes (not a dry run). A dry run ignores the group.
    pub execute: bool,
    /// Records per second.
    pub rate_limit: Option<f64>,
}

/// An option combination [`check_options`] refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionsProblem {
    /// Stopping at the end needs a start before it: from the current end
    /// there is nothing to read.
    UntilEndWithoutStart,
    /// The rate limit is zero, negative or not a number.
    RateLimitNotPositive,
}

impl std::fmt::Display for OptionsProblem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::UntilEndWithoutStart => {
                "Stopping at the end needs a start: the beginning, a timestamp, or a consumer \
                 group on a writing run (from the current end there is nothing to read)"
            }
            Self::RateLimitNotPositive => "The rate limit must be greater than 0",
        })
    }
}

impl std::error::Error for OptionsProblem {}

/// Option combinations that cannot work, found before connecting. Check the
/// steps with [`Pipeline::parse`].
///
/// ```
/// use ferrika_ops::transform::{check_options, OptionsProblem, RunOptions};
///
/// let until_end = RunOptions { until_end: true, ..RunOptions::default() };
/// assert_eq!(check_options(&until_end), Err(OptionsProblem::UntilEndWithoutStart));
/// let from_start = RunOptions { from_beginning: true, ..until_end };
/// assert_eq!(check_options(&from_start), Ok(()));
/// ```
pub fn check_options(options: &RunOptions) -> Result<(), OptionsProblem> {
    if options.until_end && !options.from_beginning && options.from_timestamp.is_none() {
        let resumes = options.execute && options.group;
        if !resumes {
            return Err(OptionsProblem::UntilEndWithoutStart);
        }
    }
    if options
        .rate_limit
        .is_some_and(|r| !r.is_finite() || r <= 0.0)
    {
        return Err(OptionsProblem::RateLimitNotPositive);
    }
    Ok(())
}

/// The copy pipeline's per-record hook for a transform: decode the value
/// (Schema Registry wire format, when a decoder is given) to JSON, then
/// apply the [`Pipeline`]. Pass it to [`crate::mirror::run_copy_pipeline`];
/// read [`Self::counters`] afterwards for the [`TransformSummary`].
pub struct DecodingTransform {
    pipeline: Pipeline,
    dst_topic: String,
    decoder: Option<PayloadDecoder>,
    counters: TransformCounters,
}

impl std::fmt::Debug for DecodingTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodingTransform")
            .field("pipeline", &self.pipeline)
            .field("dst_topic", &self.dst_topic)
            .field("decodes", &self.decoder.is_some())
            .field("counters", &self.counters)
            .finish()
    }
}

impl DecodingTransform {
    /// Applies `pipeline` and writes to `dst_topic`; values are not decoded.
    pub fn new(pipeline: Pipeline, dst_topic: impl Into<String>) -> Self {
        Self {
            pipeline,
            dst_topic: dst_topic.into(),
            decoder: None,
            counters: TransformCounters::default(),
        }
    }

    /// Decodes values with `decoder` first; decoded values are written as
    /// JSON. A value that does not decode goes on as it was read.
    pub fn with_decoder(mut self, decoder: Option<PayloadDecoder>) -> Self {
        self.decoder = decoder;
        self
    }

    pub fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    /// What the steps did so far.
    pub fn counters(&self) -> &TransformCounters {
        &self.counters
    }
}

impl RecordTransform for DecodingTransform {
    async fn apply(&mut self, msg: &ConsumedMessage) -> Option<OwnedProducerRecord> {
        let decoded = match (&mut self.decoder, msg.payload.as_deref()) {
            // A value that did not decode (no wire format, unknown
            // schema) is not JSON text and goes on as it was read.
            (Some(decoder), Some(payload)) => decoder
                .decode(payload)
                .await
                .and_then(|d| serde_json::from_str::<Value>(&d.text).ok()),
            _ => None,
        };
        self.pipeline
            .apply(msg, decoded, &self.dst_topic, &mut self.counters)
    }
}

/// The numbers a transform run reports, on screen and (without record data)
/// in the audit log; serializable for JSON output and audit detail.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransformSummary {
    pub src_topic: String,
    pub dst_topic: String,
    pub dry_run: bool,
    /// Records read from the source.
    pub read: u64,
    /// Records the steps kept.
    pub kept: u64,
    /// Records written (or, in a dry run, previewed).
    pub written: u64,
    /// Records a filter step dropped.
    pub filtered: u64,
    /// Records the sample step left out.
    pub sampled_out: u64,
    /// Values that were not JSON objects, passed through field steps.
    pub not_json: u64,
    /// Records whose rekey field was missing (key kept).
    pub rekey_missing: u64,
    /// Records the destination did not acknowledge.
    pub failed: u64,
    pub stopped_on_failures: bool,
    pub elapsed_secs: f64,
    /// SHA-256 of the steps as given ([`Pipeline::digest`]).
    pub steps_sha256: String,
}

impl TransformSummary {
    /// The summary of a run from `src_topic` to `dst_topic` that ended with
    /// `stats` and `counters`; `digest` is [`Pipeline::digest`].
    pub fn new(
        src_topic: &str,
        dst_topic: &str,
        dry_run: bool,
        stats: &MirrorStats,
        counters: &TransformCounters,
        digest: String,
    ) -> Self {
        Self {
            src_topic: src_topic.to_string(),
            dst_topic: dst_topic.to_string(),
            dry_run,
            read: stats.read_count,
            kept: stats.read_count.saturating_sub(stats.filtered_count),
            written: stats.mirrored_count,
            filtered: counters.filtered,
            sampled_out: counters.sampled_out,
            not_json: counters.not_json,
            rekey_missing: counters.rekey_missing,
            failed: stats.failed_count,
            stopped_on_failures: stats.stopped_on_failures,
            elapsed_secs: stats.elapsed_secs,
            steps_sha256: digest,
        }
    }

    /// The audit log's account of the run: counts and the steps' digest,
    /// never the steps themselves (a filter or a set step can quote a value).
    pub fn audit_detail(&self) -> String {
        format!(
            "read {}, written {}, filtered {}, sampled out {}, not JSON {}, failed {}; steps sha256:{}",
            self.read,
            self.written,
            self.filtered,
            self.sampled_out,
            self.not_json,
            self.failed,
            self.steps_sha256
        )
    }

    /// The error to report when a write failed.
    pub fn failure(&self) -> Option<KafkaError> {
        if self.failed == 0 {
            return None;
        }
        let stopped = if self.stopped_on_failures {
            "; stopped early after reaching the failure limit"
        } else {
            ""
        };
        Some(KafkaError::Check(format!(
            "{} of {} transformed records failed delivery{stopped}",
            self.failed,
            self.failed + self.written
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn msg(offset: i64, key: Option<&str>, value: Option<&[u8]>) -> ConsumedMessage {
        ConsumedMessage {
            topic: "src".into(),
            partition: 1,
            offset,
            key: key.map(|k| k.as_bytes().to_vec()),
            payload: value.map(<[u8]>::to_vec),
            timestamp_ms: Some(1_700_000_000_000),
            headers: vec![("source".to_string(), b"web".to_vec())],
            null_headers: Vec::new(),
        }
    }

    fn json_msg(offset: i64, value: &Value) -> ConsumedMessage {
        msg(offset, Some("order-1"), Some(value.to_string().as_bytes()))
    }

    fn pipeline(steps: &[(StepKind, &str)]) -> Pipeline {
        Pipeline::parse(steps.iter().copied()).unwrap()
    }

    fn run(p: &Pipeline, m: &ConsumedMessage) -> (Option<OwnedProducerRecord>, TransformCounters) {
        let mut counters = TransformCounters::default();
        let out = p.apply(m, None, "dst", &mut counters);
        (out, counters)
    }

    fn value_of(record: &OwnedProducerRecord) -> Value {
        serde_json::from_slice(record.payload.as_deref().unwrap()).unwrap()
    }

    const ORDER: &str = r#"{"id":"o1","amount":120,"user":{"country":"KR","email":"kim@example.com"},"items":[{"sku":"a"},{"sku":"b"}]}"#;

    fn order() -> Value {
        serde_json::from_str(ORDER).unwrap()
    }

    #[test]
    fn field_paths_parse_and_reject_empty_segments() {
        assert_eq!(
            FieldPath::parse("user.email").unwrap(),
            FieldPath::parse("$.user.email").unwrap()
        );
        assert_eq!(
            FieldPath::parse(".user").unwrap(),
            FieldPath::parse("user").unwrap()
        );
        assert!(FieldPath::parse("").is_err());
        assert!(FieldPath::parse("$.").is_err());
        assert!(FieldPath::parse("a..b").is_err());
        let deep = vec!["a"; MAX_PATH_DEPTH + 1].join(".");
        assert!(FieldPath::parse(&deep).is_err());
    }

    #[test]
    fn field_paths_get_set_and_remove() {
        let mut v = order();
        let sku = FieldPath::parse("items.1.sku").unwrap();
        assert_eq!(sku.get(&v), Some(&json!("b")));
        assert!(FieldPath::parse("meta.env")
            .unwrap()
            .set(&mut v, json!("staging")));
        assert_eq!(v["meta"]["env"], json!("staging"));
        // A scalar parent cannot hold a field; nothing changes.
        let before = v.clone();
        assert!(!FieldPath::parse("amount.cents")
            .unwrap()
            .set(&mut v, json!(1)));
        assert!(!FieldPath::parse("items.9.sku")
            .unwrap()
            .set(&mut v, json!(1)));
        assert_eq!(v, before);
        assert_eq!(
            FieldPath::parse("user.email").unwrap().remove(&mut v),
            Some(json!("kim@example.com"))
        );
        assert_eq!(FieldPath::parse("user.email").unwrap().remove(&mut v), None);
        // Removing keeps the order of the other fields.
        let keys: Vec<&String> = v.as_object().unwrap().keys().collect();
        assert_eq!(keys, ["id", "amount", "user", "items", "meta"]);
    }

    #[test]
    fn steps_parse_with_flag_named_errors() {
        assert!(Step::parse(StepKind::Filter, "$.amount >").is_err());
        assert!(Step::parse(StepKind::Filter, "?$.amount > 1").is_ok());
        let err = Step::parse(StepKind::Rename, "a").unwrap_err();
        assert!(err.starts_with("--rename a:"), "{err}");
        assert!(Step::parse(StepKind::Rename, "a=a").is_err());
        let err = Step::parse(StepKind::Set, "env=staging").unwrap_err();
        assert!(err.contains("must be JSON"), "{err}");
        assert!(Step::parse(StepKind::Set, "env=\"staging\"").is_ok());
        assert!(Step::parse(StepKind::Set, "n= 3 ").is_ok());
        assert!(matches!(
            Step::parse(StepKind::Mask, "").unwrap(),
            Step::Mask(MaskStrategy::Partial)
        ));
        assert!(matches!(
            Step::parse(StepKind::Mask, "redact").unwrap(),
            Step::Mask(MaskStrategy::Redact)
        ));
        assert!(Step::parse(StepKind::Mask, "full").is_err());
        assert!(Step::parse(StepKind::Sample, "0").is_err());
        assert!(Step::parse(StepKind::Sample, "-3").is_err());
        assert!(matches!(
            Step::parse(StepKind::Sample, "10").unwrap(),
            Step::Sample(10)
        ));
        let too_many = vec![(StepKind::Drop, "a"); MAX_STEPS + 1];
        assert!(Pipeline::parse(too_many).is_err());
    }

    #[test]
    fn filters_on_fields_key_and_headers() {
        let m = json_msg(0, &order());
        for (expr, keep) in [
            (r#"$.user.country == "KR""#, true),
            ("$.amount > 100", true),
            ("$.amount > 500", false),
            (r#"key =~ "^order-""#, true),
            (r#"headers.source == "web""#, true),
            (r#"headers.source == "app""#, false),
        ] {
            let p = pipeline(&[(StepKind::Filter, expr)]);
            let (out, counters) = run(&p, &m);
            assert_eq!(out.is_some(), keep, "{expr}");
            assert_eq!(counters.filtered, u64::from(!keep), "{expr}");
        }
    }

    #[test]
    fn unchanged_values_are_copied_byte_for_byte() {
        let raw = br#"{ "id" : "o1",   "amount": 120 }"#;
        let m = msg(4, Some("k"), Some(raw));
        let p = pipeline(&[(StepKind::Filter, "$.amount > 100")]);
        let (out, _) = run(&p, &m);
        let out = out.unwrap();
        assert_eq!(out.payload.as_deref(), Some(&raw[..]));
        assert_eq!(out.topic, "dst");
        assert_eq!(out.key.as_deref(), Some(&b"k"[..]));
        assert_eq!(out.timestamp, Some(1_700_000_000_000));
        assert_eq!(out.partition, None);
        assert_eq!(out.headers, m.headers);
    }

    #[test]
    fn drop_rename_and_set_change_the_value_in_order() {
        let p = pipeline(&[
            (StepKind::Drop, "user.email"),
            (StepKind::Rename, "amount=total"),
            (StepKind::Set, "env=\"staging\""),
            (StepKind::Set, "user.tier={\"level\":2}"),
            // The renamed field is visible to later steps.
            (StepKind::Filter, "$.total == 120 && !$.amount"),
        ]);
        let (out, _) = run(&p, &json_msg(0, &order()));
        let v = value_of(&out.unwrap());
        assert_eq!(v["user"], json!({"country": "KR", "tier": {"level": 2}}));
        assert_eq!(v["total"], json!(120));
        assert!(v.get("amount").is_none());
        assert_eq!(v["env"], json!("staging"));
    }

    #[test]
    fn rename_onto_an_impossible_path_keeps_the_field() {
        let p = pipeline(&[(StepKind::Rename, "id=amount.id")]);
        let (out, _) = run(&p, &json_msg(0, &order()));
        let v = value_of(&out.unwrap());
        assert_eq!(v["id"], json!("o1"));
        assert_eq!(v["amount"], json!(120));
    }

    #[test]
    fn rekey_takes_text_json_or_null_and_counts_missing_fields() {
        let m = json_msg(0, &order());
        let (out, _) = run(&pipeline(&[(StepKind::Rekey, "user.country")]), &m);
        assert_eq!(out.unwrap().key.as_deref(), Some(&b"KR"[..]));
        let (out, _) = run(&pipeline(&[(StepKind::Rekey, "amount")]), &m);
        assert_eq!(out.unwrap().key.as_deref(), Some(&b"120"[..]));
        let (out, _) = run(&pipeline(&[(StepKind::Rekey, "items.0")]), &m);
        assert_eq!(out.unwrap().key.as_deref(), Some(&br#"{"sku":"a"}"#[..]));
        let null = json_msg(0, &json!({"k": null}));
        let (out, _) = run(&pipeline(&[(StepKind::Rekey, "k")]), &null);
        assert_eq!(out.unwrap().key, None);
        let (out, counters) = run(&pipeline(&[(StepKind::Rekey, "nope")]), &m);
        assert_eq!(out.unwrap().key.as_deref(), Some(&b"order-1"[..]));
        assert_eq!(counters.rekey_missing, 1);
    }

    #[test]
    fn non_json_values_pass_field_steps_untouched_and_are_counted() {
        let p = pipeline(&[
            (StepKind::Drop, "a"),
            (StepKind::Set, "b=1"),
            (StepKind::Rekey, "a"),
        ]);
        for raw in [
            &b"plain text"[..],
            &b"[1,2]"[..],
            &b"42"[..],
            &[0xff, 0x00, 0x10][..],
        ] {
            let m = msg(0, Some("k"), Some(raw));
            let (out, counters) = run(&p, &m);
            let out = out.unwrap();
            assert_eq!(out.payload.as_deref(), Some(raw));
            assert_eq!(out.key.as_deref(), Some(&b"k"[..]));
            assert_eq!(counters.not_json, 1);
        }
        // A field filter on a non-JSON value is false; `$` is the text.
        let m = msg(0, None, Some(b"plain text"));
        assert!(run(&pipeline(&[(StepKind::Filter, "$.a == 1")]), &m)
            .0
            .is_none());
        assert!(run(&pipeline(&[(StepKind::Filter, "!$.a")]), &m)
            .0
            .is_some());
        assert!(run(
            &pipeline(&[(StepKind::Filter, r#"$ contains "plain""#)]),
            &m
        )
        .0
        .is_some());
        // Tombstones stay tombstones and are not counted as non-JSON.
        let tombstone = msg(0, Some("k"), None);
        let (out, counters) = run(&p, &tombstone);
        assert_eq!(out.unwrap().payload, None);
        assert_eq!(counters.not_json, 0);
        // Without field steps nothing is counted.
        let (_, counters) = run(&pipeline(&[(StepKind::Sample, "1")]), &m);
        assert_eq!(counters.not_json, 0);
    }

    #[test]
    fn byte_order_mark_still_counts_as_json() {
        let mut raw = b"\xEF\xBB\xBF".to_vec();
        raw.extend_from_slice(br#"{"a":1,"b":2}"#);
        let m = msg(0, None, Some(&raw));
        let (out, counters) = run(&pipeline(&[(StepKind::Drop, "a")]), &m);
        assert_eq!(value_of(&out.unwrap()), json!({"b": 2}));
        assert_eq!(counters.not_json, 0);
    }

    #[test]
    fn sample_keeps_one_in_n_by_offset() {
        let p = pipeline(&[(StepKind::Sample, "3")]);
        let mut counters = TransformCounters::default();
        let kept: Vec<i64> = (0..10)
            .filter(|&o| {
                p.apply(&json_msg(o, &order()), None, "dst", &mut counters)
                    .is_some()
            })
            .collect();
        assert_eq!(kept, [0, 3, 6, 9]);
        assert_eq!(counters.sampled_out, 6);
    }

    #[test]
    fn null_headers_are_kept_in_place_through_every_step() {
        let mut m = msg(0, Some("k"), Some(br#"{"user":{"email":"a@b.c","n":1}}"#));
        m.headers = vec![
            ("authorization".to_string(), b"Bearer abc.def".to_vec()),
            ("source".to_string(), b"web".to_vec()),
        ];
        m.null_headers = vec![(0, "trace".to_string()), (2, "authorization".to_string())];
        let p = pipeline(&[
            (
                StepKind::Filter,
                r#"!headers.trace exists && headers.source == "web""#,
            ),
            (StepKind::Drop, "user.email"),
            (StepKind::Rename, "user.n=user.count"),
            (StepKind::Mask, "redact"),
        ]);
        let out = run(&p, &m).0.expect("kept");
        assert_eq!(out.null_headers, m.null_headers);
        let all = ferrika_core::kafka::consumer::merge_headers(&out.headers, &out.null_headers);
        let names: Vec<(&str, bool)> = all.iter().map(|(k, v)| (*k, v.is_none())).collect();
        assert_eq!(
            names,
            [
                ("trace", true),
                ("authorization", false),
                ("authorization", true),
                ("source", false)
            ]
        );
        // The non-null header of the same name is masked, the null one stays null.
        assert_ne!(out.headers[0].1, b"Bearer abc.def".to_vec());
        // A null header reads as missing in a filter.
        let p = pipeline(&[(StepKind::Filter, "headers.trace exists")]);
        assert!(run(&p, &m).0.is_none());
    }

    #[test]
    fn mask_hides_pii_in_value_key_and_headers() {
        let p = pipeline(&[(StepKind::Mask, "")]);
        let mut m = json_msg(
            0,
            &json!({"email": "kim@example.com", "password": "hunter2"}),
        );
        m.key = Some(b"kim@example.com".to_vec());
        m.headers = vec![("authorization".to_string(), b"Bearer abc.def".to_vec())];
        let (out, _) = run(&p, &m);
        let out = out.unwrap();
        let text = String::from_utf8(out.payload.clone().unwrap()).unwrap();
        assert!(!text.contains("kim@example.com"), "{text}");
        assert!(!text.contains("hunter2"), "{text}");
        let key = String::from_utf8(out.key.unwrap()).unwrap();
        assert!(!key.contains("kim@example.com"), "{key}");
        let header = String::from_utf8(out.headers[0].1.clone()).unwrap();
        assert!(!header.contains("abc.def"), "{header}");
        // Binary values cannot be scanned and are hidden whole.
        let (out, _) = run(&p, &msg(0, None, Some(&[0xff, 0xfe, 0x00])));
        let text = String::from_utf8(out.unwrap().payload.unwrap()).unwrap();
        assert!(text.contains("hidden"), "{text}");
        // Filters after the mask see masked values only.
        let p = pipeline(&[
            (StepKind::Mask, ""),
            (StepKind::Filter, r#"$.email == "kim@example.com""#),
        ]);
        assert!(run(&p, &json_msg(0, &json!({"email": "kim@example.com"})))
            .0
            .is_none());
        assert!(p.masks_first());
        assert!(!pipeline(&[(StepKind::Drop, "a"), (StepKind::Mask, "")]).masks_first());
    }

    #[test]
    fn decoded_values_are_written_as_json() {
        let p = pipeline(&[(StepKind::Filter, "$.amount > 100")]);
        let m = msg(0, None, Some(&[0, 0, 0, 0, 7, 2, 4]));
        let mut counters = TransformCounters::default();
        let out = p.apply(&m, Some(order()), "dst", &mut counters).unwrap();
        assert_eq!(value_of(&out), order());
        let out = p.apply(&m, Some(json!(5)), "dst", &mut counters);
        assert!(out.is_none(), "a decoded scalar has no fields");
    }

    #[test]
    fn digest_follows_the_steps_and_their_order() {
        let a = pipeline(&[(StepKind::Drop, "a"), (StepKind::Mask, "")]);
        let b = pipeline(&[(StepKind::Mask, ""), (StepKind::Drop, "a")]);
        assert_eq!(a.digest().len(), 64);
        assert_eq!(a.digest(), a.clone().digest());
        assert_ne!(a.digest(), b.digest());
        assert!(a.digest().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn step_kind_names_are_the_flags() {
        let names: Vec<&str> = StepKind::ALL.iter().map(|k| k.name()).collect();
        assert_eq!(
            names,
            ["filter", "drop", "rename", "set", "rekey", "mask", "sample"]
        );
    }

    #[test]
    fn options_need_a_start_before_the_end() {
        let base = RunOptions {
            until_end: true,
            ..RunOptions::default()
        };
        assert_eq!(
            check_options(&base),
            Err(OptionsProblem::UntilEndWithoutStart)
        );
        for ok in [
            RunOptions {
                from_beginning: true,
                ..base.clone()
            },
            RunOptions {
                from_timestamp: Some(5),
                ..base.clone()
            },
            RunOptions {
                group: true,
                execute: true,
                ..base.clone()
            },
        ] {
            assert_eq!(check_options(&ok), Ok(()), "{ok:?}");
        }
        // A dry run ignores the group, so it starts at the end.
        let dry = RunOptions {
            group: true,
            ..base
        };
        assert!(check_options(&dry).is_err());
        let rate = |r: f64| RunOptions {
            rate_limit: Some(r),
            ..RunOptions::default()
        };
        assert_eq!(
            check_options(&rate(0.0)),
            Err(OptionsProblem::RateLimitNotPositive)
        );
        assert!(check_options(&rate(f64::NAN)).is_err());
        assert_eq!(check_options(&rate(50.0)), Ok(()));
    }

    fn stats(read: u64, filtered: u64, written: u64, failed: u64) -> MirrorStats {
        let mut stats = MirrorStats::new();
        stats.read_count = read;
        stats.filtered_count = filtered;
        stats.mirrored_count = written;
        stats.failed_count = failed;
        stats
    }

    #[test]
    fn summary_counts_and_audit_detail_hold_no_record_data() {
        let counters = TransformCounters {
            filtered: 6,
            sampled_out: 1,
            not_json: 2,
            rekey_missing: 0,
        };
        let summary = TransformSummary::new(
            "orders",
            "orders-copy",
            false,
            &stats(10, 7, 3, 0),
            &counters,
            "ab12".into(),
        );
        assert_eq!(summary.kept, 3);
        assert!(!summary.dry_run);
        assert_eq!(
            summary.audit_detail(),
            "read 10, written 3, filtered 6, sampled out 1, not JSON 2, failed 0; steps sha256:ab12"
        );
        assert!(summary.failure().is_none());
        let json = serde_json::to_value(&summary).unwrap();
        assert_eq!(json["steps_sha256"], "ab12");
        let back: TransformSummary = serde_json::from_value(json).unwrap();
        assert_eq!(back, summary);

        let failed = TransformSummary::new(
            "a",
            "b",
            false,
            &stats(5, 0, 3, 2),
            &counters,
            String::new(),
        );
        let err = failed.failure().unwrap().to_string();
        assert!(err.contains("2 of 5 transformed records failed"), "{err}");
    }

    #[tokio::test]
    async fn the_decoding_transform_applies_the_steps_without_a_decoder() {
        let steps = pipeline(&[
            (StepKind::Drop, "email"),
            (StepKind::Filter, "$.amount > 1"),
        ]);
        let mut transform = DecodingTransform::new(steps, "dst").with_decoder(None);
        let kept = transform
            .apply(&json_msg(
                0,
                &json!({"amount": 5, "email": "kim@example.com"}),
            ))
            .await
            .unwrap();
        assert_eq!(kept.topic, "dst");
        let value: Value = serde_json::from_slice(kept.payload.as_deref().unwrap()).unwrap();
        assert_eq!(value, json!({"amount": 5}));
        assert!(transform
            .apply(&json_msg(1, &json!({"amount": 0})))
            .await
            .is_none());
        assert_eq!(transform.counters().filtered, 1);
        assert_eq!(transform.pipeline().steps().len(), 2);
    }
}
