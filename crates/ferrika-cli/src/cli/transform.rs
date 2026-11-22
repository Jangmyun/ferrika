//! `ferrika transform`: copy records from one topic to another through a
//! short pipeline of steps (filter, drop, rename, set, rekey, mask, sample).
//!
//! The steps live in `ferrika_ops::transform`; the copy loop (consumer,
//! producer, at-least-once commits, bounded runs) is the one `mirror` uses
//! (`ferrika_ops::mirror::run_copy_pipeline`). Without `--execute` the
//! command is a dry run that prints the transformed records.

use std::io::IsTerminal;

use clap::{Arg, ArgAction, ArgMatches, Args, Command, FromArgMatches};
use colored::Colorize;
use comfy_table::{Cell, Color};
use serde_json::{json, Value};

use crate::cli::mirror::{resolve_cluster_config, CopyRoute};
use crate::cli::{output, print_connection_hint, GlobalOpts, OutputFormat};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::producer::OwnedProducerRecord;
use crate::kafka::schema::PayloadDecoder;
use crate::ops::mirror::{
    is_self_mirror, run_copy_pipeline, CopyDestination, MirrorConfig, MirrorStats,
};
use crate::ops::transform::{
    check_options, DecodingTransform, OptionsProblem, Pipeline, RunOptions, StepKind,
    TransformSummary,
};

/// Records a dry run prints when `--max-records` is not given.
pub const DRY_RUN_LIMIT: usize = 10;

/// The label the policy gate and the audit log use for a writing run.
pub const EXECUTE_LABEL: &str = "transform --execute";

const STEP_HEADING: &str = "Steps (applied in the order given)";

/// Command line arguments for `ferrika transform`.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct TransformArgs {
    /// Topic to read from, on the cluster the global options select
    pub src_topic: String,

    /// Topic to write the transformed records to
    #[arg(long = "to", value_name = "DST_TOPIC")]
    pub dst_topic: String,

    /// Profile of the destination cluster (defaults to the source cluster)
    #[arg(long, conflicts_with = "dst_bootstrap_servers")]
    pub dst_profile: Option<String>,

    /// Bootstrap servers of the destination cluster (defaults to the source cluster)
    #[arg(long)]
    pub dst_bootstrap_servers: Option<String>,

    #[command(flatten)]
    pub steps: StepArgs,

    /// Start at the earliest offset (with --group: only when the group has
    /// no committed offset yet)
    #[arg(short = 'b', long)]
    pub from_beginning: bool,

    /// Start each partition at its first record at or after this epoch
    /// millisecond
    #[arg(long, value_name = "MS", conflicts_with_all = ["from_beginning", "group"])]
    pub from_timestamp: Option<i64>,

    /// Stop once every partition is read up to the end it had when the run
    /// started, instead of waiting for new records
    #[arg(long)]
    pub until_end: bool,

    /// Stop after this many records were written (a dry run prints 10
    /// unless this is given)
    #[arg(short = 'n', long = "max-records", visible_alias = "max-messages")]
    pub max_records: Option<usize>,

    /// Consumer group on the source cluster: a later run resumes where this
    /// one stopped. An offset is committed only after the destination
    /// acknowledged the record (at least once). Ignored by a dry run.
    #[arg(short = 'g', long, value_name = "GROUP")]
    pub group: Option<String>,

    /// Read only this partition
    #[arg(short = 'p', long)]
    pub partition: Option<i32>,

    /// Rate limit in records per second
    #[arg(short = 'r', long)]
    pub rate_limit: Option<f64>,

    /// Decode Schema Registry wire-format values (Avro, Protobuf, JSON
    /// Schema) to JSON first; decoded values are written as JSON
    #[arg(short = 'D', long = "decode-wire-format")]
    pub decode_wire_format: bool,

    /// Schema Registry URL for --decode-wire-format (defaults to the profile's)
    #[arg(short = 'S', long = "schema-registry")]
    pub schema_registry: Option<String>,

    /// Write the records to the destination topic. Without it the command
    /// is a dry run that prints the transformed records and writes nothing.
    #[arg(long, conflicts_with = "dry_run")]
    pub execute: bool,

    /// Print the transformed records instead of writing them (the default)
    #[arg(long)]
    pub dry_run: bool,
}

/// The pipeline steps, in the order they were given on the command line.
///
/// Each kind has its own repeatable flag (`--filter`, `--drop`, ...); clap
/// keeps each flag's values apart, so the order across flags is rebuilt
/// from the argument indices.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StepArgs(pub Vec<(StepKind, String)>);

impl StepArgs {
    fn arg(kind: StepKind) -> Arg {
        let (value_name, help) = match kind {
            StepKind::Filter => (
                "EXPR",
                "Keep records matching an expression, e.g. '$.amount > 100', \
                 '$.user.country == \"KR\"', 'key =~ \"^order-\"', 'headers.source == \"web\"' \
                 (the syntax of consume --where)",
            ),
            StepKind::Drop => (
                "FIELD",
                "Remove a value field (dotted path, e.g. user.email)",
            ),
            StepKind::Rename => ("OLD=NEW", "Move a value field to another name or path"),
            StepKind::Set => (
                "FIELD=JSON",
                "Set a value field to a JSON value, e.g. env='\"staging\"' or retries=0",
            ),
            StepKind::Rekey => ("FIELD", "Use a value field as the new record key"),
            StepKind::Mask => (
                "STRATEGY",
                "Mask PII in the key, value and header values (partial, the default, or \
                 --mask=redact). Required as the first step when the policy enforces masking",
            ),
            StepKind::Sample => (
                "N",
                "Keep one record in N: those whose offset is a multiple of N",
            ),
        };
        let arg = Arg::new(kind.name())
            .long(kind.name())
            .value_name(value_name)
            .help(help)
            .help_heading(STEP_HEADING)
            .action(ArgAction::Append);
        match kind {
            StepKind::Mask => arg
                .num_args(0..=1)
                .require_equals(true)
                .default_missing_value("partial")
                .value_parser(["partial", "redact"]),
            _ => arg.num_args(1).allow_hyphen_values(true),
        }
    }
}

impl FromArgMatches for StepArgs {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, clap::Error> {
        let mut steps: Vec<(usize, StepKind, String)> = Vec::new();
        for kind in StepKind::ALL {
            let id = kind.name();
            let values = matches.try_get_many::<String>(id).ok().flatten();
            let indices = matches.indices_of(id);
            if let (Some(values), Some(indices)) = (values, indices) {
                for (value, index) in values.zip(indices) {
                    steps.push((index, kind, value.clone()));
                }
            }
        }
        steps.sort_by_key(|(index, _, _)| *index);
        Ok(Self(
            steps
                .into_iter()
                .map(|(_, kind, value)| (kind, value))
                .collect(),
        ))
    }

    fn update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), clap::Error> {
        *self = Self::from_arg_matches(matches)?;
        Ok(())
    }
}

impl Args for StepArgs {
    fn augment_args(cmd: Command) -> Command {
        StepKind::ALL
            .into_iter()
            .fold(cmd, |cmd, kind| cmd.arg(Self::arg(kind)))
    }

    fn augment_args_for_update(cmd: Command) -> Command {
        Self::augment_args(cmd)
    }
}

impl CopyRoute for TransformArgs {
    fn label(&self) -> &'static str {
        EXECUTE_LABEL
    }
    fn src_topic(&self) -> &str {
        &self.src_topic
    }
    fn dst_topic(&self) -> &str {
        &self.dst_topic
    }
    // The source is the cluster the global options select.
    fn src_profile(&self) -> Option<&str> {
        None
    }
    fn dst_profile(&self) -> Option<&str> {
        self.dst_profile.as_deref()
    }
    fn src_bootstrap_servers(&self) -> Option<&str> {
        None
    }
    fn dst_bootstrap_servers(&self) -> Option<&str> {
        self.dst_bootstrap_servers.as_deref()
    }
    fn masks_first(&self) -> bool {
        self.steps
            .0
            .first()
            .is_some_and(|(kind, _)| *kind == StepKind::Mask)
    }
    fn masking_hint(&self) -> &'static str {
        " Add --mask as the first step to copy masked records."
    }
}

/// Policy checks for a writing run (`--execute`), which `main` runs before
/// the command: enforced masking (unless `--mask` comes first), the export
/// gate when the destination is another cluster, and the write gate for the
/// destination cluster. See `mirror::authorize`.
pub fn authorize(args: &TransformArgs, global: &GlobalOpts) -> Result<(), String> {
    crate::cli::mirror::authorize(args, global)
}

/// Option combinations that cannot work, found before connecting
/// (`ops::transform::check_options`), with the flags named.
fn check_args(args: &TransformArgs) -> Result<(), String> {
    check_options(&RunOptions {
        from_beginning: args.from_beginning,
        from_timestamp: args.from_timestamp,
        until_end: args.until_end,
        group: args.group.is_some(),
        execute: args.execute,
        rate_limit: args.rate_limit,
    })
    .map_err(|problem| {
        match problem {
            OptionsProblem::UntilEndWithoutStart => {
                "--until-end needs a start: --from-beginning, --from-timestamp or \
                 --group with --execute (from the current end there is nothing to read)"
            }
            OptionsProblem::RateLimitNotPositive => "--rate-limit must be greater than 0",
        }
        .to_string()
    })
}

/// The summary of a run of `args`.
fn run_summary(
    args: &TransformArgs,
    stats: &MirrorStats,
    transform: &DecodingTransform,
    digest: String,
) -> TransformSummary {
    TransformSummary::new(
        &args.src_topic,
        &args.dst_topic,
        !args.execute,
        stats,
        transform.counters(),
        digest,
    )
}

/// The summary as the table output prints it.
fn summary_table(summary: &TransformSummary) -> comfy_table::Table {
    let mut table = output::table(vec![
        Cell::new("TRANSFORM").fg(Color::Cyan),
        Cell::new("VALUE").fg(Color::Cyan),
    ]);
    let written = if summary.dry_run {
        "Printed"
    } else {
        "Written"
    };
    let rows: [(&str, String, Color); 9] = [
        ("Source Topic", summary.src_topic.clone(), Color::Yellow),
        ("Destination Topic", summary.dst_topic.clone(), Color::Green),
        ("Read", summary.read.to_string(), Color::White),
        ("Kept by the steps", summary.kept.to_string(), Color::White),
        (written, summary.written.to_string(), Color::Cyan),
        (
            "Dropped by --filter",
            summary.filtered.to_string(),
            Color::DarkGrey,
        ),
        (
            "Left out by --sample",
            summary.sampled_out.to_string(),
            Color::DarkGrey,
        ),
        (
            "Not JSON (passed through)",
            summary.not_json.to_string(),
            Color::DarkGrey,
        ),
        (
            "Duration",
            format!("{:.2} s", summary.elapsed_secs),
            Color::Blue,
        ),
    ];
    for (name, value, color) in rows {
        table.add_row(vec![
            Cell::new(name).fg(Color::DarkGrey),
            Cell::new(value).fg(color),
        ]);
    }
    if summary.rekey_missing > 0 {
        table.add_row(vec![
            Cell::new("--rekey field missing (key kept)").fg(Color::DarkGrey),
            Cell::new(summary.rekey_missing.to_string()).fg(Color::Yellow),
        ]);
    }
    if summary.failed > 0 {
        table.add_row(vec![
            Cell::new("Failed deliveries").fg(Color::DarkGrey),
            Cell::new(summary.failed.to_string()).fg(Color::Red),
        ]);
    }
    table
}

/// One transformed record of a dry run, as a JSON line: the key and header
/// values as text, the value as JSON when it is JSON.
fn preview_line(record: &OwnedProducerRecord) -> Value {
    let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
    let value = record.payload.as_deref().map(|payload| {
        serde_json::from_slice::<Value>(payload).unwrap_or_else(|_| Value::String(text(payload)))
    });
    let headers: Vec<Value> = record
        .headers
        .iter()
        .map(|(name, value)| json!([name, text(value)]))
        .collect();
    json!({
        "key": record.key.as_deref().map(text),
        "value": value,
        "headers": headers,
    })
}

/// Main entry point for `ferrika transform`.
pub async fn run(args: TransformArgs, global: &GlobalOpts) -> KafkaResult<()> {
    check_args(&args).map_err(KafkaError::config)?;
    let pipeline = Pipeline::parse(
        args.steps
            .0
            .iter()
            .map(|(kind, text)| (*kind, text.as_str())),
    )
    .map_err(KafkaError::config)?;
    let digest = pipeline.digest();
    let policy = crate::governance::policy::current();
    // A writing run was checked in `main` (`authorize`); a dry run prints
    // records, so it must mask them first just the same.
    if !args.execute && policy.masking_enforced() && !pipeline.masks_first() {
        return Err(KafkaError::config(format!(
            "Refused by {}: PII masking is enforced. Add --mask as the first step to preview records.",
            policy.label()
        )));
    }
    let table = global.format != OutputFormat::Json;

    let mut src_cfg = resolve_cluster_config(None, None, global);
    match (&args.group, args.execute) {
        (Some(group), true) => src_cfg = src_cfg.with_group_id(group),
        (Some(_), false) => eprintln!(
            "{}",
            "--group is ignored in a dry run: no group is joined and nothing is committed."
                .bright_black()
        ),
        (None, _) => {}
    }
    let dst_cfg = resolve_cluster_config(
        args.dst_profile.as_deref(),
        args.dst_bootstrap_servers.as_deref(),
        global,
    );
    if is_self_mirror(
        &src_cfg.bootstrap_servers,
        &args.src_topic,
        &dst_cfg.bootstrap_servers,
        &args.dst_topic,
    ) {
        return Err(KafkaError::config(format!(
            "Source and destination are the same topic '{}' on the same cluster; \
             the transform would read its own output. Use a different destination topic.",
            args.src_topic
        )));
    }

    let decoder = if args.decode_wire_format {
        let url = args
            .schema_registry
            .clone()
            .or_else(|| src_cfg.schema_registry_url.clone());
        if url.is_none() {
            eprintln!(
                "{}",
                "No Schema Registry is configured: only JSON Schema values can be decoded; \
                 Avro and Protobuf values pass through unchanged."
                    .bright_black()
            );
        }
        Some(PayloadDecoder::new(url.as_deref()).with_timeout(global.http_timeout()))
    } else {
        None
    };

    let limit = args
        .max_records
        .or((!args.execute).then_some(DRY_RUN_LIMIT));
    let config = MirrorConfig::new(&args.src_topic, &args.dst_topic)
        .with_from_beginning(args.from_beginning)
        .with_from_timestamp(args.from_timestamp)
        .with_until_end(args.until_end)
        .with_partition(args.partition)
        .with_max_messages(limit)
        .with_rate_limit(args.rate_limit);

    if table {
        let mode = if args.execute {
            "writing".green().bold().to_string()
        } else {
            "dry run, nothing is written (add --execute to write)"
                .yellow()
                .to_string()
        };
        eprintln!(
            "Transforming '{}' -> '{}' ({mode}), {} step(s)",
            args.src_topic.yellow().bold(),
            args.dst_topic.green().bold(),
            pipeline.steps().len()
        );
        if !args.from_beginning && args.from_timestamp.is_none() && args.group.is_none() {
            eprintln!(
                "{}",
                "Starting at the end of the topic: waiting for new records \
                 (add --from-beginning or --from-timestamp to read existing ones)."
                    .bright_black()
            );
        }
    }

    let mut transform =
        DecodingTransform::new(pipeline, args.dst_topic.clone()).with_decoder(decoder);
    let result = if args.execute {
        let progress = table && std::io::stderr().is_terminal();
        run_copy_pipeline(
            &src_cfg,
            CopyDestination::Cluster(&dst_cfg),
            config,
            &mut transform,
            move |stats: &MirrorStats| {
                if progress {
                    eprint!(
                        "\rRead: {} | Written: {} | Skipped: {} | Failed: {} | {:.1}s    ",
                        stats.read_count,
                        stats.mirrored_count.to_string().green(),
                        stats.filtered_count,
                        stats.failed_count,
                        stats.elapsed_secs
                    );
                }
            },
        )
        .await
    } else {
        let mut print = |record: &OwnedProducerRecord| println!("{}", preview_line(record));
        run_copy_pipeline(
            &src_cfg,
            CopyDestination::Preview(&mut print),
            config,
            &mut transform,
            |_: &MirrorStats| {},
        )
        .await
    };
    let stats = match result {
        Ok(stats) => stats,
        Err(err) => {
            print_connection_hint(&src_cfg, &err);
            return Err(err);
        }
    };

    let summary = run_summary(&args, &stats, &transform, digest);
    if args.execute {
        crate::cli::export_finished(stats.mirrored_count);
        crate::cli::set_audit_result_detail(summary.audit_detail());
    }
    match (global.format, args.execute) {
        (OutputFormat::Json, true) => output::print_json(&summary)?,
        // A dry run's stdout holds the records.
        (OutputFormat::Json, false) => match serde_json::to_string(&summary) {
            Ok(text) => eprintln!("{text}"),
            Err(e) => return Err(KafkaError::Serialization(e.to_string())),
        },
        (OutputFormat::Table, true) => {
            eprintln!();
            println!("{}", summary_table(&summary));
        }
        (OutputFormat::Table, false) => eprintln!("{}", summary_table(&summary)),
    }
    match summary.failure() {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    struct TestCli {
        #[command(flatten)]
        args: TransformArgs,
    }

    fn parse(extra: &[&str]) -> TransformArgs {
        TestCli::try_parse_from(
            ["test", "orders", "--to", "orders-copy"]
                .iter()
                .chain(extra),
        )
        .unwrap_or_else(|e| panic!("{extra:?} should parse: {e}"))
        .args
    }

    #[test]
    fn steps_keep_their_command_line_order_across_flags() {
        let args = parse(&[
            "--mask",
            "--filter",
            "$.amount > 100",
            "--drop",
            "user.email",
            "--sample",
            "10",
            "--drop",
            "card",
            "--rename",
            "amount=total",
            "--mask=redact",
            "--set",
            "env=\"staging\"",
            "--rekey",
            "user.id",
        ]);
        assert_eq!(
            args.steps.0,
            vec![
                (StepKind::Mask, "partial".to_string()),
                (StepKind::Filter, "$.amount > 100".to_string()),
                (StepKind::Drop, "user.email".to_string()),
                (StepKind::Sample, "10".to_string()),
                (StepKind::Drop, "card".to_string()),
                (StepKind::Rename, "amount=total".to_string()),
                (StepKind::Mask, "redact".to_string()),
                (StepKind::Set, "env=\"staging\"".to_string()),
                (StepKind::Rekey, "user.id".to_string()),
            ]
        );
        assert!(args.masks_first());
        assert!(Pipeline::parse(args.steps.0.iter().map(|(k, v)| (*k, v.as_str()))).is_ok());
    }

    #[test]
    fn bare_mask_does_not_take_the_next_argument() {
        let args = parse(&["--mask", "--drop", "a"]);
        assert_eq!(
            args.steps.0,
            vec![
                (StepKind::Mask, "partial".to_string()),
                (StepKind::Drop, "a".to_string())
            ]
        );
        assert!(TestCli::try_parse_from(["test", "a", "--to", "b", "--mask=full"]).is_err());
        assert!(!parse(&["--drop", "a", "--mask"]).masks_first());
        assert!(!parse(&[]).masks_first());
    }

    #[test]
    fn defaults_are_a_dry_run_from_the_end() {
        let args = parse(&[]);
        assert_eq!(args.src_topic, "orders");
        assert_eq!(args.dst_topic, "orders-copy");
        assert!(!args.execute);
        assert!(args.steps.0.is_empty());
        assert_eq!(args.max_records, None);
        assert!(!args.from_beginning && !args.until_end);
        // --max-messages is accepted like mirror's.
        assert_eq!(parse(&["--max-messages", "5"]).max_records, Some(5));
    }

    #[test]
    fn conflicting_options_are_rejected() {
        for extra in [
            &["--execute", "--dry-run"][..],
            &["--from-timestamp", "1", "--from-beginning"][..],
            &["--from-timestamp", "1", "--group", "g"][..],
            &["--dst-profile", "a", "--dst-bootstrap-servers", "h:9092"][..],
        ] {
            let argv = ["test", "a", "--to", "b"].iter().chain(extra);
            assert!(TestCli::try_parse_from(argv).is_err(), "{extra:?}");
        }
        assert!(
            TestCli::try_parse_from(["test", "a"]).is_err(),
            "--to is required"
        );
    }

    #[test]
    fn until_end_needs_a_start() {
        assert!(check_args(&parse(&["--until-end"])).is_err());
        assert!(check_args(&parse(&["--until-end", "-b"])).is_ok());
        assert!(check_args(&parse(&["--until-end", "--from-timestamp", "5"])).is_ok());
        assert!(check_args(&parse(&["--until-end", "-g", "g", "--execute"])).is_ok());
        // A dry run ignores the group, so it starts at the end.
        assert!(check_args(&parse(&["--until-end", "-g", "g"])).is_err());
        assert!(check_args(&parse(&["-r", "0"])).is_err());
        assert!(check_args(&parse(&["-r", "50"])).is_ok());
    }

    #[test]
    fn route_reads_the_global_cluster_and_writes_the_destination() {
        let args = parse(&["--dst-profile", "staging"]);
        assert_eq!(args.label(), "transform --execute");
        assert_eq!(args.src_profile(), None);
        assert_eq!(args.src_bootstrap_servers(), None);
        assert_eq!(args.dst_profile(), Some("staging"));
        assert!(crate::cli::mirror::names_clusters(&args));
        assert!(!crate::cli::mirror::names_clusters(&parse(&[])));
        assert!(crate::cli::mirror::names_clusters(&parse(&[
            "--dst-bootstrap-servers",
            "h:9092"
        ])));
    }

    #[test]
    fn summary_follows_the_run_mode_and_names_no_steps() {
        let args = parse(&["--execute", "--filter", "$.email == \"kim@example.com\""]);
        let pipeline = Pipeline::parse(args.steps.0.iter().map(|(k, v)| (*k, v.as_str()))).unwrap();
        let digest = pipeline.digest();
        let transform = DecodingTransform::new(pipeline, "orders-copy");
        let mut stats = MirrorStats::new();
        stats.read_count = 10;
        stats.mirrored_count = 3;
        let summary = run_summary(&args, &stats, &transform, digest);
        assert!(!summary.dry_run);
        assert_eq!(summary.src_topic, "orders");
        assert!(!summary.audit_detail().contains("kim@example.com"));
        assert!(run_summary(&parse(&[]), &stats, &transform, String::new()).dry_run);
        assert!(summary_table(&summary).to_string().contains("Written"));
    }

    #[test]
    fn preview_lines_show_json_values_and_text() {
        let record = OwnedProducerRecord::new("dst")
            .with_key("k1")
            .with_payload(br#"{"a":1}"#.to_vec())
            .with_header("source", b"web".to_vec());
        assert_eq!(
            preview_line(&record),
            json!({"key": "k1", "value": {"a": 1}, "headers": [["source", "web"]]})
        );
        let text = OwnedProducerRecord::new("dst").with_payload(b"plain".to_vec());
        assert_eq!(
            preview_line(&text),
            json!({"key": null, "value": "plain", "headers": []})
        );
        let tombstone = OwnedProducerRecord::new("dst").with_key("k");
        assert_eq!(preview_line(&tombstone)["value"], Value::Null);
    }
}
