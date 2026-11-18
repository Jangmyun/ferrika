//! `ferrika report`: one Markdown (or JSON) incident report for the active
//! cluster: health checks, consumer lag and its trend, and the audit log.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, ValueEnum};
use colored::Colorize;

use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::clock::now_ms;
use crate::model::cluster::ClusterSnapshot;
use crate::ops::report::{
    build, fill_consume_rates, parse_window, render_markdown, LagSample, PolicySummary, Report,
    ReportContext, TrendSource, AUDIT_SCAN,
};

#[derive(Args, Debug, Clone)]
pub struct ReportArgs {
    /// Time window ending now, e.g. 30m, 2h, 1d. Limits the audit timeline;
    /// without it the latest 100 entries for this cluster are listed.
    #[arg(long, value_name = "DURATION", value_parser = parse_window)]
    pub since: Option<Duration>,
    /// Write the report to FILE instead of stdout
    #[arg(short = 'o', long, value_name = "FILE")]
    pub output: Option<PathBuf>,
    /// Report format (the global `--format json` also selects JSON)
    #[arg(long = "format", value_enum, default_value_t = ReportFormat::Md)]
    pub report_format: ReportFormat,
    /// Lag samples to take for the trend; 1 reports a snapshot only.
    /// Ferrika keeps no lag history, so the trend covers this run.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..=120))]
    pub samples: u32,
    /// Seconds between lag samples
    #[arg(long, value_name = "SECONDS", default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=300))]
    pub interval: u64,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportFormat {
    Md,
    Json,
}

pub async fn run(args: ReportArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let report = collect(&args, global).await;
    let json = args.report_format == ReportFormat::Json || global.format == OutputFormat::Json;
    match &args.output {
        None if json => output::print_json(&report),
        None => {
            print!("{}", render_markdown(&report));
            Ok(())
        }
        Some(path) => {
            let text = if json {
                serde_json::to_string_pretty(&report)
                    .map_err(|e| KafkaError::Serialization(e.to_string()))?
            } else {
                render_markdown(&report)
            };
            std::fs::write(path, text)
                .map_err(|e| KafkaError::File(format!("{}: {e}", path.display())))?;
            eprintln!(
                "{} Report written to {}",
                "✔".green().bold(),
                path.display()
            );
            Ok(())
        }
    }
}

/// Samples the cluster and reads the audit log. An unreachable cluster is
/// reported in the document rather than failing it: that is when a report
/// is needed most.
async fn collect(args: &ReportArgs, global: &GlobalOpts) -> Report {
    let config = global.to_kafka_config();
    let profile = global.resolved_profile_name().unwrap_or_default();
    let policy = crate::governance::policy::current();
    let mut notes = Vec::new();
    let mut cluster_error = None;
    let mut first: Option<(i64, ClusterSnapshot)> = None;
    let mut latest: Option<(i64, ClusterSnapshot)> = None;
    let mut samples: Vec<LagSample> = Vec::new();

    for ix in 0..args.samples {
        if ix > 0 {
            tokio::time::sleep(Duration::from_secs(args.interval)).await;
        }
        if args.samples > 1 {
            eprintln!(
                "{}",
                format!("Sampling lag {}/{}…", ix + 1, args.samples).bright_black()
            );
        }
        match crate::cli::health::snapshot(global).await {
            Ok(data) => {
                let at = now_ms();
                samples.push(LagSample::of(at, &data));
                if first.is_none() {
                    first = Some((at, data.clone()));
                }
                latest = Some((at, data));
            }
            Err(e) if latest.is_none() && ix + 1 == args.samples => {
                cluster_error = Some(e.to_string());
            }
            Err(e) => notes.push(format!("Lag sample {} failed: {e}", ix + 1)),
        }
    }

    let mut data = match latest {
        Some((at, mut data)) => {
            if let Some((first_at, first)) = &first {
                fill_consume_rates(first, *first_at, &mut data, at);
            }
            data
        }
        None => ClusterSnapshot::empty(profile.clone(), config.bootstrap_servers.clone()),
    };
    data.cluster_name = profile.clone();

    let ctx = ReportContext {
        policy: PolicySummary::of(policy, &profile, &config.bootstrap_servers),
        profile,
        bootstrap: config.bootstrap_servers.clone(),
        generated_at_ms: now_ms(),
        window: args.since,
        cluster_error,
        notes,
    };
    let audit = crate::governance::audit::recent(AUDIT_SCAN);
    build(ctx, &data, &samples, TrendSource::Sampled, &audit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: ReportArgs,
    }

    #[test]
    fn report_options_parse() {
        let cli = Cli::try_parse_from([
            "report",
            "--since",
            "30m",
            "-o",
            "out.md",
            "--samples",
            "3",
            "--interval",
            "2",
        ])
        .unwrap();
        assert_eq!(cli.args.since, Some(Duration::from_secs(1800)));
        assert_eq!(cli.args.output, Some(PathBuf::from("out.md")));
        assert_eq!(cli.args.report_format, ReportFormat::Md);
        assert_eq!((cli.args.samples, cli.args.interval), (3, 2));
        let cli = Cli::try_parse_from(["report", "--format", "json"]).unwrap();
        assert_eq!(cli.args.report_format, ReportFormat::Json);
        assert!(Cli::try_parse_from(["report", "--since", "30"]).is_err());
        assert!(Cli::try_parse_from(["report", "--samples", "0"]).is_err());
    }
}
