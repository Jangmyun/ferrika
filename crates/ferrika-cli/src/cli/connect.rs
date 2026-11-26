//! `ferrika connect`: Kafka Connect connectors.

use clap::{ArgGroup, Args, Subcommand};
use colored::Colorize;
use comfy_table::{Cell, Color};

use crate::cli::record::{self, Change};
use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::connect::{ConnectClient, ConnectorAction, ConnectorOffset};
use crate::model::history::{Reversibility, State};
use crate::ops::history::capture;

#[derive(Args, Debug, Clone)]
pub struct ConnectArgs {
    #[command(subcommand)]
    pub command: ConnectSubcommands,
    /// Kafka Connect REST URL (defaults to the profile's)
    #[arg(long, global = true, value_name = "URL")]
    pub url: Option<String>,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum ConnectSubcommands {
    /// List connectors with their state and tasks
    List,
    /// Show one connector's config and tasks (with failure traces)
    Status { name: String },
    /// Pause a connector
    Pause { name: String },
    /// Resume a paused connector
    Resume { name: String },
    /// Restart a connector and its tasks
    Restart { name: String },
    /// Restart one task of a connector (e.g. a failed one)
    RestartTask { name: String, task: i64 },
    /// Stop a connector (Kafka Connect 3.5+): its tasks shut down and the
    /// config stays; `connect resume` starts it again. Offsets can be reset
    /// or set only while it is stopped
    Stop { name: String },
    /// Show a connector's offsets (Kafka Connect 3.5+), or reset or set
    /// them (3.6+, only while the connector is stopped; a preview without
    /// --execute)
    #[command(group(ArgGroup::new("change").args(["reset", "set"])))]
    Offsets {
        name: String,
        /// Delete every offset of the connector, so it starts over as if new
        #[arg(long)]
        reset: bool,
        /// Set these offsets: a JSON file or inline JSON, as Kafka Connect
        /// shows them ({"offsets": [{"partition": {...}, "offset": {...}}]});
        /// an offset of null resets that partition
        #[arg(long, value_name = "FILE|JSON")]
        set: Option<String>,
        /// Make the change (without it, only show what would change)
        #[arg(long, requires = "change")]
        execute: bool,
    },
    /// Create a connector, or replace an existing connector's config, from a
    /// JSON file (the flat config, or {"name": ..., "config": {...}})
    Apply {
        /// JSON file with the connector config
        #[arg(short = 'f', long)]
        file: String,
        /// Connector name (defaults to "name" in the file)
        #[arg(long)]
        name: Option<String>,
    },
    /// List the connector classes installed on the workers
    Plugins,
    /// Check a connector config file against its class without creating it
    Validate {
        /// JSON file with the connector config
        #[arg(short = 'f', long)]
        file: String,
    },
    /// Delete a connector
    Delete {
        name: String,
        /// Delete without asking
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

pub async fn run(args: ConnectArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let url = args
        .url
        .clone()
        .or_else(|| global.to_kafka_config().connect_url)
        .ok_or_else(|| {
            KafkaError::config(
                "No Kafka Connect URL: pass --url or add one to the profile \
                 (ferrika profile add <name> -b <servers> --connect http://host:8083)",
            )
        })?;
    let client = ConnectClient::new(&url)?.with_timeout(global.http_timeout());
    let action = match &args.command {
        ConnectSubcommands::Pause { name } => Some((name, ConnectorAction::Pause)),
        ConnectSubcommands::Resume { name } => Some((name, ConnectorAction::Resume)),
        ConnectSubcommands::Restart { name } => Some((name, ConnectorAction::Restart)),
        ConnectSubcommands::RestartTask { name, task } => {
            Some((name, ConnectorAction::RestartTask(*task)))
        }
        _ => None,
    };
    match &args.command {
        ConnectSubcommands::Apply { file, name } => {
            let text = std::fs::read_to_string(file)
                .map_err(|e| KafkaError::File(format!("Cannot read '{file}': {e}")))?;
            let (file_name, config) = crate::kafka::connect::parse_config_json(&text)?;
            let name = name.clone().or(file_name).ok_or_else(|| {
                KafkaError::config("Name the connector with --name or \"name\" in the file")
            })?;
            let applied = State::Connector {
                name: name.clone(),
                config: Some(config.clone()),
                paused: false,
                stopped: false,
                secret_keys: config
                    .iter()
                    .filter(|(k, v)| {
                        crate::kafka::connect::is_sensitive_key(k) && !v.starts_with("${")
                    })
                    .map(|(k, _)| k.clone())
                    .collect(),
            };
            let captured = capture::connector(&client, &name).await.map(|before| {
                let mut after = applied;
                if let (
                    State::Connector {
                        paused: was_paused,
                        stopped: was_stopped,
                        ..
                    },
                    State::Connector {
                        paused, stopped, ..
                    },
                ) = (&before, &mut after)
                {
                    *paused = *was_paused;
                    *stopped = *was_stopped;
                }
                (vec![before], vec![after])
            });
            let pending = record::start(
                global,
                record::connect_scope(global, &url),
                record::change_from_capture(Reversibility::Reversible, captured),
            );
            let result = client.put_config(&name, &config).await;
            record::finish(pending, &result, &format!("Applied the config of {name}"));
            result?;
            if global.format == OutputFormat::Json {
                return output::print_json(&serde_json::json!({
                    "connector": name,
                    "action": "apply",
                    "applied": true,
                }));
            }
            println!(
                "{} Applied the config of '{}'.",
                "✔".green().bold(),
                name.cyan()
            );
            return Ok(());
        }
        ConnectSubcommands::Stop { name } => {
            let captured = capture::connector(&client, name).await.map(|before| {
                let mut after = before.clone();
                if let State::Connector {
                    paused, stopped, ..
                } = &mut after
                {
                    *paused = false;
                    *stopped = true;
                }
                (vec![before], vec![after])
            });
            let pending = record::start(
                global,
                record::connect_scope(global, &url),
                record::change_from_capture(Reversibility::Reversible, captured),
            );
            let result = client.stop(name).await;
            record::finish(pending, &result, &format!("Stop requested for {name}"));
            result?;
            if global.format == OutputFormat::Json {
                return output::print_json(&serde_json::json!({
                    "connector": name,
                    "action": "stop",
                    "requested": true,
                }));
            }
            println!(
                "{} Stop requested for '{}'. Start it again with `ferrika connect resume {}`.",
                "✔".green().bold(),
                name.cyan(),
                name
            );
            return Ok(());
        }
        ConnectSubcommands::Offsets {
            name,
            reset,
            set,
            execute,
        } => {
            let change = match (reset, set) {
                (true, _) => Some(OffsetChange::Reset),
                (false, Some(source)) => Some(OffsetChange::Set(read_offsets(source)?)),
                (false, None) => None,
            };
            return match change {
                None => show_offsets(&client, name, global).await,
                Some(change) => change_offsets(&client, &url, name, change, *execute, global).await,
            };
        }
        ConnectSubcommands::Plugins => {
            let plugins = client.plugins().await?;
            if global.format == OutputFormat::Json {
                let items: Vec<_> = plugins
                    .iter()
                    .map(|(class, kind, version)| {
                        serde_json::json!({"class": class, "type": kind, "version": version})
                    })
                    .collect();
                output::print_json(&items)?;
                return Ok(());
            }
            let mut table = output::table(vec!["CLASS", "TYPE", "VERSION"]);
            for (class, kind, version) in &plugins {
                table.add_row(vec![Cell::new(class), Cell::new(kind), Cell::new(version)]);
            }
            println!("{table}");
            return Ok(());
        }
        ConnectSubcommands::Validate { file } => {
            let text = std::fs::read_to_string(file)
                .map_err(|e| KafkaError::File(format!("Cannot read '{file}': {e}")))?;
            let (_, config) = crate::kafka::connect::parse_config_json(&text)?;
            let problems = client.validate(&config).await?;
            if global.format == OutputFormat::Json {
                output::print_json(&serde_json::json!({
                    "valid": problems.is_empty(),
                    "problems": problems,
                }))?;
                if problems.is_empty() {
                    return Ok(());
                }
            } else if problems.is_empty() {
                println!("{} The config is valid.", "✔".green().bold());
                return Ok(());
            }
            if global.format != OutputFormat::Json {
                for p in &problems {
                    eprintln!("{} {p}", "✖".red());
                }
            }
            return Err(KafkaError::Check(format!(
                "{} problem(s) in the connector config",
                problems.len()
            )));
        }
        ConnectSubcommands::Delete { name, yes } => {
            if !yes {
                crate::cli::offset::confirm(
                    &format!("Delete connector '{}'? [y/N]: ", name.red().bold()),
                    "Aborted connector deletion.",
                )?;
            }
            let captured = capture::connector(&client, name).await.map(|before| {
                let after = State::Connector {
                    name: name.clone(),
                    config: None,
                    paused: false,
                    stopped: false,
                    secret_keys: Vec::new(),
                };
                (vec![before], vec![after])
            });
            let pending = record::start(
                global,
                record::connect_scope(global, &url),
                record::change_from_capture(Reversibility::Partial, captured).limit(
                    "undo creates the connector again with its config; task history is not restored",
                ),
            );
            let result = client.delete(name).await;
            record::finish(pending, &result, &format!("Deleted connector {name}"));
            result?;
            if global.format == OutputFormat::Json {
                return output::print_json(&serde_json::json!({
                    "connector": name,
                    "action": "delete",
                    "deleted": true,
                }));
            }
            println!(
                "{} Deleted connector '{}'.",
                "✔".green().bold(),
                name.cyan()
            );
            return Ok(());
        }
        _ => {}
    }
    if let Some((name, action)) = action {
        let change = match action {
            ConnectorAction::Pause | ConnectorAction::Resume => {
                let pause = action == ConnectorAction::Pause;
                let captured = capture::connector(&client, name).await.map(|before| {
                    let mut after = before.clone();
                    // Pausing or resuming a stopped connector also ends
                    // the stop.
                    if let State::Connector {
                        paused, stopped, ..
                    } = &mut after
                    {
                        *paused = pause;
                        *stopped = false;
                    }
                    (vec![before], vec![after])
                });
                record::change_from_capture(Reversibility::Reversible, captured)
            }
            ConnectorAction::Restart | ConnectorAction::RestartTask(_) => Change::irreversible(
                Reversibility::NotApplicable,
                "a restart has no lasting effect to undo",
            ),
        };
        let pending = record::start(global, record::connect_scope(global, &url), change);
        let result = client.act(name, action).await;
        record::finish(
            pending,
            &result,
            &format!("{} requested for {name}", action.label()),
        );
        result?;
        if global.format == OutputFormat::Json {
            return output::print_json(&action_json(name, action));
        }
        println!(
            "{} {} requested for '{}'.",
            "✔".green().bold(),
            action.label(),
            name.cyan()
        );
        return Ok(());
    }
    let connectors = client.list().await?;
    let only = match &args.command {
        ConnectSubcommands::Status { name } => {
            let found: Vec<_> = connectors.into_iter().filter(|c| &c.name == name).collect();
            if found.is_empty() {
                return Err(KafkaError::Admin(format!("No connector named '{name}'")));
            }
            found
        }
        _ => connectors,
    };
    if global.format == OutputFormat::Json {
        output::print_json(&masked(only))?;
        return Ok(());
    }
    if let ConnectSubcommands::Status { .. } = args.command {
        for c in &only {
            println!(
                "{} ({}, {}) on {}",
                c.name.bold(),
                c.kind,
                state_text(&c.state),
                c.worker
            );
            if let Some(trace) = &c.trace {
                for line in trace.lines().take(TRACE_LINES) {
                    println!("    {}", line.bright_black());
                }
            }
            for (k, v) in &c.config {
                println!("  {k} = {}", crate::kafka::connect::display_value(k, v));
            }
            for t in &c.tasks {
                println!("  task {} {} on {}", t.id, state_text(&t.state), t.worker);
                if let Some(trace) = &t.trace {
                    for line in trace.lines().take(TRACE_LINES) {
                        println!("      {}", line.bright_black());
                    }
                }
            }
        }
        return Ok(());
    }
    if only.is_empty() {
        println!("No connectors.");
        return Ok(());
    }
    let mut table = output::table(vec!["CONNECTOR", "TYPE", "STATE", "TASKS", "CLASS"]);
    for c in &only {
        table.add_row(vec![
            Cell::new(&c.name),
            Cell::new(&c.kind),
            Cell::new(&c.state).fg(state_color(&c.state)),
            Cell::new(format!("{}/{} running", c.running_tasks(), c.tasks.len())),
            Cell::new(&c.class),
        ]);
    }
    println!("{table}");
    Ok(())
}

/// Lines of a failure trace `connect status` shows (`-f json` has all).
const TRACE_LINES: usize = 8;

/// What `connect offsets` changes.
#[derive(Debug, Clone, PartialEq)]
enum OffsetChange {
    /// Delete every offset (`--reset`).
    Reset,
    /// Set these partitions' offsets (`--set`).
    Set(Vec<ConnectorOffset>),
}

impl OffsetChange {
    fn label(&self) -> &'static str {
        match self {
            Self::Reset => "reset",
            Self::Set(_) => "set",
        }
    }

    fn verb(&self) -> &'static str {
        match self {
            Self::Reset => "Reset",
            Self::Set(_) => "Set",
        }
    }

    /// The partitions the change touches with their new offsets, given
    /// the `current` ones.
    fn planned(&self, current: &[ConnectorOffset]) -> Vec<ConnectorOffset> {
        match self {
            Self::Reset => current
                .iter()
                .map(|c| ConnectorOffset {
                    partition: c.partition.clone(),
                    offset: None,
                })
                .collect(),
            Self::Set(offsets) => offsets.clone(),
        }
    }
}

/// The offsets of `--set`: inline JSON (starting with `{` or `[`) or the
/// path of a JSON file.
fn read_offsets(source: &str) -> KafkaResult<Vec<ConnectorOffset>> {
    let trimmed = source.trim_start();
    let text = if trimmed.starts_with('{') || trimmed.starts_with('[') {
        source.to_string()
    } else {
        std::fs::read_to_string(source)
            .map_err(|e| KafkaError::File(format!("Cannot read '{source}': {e}")))?
    };
    let offsets = crate::kafka::connect::parse_offsets_json(&text)?;
    if offsets.is_empty() {
        return Err(KafkaError::config(
            "--set names no partitions; use --reset to delete every offset",
        ));
    }
    Ok(offsets)
}

/// `connect offsets NAME`: a table (sink connectors by topic and
/// partition), or `{"connector", "offsets": [...]}` under `-f json`.
async fn show_offsets(client: &ConnectClient, name: &str, global: &GlobalOpts) -> KafkaResult<()> {
    let offsets = client.offsets(name).await?;
    if global.format == OutputFormat::Json {
        return output::print_json(&serde_json::json!({
            "connector": name,
            "offsets": offsets,
        }));
    }
    if offsets.is_empty() {
        println!("Connector '{name}' has no offsets yet.");
        return Ok(());
    }
    println!("{}", offsets_table(&offsets, None));
    Ok(())
}

/// The offsets as a table. With `planned` (a change), one row per
/// partition the change touches, its `current` offset (`-` without one)
/// and a NEW column.
fn offsets_table(
    current: &[ConnectorOffset],
    planned: Option<&[ConnectorOffset]>,
) -> comfy_table::Table {
    // (partition, current offset, new offset)
    let rows: Vec<(
        &ConnectorOffset,
        Option<&ConnectorOffset>,
        Option<&ConnectorOffset>,
    )> = match planned {
        None => current.iter().map(|o| (o, Some(o), None)).collect(),
        Some(planned) => planned
            .iter()
            .map(|p| {
                let now = current.iter().find(|o| o.partition == p.partition);
                (p, now, Some(p))
            })
            .collect(),
    };
    let mut all: Vec<ConnectorOffset> = current.to_vec();
    all.extend(planned.unwrap_or_default().iter().cloned());
    let sink = crate::kafka::connect::all_sink(&all);
    let mut header = if sink {
        vec!["TOPIC", "PARTITION", "OFFSET"]
    } else {
        vec!["PARTITION", "OFFSET"]
    };
    if planned.is_some() {
        header.push("NEW");
    }
    let mut table = output::table(header);
    for (partition, now, new) in rows {
        let now_text = now.map_or_else(|| "-".to_string(), |o| offset_text(o, sink));
        let mut row = match partition.as_sink().filter(|_| sink) {
            Some(s) => vec![
                Cell::new(s.topic),
                Cell::new(s.partition),
                Cell::new(&now_text),
            ],
            None => vec![Cell::new(partition.partition_text()), Cell::new(&now_text)],
        };
        if let Some(new) = new {
            let text = offset_text(new, sink);
            let color = if text == now_text {
                Color::White
            } else {
                Color::Yellow
            };
            row.push(Cell::new(text).fg(color));
        }
        table.add_row(row);
    }
    table
}

/// An offset as shown: the number for sink connectors, else its JSON;
/// `-` without one.
fn offset_text(o: &ConnectorOffset, sink: bool) -> String {
    let text = if sink {
        o.as_sink().and_then(|s| s.offset).map(|n| n.to_string())
    } else {
        o.offset_text()
    };
    text.unwrap_or_else(|| "-".to_string())
}

/// `connect offsets NAME --reset|--set ...`: shows the plan, and with
/// `--execute` makes it, recorded so `ferrika undo` sets the previous
/// offsets back. Refused unless the connector is STOPPED.
async fn change_offsets(
    client: &ConnectClient,
    url: &str,
    name: &str,
    change: OffsetChange,
    execute: bool,
    global: &GlobalOpts,
) -> KafkaResult<()> {
    let state = client
        .status(name)
        .await?
        .ok_or_else(|| KafkaError::Admin(format!("No connector named '{name}'")))?;
    let current = client.offsets(name).await?;
    let planned = change.planned(&current);
    let stopped = state == "STOPPED";
    if execute && !stopped {
        return Err(KafkaError::Admin(format!(
            "Connector '{name}' is {state}; its offsets can be changed only while it is stopped. \
             Run `ferrika connect stop {name}` first, then this command again."
        )));
    }
    let before =
        crate::ops::history::capture::select_connector_offsets(current.clone(), Some(&planned));
    let plan_json = |executed: bool| {
        serde_json::json!({
            "connector": name,
            "action": change.label(),
            "state": state,
            "executed": executed,
            "current": before,
            "new": planned,
        })
    };
    if global.format != OutputFormat::Json {
        if planned.is_empty() {
            println!("Connector '{name}' has no offsets to reset.");
        } else {
            println!(
                "{} the offsets of connector '{}' ({}):",
                change.verb(),
                name.cyan(),
                state_text(&state)
            );
            println!("{}", offsets_table(&current, Some(&planned)));
        }
    }
    if !execute {
        if global.format == OutputFormat::Json {
            return output::print_json(&plan_json(false));
        }
        if !stopped {
            println!(
                "{} The connector is {state}: stop it first with `ferrika connect stop {name}`.",
                "note:".yellow().bold()
            );
        }
        println!(
            "\n{} Offsets were NOT changed. Re-run with '--execute' to change them.",
            "[DRY-RUN MODE]".yellow().bold()
        );
        return Ok(());
    }
    let recorded = |offsets: Vec<ConnectorOffset>| State::ConnectorOffsets {
        name: name.to_string(),
        offsets,
    };
    let pending = record::start(
        global,
        record::connect_scope(global, url),
        Change::new(
            Reversibility::Reversible,
            vec![recorded(before.clone())],
            vec![recorded(planned.clone())],
        )
        .limit("undo sets the previous offsets back only while the connector is stopped"),
    );
    let result = match &change {
        OffsetChange::Reset => client.reset_offsets(name).await,
        OffsetChange::Set(offsets) => client.alter_offsets(name, offsets).await,
    };
    record::finish(
        pending,
        &result,
        &format!("{} the offsets of connector {name}", change.verb()),
    );
    let message = result?;
    if global.format == OutputFormat::Json {
        let mut out = plan_json(true);
        out["message"] = serde_json::json!(message);
        return output::print_json(&out);
    }
    println!(
        "{} {} the offsets of '{}'. Start it again with `ferrika connect resume {}`.",
        "✔".green().bold(),
        change.verb(),
        name.cyan(),
        name
    );
    Ok(())
}

fn state_color(state: &str) -> Color {
    match state {
        "RUNNING" => Color::Green,
        "FAILED" => Color::Red,
        "PAUSED" | "STOPPED" => Color::Yellow,
        _ => Color::White,
    }
}

fn state_text(state: &str) -> colored::ColoredString {
    match state {
        "RUNNING" => state.green(),
        "FAILED" => state.red(),
        _ => state.yellow(),
    }
}

/// `connectors` with sensitive config values masked, as the text view shows
/// them: JSON output goes to logs and tickets as readily as the screen.
fn masked(
    mut connectors: Vec<crate::kafka::connect::ConnectorView>,
) -> Vec<crate::kafka::connect::ConnectorView> {
    for c in &mut connectors {
        for (k, v) in c.config.iter_mut() {
            *v = crate::kafka::connect::display_value(k, v);
        }
    }
    connectors
}

/// A pause, resume or restart under `-f json`: the connector, the action
/// (`pause`, `resume`, `restart`, `restart-task` with its `task`) and that
/// it was requested.
fn action_json(name: &str, action: ConnectorAction) -> serde_json::Value {
    let (label, task) = match action {
        ConnectorAction::Pause => ("pause", None),
        ConnectorAction::Resume => ("resume", None),
        ConnectorAction::Restart => ("restart", None),
        ConnectorAction::RestartTask(task) => ("restart-task", Some(task)),
    };
    let mut value = serde_json::json!({
        "connector": name,
        "action": label,
        "requested": true,
    });
    if let (Some(task), Some(obj)) = (task, value.as_object_mut()) {
        obj.insert("task".into(), serde_json::json!(task));
    }
    value
}

#[cfg(test)]
mod tests {
    #[test]
    fn action_json_names_the_action_and_task() {
        use crate::kafka::connect::ConnectorAction;
        let v = super::action_json("sink", ConnectorAction::Pause);
        assert_eq!(v["action"], "pause");
        assert!(v.get("task").is_none());
        let v = super::action_json("sink", ConnectorAction::RestartTask(2));
        assert_eq!(
            (v["action"].clone(), v["task"].clone()),
            ("restart-task".into(), 2.into())
        );
        assert_eq!(v["requested"], true);
    }

    use super::*;

    #[test]
    fn json_output_masks_connector_secrets() {
        let view = crate::kafka::connect::ConnectorView {
            name: "jdbc-sink".into(),
            kind: "sink".into(),
            state: "RUNNING".into(),
            worker: "w1".into(),
            trace: None,
            class: "JdbcSink".into(),
            config: [
                ("connection.password", "hunter2"),
                ("connection.user", "app"),
                ("sasl.jaas.config", "... password=\"s3cret\";"),
                ("db.password", "${file:/secrets:db}"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            tasks: Vec::new(),
        };
        let json = serde_json::to_string(&masked(vec![view])).unwrap();
        assert!(
            !json.contains("hunter2") && !json.contains("s3cret"),
            "{json}"
        );
        assert!(json.contains("\"connection.user\":\"app\""), "{json}");
        // Config provider references are not secrets.
        assert!(json.contains("${file:/secrets:db}"), "{json}");
    }
}

#[cfg(test)]
mod offset_tests {
    use super::*;

    #[test]
    fn set_takes_inline_json_or_a_file() {
        let inline = read_offsets(
            r#"{"offsets":[{"partition":{"kafka_topic":"t","kafka_partition":0},"offset":{"kafka_offset":3}}]}"#,
        )
        .unwrap();
        assert_eq!(inline, vec![ConnectorOffset::sink("t", 0, Some(3))]);
        let dir = std::env::temp_dir().join(format!("ferrika-offsets-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("offsets.json");
        std::fs::write(
            &file,
            r#"[{"partition":{"filename":"/in.txt"},"offset":{"position":10}}]"#,
        )
        .unwrap();
        let from_file = read_offsets(file.to_str().unwrap()).unwrap();
        assert_eq!(from_file[0].partition_text(), r#"{"filename":"/in.txt"}"#);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(read_offsets(r#"{"offsets":[]}"#).is_err());
        assert!(read_offsets("no-such-file.json").is_err());
    }

    #[test]
    fn a_reset_clears_every_current_partition() {
        let current = vec![
            ConnectorOffset::sink("t", 0, Some(7)),
            ConnectorOffset::sink("t", 1, Some(9)),
        ];
        assert_eq!(
            OffsetChange::Reset.planned(&current),
            vec![
                ConnectorOffset::sink("t", 0, None),
                ConnectorOffset::sink("t", 1, None)
            ]
        );
        let set = OffsetChange::Set(vec![ConnectorOffset::sink("t", 1, Some(0))]);
        assert_eq!(set.planned(&current).len(), 1);
        assert_eq!((set.label(), OffsetChange::Reset.label()), ("set", "reset"));
    }

    #[test]
    fn offset_tables_show_sink_numbers_and_source_json() {
        let current = vec![ConnectorOffset::sink("orders", 0, Some(7))];
        let planned = vec![
            ConnectorOffset::sink("orders", 0, Some(1)),
            ConnectorOffset::sink("orders", 1, Some(4)),
        ];
        let text = offsets_table(&current, Some(&planned)).to_string();
        assert!(text.contains("TOPIC") && text.contains("NEW"), "{text}");
        assert!(text.contains('7') && text.contains('4'), "{text}");
        let source = parse_source();
        let text = offsets_table(&source, None).to_string();
        assert!(text.contains(r#"{"filename":"/in.txt"}"#), "{text}");
        assert!(text.contains(r#"{"position":10}"#), "{text}");
        assert!(!text.contains("TOPIC"), "{text}");
        assert_eq!(offset_text(&ConnectorOffset::sink("t", 0, None), true), "-");
    }

    fn parse_source() -> Vec<ConnectorOffset> {
        crate::kafka::connect::parse_offsets_json(
            r#"[{"partition":{"filename":"/in.txt"},"offset":{"position":10}}]"#,
        )
        .unwrap()
    }
}
