//! `ferrika connect`: Kafka Connect connectors.

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::{Cell, Color};

use crate::cli::record::{self, Change};
use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::connect::{ConnectClient, ConnectorAction};
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
    let client = ConnectClient::new(&url)?;
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
                if let (State::Connector { paused: was, .. }, State::Connector { paused, .. }) =
                    (&before, &mut after)
                {
                    *paused = *was;
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
            println!(
                "{} Applied the config of '{}'.",
                "✔".green().bold(),
                name.cyan()
            );
            return Ok(());
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
            if problems.is_empty() {
                println!("{} The config is valid.", "✔".green().bold());
                return Ok(());
            }
            for p in &problems {
                eprintln!("{} {p}", "✖".red());
            }
            return Err(KafkaError::Check(format!(
                "{} problem(s) in the connector config",
                problems.len()
            )));
        }
        ConnectSubcommands::Delete { name, yes } => {
            if !yes {
                use std::io::Write as _;
                print!("Delete connector '{}'? [y/N]: ", name.red().bold());
                std::io::stdout().flush().ok();
                let mut input = String::new();
                std::io::stdin()
                    .read_line(&mut input)
                    .map_err(|e| KafkaError::io(e.to_string()))?;
                if !matches!(input.trim().to_lowercase().as_str(), "y" | "yes") {
                    println!("{}", "Aborted.".yellow());
                    return Ok(());
                }
            }
            let captured = capture::connector(&client, name).await.map(|before| {
                let after = State::Connector {
                    name: name.clone(),
                    config: None,
                    paused: false,
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
                    if let State::Connector { paused, .. } = &mut after {
                        *paused = pause;
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
        output::print_json(&only)?;
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
            for (k, v) in &c.config {
                println!("  {k} = {}", crate::kafka::connect::display_value(k, v));
            }
            for t in &c.tasks {
                println!("  task {} {} on {}", t.id, state_text(&t.state), t.worker);
                if let Some(trace) = &t.trace {
                    for line in trace.lines().take(8) {
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
