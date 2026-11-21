use std::fs;

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::{Cell, CellAlignment, Color};
use serde_json::json;

use crate::cli::record;
use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::codec::schema::{
    format_hexdump, parse_confluent_wire_format, parse_hex_string, SchemaDefinition,
};
use crate::codec::schema_compat::{evaluate_compatibility, CompatibilityMode, CompatibilityReport};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::schema::SchemaRegistryClient;
use crate::model::history::{Reversibility, State};
use crate::ops::history::capture;

/// Command line arguments for Schema Registry operations and wire format decoding.
#[derive(Args, Debug, Clone)]
pub struct SchemaArgs {
    #[command(subcommand)]
    pub command: SchemaSubcommands,
}

/// Supported subcommands for `ferrika schema`.
#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum SchemaSubcommands {
    /// List all registered subjects in Confluent Schema Registry
    List {
        /// Schema Registry URL (defaults to the profile's, else http://localhost:8081)
        #[arg(short = 'S', long)]
        registry_url: Option<String>,
    },
    /// Fetch schema definition by its global ID from Schema Registry
    Get {
        /// Schema ID to look up
        id: u32,
        /// Schema Registry URL (defaults to the profile's, else http://localhost:8081)
        #[arg(short = 'S', long)]
        registry_url: Option<String>,
    },
    /// Fetch latest schema definition for a specific subject
    Subject {
        /// Subject name (e.g. "orders-value")
        subject: String,
        /// Schema Registry URL (defaults to the profile's, else http://localhost:8081)
        #[arg(short = 'S', long)]
        registry_url: Option<String>,
    },
    /// List the field paths of a subject's schema, as filter expressions
    /// (`--where`, the Message Studio filter) write them
    Fields {
        /// Subject name (e.g. "orders-value"); or use --topic
        #[arg(required_unless_present = "topic", conflicts_with = "topic")]
        subject: Option<String>,
        /// Use the topic's `<topic>-value` subject (`-key` with --key)
        #[arg(long)]
        topic: Option<String>,
        /// With --topic: the key subject instead of the value subject
        #[arg(long, requires = "topic")]
        key: bool,
        /// Subject version (default: the latest)
        #[arg(long)]
        version: Option<u32>,
        /// Protobuf message (full or short name; default: the first)
        #[arg(long)]
        message: Option<String>,
        /// One path per line, nothing else (for scripts and fzf)
        #[arg(long)]
        plain: bool,
        /// Schema Registry URL (defaults to the profile's, else http://localhost:8081)
        #[arg(short = 'S', long)]
        registry_url: Option<String>,
    },
    /// Decode a raw hexadecimal wire-format payload and inspect schema ID & body
    Decode {
        /// Hex string of the Kafka message (with or without spaces/0x prefix)
        hex_data: String,
    },
    /// Check compatibility of a candidate schema against the latest registered schema in Schema Registry
    Check {
        /// Subject name to test compatibility against
        subject: String,
        /// Path to candidate schema file (.json or .avsc)
        #[arg(short = 'f', long)]
        schema_file: String,
        /// Schema Registry URL (defaults to the profile's, else http://localhost:8081)
        #[arg(short = 'S', long)]
        registry_url: Option<String>,
        /// Compatibility mode: backward, forward, full, none (or *_transitive)
        #[arg(short = 'm', long, default_value = "backward")]
        mode: String,
        /// Fail with non-zero exit code if candidate schema is incompatible
        #[arg(long)]
        fail_on_incompatible: bool,
    },
    /// Register a schema file as the next version of a subject (checked for
    /// compatibility by the registry)
    Register {
        /// Subject name (e.g. "orders-value")
        subject: String,
        /// Path to the schema file
        #[arg(short = 'f', long)]
        schema_file: String,
        /// Schema type: AVRO, JSON or PROTOBUF
        #[arg(short = 't', long, default_value = "AVRO")]
        schema_type: String,
        /// Schema Registry URL (defaults to the profile's, else http://localhost:8081)
        #[arg(short = 'S', long)]
        registry_url: Option<String>,
    },
    /// Show or set a subject's compatibility level
    Compat {
        /// Subject name
        subject: String,
        /// New level: BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
        /// FULL, FULL_TRANSITIVE or NONE
        #[arg(long, value_name = "LEVEL")]
        set: Option<String>,
        /// Schema Registry URL (defaults to the profile's, else http://localhost:8081)
        #[arg(short = 'S', long)]
        registry_url: Option<String>,
    },
    /// Delete a subject and all its versions (soft delete)
    DeleteSubject {
        /// Subject name
        subject: String,
        /// Delete without asking
        #[arg(short = 'y', long)]
        yes: bool,
        /// Schema Registry URL (defaults to the profile's, else http://localhost:8081)
        #[arg(short = 'S', long)]
        registry_url: Option<String>,
    },
    /// Compare two local schema files and analyze evolutionary compatibility
    DiffSchema {
        /// Path to base/previous schema file
        old_file: String,
        /// Path to candidate/new schema file
        new_file: String,
        /// Compatibility mode: backward, forward, full, none (or *_transitive)
        #[arg(short = 'm', long, default_value = "backward")]
        mode: String,
        /// Fail with non-zero exit code if incompatible
        #[arg(long)]
        fail_on_incompatible: bool,
    },
}

/// The registry URL to use: the flag, else the profile's, else localhost.
fn registry(flag: Option<String>, global: &GlobalOpts) -> String {
    flag.or_else(|| global.to_kafka_config().schema_registry_url)
        .unwrap_or_else(|| "http://localhost:8081".to_string())
}

async fn handle_register(
    subject: &str,
    schema_file: &str,
    schema_type: &str,
    registry_url: &str,
    global: &GlobalOpts,
) -> KafkaResult<()> {
    let schema = std::fs::read_to_string(schema_file)
        .map_err(|e| KafkaError::File(format!("Cannot read '{schema_file}': {e}")))?;
    let client = SchemaRegistryClient::new(registry_url).with_timeout(global.http_timeout());
    let captured = capture::subject_versions(&client, subject, false)
        .await
        .map(|before| (vec![before], Vec::new()));
    let pending = record::start(
        global,
        record::registry_scope(global, registry_url),
        record::change_from_capture(Reversibility::Partial, captured)
            .limit("undo soft-deletes the new version; the schema id stays known to the registry"),
    );
    let id = client.register(subject, &schema, schema_type).await;
    if let Some(pending) = pending {
        // The new version number is only known now.
        let after = capture::subject_versions(&client, subject, false)
            .await
            .ok()
            .map(|state| vec![state]);
        match &id {
            Ok(id) => pending.finish(true, &format!("Registered schema id {id}"), after),
            Err(e) => pending.finish(false, &e.to_string(), after),
        }
    }
    let id = id?;
    println!(
        "{} Registered under '{}' with schema id {}.",
        "✔".green().bold(),
        subject.cyan(),
        id
    );
    Ok(())
}

async fn handle_compat(
    subject: &str,
    set: Option<String>,
    registry_url: &str,
    global: &GlobalOpts,
) -> KafkaResult<()> {
    let client = SchemaRegistryClient::new(registry_url).with_timeout(global.http_timeout());
    if let Some(level) = set {
        let level = level.to_uppercase();
        let after = State::SubjectCompat {
            subject: subject.to_string(),
            level: Some(level.clone()),
        };
        let captured = capture::subject_compat(&client, subject)
            .await
            .map(|before| (vec![before], vec![after]));
        let pending = record::start(
            global,
            record::registry_scope(global, registry_url),
            record::change_from_capture(Reversibility::Reversible, captured),
        );
        let result = client.set_compatibility(subject, &level).await;
        record::finish(pending, &result, &format!("{subject} now uses {level}"));
        result?;
        println!(
            "{} '{}' now uses {}.",
            "✔".green().bold(),
            subject.cyan(),
            level
        );
    } else {
        println!("{}", client.compatibility(subject).await?);
    }
    Ok(())
}

async fn handle_delete_subject(
    subject: &str,
    yes: bool,
    registry_url: &str,
    global: &GlobalOpts,
) -> KafkaResult<()> {
    if !yes {
        use std::io::Write as _;
        print!(
            "Delete subject '{}' and all its versions? [y/N]: ",
            subject.red().bold()
        );
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
    let client = SchemaRegistryClient::new(registry_url).with_timeout(global.http_timeout());
    let captured = capture::subject_versions(&client, subject, true)
        .await
        .map(|before| {
            let after = State::SubjectVersions {
                subject: subject.to_string(),
                versions: Vec::new(),
            };
            (vec![before], vec![after])
        });
    let pending = record::start(
        global,
        record::registry_scope(global, registry_url),
        record::change_from_capture(Reversibility::Partial, captured)
            .limit("undo registers the schemas again in order; they get new version numbers"),
    );
    let versions = client.delete_subject(subject).await;
    record::finish(pending, &versions, &format!("Deleted subject {subject}"));
    let versions = versions?;
    println!(
        "{} Deleted '{}' (versions {:?}).",
        "✔".green().bold(),
        subject.cyan(),
        versions
    );
    Ok(())
}

/// Main entry point for `ferrika schema`.
pub async fn run(args: SchemaArgs, global: &GlobalOpts) -> KafkaResult<()> {
    handle_schema_command(args, global).await
}

/// Handler for schema subcommands.
pub async fn handle_schema_command(args: SchemaArgs, global: &GlobalOpts) -> KafkaResult<()> {
    match args.command {
        SchemaSubcommands::Register {
            subject,
            schema_file,
            schema_type,
            registry_url,
        } => {
            handle_register(
                &subject,
                &schema_file,
                &schema_type,
                &registry(registry_url, global),
                global,
            )
            .await
        }
        SchemaSubcommands::Compat {
            subject,
            set,
            registry_url,
        } => handle_compat(&subject, set, &registry(registry_url, global), global).await,
        SchemaSubcommands::DeleteSubject {
            subject,
            yes,
            registry_url,
        } => handle_delete_subject(&subject, yes, &registry(registry_url, global), global).await,
        SchemaSubcommands::List { registry_url } => {
            handle_list_subjects(&registry(registry_url, global), global).await
        }
        SchemaSubcommands::Get { id, registry_url } => {
            handle_get_schema(id, &registry(registry_url, global), global).await
        }
        SchemaSubcommands::Subject {
            subject,
            registry_url,
        } => handle_get_subject(&subject, &registry(registry_url, global), global).await,
        SchemaSubcommands::Fields {
            subject,
            topic,
            key,
            version,
            message,
            plain,
            registry_url,
        } => {
            let subject = fields_subject(subject, topic.as_deref(), key)?;
            let request = FieldsRequest {
                subject: &subject,
                version,
                message: message.as_deref(),
                plain,
            };
            handle_fields(request, &registry(registry_url, global), global).await
        }
        SchemaSubcommands::Decode { hex_data } => handle_decode_hex(&hex_data, global),
        SchemaSubcommands::Check {
            subject,
            schema_file,
            registry_url,
            mode,
            fail_on_incompatible,
        } => {
            handle_check_schema(
                &subject,
                &schema_file,
                &registry(registry_url, global),
                &mode,
                fail_on_incompatible,
                global,
            )
            .await
        }
        SchemaSubcommands::DiffSchema {
            old_file,
            new_file,
            mode,
            fail_on_incompatible,
        } => handle_diff_schema(&old_file, &new_file, &mode, fail_on_incompatible, global),
    }
}

/// The subject `schema fields` reads: the one given, else `<topic>-value`
/// (`<topic>-key` with `key`).
fn fields_subject(subject: Option<String>, topic: Option<&str>, key: bool) -> KafkaResult<String> {
    match (subject, topic) {
        (Some(_), _) if key => Err(KafkaError::config("--key goes with --topic")),
        (Some(subject), _) => Ok(subject),
        (None, Some(topic)) => Ok(format!("{topic}-{}", if key { "key" } else { "value" })),
        (None, None) => Err(KafkaError::config("Give a subject or --topic <TOPIC>")),
    }
}

struct FieldsRequest<'a> {
    subject: &'a str,
    version: Option<u32>,
    message: Option<&'a str>,
    plain: bool,
}

/// One line of `schema fields` output.
#[derive(serde::Serialize)]
struct FieldRow {
    path: String,
    #[serde(rename = "type")]
    kind: String,
    optional: bool,
    recursive: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    oneof: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    values: Vec<String>,
}

fn field_rows(catalog: &crate::codec::schema_fields::FieldCatalog) -> Vec<FieldRow> {
    catalog
        .entries
        .iter()
        .map(|e| FieldRow {
            path: e.path(),
            kind: e.kind.label(),
            optional: e.optional,
            recursive: e.recursive,
            oneof: e.group.clone(),
            values: e.enum_values.clone(),
        })
        .collect()
}

/// "optional, recursive, oneof payment" for the FLAGS column.
fn field_flags(row: &FieldRow) -> String {
    let mut flags = Vec::new();
    if row.optional {
        flags.push("optional".to_string());
    }
    if row.recursive {
        flags.push("recursive".to_string());
    }
    if let Some(group) = &row.oneof {
        flags.push(format!("oneof {group}"));
    }
    flags.join(", ")
}

/// The first few values and how many there are in all.
fn field_values(values: &[String]) -> String {
    const SHOWN: usize = 5;
    let mut text = values
        .iter()
        .take(SHOWN)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if values.len() > SHOWN {
        text.push_str(&format!(" … ({} in all)", values.len()));
    }
    text
}

async fn handle_fields(
    request: FieldsRequest<'_>,
    registry_url: &str,
    global: &GlobalOpts,
) -> KafkaResult<()> {
    use crate::codec::schema_fields::{from_definition, Limits};
    let client = SchemaRegistryClient::new(registry_url).with_timeout(global.http_timeout());
    let def = client
        .subject_schema_resolved(request.subject, request.version)
        .await?;
    let catalog = from_definition(&def, request.message, &Limits::default())?;
    let messages = if def.schema_type.eq_ignore_ascii_case("PROTOBUF") {
        crate::codec::proto_schema::ProtoSchema::parse_with_imports(
            &def.schema,
            &def.resolved_references,
        )
        .map(|s| s.top_message_names())
        .unwrap_or_default()
    } else {
        Vec::new()
    };
    let rows = field_rows(&catalog);
    if request.plain {
        for row in &rows {
            println!("{}", row.path);
        }
        return Ok(());
    }
    match global.format {
        OutputFormat::Json => output::print_json(&json!({
            "subject": request.subject,
            "id": def.id,
            "version": def.version,
            "schema_type": def.schema_type,
            "message": request.message.or(messages.first().map(String::as_str)),
            "messages": messages,
            "truncated": catalog.truncated,
            "notes": catalog.notes,
            "fields": rows,
        })),
        OutputFormat::Table => {
            println!(
                "{} {} · {} schema {}{}",
                "Fields of".cyan(),
                request.subject.green().bold(),
                def.schema_type,
                def.id,
                def.version
                    .map(|v| format!(" (version {v})"))
                    .unwrap_or_default()
            );
            if messages.len() > 1 {
                let chosen = request.message.or(messages.first().map(String::as_str));
                println!(
                    "Messages: {} (showing {}; pick one with --message)",
                    messages.join(", "),
                    chosen.unwrap_or_default()
                );
            }
            if rows.is_empty() {
                println!("{} The schema has no fields.", "ℹ".blue());
                return Ok(());
            }
            let mut table = output::table(vec![
                Cell::new("PATH").fg(Color::Cyan),
                Cell::new("TYPE").fg(Color::Cyan),
                Cell::new("FLAGS").fg(Color::Cyan),
                Cell::new("VALUES").fg(Color::Cyan),
            ]);
            for row in &rows {
                table.add_row(vec![
                    Cell::new(&row.path).fg(Color::Green),
                    Cell::new(&row.kind).fg(Color::Magenta),
                    Cell::new(field_flags(row)).fg(Color::DarkGrey),
                    Cell::new(field_values(&row.values)),
                ]);
            }
            println!("{table}");
            if catalog.truncated {
                println!(
                    "{} Some fields were left out (depth or count limit).",
                    "⚠".yellow()
                );
            }
            for note in &catalog.notes {
                println!("{} {note}", "ℹ".blue());
            }
            Ok(())
        }
    }
}

/// `url` as it may be printed: without `user:password@` (a profile's URL
/// can carry the registry's API key and secret).
fn shown_url(url: &str) -> String {
    crate::model::format::redact_url(url)
}

async fn handle_list_subjects(registry_url: &str, global: &GlobalOpts) -> KafkaResult<()> {
    let client = SchemaRegistryClient::new(registry_url).with_timeout(global.http_timeout());

    if global.format != OutputFormat::Json {
        println!(
            "{} Fetching registered subjects from Schema Registry at {}\n",
            "🔍".cyan(),
            shown_url(registry_url).yellow()
        );
    }

    let subjects = client.list_subjects().await?;

    match global.format {
        OutputFormat::Json => {
            output::print_json(&json!({
                "registry_url": shown_url(registry_url),
                "total": subjects.len(),
                "subjects": subjects,
            }))?;
        }
        OutputFormat::Table => {
            if subjects.is_empty() {
                println!(
                    "{} No registered subjects found in Schema Registry.",
                    "ℹ".blue()
                );
                return Ok(());
            }

            let mut table = output::table(vec![
                Cell::new("#").fg(Color::Cyan),
                Cell::new("SUBJECT NAME")
                    .fg(Color::Cyan)
                    .set_alignment(CellAlignment::Left),
            ]);

            for (i, subj) in subjects.iter().enumerate() {
                table.add_row(vec![
                    Cell::new((i + 1).to_string()).fg(Color::DarkGrey),
                    Cell::new(subj).fg(Color::Green),
                ]);
            }

            println!("{}", table);
            println!(
                "\n{} Total subjects registered: {}",
                "✔".green(),
                subjects.len().to_string().yellow().bold()
            );
        }
    }

    Ok(())
}

async fn handle_get_schema(id: u32, registry_url: &str, global: &GlobalOpts) -> KafkaResult<()> {
    let client = SchemaRegistryClient::new(registry_url).with_timeout(global.http_timeout());

    if global.format != OutputFormat::Json {
        println!(
            "{} Fetching Schema ID {} from {}\n",
            "🔍".cyan(),
            id.to_string().yellow().bold(),
            shown_url(registry_url).blue()
        );
    }

    let def = client.get_schema_by_id(id).await?;

    render_schema_definition(&def, global);
    Ok(())
}

async fn handle_get_subject(
    subject: &str,
    registry_url: &str,
    global: &GlobalOpts,
) -> KafkaResult<()> {
    let client = SchemaRegistryClient::new(registry_url).with_timeout(global.http_timeout());

    if global.format != OutputFormat::Json {
        println!(
            "{} Fetching latest schema for subject '{}' from {}\n",
            "🔍".cyan(),
            subject.green().bold(),
            shown_url(registry_url).blue()
        );
    }

    let def = client.get_latest_subject_schema(subject).await?;

    render_schema_definition(&def, global);
    Ok(())
}

fn render_schema_definition(def: &SchemaDefinition, global: &GlobalOpts) {
    match global.format {
        OutputFormat::Json => {
            if let Err(e) = output::print_json(def) {
                crate::cli::exit_with_error(&e);
            }
        }
        OutputFormat::Table => {
            let mut table = output::table(vec![
                Cell::new("PROPERTY").fg(Color::Cyan),
                Cell::new("VALUE").fg(Color::Cyan),
            ]);

            table.add_row(vec![
                Cell::new("Schema ID").fg(Color::DarkGrey),
                Cell::new(def.id.to_string()).fg(Color::Yellow),
            ]);

            table.add_row(vec![
                Cell::new("Schema Type").fg(Color::DarkGrey),
                Cell::new(&def.schema_type).fg(Color::Magenta),
            ]);

            if let Some(ref subj) = def.subject {
                table.add_row(vec![
                    Cell::new("Subject").fg(Color::DarkGrey),
                    Cell::new(subj).fg(Color::Green),
                ]);
            }

            if let Some(v) = def.version {
                table.add_row(vec![
                    Cell::new("Version").fg(Color::DarkGrey),
                    Cell::new(v.to_string()).fg(Color::Blue),
                ]);
            }

            println!("{}", table);

            // Pretty print schema content
            println!("\n{}", "--- Schema Content ---".cyan().bold());
            if let Ok(parsed_json) = serde_json::from_str::<serde_json::Value>(&def.schema) {
                if let Ok(pretty) = serde_json::to_string_pretty(&parsed_json) {
                    println!("{}", pretty.green());
                } else {
                    println!("{}", def.schema);
                }
            } else {
                println!("{}", def.schema);
            }
        }
    }
}

fn handle_decode_hex(hex_data: &str, global: &GlobalOpts) -> KafkaResult<()> {
    let bytes = parse_hex_string(hex_data)?;

    let wire_msg = parse_confluent_wire_format(&bytes);

    match global.format {
        OutputFormat::Json => {
            let json_out = if let Some(ref msg) = wire_msg {
                json!({
                    "is_confluent_wire_format": true,
                    "schema_id": msg.schema_id,
                    "raw_header_hex": format!("{:02x?}", msg.raw_header),
                    "payload_len": msg.payload.len(),
                    "is_utf8": msg.is_utf8,
                    "is_json": msg.is_json,
                    "payload_text": msg.payload_as_str(),
                })
            } else {
                json!({
                    "is_confluent_wire_format": false,
                    "total_bytes": bytes.len(),
                    "is_utf8": std::str::from_utf8(&bytes).is_ok(),
                    "text": std::str::from_utf8(&bytes).ok(),
                })
            };
            output::print_json(&json_out)?;
        }
        OutputFormat::Table => {
            println!("{} Wire-Format Decoding Result:\n", "📦".cyan());

            if let Some(ref msg) = wire_msg {
                println!(
                    "  {} Confluent Schema Registry Wire-Format detected!",
                    "✔".green().bold()
                );
                println!(
                    "  Schema ID:      {}",
                    msg.schema_id.to_string().yellow().bold()
                );
                println!(
                    "  Header Bytes:   [0x{:02x}, 0x{:02x}, 0x{:02x}, 0x{:02x}, 0x{:02x}]",
                    msg.raw_header[0],
                    msg.raw_header[1],
                    msg.raw_header[2],
                    msg.raw_header[3],
                    msg.raw_header[4]
                );
                println!("  Payload Length: {} bytes", msg.payload.len());

                if let Some(text) = msg.payload_as_str() {
                    println!("\n{}", "--- Decoded Text / JSON Payload ---".cyan());
                    if msg.is_json {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
                            if let Ok(pretty) = serde_json::to_string_pretty(&v) {
                                println!("{}", pretty.green());
                            } else {
                                println!("{}", text.green());
                            }
                        } else {
                            println!("{}", text.green());
                        }
                    } else {
                        println!("{}", text.green());
                    }
                } else {
                    println!(
                        "\n{}",
                        "--- Binary Payload Hexdump (without wire header) ---".cyan()
                    );
                    println!("{}", format_hexdump(&msg.payload));
                }
            } else {
                println!(
                    "  {} Not a Confluent Wire Format message (Magic byte 0x00 not found or <5 bytes).",
                    "ℹ".yellow()
                );
                println!("  Total Bytes: {}", bytes.len());
                if let Ok(text) = std::str::from_utf8(&bytes) {
                    println!("\n{}", "--- Decoded Plain Text ---".cyan());
                    println!("{}", text.green());
                } else {
                    println!("\n{}", "--- Raw Hexdump ---".cyan());
                    println!("{}", format_hexdump(&bytes));
                }
            }
        }
    }

    Ok(())
}

/// Parses a compatibility level; the flag is true for the `*_TRANSITIVE`
/// variants (check against every registered version, not just the latest).
fn parse_compat_mode(mode_str: &str) -> KafkaResult<(CompatibilityMode, bool)> {
    let normalized = mode_str.trim().to_ascii_lowercase().replace('-', "_");
    Ok(match normalized.as_str() {
        "backward" => (CompatibilityMode::Backward, false),
        "backward_transitive" => (CompatibilityMode::Backward, true),
        "forward" => (CompatibilityMode::Forward, false),
        "forward_transitive" => (CompatibilityMode::Forward, true),
        "full" => (CompatibilityMode::Full, false),
        "full_transitive" => (CompatibilityMode::Full, true),
        "none" => (CompatibilityMode::None, false),
        _ => {
            return Err(KafkaError::config(format!(
                "Unknown compatibility mode '{mode_str}' (expected backward, forward, full, none, \
                 backward_transitive, forward_transitive or full_transitive)"
            )))
        }
    })
}

/// Folds the reports from checking one candidate against several
/// registered versions into one; violations name the version they break.
fn merge_compat_reports(
    subject: &str,
    mode: CompatibilityMode,
    reports: Vec<(u32, CompatibilityReport)>,
) -> CompatibilityReport {
    let mut merged = CompatibilityReport {
        subject: Some(subject.to_string()),
        mode,
        is_compatible: true,
        violations: Vec::new(),
        added_fields: Vec::new(),
        deleted_fields: Vec::new(),
        modified_fields: Vec::new(),
    };
    for (version, report) in reports {
        merged.is_compatible &= report.is_compatible;
        for mut v in report.violations {
            v.description = format!("[vs version {version}] {}", v.description);
            merged.violations.push(v);
        }
        for (into, from) in [
            (&mut merged.added_fields, report.added_fields),
            (&mut merged.deleted_fields, report.deleted_fields),
            (&mut merged.modified_fields, report.modified_fields),
        ] {
            for field in from {
                if !into.contains(&field) {
                    into.push(field);
                }
            }
        }
    }
    merged
}

async fn handle_check_schema(
    subject: &str,
    schema_file: &str,
    registry_url: &str,
    mode_str: &str,
    fail_on_incompatible: bool,
    global: &GlobalOpts,
) -> KafkaResult<()> {
    let (mode, transitive) = parse_compat_mode(mode_str)?;
    let candidate_schema = fs::read_to_string(schema_file).map_err(|e| {
        KafkaError::io(format!(
            "Failed to read candidate schema file '{schema_file}': {e}"
        ))
    })?;

    let client = SchemaRegistryClient::new(registry_url).with_timeout(global.http_timeout());
    // Transitive: the number of registered versions checked.
    let mut checked: Option<usize> = None;
    let report = if transitive {
        let mut reports = Vec::new();
        let versions = client.list_versions(subject).await?;
        checked = Some(versions.len());
        for version in versions {
            let def = client.get_subject_version(subject, version).await?;
            let report =
                evaluate_compatibility(Some(subject), &def.schema, &candidate_schema, mode)
                    .map_err(|e| {
                        KafkaError::schema(format!(
                            "Compatibility evaluation against version {version} failed: {e}"
                        ))
                    })?;
            reports.push((version, report));
        }
        merge_compat_reports(subject, mode, reports)
    } else {
        let latest_def = client.get_latest_subject_schema(subject).await?;
        evaluate_compatibility(Some(subject), &latest_def.schema, &candidate_schema, mode)
            .map_err(|e| KafkaError::schema(format!("Compatibility evaluation failed: {e}")))?
    };

    render_compatibility_report(&report, checked, global)?;

    if fail_on_incompatible && !report.is_compatible {
        let against = match checked {
            Some(n) => format!("{n} registered version(s)"),
            None => "the latest registered schema".to_string(),
        };
        return Err(KafkaError::Check(format!(
            "Schema Compatibility Failure: Candidate schema for subject '{subject}' is incompatible with {against} ({} violations)",
            report.violations.len()
        )));
    }

    Ok(())
}

fn handle_diff_schema(
    old_file: &str,
    new_file: &str,
    mode_str: &str,
    fail_on_incompatible: bool,
    global: &GlobalOpts,
) -> KafkaResult<()> {
    // With a single base file, a transitive check is the plain check.
    let (mode, _transitive) = parse_compat_mode(mode_str)?;
    let old_schema = fs::read_to_string(old_file).map_err(|e| {
        KafkaError::io(format!("Failed to read base schema file '{old_file}': {e}"))
    })?;
    let new_schema = fs::read_to_string(new_file).map_err(|e| {
        KafkaError::io(format!(
            "Failed to read candidate schema file '{new_file}': {e}"
        ))
    })?;

    let report = evaluate_compatibility(None, &old_schema, &new_schema, mode)
        .map_err(|e| KafkaError::schema(format!("Compatibility evaluation failed: {e}")))?;

    render_compatibility_report(&report, None, global)?;

    if fail_on_incompatible && !report.is_compatible {
        return Err(KafkaError::Check(format!(
            "Schema Compatibility Failure: Candidate schema is incompatible with base schema ({} violations)",
            report.violations.len()
        )));
    }

    Ok(())
}

/// The mode as the registry names it: `BACKWARD`, or `BACKWARD_TRANSITIVE`
/// when `transitive_versions` (the versions checked) is set.
fn mode_label(mode: CompatibilityMode, transitive_versions: Option<usize>) -> String {
    match transitive_versions {
        Some(_) if mode != CompatibilityMode::None => format!("{}_TRANSITIVE", mode.as_str()),
        _ => mode.as_str().to_string(),
    }
}

/// Prints `report`; `transitive_versions` is the number of registered
/// versions a transitive check compared against.
fn render_compatibility_report(
    report: &CompatibilityReport,
    transitive_versions: Option<usize>,
    global: &GlobalOpts,
) -> KafkaResult<()> {
    if global.format == OutputFormat::Json {
        let mut json = serde_json::to_value(report).map_err(|e| {
            KafkaError::Serialization(format!("Failed to serialize the report: {e}"))
        })?;
        if let (Some(obj), Some(n)) = (json.as_object_mut(), transitive_versions) {
            obj.insert(
                "mode".into(),
                mode_label(report.mode, transitive_versions).into(),
            );
            obj.insert("versions_checked".into(), n.into());
        }
        output::print_json(&json)?;
        return Ok(());
    }

    let verdict_badge = if report.is_compatible {
        Cell::new("✔ COMPATIBLE")
            .fg(Color::Green)
            .set_alignment(CellAlignment::Center)
    } else {
        Cell::new(format!(
            "✖ INCOMPATIBLE ({} VIOLATIONS)",
            report.violations.len()
        ))
        .fg(Color::Red)
        .set_alignment(CellAlignment::Center)
    };

    println!("\n══ Schema Registry Evolution & Compatibility Report ══");

    let mut summary_table = output::fixed_table(vec![
        Cell::new("Subject").fg(Color::Cyan),
        Cell::new("Enforced Mode").fg(Color::Cyan),
        Cell::new("Verdict").fg(Color::Cyan),
        Cell::new("Added Fields").fg(Color::Cyan),
        Cell::new("Deleted Fields").fg(Color::Cyan),
        Cell::new("Modified Fields").fg(Color::Cyan),
    ]);

    summary_table.add_row(vec![
        Cell::new(report.subject.as_deref().unwrap_or("[offline file diff]")).fg(Color::White),
        Cell::new(match transitive_versions {
            Some(n) => format!(
                "{} ({n} versions)",
                mode_label(report.mode, transitive_versions)
            ),
            None => mode_label(report.mode, None),
        })
        .fg(Color::Yellow),
        verdict_badge,
        Cell::new(report.added_fields.len().to_string()).set_alignment(CellAlignment::Right),
        Cell::new(report.deleted_fields.len().to_string()).set_alignment(CellAlignment::Right),
        Cell::new(report.modified_fields.len().to_string()).set_alignment(CellAlignment::Right),
    ]);

    println!("{summary_table}\n");

    if !report.violations.is_empty() {
        let mut viol_table = output::fixed_table(vec![
            Cell::new("Violating Field").fg(Color::Cyan),
            Cell::new("Rule Broken").fg(Color::Cyan),
            Cell::new("Diagnostic Description").fg(Color::Cyan),
        ]);

        for v in &report.violations {
            viol_table.add_row(vec![
                Cell::new(&v.field_name).fg(Color::Red),
                Cell::new(&v.rule).fg(Color::Yellow),
                Cell::new(&v.description).fg(Color::White),
            ]);
        }

        println!("{viol_table}\n");
    } else {
        println!("✨ All field evolution rules pass without breaking changes!\n");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn printed_registry_urls_carry_no_credentials() {
        for url in [
            "https://KEY:s3cr3t@psrc.example.com/",
            "KEY:s3cr3t@psrc.example.com:8081",
        ] {
            let shown = super::shown_url(url);
            assert!(
                !shown.contains("s3cr3t") && !shown.contains("KEY"),
                "{shown}"
            );
            assert!(shown.contains("psrc.example.com"), "{shown}");
        }
    }

    use super::*;

    #[test]
    fn transitive_checks_are_labelled_as_such() {
        use super::{mode_label, CompatibilityMode};
        assert_eq!(mode_label(CompatibilityMode::Backward, None), "BACKWARD");
        assert_eq!(
            mode_label(CompatibilityMode::Full, Some(3)),
            "FULL_TRANSITIVE"
        );
        assert_eq!(mode_label(CompatibilityMode::None, Some(3)), "NONE");
    }

    #[test]
    fn test_parse_compat_mode_rejects_unknown_and_reads_transitive() {
        assert_eq!(
            parse_compat_mode("BACKWARD").unwrap(),
            (CompatibilityMode::Backward, false)
        );
        assert_eq!(
            parse_compat_mode("forward_transitive").unwrap(),
            (CompatibilityMode::Forward, true)
        );
        assert_eq!(
            parse_compat_mode("FULL_TRANSITIVE").unwrap(),
            (CompatibilityMode::Full, true)
        );
        assert_eq!(
            parse_compat_mode("backward-transitive").unwrap(),
            (CompatibilityMode::Backward, true)
        );
        assert!(parse_compat_mode("fwd").is_err());
        assert!(parse_compat_mode("fulll").is_err());
        assert!(parse_compat_mode("").is_err());
    }

    #[test]
    fn test_transitive_check_fails_if_any_version_breaks() {
        let v1 = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}"#;
        let v2 = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}"#;
        // Candidate narrows back to int: fine against v1, breaks v2 under FULL.
        let candidate = v1;
        let reports = [(1, v1), (2, v2)]
            .into_iter()
            .map(|(v, s)| {
                let r = evaluate_compatibility(Some("s"), s, candidate, CompatibilityMode::Full)
                    .unwrap();
                (v, r)
            })
            .collect::<Vec<_>>();
        assert!(reports[0].1.is_compatible);
        let merged = merge_compat_reports("s", CompatibilityMode::Full, reports);
        assert!(!merged.is_compatible);
        assert_eq!(merged.violations.len(), 1);
        assert!(merged.violations[0]
            .description
            .starts_with("[vs version 2]"));
        assert_eq!(merged.modified_fields, vec!["a"]);
    }

    #[test]
    fn schema_fields_parses_subject_or_topic() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(subcommand)]
            command: SchemaSubcommands,
        }

        let parse = |args: &[&str]| TestCli::try_parse_from(args).map(|c| c.command);
        let SchemaSubcommands::Fields {
            subject,
            topic,
            key,
            plain,
            message,
            ..
        } = parse(&[
            "schema",
            "fields",
            "orders-value",
            "--plain",
            "--message",
            "Order",
        ])
        .unwrap()
        else {
            panic!("expected fields");
        };
        assert_eq!(subject.as_deref(), Some("orders-value"));
        assert_eq!((topic, key, plain), (None, false, true));
        assert_eq!(message.as_deref(), Some("Order"));
        let SchemaSubcommands::Fields {
            subject,
            topic,
            key,
            ..
        } = parse(&["schema", "fields", "--topic", "orders", "--key"]).unwrap()
        else {
            panic!("expected fields");
        };
        assert_eq!(
            fields_subject(subject, topic.as_deref(), key).unwrap(),
            "orders-key"
        );
        assert_eq!(
            fields_subject(None, Some("orders"), false).unwrap(),
            "orders-value"
        );
        // A subject or a topic, not both or neither; --key needs --topic.
        assert!(parse(&["schema", "fields"]).is_err());
        assert!(parse(&["schema", "fields", "s", "--topic", "t"]).is_err());
        assert!(fields_subject(Some("s".into()), None, true).is_err());
    }

    #[test]
    fn schema_fields_rows_show_flags_and_a_few_values() {
        let row = FieldRow {
            path: "$.card".into(),
            kind: "string".into(),
            optional: true,
            recursive: false,
            oneof: Some("payment".into()),
            values: (1..=7).map(|i| format!("V{i}")).collect(),
        };
        assert_eq!(field_flags(&row), "optional, oneof payment");
        assert_eq!(field_values(&row.values), "V1, V2, V3, V4, V5 … (7 in all)");
        assert_eq!(field_values(&row.values[..2]), "V1, V2");
    }

    #[test]
    fn test_clap_schema_args_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(subcommand)]
            command: SchemaSubcommands,
        }

        let cli = TestCli::parse_from(["schema", "list", "-S", "http://registry:8081"]);
        match cli.command {
            SchemaSubcommands::List { registry_url } => {
                assert_eq!(registry_url.as_deref(), Some("http://registry:8081"));
            }
            _ => panic!("Expected List subcommand"),
        }

        let cli = TestCli::parse_from(["schema", "get", "42"]);
        match cli.command {
            SchemaSubcommands::Get { id, registry_url } => {
                assert_eq!(id, 42);
                assert_eq!(registry_url, None);
            }
            _ => panic!("Expected Get subcommand"),
        }

        let cli = TestCli::parse_from(["schema", "decode", "00 00 00 00 05 68 69"]);
        match cli.command {
            SchemaSubcommands::Decode { hex_data } => {
                assert_eq!(hex_data, "00 00 00 00 05 68 69");
            }
            _ => panic!("Expected Decode subcommand"),
        }

        let cli = TestCli::parse_from([
            "schema",
            "check",
            "orders-value",
            "-f",
            "./schema.json",
            "-m",
            "forward",
            "--fail-on-incompatible",
        ]);
        match cli.command {
            SchemaSubcommands::Check {
                subject,
                schema_file,
                mode,
                fail_on_incompatible,
                ..
            } => {
                assert_eq!(subject, "orders-value");
                assert_eq!(schema_file, "./schema.json");
                assert_eq!(mode, "forward");
                assert!(fail_on_incompatible);
            }
            _ => panic!("Expected Check subcommand"),
        }

        let cli = TestCli::parse_from([
            "schema",
            "diff-schema",
            "./v1.json",
            "./v2.json",
            "-m",
            "full",
        ]);
        match cli.command {
            SchemaSubcommands::DiffSchema {
                old_file,
                new_file,
                mode,
                fail_on_incompatible,
            } => {
                assert_eq!(old_file, "./v1.json");
                assert_eq!(new_file, "./v2.json");
                assert_eq!(mode, "full");
                assert!(!fail_on_incompatible);
            }
            _ => panic!("Expected DiffSchema subcommand"),
        }
    }

    #[test]
    fn test_handle_decode_hex_success() {
        let global = GlobalOpts {
            bootstrap_servers: None,
            profile: None,
            format: OutputFormat::Table,
            timeout: 30,
            resolved: None,
            write: None,
        };

        let hex = "00 00 00 00 2a 7b 22 69 64 22 3a 31 7d";
        let res = handle_decode_hex(hex, &global);
        assert!(res.is_ok());
    }
}
