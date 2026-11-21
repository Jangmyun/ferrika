use std::time::Duration;

use clap::Args;
use colored::Colorize;
use tokio::io::{AsyncBufReadExt, BufReader};

/// Maximum number of unacknowledged messages while streaming from stdin.
const STDIN_MAX_IN_FLIGHT: usize = 10_000;

use crate::cli::{output, print_connection_hint, GlobalOpts, OutputFormat};
use crate::kafka::producer::{
    DeliveryOutcome, DeliveryPipeline, KafkaProducerClient, OwnedProducerRecord,
};

/// Command line arguments for message producing.
#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct ProduceArgs {
    /// Target topic name
    pub topic: String,

    /// Direct message payload to produce (if omitted, reads streaming lines from stdin)
    #[arg(short, long)]
    pub message: Option<String>,

    /// Optional message key
    #[arg(short, long)]
    pub key: Option<String>,

    /// Target partition (if omitted, Kafka uses default hash partitioning)
    #[arg(short, long)]
    pub partition: Option<i32>,

    /// Message headers in KEY:VALUE format (can be specified multiple times)
    #[arg(short = 'H', long = "header")]
    pub headers: Vec<String>,

    /// Separator between key and value when reading from stdin (e.g. ':' or '=')
    #[arg(short = 's', long = "key-separator")]
    pub key_separator: Option<String>,

    /// Serialize each JSON payload with the latest schema of <topic>-value
    /// (Avro, Protobuf or JSON Schema) in the Confluent wire format
    #[arg(long)]
    pub schema: bool,

    /// Subject to take the schema from (implies --schema)
    #[arg(long)]
    pub subject: Option<String>,

    /// Schema Registry URL (defaults to the profile's)
    #[arg(short = 'S', long = "schema-registry")]
    pub schema_registry: Option<String>,
}

type SchemaDef = crate::codec::schema::SchemaDefinition;

/// The value schema `--schema`/`--subject` asks for, and the matching
/// `<name>-key` schema if the registry has one; fetched once.
async fn value_schema(
    args: &ProduceArgs,
    registry_url: Option<String>,
    timeout: std::time::Duration,
) -> anyhow::Result<Option<(SchemaDef, Option<SchemaDef>)>> {
    if !args.schema && args.subject.is_none() {
        return Ok(None);
    }
    let subject = args
        .subject
        .clone()
        .unwrap_or_else(|| format!("{}-value", args.topic));
    let url = args
        .schema_registry
        .clone()
        .or(registry_url)
        .ok_or_else(|| {
            crate::error::KafkaError::config(
                "--schema needs a Schema Registry: pass -S <url> or set one on the profile",
            )
        })?;
    let client = crate::kafka::schema::SchemaRegistryClient::new(&url).with_timeout(timeout);
    let def = client.get_latest_subject_schema(&subject).await?;
    let key_subject = format!("{}-key", subject.strip_suffix("-value").unwrap_or(&subject));
    let key_def = match client.get_latest_subject_schema(&key_subject).await {
        Ok(key_def) => {
            eprintln!(
                "{}",
                format!(
                    "Serializing keys with {} schema {} (subject '{key_subject}')",
                    key_def.schema_type, key_def.id
                )
                .bright_black()
            );
            Some(key_def)
        }
        Err(e) if e.to_string().contains("returned 404") => None,
        Err(e) => return Err(e.into()),
    };
    eprintln!(
        "{}",
        format!(
            "Serializing with {} schema {} (subject '{subject}')",
            def.schema_type, def.id
        )
        .bright_black()
    );
    Ok(Some((def, key_def)))
}

/// The key bytes: as given, or serialized with `schema` (text that is not
/// JSON is taken as a JSON string).
fn key_bytes(text: &str, schema: Option<&SchemaDef>) -> crate::error::KafkaResult<Vec<u8>> {
    match schema {
        Some(def) => {
            let json = if serde_json::from_str::<serde_json::Value>(text).is_ok() {
                text.to_string()
            } else {
                serde_json::Value::String(text.to_string()).to_string()
            };
            crate::codec::schema::encode_with_schema(def, &json)
        }
        None => Ok(text.as_bytes().to_vec()),
    }
}

/// The payload bytes: as given, or serialized with `schema`.
fn payload_bytes(
    text: &str,
    schema: Option<&crate::codec::schema::SchemaDefinition>,
) -> crate::error::KafkaResult<Vec<u8>> {
    match schema {
        Some(def) => crate::codec::schema::encode_with_schema(def, text),
        None => Ok(text.as_bytes().to_vec()),
    }
}

/// Executes the producer CLI command.
pub async fn run(args: ProduceArgs, global: &GlobalOpts) -> anyhow::Result<()> {
    let kafka_config = global.to_kafka_config();
    let producer = match KafkaProducerClient::new(&kafka_config) {
        Ok(client) => client,
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err.into());
        }
    };

    let parsed_headers = parse_headers(&args.headers)?;
    let schemas = value_schema(
        &args,
        kafka_config.schema_registry_url.clone(),
        kafka_config.request_timeout,
    )
    .await?;
    let (schema, key_schema) = match schemas {
        Some((value, key)) => (Some(value), key),
        None => (None, None),
    };

    if let Some(msg_content) = args.message.as_deref() {
        // Single message mode
        let mut record = OwnedProducerRecord::new(&args.topic)
            .with_payload(payload_bytes(msg_content, schema.as_ref())?);

        if let Some(ref k) = args.key {
            record = record.with_key(key_bytes(k, key_schema.as_ref())?);
        }
        if let Some(p) = args.partition {
            record = record.with_partition(p);
        }
        for (k, v) in parsed_headers {
            record = record.with_header(k, v);
        }

        match producer.send(&record, None).await {
            Ok(receipt) => {
                match global.format {
                    OutputFormat::Table => {
                        println!(
                            "{} Message produced to topic '{}' [partition: {}, offset: {}]",
                            "✔".green().bold(),
                            receipt.topic.cyan().bold(),
                            receipt.partition.to_string().green(),
                            receipt.offset.to_string().yellow().bold()
                        );
                    }
                    OutputFormat::Json => {
                        let res = serde_json::json!({
                            "status": "success",
                            "topic": receipt.topic,
                            "partition": receipt.partition,
                            "offset": receipt.offset,
                        });
                        output::print_json(&res)?;
                    }
                }
                Ok(())
            }
            Err(err) => {
                print_connection_hint(&global.to_kafka_config(), &err);
                Err(err.into())
            }
        }
    } else {
        // Streaming stdin pipe mode
        println!(
            "{}",
            format!(
                "Listening for input from stdin (producing to '{}')... Press Ctrl+C or send EOF to finish.",
                args.topic
            )
            .bright_black()
            .italic()
        );

        let mut reader = BufReader::new(tokio::io::stdin());
        let mut line_buf: Vec<u8> = Vec::with_capacity(4096);
        let mut pipeline = DeliveryPipeline::new(STDIN_MAX_IN_FLIGHT);
        let mut delivered: usize = 0;
        let mut failed: usize = 0;
        let mut local_errors: usize = 0;
        let mut skipped_blank: usize = 0;
        let mut on_outcome = |outcome: DeliveryOutcome| match outcome.result {
            Ok(receipt) => {
                delivered += 1;
                tracing::debug!(
                    partition = receipt.partition,
                    offset = receipt.offset,
                    "Message delivered"
                );
            }
            Err(err) => {
                failed += 1;
                if failed <= 5 {
                    eprintln!("{} Failed to deliver message: {}", "✖".red(), err);
                }
            }
        };
        let ctrl_c = tokio::signal::ctrl_c();
        tokio::pin!(ctrl_c);

        loop {
            line_buf.clear();
            let read = tokio::select! {
                read = reader.read_until(b'\n', &mut line_buf) => read,
                _ = &mut ctrl_c => {
                    println!("\n{}", "Interrupted by user (Ctrl+C). Flushing pending messages...".yellow());
                    break;
                }
            };
            match read {
                Ok(0) => break, // EOF
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Error reading stdin: {e}");
                    local_errors += 1;
                    break;
                }
            }

            let line = strip_line_ending(&line_buf);
            // A blank line is almost always a stray newline, not an intended
            // empty message (and an empty value is rejected by compacted topics,
            // failing the whole batch).
            if line.iter().all(u8::is_ascii_whitespace) {
                skipped_blank += 1;
                continue;
            }
            let mut record = match std::str::from_utf8(line) {
                Ok(text) => {
                    let (extracted_key, payload) =
                        parse_input_line(text, args.key_separator.as_deref(), args.key.as_deref());
                    let bytes = match payload_bytes(payload, schema.as_ref()) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            local_errors += 1;
                            if local_errors <= 5 {
                                eprintln!("{} Skipped a line: {e}", "✖".red());
                            }
                            continue;
                        }
                    };
                    let mut record = OwnedProducerRecord::new(&args.topic).with_payload(bytes);
                    if let Some(k) = extracted_key {
                        match key_bytes(k, key_schema.as_ref()) {
                            Ok(k) => record = record.with_key(k),
                            Err(e) => {
                                local_errors += 1;
                                if local_errors <= 5 {
                                    eprintln!("{} Skipped a line: {e}", "✖".red());
                                }
                                continue;
                            }
                        }
                    }
                    record
                }
                // Non-UTF-8 input is sent as raw bytes; key separators only apply to text.
                Err(_) if schema.is_some() => {
                    local_errors += 1;
                    eprintln!("{} Skipped a line that is not UTF-8 JSON", "✖".red());
                    continue;
                }
                Err(_) => {
                    let mut record = OwnedProducerRecord::new(&args.topic).with_payload(line);
                    if let Some(ref k) = args.key {
                        record = record.with_key(k.as_bytes());
                    }
                    record
                }
            };
            if let Some(p) = args.partition {
                record = record.with_partition(p);
            }
            for (k, v) in &parsed_headers {
                record = record.with_header(k.clone(), v.clone());
            }

            match pipeline.submit(&producer, &record).await {
                Ok(Some(outcome)) => on_outcome(outcome),
                Ok(None) => {}
                Err(err) => {
                    eprintln!("{} Failed to enqueue message: {}", "✖".red(), err);
                    local_errors += 1;
                }
            }
        }

        while let Some(outcome) = pipeline.next_outcome().await {
            on_outcome(outcome);
        }
        let _ = producer.flush(Some(Duration::from_secs(5)));
        println!(
            "{} Finished. Produced {} message(s) to topic '{}'.",
            "✔".green().bold(),
            delivered.to_string().cyan().bold(),
            args.topic.yellow().bold()
        );
        if skipped_blank > 0 {
            eprintln!(
                "{}",
                format!("Skipped {skipped_blank} blank line(s).").bright_black()
            );
        }

        let failed = failed + local_errors;
        if failed > 0 {
            anyhow::bail!("{failed} message(s) failed to be produced");
        }
        Ok(())
    }
}

/// Removes a trailing `\n` or `\r\n` from a raw input line.
fn strip_line_ending(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    line.strip_suffix(b"\r").unwrap_or(line)
}

/// Parses headers in KEY:VALUE format, keeping their order and repeated keys.
pub fn parse_headers(raw_headers: &[String]) -> anyhow::Result<Vec<(String, Vec<u8>)>> {
    let mut list = Vec::with_capacity(raw_headers.len());
    for entry in raw_headers {
        let (key, value) = crate::kafka::filter::parse_header_pair(entry)?;
        list.push((key.to_string(), value.as_bytes().to_vec()));
    }
    Ok(list)
}

/// Parses a line of text, extracting key and payload according to separator rules.
pub fn parse_input_line<'a>(
    line: &'a str,
    key_separator: Option<&str>,
    override_key: Option<&'a str>,
) -> (Option<&'a str>, &'a str) {
    if let Some(key) = override_key {
        return (Some(key), line);
    }
    if let Some(sep) = key_separator {
        if let Some(idx) = line.find(sep) {
            let key = &line[..idx];
            let payload = &line[idx + sep.len()..];
            return (Some(key), payload);
        }
    }
    (None, line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_headers_valid() {
        let raw = vec![
            "content-type:application/json".to_string(),
            "trace_id:12345".to_string(),
        ];
        let headers = parse_headers(&raw).unwrap();
        assert_eq!(
            headers,
            [
                ("content-type".to_string(), b"application/json".to_vec()),
                ("trace_id".to_string(), b"12345".to_vec()),
            ]
        );
    }

    #[test]
    fn parse_headers_keeps_repeated_keys_in_order() {
        let raw = vec!["a:1".to_string(), "a:2".to_string(), "b:3".to_string()];
        assert_eq!(
            parse_headers(&raw).unwrap(),
            [
                ("a".to_string(), b"1".to_vec()),
                ("a".to_string(), b"2".to_vec()),
                ("b".to_string(), b"3".to_vec()),
            ]
        );
    }

    #[test]
    fn test_parse_headers_trims_key_and_value_like_header_filters() {
        let raw = vec![" env : prod ".to_string()];
        let headers = parse_headers(&raw).unwrap();
        assert_eq!(headers, [("env".to_string(), b"prod".to_vec())]);
        assert!(parse_headers(&[":v".to_string()]).is_err());
    }

    #[test]
    fn test_parse_headers_invalid() {
        let raw = vec!["invalid_no_colon".to_string()];
        assert!(parse_headers(&raw).is_err());
    }

    #[test]
    fn test_parse_input_line_with_separator() {
        let line = "user_42:{\"name\": \"Alice\"}";
        let (key, payload) = parse_input_line(line, Some(":"), None);
        assert_eq!(key, Some("user_42"));
        assert_eq!(payload, "{\"name\": \"Alice\"}");
    }

    #[test]
    fn test_parse_input_line_without_separator() {
        let line = "{\"name\": \"Bob\"}";
        let (key, payload) = parse_input_line(line, None, None);
        assert_eq!(key, None);
        assert_eq!(payload, "{\"name\": \"Bob\"}");
    }

    #[test]
    fn test_parse_input_line_with_override_key() {
        let line = "test payload";
        let (key, payload) = parse_input_line(line, Some(":"), Some("forced_key"));
        assert_eq!(key, Some("forced_key"));
        assert_eq!(payload, "test payload");
    }

    #[test]
    fn test_clap_produce_args_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(flatten)]
            args: ProduceArgs,
        }

        let cli = TestCli::try_parse_from([
            "test",
            "my-topic",
            "-m",
            "hello world",
            "-k",
            "key1",
            "-p",
            "2",
            "-H",
            "env:prod",
        ])
        .unwrap();

        assert_eq!(cli.args.topic, "my-topic");
        assert_eq!(cli.args.message.as_deref(), Some("hello world"));
        assert_eq!(cli.args.key.as_deref(), Some("key1"));
        assert_eq!(cli.args.partition, Some(2));
        assert_eq!(cli.args.headers, vec!["env:prod".to_string()]);
    }
}
