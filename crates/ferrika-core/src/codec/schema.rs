//! Confluent Schema Registry wire format and schema definitions: the
//! 5-byte header, encoding JSON with a schema, and the registry's schema
//! types. The REST client is `kafka::schema`.

use serde::{Deserialize, Serialize};

use crate::error::{KafkaError, KafkaResult};
use crate::model::schema::{SchemaRef, SchemaReference};

/// Represents a parsed message conforming to the Confluent Schema Registry Wire Format.
///
/// Specification:
/// - Byte 0: Magic byte (0x00)
/// - Bytes 1..5: 4-byte Big-Endian Schema ID (u32)
/// - Bytes 5..N: Serialized payload (e.g. Avro binary, Protobuf, or JSON Schema)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireFormatMessage {
    /// Schema ID registered in Schema Registry.
    pub schema_id: u32,
    /// 5-byte raw header containing magic byte and big-endian schema id.
    pub raw_header: [u8; 5],
    /// Remaining payload without the 5-byte wire format header.
    pub payload: Vec<u8>,
    /// True if payload is valid UTF-8 string.
    pub is_utf8: bool,
    /// True if payload is valid JSON object or array.
    pub is_json: bool,
}

impl WireFormatMessage {
    /// Returns payload as a UTF-8 string slice if valid.
    pub fn payload_as_str(&self) -> Option<&str> {
        if self.is_utf8 {
            std::str::from_utf8(&self.payload).ok()
        } else {
            None
        }
    }

    /// Returns a human-friendly string preview of the payload (either decoded text or hex representation).
    pub fn preview(&self, max_len: usize) -> String {
        if let Some(text) = self.payload_as_str() {
            let trimmed = text.trim();
            if trimmed.len() > max_len {
                // Cut at a char boundary at or before `max_len` bytes.
                let end = (0..=max_len)
                    .rev()
                    .find(|&i| trimmed.is_char_boundary(i))
                    .unwrap_or(0);
                format!("{}...", &trimmed[..end])
            } else {
                trimmed.to_string()
            }
        } else {
            let hex_preview = self
                .payload
                .iter()
                .take(max_len / 2)
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            if self.payload.len() > max_len / 2 {
                format!("[binary {}B: {}...]", self.payload.len(), hex_preview)
            } else {
                format!("[binary {}B: {}]", self.payload.len(), hex_preview)
            }
        }
    }
}

/// Parses raw Kafka message bytes according to Confluent Schema Registry wire format.
///
/// Returns `Some(WireFormatMessage)` if the byte slice starts with `0x00` and has at least 5 bytes.
/// Returns `None` if the payload does not follow the wire format standard.
pub fn parse_confluent_wire_format(bytes: &[u8]) -> Option<WireFormatMessage> {
    if bytes.len() < 5 || bytes[0] != 0x00 {
        return None;
    }

    let mut header = [0u8; 5];
    header.copy_from_slice(&bytes[0..5]);

    let schema_id = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    let payload = bytes[5..].to_vec();

    let is_utf8 = std::str::from_utf8(&payload).is_ok();
    let is_json = if is_utf8 {
        serde_json::from_slice::<serde_json::Value>(&payload).is_ok()
    } else {
        false
    };

    Some(WireFormatMessage {
        schema_id,
        raw_header: header,
        payload,
        is_utf8,
        is_json,
    })
}

/// Prepends the 5-byte Confluent Wire Format header (0x00 + 4-byte big-endian schema id) to payload.
pub fn encode_confluent_wire_format(schema_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(0x00);
    out.extend_from_slice(&schema_id.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Formats a byte slice as a standard canonical hexdump (16 bytes per line with ASCII column).
pub fn format_hexdump(data: &[u8]) -> String {
    if data.is_empty() {
        return "[empty payload]".to_string();
    }

    let mut out = crate::model::format::hexdump_lines(data, 16, Some(8));
    // Remove trailing newline
    if out.ends_with('\n') {
        out.pop();
    }

    out
}

/// Parses a hex string (e.g. "00 00 00 01 68 65 6c 6c 6f" or "0000000168656c6c6f") into byte buffer.
pub fn parse_hex_string(input: &str) -> KafkaResult<Vec<u8>> {
    let cleaned: String = input
        .chars()
        .filter(|c| !c.is_whitespace() && *c != ':' && *c != ',')
        .collect();

    let hex_str = if cleaned.starts_with("0x") || cleaned.starts_with("0X") {
        &cleaned[2..]
    } else {
        &cleaned
    };

    // Checked up front: the byte-pair slicing below needs ASCII only.
    if let Some(bad) = hex_str.chars().find(|c| !c.is_ascii_hexdigit()) {
        return Err(KafkaError::schema(format!(
            "Invalid hex character '{bad}' in '{hex_str}'"
        )));
    }

    if hex_str.len() % 2 != 0 {
        return Err(KafkaError::schema(format!(
            "Hex string has odd length ({} characters): '{}'",
            hex_str.len(),
            hex_str
        )));
    }

    let mut bytes = Vec::with_capacity(hex_str.len() / 2);
    for i in (0..hex_str.len()).step_by(2) {
        let byte_str = &hex_str[i..i + 2];
        let byte = u8::from_str_radix(byte_str, 16)
            .map_err(|e| KafkaError::schema(format!("Invalid hex byte '{}': {}", byte_str, e)))?;
        bytes.push(byte);
    }

    Ok(bytes)
}

/// Represents a schema definition retrieved from Confluent Schema Registry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaDefinition {
    /// Schema ID in Schema Registry.
    #[serde(default)]
    pub id: u32,
    /// The raw schema string (JSON schema, Avro JSON, or Proto definition).
    #[serde(default)]
    pub schema: String,
    /// Schema type: "AVRO", "PROTOBUF", or "JSON". Defaults to "AVRO".
    #[serde(rename = "schemaType", default = "default_schema_type")]
    pub schema_type: String,
    /// Schema version if fetched under a subject context.
    #[serde(default)]
    pub version: Option<u32>,
    /// Schema subject name if known.
    #[serde(default)]
    pub subject: Option<String>,
    /// Other subjects this schema imports (Avro named types, .proto files).
    #[serde(default)]
    pub references: Vec<SchemaReference>,
    /// The referenced schemas' text, fetched by the client (transitively).
    #[serde(skip)]
    pub resolved_references: Vec<String>,
    /// The reference name (`name`) of each entry in `resolved_references`;
    /// JSON Schema `$ref`s use it.
    #[serde(skip)]
    pub resolved_reference_names: Vec<String>,
}

/// Parses a registry schema response. Every field of [`SchemaDefinition`]
/// has a default, so `{}` or `[]` would otherwise pass as an empty schema:
/// a body without a string `schema` (and, if `require_id`, a numeric `id`)
/// is an error.
pub(crate) fn parse_definition(body: &str, require_id: bool) -> Result<SchemaDefinition, String> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("not valid JSON: {e}"))?;
    let obj = value
        .as_object()
        .ok_or("expected a JSON object with a \"schema\" field")?;
    if !obj.get("schema").is_some_and(serde_json::Value::is_string) {
        return Err("response has no \"schema\" field".to_string());
    }
    if require_id && !obj.get("id").is_some_and(serde_json::Value::is_u64) {
        return Err("response has no \"id\" field".to_string());
    }
    serde_json::from_value(value).map_err(|e| format!("unexpected format: {e}"))
}

fn default_schema_type() -> String {
    "AVRO".to_string()
}

/// A record payload decoded through its Confluent wire-format header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedPayload {
    /// The payload as text (Avro rendered as JSON).
    pub text: String,
    /// What was decoded, e.g. "AVRO schema 12", for display.
    pub label: String,
    /// The registry schema it was decoded with; `None` when no schema
    /// could be looked up (no Schema Registry, lookup failed).
    pub schema_ref: Option<SchemaRef>,
}

/// `def`'s Avro schema and the schemas it references.
pub(crate) fn parse_avro_with_references(
    def: &SchemaDefinition,
) -> KafkaResult<(crate::codec::avro::Schema, Vec<crate::codec::avro::Schema>)> {
    let schema = crate::codec::avro::Schema::parse(&def.schema)?;
    let refs = def
        .resolved_references
        .iter()
        .map(|r| crate::codec::avro::Schema::parse(r))
        .collect::<KafkaResult<Vec<_>>>()?;
    Ok((schema, refs))
}

/// Serializes JSON text with `def` (Avro, Protobuf's first message, or JSON
/// Schema) into the Confluent wire format.
pub fn encode_with_schema(def: &SchemaDefinition, json_text: &str) -> KafkaResult<Vec<u8>> {
    let value: serde_json::Value = serde_json::from_str(json_text)
        .map_err(|e| KafkaError::schema(format!("The payload is not valid JSON: {e}")))?;
    let body = match def.schema_type.to_ascii_uppercase().as_str() {
        "AVRO" => {
            let (schema, refs) = parse_avro_with_references(def)?;
            crate::codec::avro::encode_with(&schema, &refs, &value)?
        }
        "PROTOBUF" => crate::codec::proto_schema::ProtoSchema::parse_with_imports(
            &def.schema,
            &def.resolved_references,
        )?
        .encode_first(&value)?,
        "JSON" => {
            let schema: serde_json::Value = serde_json::from_str(&def.schema)
                .map_err(|e| KafkaError::schema(format!("The JSON schema is not JSON: {e}")))?;
            // Referenced subjects that are not JSON are left out; a $ref to
            // them is then skipped like any unresolvable external one.
            let external: Vec<(String, serde_json::Value)> = def
                .resolved_reference_names
                .iter()
                .zip(&def.resolved_references)
                .filter_map(|(name, text)| Some((name.clone(), serde_json::from_str(text).ok()?)))
                .collect();
            let problems =
                crate::codec::json_schema::validate_with_references(&schema, &external, &value);
            if !problems.is_empty() {
                let shown: Vec<&str> = problems.iter().take(5).map(String::as_str).collect();
                let more = problems.len().saturating_sub(shown.len());
                return Err(KafkaError::schema(format!(
                    "The payload does not match JSON schema {}: {}{}",
                    def.id,
                    shown.join("; "),
                    if more > 0 {
                        format!(" (+{more} more)")
                    } else {
                        String::new()
                    }
                )));
            }
            value.to_string().into_bytes()
        }
        other => {
            return Err(KafkaError::schema(format!(
                "Cannot serialize with a {other} schema"
            )))
        }
    };
    Ok(encode_confluent_wire_format(def.id, &body))
}

/// A decoded key for display: a JSON string loses its quotes (`"o1"` -> `o1`).
pub fn key_text(decoded: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(decoded) {
        Ok(serde_json::Value::String(s)) => s,
        _ => decoded.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_definition_rejects_bodies_without_a_schema() {
        for body in ["{}", r#"{"a":1}"#, "[]", "null", r#"{"schema":5}"#, "nope"] {
            assert!(parse_definition(body, false).is_err(), "{body}");
        }
        let def = parse_definition(r#"{"schema":"\"string\""}"#, false).unwrap();
        assert_eq!(def.schema, "\"string\"");
        // The by-subject lookup also needs the id.
        assert!(parse_definition(r#"{"schema":"x"}"#, true).is_err());
        assert_eq!(
            parse_definition(r#"{"id":7,"schema":"x"}"#, true)
                .unwrap()
                .id,
            7
        );
    }

    #[test]
    fn test_parse_confluent_wire_format_valid_json() {
        let json_data = br#"{"order_id":1001,"status":"PAID"}"#;
        let wire_bytes = encode_confluent_wire_format(42, json_data);

        let parsed = parse_confluent_wire_format(&wire_bytes);
        assert!(parsed.is_some());

        let msg = parsed.unwrap();
        assert_eq!(msg.schema_id, 42);
        assert_eq!(msg.raw_header, [0x00, 0x00, 0x00, 0x00, 0x2a]);
        assert_eq!(msg.payload, json_data);
        assert!(msg.is_utf8);
        assert!(msg.is_json);
        assert_eq!(
            msg.payload_as_str(),
            Some(r#"{"order_id":1001,"status":"PAID"}"#)
        );
    }

    #[test]
    fn test_parse_confluent_wire_format_invalid_header() {
        // Less than 5 bytes
        assert!(parse_confluent_wire_format(&[0x00, 0x01, 0x02]).is_none());

        // Byte 0 is not magic 0x00
        assert!(parse_confluent_wire_format(&[0x01, 0x00, 0x00, 0x00, 0x01, 0x20]).is_none());

        // Plain string without magic byte
        assert!(parse_confluent_wire_format(b"Hello Kafka World!").is_none());
    }

    #[test]
    fn test_parse_confluent_wire_format_binary_payload() {
        let binary_payload = vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0xff];
        let wire_bytes = encode_confluent_wire_format(1024, &binary_payload);

        let parsed =
            parse_confluent_wire_format(&wire_bytes).expect("Should parse binary wire format");
        assert_eq!(parsed.schema_id, 1024);
        assert_eq!(parsed.payload, binary_payload);
        assert!(!parsed.is_json);
        let preview = parsed.preview(30);
        assert!(preview.contains("binary 6B"));
    }

    #[test]
    fn test_format_hexdump() {
        let data = b"Hello, Ferrika Kafka!";
        let dump = format_hexdump(data);

        assert!(dump.contains("00000000"));
        assert!(dump.contains("48 65 6c 6c 6f 2c 20 46"));
        assert!(dump.contains("|Hello, Ferrika K|"));
        assert!(dump.contains("|afka!           |") || dump.contains("|afka!|"));

        let empty_dump = format_hexdump(&[]);
        assert_eq!(empty_dump, "[empty payload]");
    }

    #[test]
    fn test_parse_hex_string_valid() {
        let hex = "00 00 00 00 2a 68 65 6c 6c 6f";
        let bytes = parse_hex_string(hex).expect("Valid hex string");
        assert_eq!(
            bytes,
            vec![0x00, 0x00, 0x00, 0x00, 0x2a, 0x68, 0x65, 0x6c, 0x6c, 0x6f]
        );

        let compact_hex = "0x000000002a68656c6c6f";
        let compact_bytes = parse_hex_string(compact_hex).expect("Valid compact hex");
        assert_eq!(compact_bytes, bytes);
    }

    #[test]
    fn test_parse_hex_string_invalid() {
        // Odd length
        let res = parse_hex_string("abc");
        assert!(res.is_err());

        // Non-hex character
        let res = parse_hex_string("00zz");
        assert!(res.is_err());

        // Multi-byte characters: an error, not a char-boundary panic.
        for bad in ["한0", "0한", "0x한0", "é0"] {
            assert!(parse_hex_string(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn test_preview_cuts_text_at_char_boundaries() {
        let wire = encode_confluent_wire_format(1, "가나다라".as_bytes());
        let parsed = parse_confluent_wire_format(&wire).expect("Should parse text wire format");
        // Each Hangul syllable is 3 bytes; 4 and 5 fall inside one.
        assert_eq!(parsed.preview(4), "가...");
        assert_eq!(parsed.preview(5), "가...");
        assert_eq!(parsed.preview(6), "가나...");
        assert_eq!(parsed.preview(1), "...");
        assert_eq!(parsed.preview(12), "가나다라");
    }

    #[test]
    fn encode_with_schema_writes_the_wire_format() {
        let def = SchemaDefinition {
            id: 7,
            schema: r#"{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}"#.into(),
            schema_type: "AVRO".into(),
            version: None,
            subject: None,
            references: Vec::new(),
            resolved_references: Vec::new(),
            resolved_reference_names: Vec::new(),
        };
        assert_eq!(
            encode_with_schema(&def, r#"{"n": 42}"#).unwrap(),
            encode_confluent_wire_format(7, &[84])
        );
        assert!(encode_with_schema(&def, r#"{"n": "x"}"#).is_err());
        assert!(encode_with_schema(&def, "nope").is_err());
        let proto = SchemaDefinition {
            schema: "message M { int32 n = 1; }".into(),
            schema_type: "PROTOBUF".into(),
            ..def.clone()
        };
        let json_def = SchemaDefinition {
            schema: r#"{"type":"object","required":["n"],"properties":{"n":{"type":"integer"}}}"#
                .into(),
            schema_type: "JSON".into(),
            ..def.clone()
        };
        assert!(encode_with_schema(&json_def, r#"{"n": 1}"#).is_ok());
        let err = encode_with_schema(&json_def, r#"{"n": "x"}"#).unwrap_err();
        assert!(err.to_string().contains("$.n: expected integer"), "{err}");
        assert_eq!(
            encode_with_schema(&proto, r#"{"n": 42}"#).unwrap(),
            encode_confluent_wire_format(7, &[0x00, 0x08, 42])
        );
    }
}
