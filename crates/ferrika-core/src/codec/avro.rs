//! Minimal Avro binary codec: turns a record encoded with a writer schema
//! (Avro JSON schema text) into `serde_json::Value`, as Schema Registry
//! clients do for display, and encodes JSON with a schema for producing. Covers the primitive and complex types of the
//! Avro 1.11 spec; logical types are shown as their underlying values.

use std::collections::HashMap;

use serde_json::{Map, Number, Value};

use crate::error::{KafkaError, KafkaResult};

/// A parsed Avro schema.
#[derive(Debug, Clone, PartialEq)]
pub enum Schema {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Record {
        name: String,
        fields: Vec<(String, Schema)>,
        /// Each field's default, if it declares one (used when encoding).
        defaults: Vec<Option<Value>>,
    },
    Enum {
        name: String,
        symbols: Vec<String>,
    },
    Array(Box<Schema>),
    Map(Box<Schema>),
    Union(Vec<Schema>),
    Fixed {
        name: String,
        size: usize,
    },
    /// A reference to a named type defined earlier (resolved while decoding).
    Ref(String),
}

fn err(msg: impl Into<String>) -> KafkaError {
    KafkaError::schema(msg.into())
}

impl Schema {
    /// Parses Avro schema JSON text.
    pub fn parse(text: &str) -> KafkaResult<Self> {
        let json: Value =
            serde_json::from_str(text).map_err(|e| err(format!("Invalid Avro schema: {e}")))?;
        Self::from_json(&json, None)
    }

    fn from_json(json: &Value, namespace: Option<&str>) -> KafkaResult<Self> {
        match json {
            Value::String(name) => Ok(Self::named_or_primitive(name, namespace)),
            Value::Array(branches) => Ok(Self::Union(
                branches
                    .iter()
                    .map(|b| Self::from_json(b, namespace))
                    .collect::<KafkaResult<_>>()?,
            )),
            Value::Object(obj) => {
                let kind = obj
                    .get("type")
                    .ok_or_else(|| err("Avro schema object without \"type\""))?;
                let Value::String(kind) = kind else {
                    // {"type": {...}} or {"type": [...]} wraps another schema.
                    return Self::from_json(kind, namespace);
                };
                let ns = obj.get("namespace").and_then(Value::as_str).or(namespace);
                match kind.as_str() {
                    "record" | "error" => {
                        let name = obj.get("name").and_then(Value::as_str).unwrap_or("record");
                        let raw_fields = obj
                            .get("fields")
                            .and_then(Value::as_array)
                            .ok_or_else(|| err(format!("Record '{name}' has no fields")))?;
                        let defaults = raw_fields
                            .iter()
                            .map(|f| f.get("default").cloned())
                            .collect();
                        let fields = raw_fields
                            .iter()
                            .map(|f| {
                                let fname = f
                                    .get("name")
                                    .and_then(Value::as_str)
                                    .ok_or_else(|| err("Record field without a name"))?;
                                let ftype = f
                                    .get("type")
                                    .ok_or_else(|| err(format!("Field '{fname}' has no type")))?;
                                Ok((fname.to_string(), Self::from_json(ftype, ns)?))
                            })
                            .collect::<KafkaResult<_>>()?;
                        Ok(Self::Record {
                            name: qualify(name, ns),
                            fields,
                            defaults,
                        })
                    }
                    "enum" => Ok(Self::Enum {
                        name: qualify(
                            obj.get("name").and_then(Value::as_str).unwrap_or("enum"),
                            ns,
                        ),
                        symbols: obj
                            .get("symbols")
                            .and_then(Value::as_array)
                            .map(|s| {
                                s.iter()
                                    .filter_map(|v| v.as_str().map(str::to_string))
                                    .collect()
                            })
                            .unwrap_or_default(),
                    }),
                    "array" => Ok(Self::Array(Box::new(Self::from_json(
                        obj.get("items").ok_or_else(|| err("Array without items"))?,
                        ns,
                    )?))),
                    "map" => Ok(Self::Map(Box::new(Self::from_json(
                        obj.get("values").ok_or_else(|| err("Map without values"))?,
                        ns,
                    )?))),
                    "fixed" => Ok(Self::Fixed {
                        name: qualify(
                            obj.get("name").and_then(Value::as_str).unwrap_or("fixed"),
                            ns,
                        ),
                        size: obj.get("size").and_then(Value::as_u64).unwrap_or(0) as usize,
                    }),
                    // A primitive with attributes, e.g. {"type": "long", "logicalType": ...}.
                    other => Ok(Self::named_or_primitive(other, ns)),
                }
            }
            _ => Err(err(format!("Unsupported Avro schema: {json}"))),
        }
    }

    fn named_or_primitive(name: &str, namespace: Option<&str>) -> Self {
        match name {
            "null" => Self::Null,
            "boolean" => Self::Boolean,
            "int" => Self::Int,
            "long" => Self::Long,
            "float" => Self::Float,
            "double" => Self::Double,
            "bytes" => Self::Bytes,
            "string" => Self::String,
            other => Self::Ref(qualify(other, namespace)),
        }
    }

    fn insert_named<'a>(name: &str, schema: &'a Schema, out: &mut HashMap<String, &'a Schema>) {
        out.insert(name.to_string(), schema);
        // Short names resolve too (references without a namespace).
        if let Some(short) = name.rsplit('.').next() {
            out.entry(short.to_string()).or_insert(schema);
        }
    }

    /// The name a union branch is selected by in Avro's JSON encoding
    /// (`{"<name>": value}`).
    fn branch_name(&self) -> &str {
        match self {
            Self::Null => "null",
            Self::Boolean => "boolean",
            Self::Int => "int",
            Self::Long => "long",
            Self::Float => "float",
            Self::Double => "double",
            Self::Bytes => "bytes",
            Self::String => "string",
            Self::Array(_) => "array",
            Self::Map(_) => "map",
            Self::Union(_) => "union",
            Self::Record { name, .. } | Self::Enum { name, .. } | Self::Fixed { name, .. } => name,
            Self::Ref(name) => name,
        }
    }

    /// Named types defined anywhere in this schema, for resolving `Ref`s.
    pub(crate) fn collect_named<'a>(&'a self, out: &mut HashMap<String, &'a Schema>) {
        match self {
            Self::Record { name, fields, .. } => {
                Self::insert_named(name, self, out);
                for (_, f) in fields {
                    f.collect_named(out);
                }
            }
            Self::Enum { name, .. } | Self::Fixed { name, .. } => {
                Self::insert_named(name, self, out)
            }
            Self::Array(s) | Self::Map(s) => s.collect_named(out),
            Self::Union(branches) => branches.iter().for_each(|b| b.collect_named(out)),
            _ => {}
        }
    }
}

fn qualify(name: &str, namespace: Option<&str>) -> String {
    match namespace {
        Some(ns) if !name.contains('.') && !ns.is_empty() => format!("{ns}.{name}"),
        _ => name.to_string(),
    }
}

/// Decodes one Avro-encoded datum written with `schema`.
pub fn decode(schema: &Schema, bytes: &[u8]) -> KafkaResult<Value> {
    decode_with(schema, &[], bytes)
}

/// [`decode`] where named types may come from referenced schemas.
pub fn decode_with(schema: &Schema, references: &[Schema], bytes: &[u8]) -> KafkaResult<Value> {
    let mut named = HashMap::new();
    for r in references {
        r.collect_named(&mut named);
    }
    schema.collect_named(&mut named);
    let mut reader = Reader {
        buf: bytes,
        pos: 0,
        empty_items: 0,
        nodes: 0,
        node_budget: MIN_NODE_BUDGET.saturating_add(bytes.len().saturating_mul(2)),
    };
    reader.value(schema, &named, 0)
}

/// Guards against hostile, self-recursive data.
const MAX_DEPTH: usize = 64;

/// The most array items that take no bytes (`null`, an empty record, a
/// zero-size fixed) one datum may hold. Their count is not bounded by the
/// data's length, so a huge block count would otherwise grow without end.
const MAX_EMPTY_ITEMS: usize = 1_000_000;

/// Decoded values one datum may produce before it is refused, plus 2 per
/// input byte. Only a recursive schema whose fields use no bytes (two refs to
/// the next record, 30 levels deep = 2^30 nodes from an empty payload) gets
/// near it; real records stay far below.
const MIN_NODE_BUDGET: usize = 1_000_000;

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    /// Array/map items decoded so far that consumed no bytes.
    empty_items: usize,
    /// Values decoded so far (every `value` call), against `node_budget`.
    nodes: usize,
    node_budget: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> KafkaResult<&[u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.buf.len())
            .ok_or_else(|| err("Avro data ends early"))?;
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    /// Zig-zag varint (int and long).
    fn long(&mut self) -> KafkaResult<i64> {
        let mut value: u64 = 0;
        for shift in (0..64).step_by(7) {
            let byte = self.take(1)?[0];
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok((value >> 1) as i64 ^ -((value & 1) as i64));
            }
        }
        Err(err("Avro varint is too long"))
    }

    fn len(&mut self) -> KafkaResult<usize> {
        usize::try_from(self.long()?).map_err(|_| err("Negative Avro length"))
    }

    fn value(
        &mut self,
        schema: &Schema,
        named: &HashMap<String, &Schema>,
        depth: usize,
    ) -> KafkaResult<Value> {
        if depth > MAX_DEPTH {
            return Err(err("Avro data nests too deeply"));
        }
        self.nodes += 1;
        if self.nodes > self.node_budget {
            return Err(err(format!(
                "Avro data expands to over {} values; refusing (recursive schema?)",
                self.node_budget
            )));
        }
        Ok(match schema {
            Schema::Null => Value::Null,
            Schema::Boolean => Value::Bool(self.take(1)?[0] != 0),
            Schema::Int | Schema::Long => Value::Number(self.long()?.into()),
            Schema::Float => {
                let b = self.take(4)?;
                let v = f32::from_le_bytes([b[0], b[1], b[2], b[3]]);
                Number::from_f64(f64::from(v)).map_or(Value::Null, Value::Number)
            }
            Schema::Double => {
                let b = self.take(8)?;
                let mut a = [0u8; 8];
                a.copy_from_slice(b);
                Number::from_f64(f64::from_le_bytes(a)).map_or(Value::Null, Value::Number)
            }
            Schema::Bytes => {
                let n = self.len()?;
                Value::String(bytes_text(self.take(n)?))
            }
            Schema::String => {
                let n = self.len()?;
                Value::String(String::from_utf8_lossy(self.take(n)?).into_owned())
            }
            Schema::Fixed { size, .. } => Value::String(bytes_text(self.take(*size)?)),
            Schema::Enum { symbols, .. } => {
                let ix = self.len()?;
                Value::String(
                    symbols
                        .get(ix)
                        .cloned()
                        .ok_or_else(|| err(format!("Avro enum index {ix} out of range")))?,
                )
            }
            Schema::Record { fields, .. } => {
                let mut obj = Map::new();
                for (name, field) in fields {
                    obj.insert(name.clone(), self.value(field, named, depth + 1)?);
                }
                Value::Object(obj)
            }
            Schema::Array(items) => {
                let mut out = Vec::new();
                self.blocks(|r| {
                    out.push(r.value(items, named, depth + 1)?);
                    Ok(())
                })?;
                Value::Array(out)
            }
            Schema::Map(values) => {
                let mut obj = Map::new();
                self.blocks(|r| {
                    let n = r.len()?;
                    let key = String::from_utf8_lossy(r.take(n)?).into_owned();
                    obj.insert(key, r.value(values, named, depth + 1)?);
                    Ok(())
                })?;
                Value::Object(obj)
            }
            Schema::Union(branches) => {
                let ix = self.len()?;
                let branch = branches
                    .get(ix)
                    .ok_or_else(|| err(format!("Avro union index {ix} out of range")))?;
                self.value(branch, named, depth + 1)?
            }
            Schema::Ref(name) => {
                let target = named
                    .get(name)
                    .or_else(|| name.rsplit('.').next().and_then(|short| named.get(short)))
                    .ok_or_else(|| err(format!("Unknown Avro type '{name}'")))?;
                self.value(target, named, depth + 1)?
            }
        })
    }

    /// Array and map items come in blocks: a count (negative: followed by a
    /// byte size), the items, ..., then a zero count.
    fn blocks(&mut self, mut item: impl FnMut(&mut Self) -> KafkaResult<()>) -> KafkaResult<()> {
        loop {
            let mut count = self.long()?;
            if count == 0 {
                return Ok(());
            }
            if count < 0 {
                count = count
                    .checked_neg()
                    .ok_or_else(|| err("Invalid Avro block count"))?;
                let _byte_size = self.long()?;
            }
            for _ in 0..count {
                let start = self.pos;
                item(self)?;
                // Items that take bytes are bounded by the data; empty ones
                // are not, so cap them.
                if self.pos == start {
                    self.empty_items += 1;
                    if self.empty_items > MAX_EMPTY_ITEMS {
                        return Err(err(format!(
                            "Avro data has over {MAX_EMPTY_ITEMS} empty array items"
                        )));
                    }
                }
            }
        }
    }
}

/// Encodes `value` (JSON) as one Avro datum written with `schema`. Union
/// values may be plain (the first branch that fits is used) or in Avro's JSON
/// form, `{"<branch type>": value}`. Missing record fields take their default,
/// or null when their type allows it.
pub fn encode(schema: &Schema, value: &Value) -> KafkaResult<Vec<u8>> {
    encode_with(schema, &[], value)
}

/// [`encode`] where named types may come from referenced schemas.
pub fn encode_with(schema: &Schema, references: &[Schema], value: &Value) -> KafkaResult<Vec<u8>> {
    let mut named = HashMap::new();
    for r in references {
        r.collect_named(&mut named);
    }
    schema.collect_named(&mut named);
    let mut out = Vec::new();
    write_value(&mut out, schema, value, &named, "", 0)?;
    Ok(out)
}

fn write_long(out: &mut Vec<u8>, v: i64) {
    let mut n = ((v << 1) ^ (v >> 63)) as u64;
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    write_long(out, bytes.len() as i64);
    out.extend_from_slice(bytes);
}

/// Avro's JSON form of bytes: one char per byte (U+0000..U+00FF); other
/// text is taken as UTF-8.
fn text_bytes(text: &str) -> Vec<u8> {
    if text.chars().all(|c| (c as u32) <= 0xff) {
        text.chars().map(|c| c as u8).collect()
    } else {
        text.as_bytes().to_vec()
    }
}

fn write_value(
    out: &mut Vec<u8>,
    schema: &Schema,
    value: &Value,
    named: &HashMap<String, &Schema>,
    path: &str,
    depth: usize,
) -> KafkaResult<()> {
    if depth > MAX_DEPTH {
        return Err(err("Avro value nests too deeply"));
    }
    let at = |what: &str| {
        let place = if path.is_empty() { "the value" } else { path };
        err(format!("Expected {what} at {place}, found {value}"))
    };
    match schema {
        Schema::Null => value.is_null().then_some(()).ok_or_else(|| at("null"))?,
        Schema::Boolean => out.push(u8::from(value.as_bool().ok_or_else(|| at("a boolean"))?)),
        Schema::Int => {
            let v = value
                .as_i64()
                .filter(|v| i32::try_from(*v).is_ok())
                .ok_or_else(|| at("an int"))?;
            write_long(out, v);
        }
        Schema::Long => write_long(out, value.as_i64().ok_or_else(|| at("a long"))?),
        Schema::Float => {
            let v = value.as_f64().ok_or_else(|| at("a float"))? as f32;
            out.extend_from_slice(&v.to_le_bytes());
        }
        Schema::Double => {
            out.extend_from_slice(&value.as_f64().ok_or_else(|| at("a double"))?.to_le_bytes())
        }
        Schema::String => write_bytes(
            out,
            value.as_str().ok_or_else(|| at("a string"))?.as_bytes(),
        ),
        Schema::Bytes => write_bytes(out, &text_bytes(value.as_str().ok_or_else(|| at("bytes"))?)),
        Schema::Fixed { size, .. } => {
            let bytes = text_bytes(value.as_str().ok_or_else(|| at("fixed bytes"))?);
            if bytes.len() != *size {
                return Err(at(&format!("{size} fixed bytes")));
            }
            out.extend_from_slice(&bytes);
        }
        Schema::Enum { symbols, .. } => {
            let symbol = value.as_str().ok_or_else(|| at("an enum symbol"))?;
            let ix = symbols
                .iter()
                .position(|s| s == symbol)
                .ok_or_else(|| at(&format!("one of {}", symbols.join("/"))))?;
            write_long(out, ix as i64);
        }
        Schema::Record {
            fields, defaults, ..
        } => {
            let obj = value.as_object().ok_or_else(|| at("an object"))?;
            // Unknown keys are an error, not dropped: this also keeps a union
            // from matching the wrong record branch.
            if let Some(unknown) = obj.keys().find(|k| !fields.iter().any(|(n, _)| n == *k)) {
                let place = if path.is_empty() { "the record" } else { path };
                return Err(err(format!("Unknown field '{unknown}' in {place}")));
            }
            for ((name, field), default) in fields.iter().zip(defaults) {
                let field_path = if path.is_empty() {
                    name.clone()
                } else {
                    format!("{path}.{name}")
                };
                let field_value = match (obj.get(name), default) {
                    (Some(v), _) => v,
                    (None, Some(d)) => d,
                    (None, None) => &Value::Null,
                };
                write_value(out, field, field_value, named, &field_path, depth + 1).map_err(
                    |e| {
                        if obj.contains_key(name) || default.is_some() {
                            e
                        } else {
                            err(format!("Missing field '{field_path}'"))
                        }
                    },
                )?;
            }
        }
        Schema::Array(items) => {
            let values = value.as_array().ok_or_else(|| at("an array"))?;
            if !values.is_empty() {
                write_long(out, values.len() as i64);
                for (i, item) in values.iter().enumerate() {
                    write_value(out, items, item, named, &format!("{path}[{i}]"), depth + 1)?;
                }
            }
            write_long(out, 0);
        }
        Schema::Map(values) => {
            let obj = value.as_object().ok_or_else(|| at("an object"))?;
            if !obj.is_empty() {
                write_long(out, obj.len() as i64);
                for (k, v) in obj {
                    write_bytes(out, k.as_bytes());
                    write_value(out, values, v, named, &format!("{path}.{k}"), depth + 1)?;
                }
            }
            write_long(out, 0);
        }
        Schema::Union(branches) => {
            // Avro JSON form: {"type name": value}.
            if let Some((key, inner)) = value
                .as_object()
                .filter(|o| o.len() == 1)
                .and_then(|o| o.iter().next())
            {
                let wanted = |b: &Schema| {
                    let name = b.branch_name();
                    name == key || name.rsplit('.').next() == Some(key.as_str())
                };
                if let Some(ix) = branches.iter().position(wanted) {
                    write_long(out, ix as i64);
                    return write_value(out, &branches[ix], inner, named, path, depth + 1);
                }
            }
            for (ix, branch) in branches.iter().enumerate() {
                let mut attempt = Vec::new();
                if write_value(&mut attempt, branch, value, named, path, depth + 1).is_ok() {
                    write_long(out, ix as i64);
                    out.extend_from_slice(&attempt);
                    return Ok(());
                }
            }
            let names: Vec<&str> = branches.iter().map(Schema::branch_name).collect();
            return Err(at(&format!("one of [{}]", names.join(", "))));
        }
        Schema::Ref(name) => {
            let target = named
                .get(name)
                .or_else(|| name.rsplit('.').next().and_then(|short| named.get(short)))
                .ok_or_else(|| err(format!("Unknown Avro type '{name}'")))?;
            write_value(out, target, value, named, path, depth + 1)?;
        }
    }
    Ok(())
}

/// Bytes as text when printable, else as \u-escaped Latin-1 like Avro's JSON encoding.
fn bytes_text(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => bytes.iter().map(|&b| b as char).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_json_that_decodes_back() {
        let schema = Schema::parse(
            r#"{"type": "record", "name": "Order", "namespace": "shop", "fields": [
                {"name": "id", "type": "long"},
                {"name": "note", "type": ["null", "string"], "default": null},
                {"name": "kind", "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B"]}},
                {"name": "again", "type": "Kind", "default": "A"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "attrs", "type": {"type": "map", "values": "int"}},
                {"name": "price", "type": "double"},
                {"name": "n", "type": ["null", "int", "string"]}
            ]}"#,
        )
        .unwrap();
        let value = serde_json::json!({
            "id": 7, "kind": "B", "tags": ["x", "y"], "attrs": {"k": 1},
            "price": 2.5, "n": {"string": "s"}
        });
        let bytes = encode(&schema, &value).unwrap();
        assert_eq!(
            decode(&schema, &bytes).unwrap(),
            serde_json::json!({
                "id": 7, "note": null, "kind": "B", "again": "A", "tags": ["x", "y"],
                "attrs": {"k": 1}, "price": 2.5, "n": "s"
            })
        );
        // A plain union value picks the first branch that fits.
        let plain = serde_json::json!({"id": 1, "kind": "A", "tags": [], "attrs": {}, "price": 0.0, "n": 5, "note": "hi"});
        let back = decode(&schema, &encode(&schema, &plain).unwrap()).unwrap();
        assert_eq!(back["n"], serde_json::json!(5));
        assert_eq!(back["note"], serde_json::json!("hi"));
        let missing = encode(&schema, &serde_json::json!({"kind": "A"})).unwrap_err();
        assert!(
            missing.to_string().contains("Missing field 'id'"),
            "{missing}"
        );
        let extra = encode(
            &schema,
            &serde_json::json!({"id": 1, "kind": "A", "nope": 1}),
        )
        .unwrap_err();
        assert!(
            extra.to_string().contains("Unknown field 'nope'"),
            "{extra}"
        );
        // A union of records picks the branch whose fields match.
        let union = Schema::parse(
            r#"[{"type": "record", "name": "A", "fields": [{"name": "x", "type": ["null", "int"], "default": null}]},
                {"type": "record", "name": "B", "fields": [{"name": "y", "type": "string"}]}]"#,
        )
        .unwrap();
        let bytes = encode(&union, &serde_json::json!({"y": "hi"})).unwrap();
        assert_eq!(
            decode(&union, &bytes).unwrap(),
            serde_json::json!({"y": "hi"})
        );
        let bad_enum = encode(&schema, &serde_json::json!({"id": 1, "kind": "Z"})).unwrap_err();
        assert!(bad_enum.to_string().contains("one of A/B"), "{bad_enum}");
    }

    fn zigzag(v: i64) -> Vec<u8> {
        let mut n = ((v << 1) ^ (v >> 63)) as u64;
        let mut out = Vec::new();
        loop {
            let byte = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                out.push(byte);
                return out;
            }
            out.push(byte | 0x80);
        }
    }

    fn string(s: &str) -> Vec<u8> {
        let mut out = zigzag(s.len() as i64);
        out.extend_from_slice(s.as_bytes());
        out
    }

    #[test]
    fn decodes_a_record_with_unions_enums_arrays_maps_and_refs() {
        let schema = Schema::parse(
            r#"{
              "type": "record", "name": "Order", "namespace": "shop",
              "fields": [
                {"name": "id", "type": "long"},
                {"name": "note", "type": ["null", "string"]},
                {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "PAID"]}},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "attrs", "type": {"type": "map", "values": "int"}},
                {"name": "price", "type": "double"},
                {"name": "paid", "type": "boolean"},
                {"name": "item", "type": {"type": "record", "name": "Item", "fields": [{"name": "sku", "type": "string"}]}},
                {"name": "first", "type": ["null", "Item"]}
              ]
            }"#,
        )
        .unwrap();
        let mut data = zigzag(-42);
        data.extend(zigzag(1)); // union branch 1: string
        data.extend(string("rush"));
        data.extend(zigzag(1)); // PAID
        data.extend(zigzag(2));
        data.extend(string("a"));
        data.extend(string("b"));
        data.extend(zigzag(0));
        data.extend(zigzag(1));
        data.extend(string("qty"));
        data.extend(zigzag(3));
        data.extend(zigzag(0));
        data.extend(2.5f64.to_le_bytes());
        data.push(1);
        data.extend(string("sku-1"));
        data.extend(zigzag(1)); // union branch 1: Item (by reference)
        data.extend(string("sku-2"));

        let value = decode(&schema, &data).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "id": -42, "note": "rush", "status": "PAID", "tags": ["a", "b"],
                "attrs": {"qty": 3}, "price": 2.5, "paid": true,
                "item": {"sku": "sku-1"}, "first": {"sku": "sku-2"}
            })
        );
    }

    #[test]
    fn truncated_data_is_an_error_not_a_panic() {
        let schema = Schema::parse(
            r#"{"type":"record","name":"R","fields":[{"name":"s","type":"string"}]}"#,
        )
        .unwrap();
        assert!(decode(&schema, &zigzag(10)).is_err());
        assert!(decode(&schema, &[]).is_err());
    }

    #[test]
    fn huge_counts_of_empty_items_are_refused() {
        for items in [
            r#""null""#,
            r#"{"type":"record","name":"E","fields":[]}"#,
            r#"{"type":"fixed","name":"F","size":0}"#,
        ] {
            let schema = Schema::parse(&format!(r#"{{"type":"array","items":{items}}}"#)).unwrap();
            // A ~2^62 block count of items that take no bytes.
            for count in [i64::MAX / 2, -(i64::MAX / 2)] {
                let mut data = zigzag(count);
                if count < 0 {
                    data.extend(zigzag(0)); // block byte size
                }
                let e = decode(&schema, &data).unwrap_err().to_string();
                assert!(e.contains("empty array items"), "{items}: {e}");
            }
            // Small counts still decode.
            let mut data = zigzag(3);
            data.extend(zigzag(0));
            let value = decode(&schema, &data).unwrap();
            assert_eq!(value.as_array().map(Vec::len), Some(3));
        }
        // i64::MIN as a block count: no negation overflow.
        let schema = Schema::parse(r#"{"type":"array","items":"long"}"#).unwrap();
        assert!(decode(&schema, &zigzag(i64::MIN)).is_err());
    }

    #[test]
    fn exponential_record_fanout_is_refused_quickly() {
        // R0={a:R1,b:R1}, R1={a:R2,b:R2}, ...: 2^30 nodes from zero bytes.
        let mut def = String::from(r#"{"type":"record","name":"R30","fields":[]}"#);
        for i in (0..30).rev() {
            def = format!(
                r#"{{"type":"record","name":"R{i}","fields":[{{"name":"a","type":{def}}},{{"name":"b","type":"R{}"}}]}}"#,
                i + 1
            );
        }
        let schema = Schema::parse(&def).unwrap();
        let e = decode(&schema, &[]).unwrap_err().to_string();
        assert!(e.contains("expands to over"), "{e}");
    }

    #[test]
    fn large_ordinary_arrays_stay_under_the_node_budget() {
        let schema = Schema::parse(r#"{"type":"array","items":"int"}"#).unwrap();
        let mut data = zigzag(500_000);
        data.extend(std::iter::repeat_n(2u8, 500_000));
        data.extend(zigzag(0));
        let value = decode(&schema, &data).unwrap();
        assert_eq!(value.as_array().map(Vec::len), Some(500_000));
    }
}
