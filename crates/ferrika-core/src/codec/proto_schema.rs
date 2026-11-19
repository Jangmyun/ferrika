//! A small `.proto` (proto2/proto3) parser and a schema-aware Protobuf
//! decoder, so records written with a Schema Registry Protobuf schema show
//! field names and typed values instead of raw field numbers.
//!
//! Only what decoding needs is parsed: messages (nested), enums, fields,
//! `oneof`, and `map<K, V>`. Options, services, extensions and imports are
//! skipped; types from imports are unknown and decode like `decode_raw`.

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::error::{KafkaError, KafkaResult};

fn err(msg: impl std::fmt::Display) -> KafkaError {
    KafkaError::Serialization(format!("Protobuf schema: {msg}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scalar {
    Double,
    Float,
    Int32,
    Int64,
    Uint32,
    Uint64,
    Sint32,
    Sint64,
    Fixed32,
    Fixed64,
    Sfixed32,
    Sfixed64,
    Bool,
    String,
    Bytes,
}

impl Scalar {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "double" => Self::Double,
            "float" => Self::Float,
            "int32" => Self::Int32,
            "int64" => Self::Int64,
            "uint32" => Self::Uint32,
            "uint64" => Self::Uint64,
            "sint32" => Self::Sint32,
            "sint64" => Self::Sint64,
            "fixed32" => Self::Fixed32,
            "fixed64" => Self::Fixed64,
            "sfixed32" => Self::Sfixed32,
            "sfixed64" => Self::Sfixed64,
            "bool" => Self::Bool,
            "string" => Self::String,
            "bytes" => Self::Bytes,
            _ => return None,
        })
    }

    /// Whether repeated values of this type may be packed.
    fn packable(self) -> bool {
        !matches!(self, Self::String | Self::Bytes)
    }
}

#[derive(Debug, Clone)]
enum FieldType {
    Scalar(Scalar),
    /// Index into `ProtoSchema::messages`.
    Message(usize),
    /// Full name of the enum.
    Enum(String),
    Map(Scalar, Box<FieldType>),
    /// A google.protobuf well-known type, shown as its JSON form.
    WellKnown(WellKnown),
    /// A type the schema does not define (e.g. from an import).
    Unknown,
}

/// google.protobuf types with a special JSON mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WellKnown {
    /// RFC 3339 text, e.g. `2026-10-12T05:00:00.123Z`.
    Timestamp,
    /// Seconds with an `s` suffix, e.g. `1.5s`.
    Duration,
    /// `google.protobuf.Int64Value` etc.: the wrapped value itself.
    Wrapper(Scalar),
    Empty,
}

impl WellKnown {
    fn parse(name: &str) -> Option<Self> {
        Some(match name.trim_start_matches('.') {
            "google.protobuf.Timestamp" => Self::Timestamp,
            "google.protobuf.Duration" => Self::Duration,
            "google.protobuf.Empty" => Self::Empty,
            "google.protobuf.DoubleValue" => Self::Wrapper(Scalar::Double),
            "google.protobuf.FloatValue" => Self::Wrapper(Scalar::Float),
            "google.protobuf.Int64Value" => Self::Wrapper(Scalar::Int64),
            "google.protobuf.UInt64Value" => Self::Wrapper(Scalar::Uint64),
            "google.protobuf.Int32Value" => Self::Wrapper(Scalar::Int32),
            "google.protobuf.UInt32Value" => Self::Wrapper(Scalar::Uint32),
            "google.protobuf.BoolValue" => Self::Wrapper(Scalar::Bool),
            "google.protobuf.StringValue" => Self::Wrapper(Scalar::String),
            "google.protobuf.BytesValue" => Self::Wrapper(Scalar::Bytes),
            _ => return None,
        })
    }
}

/// `(seconds, nanos)` from a Timestamp or Duration message body.
fn seconds_nanos(bytes: &[u8]) -> KafkaResult<(i64, i32)> {
    let (mut seconds, mut nanos) = (0i64, 0i32);
    let mut pos = 0;
    while pos < bytes.len() {
        let tag = read_varint(bytes, &mut pos)?;
        let raw = read_field(bytes, &mut pos, (tag & 7) as u8)?;
        match (tag >> 3, raw) {
            (1, Raw::Varint(v)) => seconds = v as i64,
            (2, Raw::Varint(v)) => nanos = v as i32,
            _ => {}
        }
    }
    Ok((seconds, nanos))
}

fn timestamp_text(seconds: i64, nanos: i32) -> Option<String> {
    if !(0..1_000_000_000).contains(&nanos) {
        return None;
    }
    let t = jiff::Timestamp::new(seconds, nanos).ok()?;
    Some(crate::model::time::rfc3339_z(t))
}

fn duration_text(seconds: i64, nanos: i32) -> String {
    if nanos == 0 {
        return format!("{seconds}s");
    }
    let sign = if seconds < 0 || nanos < 0 { "-" } else { "" };
    let frac = format!("{:09}", nanos.unsigned_abs());
    format!(
        "{sign}{}.{}s",
        seconds.unsigned_abs(),
        frac.trim_end_matches('0')
    )
}

fn parse_duration(text: &str) -> Option<(i64, i32)> {
    let body = text.strip_suffix('s')?;
    let (negative, body) = match body.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, body),
    };
    let (whole, frac) = body.split_once('.').unwrap_or((body, ""));
    if !whole.chars().all(|c| c.is_ascii_digit()) || whole.is_empty() {
        return None;
    }
    let seconds: i64 = whole.parse().ok()?;
    if frac.len() > 9 || !frac.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let nanos: i32 = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<9}").parse().ok()?
    };
    Some(if negative {
        (-seconds, -nanos)
    } else {
        (seconds, nanos)
    })
}

#[derive(Debug, Clone)]
struct FieldDef {
    name: String,
    number: u64,
    repeated: bool,
    /// The type as written, resolved into `ty` after parsing.
    type_name: String,
    map_types: Option<(String, String)>,
    ty: FieldType,
    /// The `oneof` the field belongs to.
    oneof: Option<String>,
}

#[derive(Debug, Clone)]
struct MessageDef {
    full_name: String,
    /// The name with its file's package (`shop.common.Money`), used to
    /// resolve type references across packages.
    qualified: String,
    fields: Vec<FieldDef>,
    /// Indexes of nested messages, in declaration order.
    nested: Vec<usize>,
}

/// A parsed `.proto` file.
#[derive(Debug, Clone, Default)]
pub struct ProtoSchema {
    package: String,
    messages: Vec<MessageDef>,
    /// Top-level messages, in declaration order.
    top: Vec<usize>,
    /// Enum full name -> number -> value name.
    enums: HashMap<String, HashMap<i64, String>>,
    /// Package-qualified enum name -> its key in `enums`.
    enum_qualified: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Number(String),
    Str(String),
    Sym(char),
}

fn tokenize(text: &str) -> KafkaResult<Vec<Token>> {
    let chars: Vec<char> = text.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '/' && chars.get(i + 1) == Some(&'/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                i += 1;
            }
            i += 2;
        } else if c == '"' || c == '\'' {
            let mut s = String::new();
            i += 1;
            while i < chars.len() && chars[i] != c {
                if chars[i] == '\\' {
                    i += 1;
                }
                if let Some(ch) = chars.get(i) {
                    s.push(*ch);
                }
                i += 1;
            }
            i += 1;
            tokens.push(Token::Str(s));
        } else if c.is_ascii_alphabetic() || c == '_' || c == '.' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_ascii_alphanumeric() || matches!(chars[i], '_' | '.'))
            {
                i += 1;
            }
            tokens.push(Token::Ident(chars[start..i].iter().collect()));
        } else if c.is_ascii_digit() || c == '-' {
            let start = i;
            i += 1;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '.') {
                i += 1;
            }
            tokens.push(Token::Number(chars[start..i].iter().collect()));
        } else {
            tokens.push(Token::Sym(c));
            i += 1;
        }
    }
    Ok(tokens)
}

fn parse_int(text: &str) -> Option<i64> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let value = if let Some(hex) = digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        i64::from_str_radix(hex, 16).ok()?
    } else if digits.len() > 1 && digits.starts_with('0') {
        i64::from_str_radix(&digits[1..], 8).ok()?
    } else {
        digits.parse().ok()?
    };
    Some(if negative { -value } else { value })
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    schema: ProtoSchema,
    /// Current nesting of message/oneof bodies, bounded by `MAX_PARSE_DEPTH`.
    depth: usize,
}

/// Deepest `message`/`oneof` nesting the parser accepts. protoc itself allows
/// 32 levels of nesting; real schemas are a handful deep. The limit keeps a
/// hostile registry from overflowing the stack with `message M{` repeated.
const MAX_PARSE_DEPTH: usize = 32;

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        self.pos += 1;
        token
    }

    fn is_sym(&self, c: char) -> bool {
        self.peek() == Some(&Token::Sym(c))
    }

    fn expect_sym(&mut self, c: char) -> KafkaResult<()> {
        match self.next() {
            Some(Token::Sym(s)) if s == c => Ok(()),
            other => Err(err(format!("expected '{c}', found {other:?}"))),
        }
    }

    fn ident(&mut self) -> KafkaResult<String> {
        match self.next() {
            Some(Token::Ident(s)) => Ok(s),
            other => Err(err(format!("expected a name, found {other:?}"))),
        }
    }

    /// Skips one statement: up to `;`, or a whole `{ ... }` block.
    fn skip_statement(&mut self) {
        let mut depth = 0usize;
        while let Some(token) = self.next() {
            match token {
                Token::Sym('{') => depth += 1,
                Token::Sym('}') => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return;
                    }
                }
                Token::Sym(';') if depth == 0 => return,
                _ => {}
            }
        }
    }

    /// Skips `[ ... ]` field options if present.
    fn skip_options(&mut self) {
        if !self.is_sym('[') {
            return;
        }
        let mut depth = 0usize;
        while let Some(token) = self.next() {
            match token {
                Token::Sym('[') => depth += 1,
                Token::Sym(']') => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return;
                    }
                }
                _ => {}
            }
        }
    }

    fn parse_file(mut self) -> KafkaResult<ProtoSchema> {
        while let Some(token) = self.peek().cloned() {
            match token {
                Token::Ident(word) if word == "package" => {
                    self.next();
                    self.schema.package = self.ident()?;
                    self.expect_sym(';')?;
                }
                Token::Ident(word) if word == "message" => {
                    self.next();
                    let index = self.parse_message("")?;
                    self.schema.top.push(index);
                }
                Token::Ident(word) if word == "enum" => {
                    self.next();
                    self.parse_enum("")?;
                }
                Token::Sym(';') => {
                    self.next();
                }
                _ => self.skip_statement(),
            }
        }
        Ok(self.schema)
    }

    fn parse_enum(&mut self, scope: &str) -> KafkaResult<()> {
        let name = self.ident()?;
        let full_name = join(scope, &name);
        self.expect_sym('{')?;
        let mut values = HashMap::new();
        loop {
            match self.peek().cloned() {
                None => return Err(err(format!("enum {name} is not closed"))),
                Some(Token::Sym('}')) => {
                    self.next();
                    break;
                }
                Some(Token::Ident(word)) if word == "option" || word == "reserved" => {
                    self.skip_statement()
                }
                Some(Token::Ident(value_name)) => {
                    self.next();
                    if self.is_sym('=') {
                        self.next();
                        if let Some(Token::Number(n)) = self.next() {
                            if let Some(n) = parse_int(&n) {
                                values.insert(n, value_name);
                            }
                        }
                        self.skip_options();
                        self.expect_sym(';')?;
                    } else {
                        self.skip_statement();
                    }
                }
                Some(_) => {
                    self.next();
                }
            }
        }
        self.schema.enums.insert(full_name, values);
        Ok(())
    }

    fn parse_message(&mut self, scope: &str) -> KafkaResult<usize> {
        let name = self.ident()?;
        let full_name = join(scope, &name);
        self.expect_sym('{')?;
        let index = self.schema.messages.len();
        self.schema.messages.push(MessageDef {
            qualified: String::new(),
            full_name: full_name.clone(),
            fields: Vec::new(),
            nested: Vec::new(),
        });
        self.parse_message_body(index, &full_name)?;
        Ok(index)
    }

    /// Parses fields and nested types up to the closing `}`.
    fn parse_message_body(&mut self, index: usize, full_name: &str) -> KafkaResult<()> {
        if self.depth >= MAX_PARSE_DEPTH {
            return Err(err(format!(
                "messages nested deeper than {MAX_PARSE_DEPTH} levels at {full_name}"
            )));
        }
        self.depth += 1;
        let result = self.parse_message_body_inner(index, full_name);
        self.depth -= 1;
        result
    }

    fn parse_message_body_inner(&mut self, index: usize, full_name: &str) -> KafkaResult<()> {
        loop {
            match self.peek().cloned() {
                None => return Err(err(format!("message {full_name} is not closed"))),
                Some(Token::Sym('}')) => {
                    self.next();
                    return Ok(());
                }
                Some(Token::Sym(';')) => {
                    self.next();
                }
                Some(Token::Ident(word)) => match word.as_str() {
                    "message" => {
                        self.next();
                        let nested = self.parse_message(full_name)?;
                        self.schema.messages[index].nested.push(nested);
                    }
                    "enum" => {
                        self.next();
                        self.parse_enum(full_name)?;
                    }
                    "oneof" => {
                        self.next();
                        let name = self.ident()?;
                        self.expect_sym('{')?;
                        // A oneof's fields belong to the enclosing message.
                        let first = self.schema.messages[index].fields.len();
                        self.parse_message_body(index, full_name)?;
                        for field in &mut self.schema.messages[index].fields[first..] {
                            field.oneof.get_or_insert_with(|| name.clone());
                        }
                    }
                    "option" | "reserved" | "extensions" | "extend" | "group" => {
                        self.skip_statement()
                    }
                    _ => {
                        let field = self.parse_field()?;
                        self.schema.messages[index].fields.push(field);
                    }
                },
                Some(_) => self.skip_statement(),
            }
        }
    }

    fn parse_field(&mut self) -> KafkaResult<FieldDef> {
        let mut first = self.ident()?;
        let mut repeated = false;
        if matches!(first.as_str(), "repeated" | "optional" | "required") {
            repeated = first == "repeated";
            first = self.ident()?;
        }
        let mut map_types = None;
        if first == "map" && self.is_sym('<') {
            self.next();
            let key = self.ident()?;
            self.expect_sym(',')?;
            let value = self.ident()?;
            self.expect_sym('>')?;
            map_types = Some((key, value));
        }
        let name = self.ident()?;
        self.expect_sym('=')?;
        let number = match self.next() {
            Some(Token::Number(n)) => parse_int(&n)
                .and_then(|n| u64::try_from(n).ok())
                .ok_or_else(|| err(format!("bad field number {n}")))?,
            other => return Err(err(format!("expected a field number, found {other:?}"))),
        };
        self.skip_options();
        self.expect_sym(';')?;
        Ok(FieldDef {
            name,
            number,
            repeated: repeated || map_types.is_some(),
            type_name: first,
            map_types,
            ty: FieldType::Unknown,
            oneof: None,
        })
    }
}

fn parse_unresolved(text: &str) -> KafkaResult<ProtoSchema> {
    let mut schema = Parser {
        tokens: tokenize(text)?,
        pos: 0,
        schema: ProtoSchema::default(),
        depth: 0,
    }
    .parse_file()?;
    let package = schema.package.clone();
    for m in &mut schema.messages {
        m.qualified = join(&package, &m.full_name);
    }
    schema.enum_qualified = schema
        .enums
        .keys()
        .map(|k| (join(&package, k), k.clone()))
        .collect();
    Ok(schema)
}

fn join(scope: &str, name: &str) -> String {
    if scope.is_empty() {
        name.to_string()
    } else {
        format!("{scope}.{name}")
    }
}

impl ProtoSchema {
    pub fn parse(text: &str) -> KafkaResult<Self> {
        Self::parse_with_imports(text, &[])
    }

    /// Parses `text` with the files it imports (Schema Registry references),
    /// so their messages and enums resolve too.
    pub fn parse_with_imports(text: &str, imports: &[String]) -> KafkaResult<Self> {
        let mut schema = parse_unresolved(text)?;
        for import in imports {
            schema.merge(parse_unresolved(import)?);
        }
        schema.resolve();
        if schema.top.is_empty() {
            return Err(err("no message types"));
        }
        Ok(schema)
    }

    /// Adds an imported file's types. Types from another package keep their
    /// package in the name (`common.Money`), as fields refer to them.
    fn merge(&mut self, other: ProtoSchema) {
        let prefix = if other.package == self.package {
            String::new()
        } else {
            other.package.clone()
        };
        for (qualified, stored) in other.enum_qualified {
            self.enum_qualified
                .insert(qualified, join(&prefix, &stored));
        }
        let offset = self.messages.len();
        for mut m in other.messages {
            m.full_name = join(&prefix, &m.full_name);
            for n in &mut m.nested {
                *n += offset;
            }
            self.messages.push(m);
        }
        for (name, values) in other.enums {
            self.enums.insert(join(&prefix, &name), values);
        }
    }

    fn resolve(&mut self) {
        // Every type by its package-qualified name.
        let names: HashMap<String, usize> = self
            .messages
            .iter()
            .enumerate()
            .map(|(i, m)| (m.qualified.clone(), i))
            .collect();
        let enums = self.enum_qualified.clone();
        let lookup = |scope: &str, name: &str| -> FieldType {
            if let Some(scalar) = Scalar::parse(name) {
                return FieldType::Scalar(scalar);
            }
            if let Some(known) = WellKnown::parse(name) {
                return FieldType::WellKnown(known);
            }
            let find = |candidate: &str| -> Option<FieldType> {
                if let Some(&i) = names.get(candidate) {
                    return Some(FieldType::Message(i));
                }
                enums
                    .get(candidate)
                    .map(|stored| FieldType::Enum(stored.clone()))
            };
            // `.pkg.Type` is fully qualified.
            if let Some(absolute) = name.strip_prefix('.') {
                return find(absolute).unwrap_or(FieldType::Unknown);
            }
            // Otherwise search from the innermost scope (message, then
            // package parts) outwards, as protoc does.
            let mut scope = scope.to_string();
            loop {
                if let Some(found) = find(&join(&scope, name)) {
                    return found;
                }
                if scope.is_empty() {
                    return FieldType::Unknown;
                }
                scope = scope
                    .rsplit_once('.')
                    .map_or(String::new(), |(s, _)| s.to_string());
            }
        };
        for message in &mut self.messages {
            let scope = message.qualified.clone();
            for field in &mut message.fields {
                field.ty = match &field.map_types {
                    Some((key, value)) => match Scalar::parse(key) {
                        Some(key) => FieldType::Map(key, Box::new(lookup(&scope, value))),
                        None => FieldType::Unknown,
                    },
                    None => lookup(&scope, &field.type_name),
                };
            }
        }
    }

    /// The message a Confluent message-index path points to (`[0]` is the
    /// first top-level message, `[1, 0]` the first message nested in the second).
    fn message_at(&self, indexes: &[i64]) -> KafkaResult<usize> {
        let mut candidates = &self.top;
        let mut found = None;
        for &i in indexes {
            let i = usize::try_from(i).map_err(|_| err("negative message index"))?;
            let message = *candidates
                .get(i)
                .ok_or_else(|| err(format!("no message at index {i}")))?;
            found = Some(message);
            candidates = &self.messages[message].nested;
        }
        found.ok_or_else(|| err("empty message index"))
    }

    /// Full name (with the package) of the message `indexes` selects.
    pub fn message_name(&self, indexes: &[i64]) -> Option<String> {
        let i = self.message_at(indexes).ok()?;
        Some(join(&self.package, &self.messages[i].full_name))
    }

    /// Decodes `bytes` as the message `indexes` selects, into JSON keyed by
    /// field name. Unknown fields keep their number as the key.
    pub fn decode(&self, indexes: &[i64], bytes: &[u8]) -> KafkaResult<Value> {
        let message = self.message_at(indexes)?;
        self.decode_message(message, bytes, 0)
    }

    fn decode_message(&self, message: usize, bytes: &[u8], depth: usize) -> KafkaResult<Value> {
        if depth > 32 {
            return Err(err("nested too deeply"));
        }
        let def = &self.messages[message];
        let mut out = Map::new();
        let mut pos = 0;
        while pos < bytes.len() {
            let key = read_varint(bytes, &mut pos)?;
            let (number, wire) = (key >> 3, (key & 7) as u8);
            let raw = read_field(bytes, &mut pos, wire)?;
            let Some(field) = def.fields.iter().find(|f| f.number == number) else {
                let value = match raw {
                    Raw::Varint(v) | Raw::Fixed64(v) => Value::from(v),
                    Raw::Fixed32(v) => Value::from(v),
                    Raw::Bytes(b) => crate::codec::protobuf::decode_raw(b)
                        .unwrap_or_else(|_| Value::String(hex(b))),
                };
                // A field from a newer schema may be repeated: collect every
                // occurrence like the raw decoder does.
                match out.get_mut(&number.to_string()) {
                    Some(Value::Array(items)) => items.push(value),
                    Some(existing) => {
                        let first = existing.take();
                        *existing = Value::Array(vec![first, value]);
                    }
                    None => {
                        out.insert(number.to_string(), value);
                    }
                }
                continue;
            };
            match (&field.ty, raw) {
                (FieldType::Map(key_type, value_type), Raw::Bytes(entry)) => {
                    let (k, v) = self.decode_map_entry(*key_type, value_type, entry, depth)?;
                    let object = out
                        .entry(field.name.clone())
                        .or_insert_with(|| Value::Object(Map::new()));
                    if let Value::Object(map) = object {
                        map.insert(k, v);
                    }
                }
                // Packed repeated numbers.
                (FieldType::Scalar(s), Raw::Bytes(b)) if field.repeated && s.packable() => {
                    for v in unpack(*s, b)? {
                        push(&mut out, field.name.clone(), v, true);
                    }
                }
                (FieldType::Enum(e), Raw::Bytes(b)) if field.repeated => {
                    let mut p = 0;
                    while p < b.len() {
                        let v = read_varint(b, &mut p)? as i64;
                        push(&mut out, field.name.clone(), self.enum_value(e, v), true);
                    }
                }
                (ty, raw) => {
                    let value = self.typed_value(ty, raw, depth)?;
                    // A singular embedded message seen twice is merged, as
                    // protobuf parsers do, instead of replaced.
                    if let (FieldType::Message(_), false, Some(Value::Object(existing))) =
                        (ty, field.repeated, out.get_mut(&field.name))
                    {
                        merge_message(existing, value);
                        continue;
                    }
                    push(&mut out, field.name.clone(), value, field.repeated);
                }
            }
        }
        Ok(Value::Object(out))
    }

    fn decode_map_entry(
        &self,
        key_type: Scalar,
        value_type: &FieldType,
        bytes: &[u8],
        depth: usize,
    ) -> KafkaResult<(String, Value)> {
        let (mut key, mut value) = (None, None);
        let mut pos = 0;
        while pos < bytes.len() {
            let tag = read_varint(bytes, &mut pos)?;
            let raw = read_field(bytes, &mut pos, (tag & 7) as u8)?;
            match tag >> 3 {
                1 => key = Some(self.typed_value(&FieldType::Scalar(key_type), raw, depth)?),
                2 => value = Some(self.typed_value(value_type, raw, depth)?),
                _ => {}
            }
        }
        let key = match key {
            Some(Value::String(s)) => s,
            Some(other) => other.to_string(),
            // A missing key is the key type's default.
            None => match key_type {
                Scalar::String | Scalar::Bytes => String::new(),
                Scalar::Bool => "false".to_string(),
                _ => "0".to_string(),
            },
        };
        Ok((key, value.unwrap_or(Value::Null)))
    }

    fn enum_value(&self, name: &str, number: i64) -> Value {
        // Enum numbers are int32 on the wire.
        let number = i64::from(number as i32);
        self.enums
            .get(name)
            .and_then(|values| values.get(&number))
            .map_or_else(|| Value::from(number), |v| Value::String(v.clone()))
    }

    fn typed_value(&self, ty: &FieldType, raw: Raw<'_>, depth: usize) -> KafkaResult<Value> {
        Ok(match (ty, raw) {
            (FieldType::Scalar(s), raw) => scalar_value(*s, raw)?,
            (FieldType::Enum(e), Raw::Varint(v)) => self.enum_value(e, v as i64),
            (FieldType::Message(m), Raw::Bytes(b)) => self.decode_message(*m, b, depth + 1)?,
            (FieldType::WellKnown(known), Raw::Bytes(b)) => match known {
                WellKnown::Timestamp => {
                    let (s, n) = seconds_nanos(b)?;
                    timestamp_text(s, n).map_or_else(
                        || serde_json::json!({"seconds": s, "nanos": n}),
                        Value::String,
                    )
                }
                WellKnown::Duration => {
                    let (s, n) = seconds_nanos(b)?;
                    Value::String(duration_text(s, n))
                }
                WellKnown::Empty => Value::Object(Map::new()),
                WellKnown::Wrapper(scalar) => {
                    // The wrapped value is field 1; absent means the default.
                    let mut pos = 0;
                    let mut value = None;
                    while pos < b.len() {
                        let tag = read_varint(b, &mut pos)?;
                        let raw = read_field(b, &mut pos, (tag & 7) as u8)?;
                        if tag >> 3 == 1 {
                            value = Some(scalar_value(*scalar, raw)?);
                        }
                    }
                    value.unwrap_or_else(|| match scalar {
                        Scalar::String | Scalar::Bytes => Value::String(String::new()),
                        Scalar::Bool => Value::Bool(false),
                        _ => Value::from(0),
                    })
                }
            },
            (_, Raw::Bytes(b)) => {
                crate::codec::protobuf::decode_raw(b).unwrap_or_else(|_| Value::String(hex(b)))
            }
            (_, Raw::Varint(v) | Raw::Fixed64(v)) => Value::from(v),
            (_, Raw::Fixed32(v)) => Value::from(v),
        })
    }
}

/// Field paths for filter autocomplete (`crate::codec::schema_fields`).
impl ProtoSchema {
    /// Full names (with the package) of the top-level messages, in order.
    pub fn top_message_names(&self) -> Vec<String> {
        self.top
            .iter()
            .map(|&i| join(&self.package, &self.messages[i].full_name))
            .collect()
    }

    /// The message `name` picks: its full name with or without the package,
    /// or its short name; the first top-level message without a name.
    fn find_message(&self, name: Option<&str>) -> KafkaResult<usize> {
        let Some(name) = name.map(|n| n.trim_start_matches('.')) else {
            return self.message_at(&[0]);
        };
        let matches = |m: &MessageDef| {
            m.qualified == name
                || m.full_name == name
                || join(&self.package, &m.full_name) == name
                || m.full_name.rsplit('.').next() == Some(name)
        };
        self.messages
            .iter()
            .position(matches)
            .ok_or_else(|| err(format!("no message named {name}")))
    }

    /// The fields of message `name` (see [`Self::find_message`]), as the
    /// decoder shows them: names as declared, `oneof` members in the
    /// enclosing message, enums by value name.
    pub(crate) fn field_entries(
        &self,
        name: Option<&str>,
        limits: &crate::codec::schema_fields::Limits,
    ) -> KafkaResult<crate::codec::schema_fields::FieldCatalog> {
        let message = self.find_message(name)?;
        let mut out = crate::codec::schema_fields::Builder::new(limits);
        self.walk_message(message, &mut Vec::new(), &mut vec![message], &mut out);
        Ok(out.finish())
    }

    fn field_kind(&self, ty: &FieldType) -> crate::codec::schema_fields::FieldKind {
        use crate::codec::schema_fields::FieldKind as K;
        let scalar = |s: Scalar| match s {
            Scalar::Double | Scalar::Float => K::Number,
            Scalar::Bool => K::Boolean,
            Scalar::String => K::String,
            Scalar::Bytes => K::Bytes,
            _ => K::Integer,
        };
        match ty {
            FieldType::Scalar(s) => scalar(*s),
            FieldType::Message(_) => K::Record,
            FieldType::Enum(_) => K::Enum,
            FieldType::Map(_, value) => K::Map(Box::new(self.field_kind(value))),
            // Timestamp and Duration show as text (compared as text).
            FieldType::WellKnown(WellKnown::Timestamp | WellKnown::Duration) => K::String,
            FieldType::WellKnown(WellKnown::Wrapper(s)) => scalar(*s),
            FieldType::WellKnown(WellKnown::Empty) => K::Record,
            FieldType::Unknown => K::Unknown,
        }
    }

    /// Value names of an enum field, by number.
    fn enum_names(&self, ty: &FieldType) -> Vec<String> {
        let FieldType::Enum(name) = ty else {
            return Vec::new();
        };
        let mut values: Vec<(&i64, &String)> = self
            .enums
            .get(name)
            .map(|v| v.iter().collect())
            .unwrap_or_default();
        values.sort();
        values.into_iter().map(|(_, n)| n.clone()).collect()
    }

    fn walk_message(
        &self,
        message: usize,
        steps: &mut Vec<crate::codec::schema_fields::PathStep>,
        stack: &mut Vec<usize>,
        out: &mut crate::codec::schema_fields::Builder,
    ) {
        use crate::codec::schema_fields::{FieldEntry, FieldKind, PathStep};
        if !out.may_descend(steps.len()) {
            return;
        }
        for field in &self.messages[message].fields {
            if let FieldType::Unknown = field.ty {
                out.note(format!(
                    "Protobuf type '{}' of field '{}' is not defined (a missing import?)",
                    field.type_name, field.name
                ));
            }
            let kind = self.field_kind(&field.ty);
            let is_list = field.repeated && field.map_types.is_none();
            steps.push(PathStep::Field(field.name.clone()));
            let recursive = matches!(field.ty, FieldType::Message(m) if stack.contains(&m));
            let pushed = out.push(FieldEntry {
                steps: steps.clone(),
                kind: if is_list {
                    FieldKind::Array(Box::new(kind.clone()))
                } else {
                    kind.clone()
                },
                optional: field.oneof.is_some(),
                enum_values: if is_list {
                    Vec::new()
                } else {
                    self.enum_names(&field.ty)
                },
                recursive: recursive && !is_list,
                group: field.oneof.clone(),
            });
            if pushed {
                match (&field.ty, is_list) {
                    (ty, true) => {
                        if out.may_descend(steps.len()) {
                            steps.push(PathStep::AnyIndex);
                            self.walk_value(ty, kind, steps, stack, out);
                            steps.pop();
                        }
                    }
                    (FieldType::Map(_, value), false) => {
                        if out.may_descend(steps.len()) {
                            steps.push(PathStep::AnyKey);
                            self.walk_value(value, self.field_kind(value), steps, stack, out);
                            steps.pop();
                        }
                    }
                    (FieldType::Message(m), false) if !recursive => {
                        stack.push(*m);
                        self.walk_message(*m, steps, stack, out);
                        stack.pop();
                    }
                    _ => {}
                }
            }
            steps.pop();
        }
    }

    /// An array element or map value at `steps`, and what it holds.
    fn walk_value(
        &self,
        ty: &FieldType,
        kind: crate::codec::schema_fields::FieldKind,
        steps: &mut Vec<crate::codec::schema_fields::PathStep>,
        stack: &mut Vec<usize>,
        out: &mut crate::codec::schema_fields::Builder,
    ) {
        let recursive = matches!(ty, FieldType::Message(m) if stack.contains(m));
        let pushed = out.push(crate::codec::schema_fields::FieldEntry {
            steps: steps.clone(),
            kind,
            optional: false,
            enum_values: self.enum_names(ty),
            recursive,
            group: None,
        });
        if let (true, false, FieldType::Message(m)) = (pushed, recursive, ty) {
            stack.push(*m);
            self.walk_message(*m, steps, stack, out);
            stack.pop();
        }
    }
}

impl ProtoSchema {
    /// Encodes JSON (keyed by field name; enums by name or number, bytes as
    /// base64) as the first top-level message, prefixed with the Confluent
    /// message-index list for it.
    pub fn encode_first(&self, value: &Value) -> KafkaResult<Vec<u8>> {
        let message = self.message_at(&[0])?;
        let mut out = vec![0u8];
        self.write_message(&mut out, message, value, "", 0)?;
        Ok(out)
    }

    fn write_message(
        &self,
        out: &mut Vec<u8>,
        message: usize,
        value: &Value,
        path: &str,
        depth: usize,
    ) -> KafkaResult<()> {
        if depth > 32 {
            return Err(err("value nests too deeply"));
        }
        let def = &self.messages[message];
        let obj = value
            .as_object()
            .ok_or_else(|| err(format!("expected an object for {}", def.full_name)))?;
        for (key, v) in obj {
            let field = def
                .fields
                .iter()
                .find(|f| &f.name == key)
                .ok_or_else(|| err(format!("{} has no field '{key}'", def.full_name)))?;
            let field_path = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            if v.is_null() {
                continue;
            }
            match (&field.ty, v) {
                (FieldType::Map(key_type, value_type), Value::Object(entries)) => {
                    for (k, item) in entries {
                        let mut entry = Vec::new();
                        let key_value = match key_type {
                            Scalar::String => Value::String(k.clone()),
                            Scalar::Bool => Value::Bool(k == "true"),
                            _ => k
                                .parse::<i64>()
                                .map(Value::from)
                                .map_err(|_| err(format!("map key '{k}' at {field_path}")))?,
                        };
                        self.write_field(
                            &mut entry,
                            1,
                            &FieldType::Scalar(*key_type),
                            &key_value,
                            &field_path,
                            depth,
                        )?;
                        self.write_field(&mut entry, 2, value_type, item, &field_path, depth)?;
                        write_tag(out, field.number, 2);
                        write_varint(out, entry.len() as u64);
                        out.extend_from_slice(&entry);
                    }
                }
                (ty, Value::Array(items)) if field.repeated => {
                    for item in items {
                        self.write_field(out, field.number, ty, item, &field_path, depth)?;
                    }
                }
                (ty, v) => self.write_field(out, field.number, ty, v, &field_path, depth)?,
            }
        }
        Ok(())
    }

    fn write_field(
        &self,
        out: &mut Vec<u8>,
        number: u64,
        ty: &FieldType,
        value: &Value,
        path: &str,
        depth: usize,
    ) -> KafkaResult<()> {
        let bad = |what: &str| err(format!("expected {what} at {path}, found {value}"));
        match ty {
            FieldType::Scalar(s) => {
                let int = || value.as_i64().ok_or_else(|| bad("an integer"));
                let float = || value.as_f64().ok_or_else(|| bad("a number"));
                match s {
                    Scalar::Int32 | Scalar::Int64 | Scalar::Uint32 | Scalar::Uint64 => {
                        let v = match value.as_u64() {
                            Some(u) => u,
                            None => int()? as u64,
                        };
                        write_tag(out, number, 0);
                        write_varint(out, v);
                    }
                    Scalar::Sint32 | Scalar::Sint64 => {
                        let v = int()?;
                        write_tag(out, number, 0);
                        write_varint(out, ((v << 1) ^ (v >> 63)) as u64);
                    }
                    Scalar::Bool => {
                        write_tag(out, number, 0);
                        write_varint(
                            out,
                            u64::from(value.as_bool().ok_or_else(|| bad("a boolean"))?),
                        );
                    }
                    Scalar::Fixed32 | Scalar::Sfixed32 => {
                        write_tag(out, number, 5);
                        out.extend_from_slice(&(int()? as u32).to_le_bytes());
                    }
                    Scalar::Float => {
                        write_tag(out, number, 5);
                        out.extend_from_slice(&(float()? as f32).to_le_bytes());
                    }
                    Scalar::Fixed64 | Scalar::Sfixed64 => {
                        write_tag(out, number, 1);
                        let v = match value.as_u64() {
                            Some(u) => u,
                            None => int()? as u64,
                        };
                        out.extend_from_slice(&v.to_le_bytes());
                    }
                    Scalar::Double => {
                        write_tag(out, number, 1);
                        out.extend_from_slice(&float()?.to_le_bytes());
                    }
                    Scalar::String => {
                        let text = value.as_str().ok_or_else(|| bad("a string"))?;
                        write_tag(out, number, 2);
                        write_varint(out, text.len() as u64);
                        out.extend_from_slice(text.as_bytes());
                    }
                    Scalar::Bytes => {
                        let text = value.as_str().ok_or_else(|| bad("base64 bytes"))?;
                        let bytes =
                            crate::codec::base64::decode(text).map_err(|_| bad("base64 bytes"))?;
                        write_tag(out, number, 2);
                        write_varint(out, bytes.len() as u64);
                        out.extend_from_slice(&bytes);
                    }
                }
            }
            FieldType::Enum(name) => {
                let n = match value {
                    Value::String(symbol) => self
                        .enums
                        .get(name)
                        .and_then(|values| {
                            values.iter().find(|(_, v)| *v == symbol).map(|(n, _)| *n)
                        })
                        .ok_or_else(|| bad(&format!("a value of {name}")))?,
                    _ => value
                        .as_i64()
                        .ok_or_else(|| bad("an enum name or number"))?,
                };
                write_tag(out, number, 0);
                write_varint(out, n as u64);
            }
            FieldType::Message(m) => {
                let mut nested = Vec::new();
                self.write_message(&mut nested, *m, value, path, depth + 1)?;
                write_tag(out, number, 2);
                write_varint(out, nested.len() as u64);
                out.extend_from_slice(&nested);
            }
            FieldType::Map(..) => return Err(bad("an object")),
            FieldType::WellKnown(known) => {
                let mut nested = Vec::new();
                match known {
                    WellKnown::Timestamp => {
                        let text = value.as_str().ok_or_else(|| bad("an RFC 3339 time"))?;
                        let t = text
                            .parse::<jiff::Timestamp>()
                            .map_err(|_| bad("an RFC 3339 time"))?;
                        self.write_field(
                            &mut nested,
                            1,
                            &FieldType::Scalar(Scalar::Int64),
                            &Value::from(t.as_second()),
                            path,
                            depth,
                        )?;
                        if t.subsec_nanosecond() > 0 {
                            self.write_field(
                                &mut nested,
                                2,
                                &FieldType::Scalar(Scalar::Int32),
                                &Value::from(t.subsec_nanosecond()),
                                path,
                                depth,
                            )?;
                        }
                    }
                    WellKnown::Duration => {
                        let (s, n) = value
                            .as_str()
                            .and_then(parse_duration)
                            .ok_or_else(|| bad("a duration like \"1.5s\""))?;
                        self.write_field(
                            &mut nested,
                            1,
                            &FieldType::Scalar(Scalar::Int64),
                            &Value::from(s),
                            path,
                            depth,
                        )?;
                        if n != 0 {
                            self.write_field(
                                &mut nested,
                                2,
                                &FieldType::Scalar(Scalar::Int32),
                                &Value::from(n),
                                path,
                                depth,
                            )?;
                        }
                    }
                    WellKnown::Empty => {}
                    WellKnown::Wrapper(scalar) => {
                        self.write_field(
                            &mut nested,
                            1,
                            &FieldType::Scalar(*scalar),
                            value,
                            path,
                            depth,
                        )?;
                    }
                }
                write_tag(out, number, 2);
                write_varint(out, nested.len() as u64);
                out.extend_from_slice(&nested);
            }
            FieldType::Unknown => {
                return Err(err(format!(
                "{path} has a type the schema does not define (imported types are not supported)"
            )))
            }
        }
        Ok(())
    }
}

fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn write_tag(out: &mut Vec<u8>, number: u64, wire: u64) {
    write_varint(out, number << 3 | wire);
}

#[derive(Debug, Clone, Copy)]
enum Raw<'a> {
    Varint(u64),
    Fixed64(u64),
    Fixed32(u32),
    Bytes(&'a [u8]),
}

fn read_varint(bytes: &[u8], pos: &mut usize) -> KafkaResult<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*pos).ok_or_else(|| err("varint ends early"))?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(err("varint too long"))
}

fn take<'a>(bytes: &'a [u8], pos: &mut usize, len: usize) -> KafkaResult<&'a [u8]> {
    let end = pos.checked_add(len).ok_or_else(|| err("length overflow"))?;
    let out = bytes
        .get(*pos..end)
        .ok_or_else(|| err("field ends early"))?;
    *pos = end;
    Ok(out)
}

fn read_field<'a>(bytes: &'a [u8], pos: &mut usize, wire: u8) -> KafkaResult<Raw<'a>> {
    Ok(match wire {
        0 => Raw::Varint(read_varint(bytes, pos)?),
        1 => {
            let mut a = [0u8; 8];
            a.copy_from_slice(take(bytes, pos, 8)?);
            Raw::Fixed64(u64::from_le_bytes(a))
        }
        2 => {
            let len =
                usize::try_from(read_varint(bytes, pos)?).map_err(|_| err("length too large"))?;
            Raw::Bytes(take(bytes, pos, len)?)
        }
        5 => {
            let mut a = [0u8; 4];
            a.copy_from_slice(take(bytes, pos, 4)?);
            Raw::Fixed32(u32::from_le_bytes(a))
        }
        other => return Err(err(format!("unsupported wire type {other}"))),
    })
}

fn scalar_value(s: Scalar, raw: Raw<'_>) -> KafkaResult<Value> {
    let zigzag = |v: u64| (v >> 1) as i64 ^ -((v & 1) as i64);
    Ok(match (s, raw) {
        (Scalar::Int32, Raw::Varint(v)) => Value::from(v as i32),
        (Scalar::Int64, Raw::Varint(v)) => Value::from(v as i64),
        (Scalar::Uint32, Raw::Varint(v)) => Value::from(v as u32),
        (Scalar::Uint64, Raw::Varint(v)) => Value::from(v),
        (Scalar::Sint32 | Scalar::Sint64, Raw::Varint(v)) => Value::from(zigzag(v)),
        (Scalar::Bool, Raw::Varint(v)) => Value::Bool(v != 0),
        (Scalar::Fixed32, Raw::Fixed32(v)) => Value::from(v),
        (Scalar::Sfixed32, Raw::Fixed32(v)) => Value::from(v as i32),
        (Scalar::Float, Raw::Fixed32(v)) => float(f64::from(f32::from_bits(v))),
        (Scalar::Fixed64, Raw::Fixed64(v)) => Value::from(v),
        (Scalar::Sfixed64, Raw::Fixed64(v)) => Value::from(v as i64),
        (Scalar::Double, Raw::Fixed64(v)) => float(f64::from_bits(v)),
        (Scalar::String, Raw::Bytes(b)) => Value::String(String::from_utf8_lossy(b).into_owned()),
        // Protobuf JSON shows bytes as base64.
        (Scalar::Bytes, Raw::Bytes(b)) => Value::String(crate::codec::base64::encode(b)),
        (s, _) => return Err(err(format!("wire type does not match {s:?}"))),
    })
}

fn float(v: f64) -> Value {
    serde_json::Number::from_f64(v).map_or_else(|| Value::String(v.to_string()), Value::Number)
}

fn unpack(s: Scalar, bytes: &[u8]) -> KafkaResult<Vec<Value>> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let raw = match s {
            Scalar::Fixed32 | Scalar::Sfixed32 | Scalar::Float => read_field(bytes, &mut pos, 5)?,
            Scalar::Fixed64 | Scalar::Sfixed64 | Scalar::Double => read_field(bytes, &mut pos, 1)?,
            _ => read_field(bytes, &mut pos, 0)?,
        };
        out.push(scalar_value(s, raw)?);
    }
    Ok(out)
}

fn push(out: &mut Map<String, Value>, key: String, value: Value, repeated: bool) {
    match out.get_mut(&key) {
        Some(Value::Array(items)) => items.push(value),
        Some(existing) => {
            // A singular field seen twice: the last one wins.
            *existing = value;
        }
        None => {
            out.insert(
                key,
                if repeated {
                    Value::Array(vec![value])
                } else {
                    value
                },
            );
        }
    }
}

/// Protobuf merge semantics on decoded JSON: repeated fields (arrays)
/// concatenate, nested messages and maps (objects) merge, scalars are
/// replaced by the later value.
fn merge_message(into: &mut Map<String, Value>, from: Value) {
    let Value::Object(from) = from else {
        return;
    };
    for (key, value) in from {
        match (into.get_mut(&key), value) {
            (Some(Value::Array(items)), Value::Array(more)) => items.extend(more),
            (Some(Value::Object(inner)), value @ Value::Object(_)) => merge_message(inner, value),
            (_, value) => {
                into.insert(key, value);
            }
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    crate::model::format::to_hex(bytes)
}

/// Reads the Confluent message-index list in front of a Protobuf payload
/// (a zig-zag varint count, then that many zig-zag indexes; a lone 0 means
/// `[0]`). Returns the indexes and the message bytes.
pub fn read_message_indexes(bytes: &[u8]) -> KafkaResult<(Vec<i64>, &[u8])> {
    let zigzag = |v: u64| (v >> 1) as i64 ^ -((v & 1) as i64);
    let mut pos = 0;
    let count = zigzag(read_varint(bytes, &mut pos)?);
    if !(0..=64).contains(&count) {
        return Err(err("bad message index count"));
    }
    let mut indexes = Vec::new();
    for _ in 0..count {
        indexes.push(zigzag(read_varint(bytes, &mut pos)?));
    }
    if indexes.is_empty() {
        indexes.push(0);
    }
    Ok((indexes, &bytes[pos..]))
}

#[cfg(test)]
mod tests {
    #[test]
    fn deeply_nested_messages_are_a_parse_error_not_a_stack_overflow() {
        let hostile = "message M{".repeat(20_000);
        let e = ProtoSchema::parse(&hostile).unwrap_err().to_string();
        assert!(e.contains("nested deeper"), "{e}");
        let oneofs = format!("message M{{{}", "oneof o{".repeat(20_000));
        assert!(ProtoSchema::parse(&oneofs).is_err());
        // 30 levels is still fine.
        let mut ok = String::new();
        for i in 0..30 {
            ok.push_str(&format!("message M{i}{{"));
        }
        ok.push_str(&"}".repeat(30));
        assert!(ProtoSchema::parse(&ok).is_ok());
    }

    use super::*;
    use serde_json::json;

    const SCHEMA: &str = r#"
        syntax = "proto3";
        package shop.v1;
        import "google/protobuf/timestamp.proto";

        // An order.
        message Order {
          string id = 1;
          int64 amount = 2 [deprecated = true];
          Status status = 3;
          repeated int32 qty = 4;
          Customer customer = 5;
          map<string, int32> counts = 6;
          oneof payment { string card = 7; bool cash = 8; }
          sint32 delta = 9;
          double price = 10;
          repeated Line lines = 11;
          google.protobuf.Timestamp at = 12;
          bytes blob = 13;
          enum Status { UNKNOWN = 0; PAID = 1; }
          message Line { string sku = 1; }
          reserved 20 to 30;
        }
        message Customer { string name = 1; .shop.v1.Order.Status last = 2; }
    "#;

    #[test]
    fn decodes_fields_by_name_with_types() {
        let schema = ProtoSchema::parse(SCHEMA).unwrap();
        let mut bytes = vec![
            0x0a, 0x02, b'o', b'1', // id = "o1"
            0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, // amount = -1
            0x18, 0x01, // status = PAID
            0x22, 0x02, 0x03, 0x04, // qty packed [3, 4]
            0x2a, 0x05, 0x0a, 0x01, b'a', 0x10, 0x01, // customer {name "a", last PAID}
            0x32, 0x05, 0x0a, 0x01, b'k', 0x10, 0x07, // counts {"k": 7}
            0x40, 0x01, // cash = true
            0x48, 0x03, // delta = -2
        ];
        bytes.push(0x51); // price = 1.5
        bytes.extend_from_slice(&1.5f64.to_le_bytes());
        bytes.extend_from_slice(&[0x5a, 0x03, 0x0a, 0x01, b'x']); // lines [{sku "x"}]
        bytes.extend_from_slice(&[0x62, 0x02, 0x08, 0x05]); // at {1: 5}
        bytes.extend_from_slice(&[0x6a, 0x02, 0xff, 0x00]); // blob
        bytes.extend_from_slice(&[0xf8, 0x01, 0x09]); // unknown field 31 = 9
        let value = schema.decode(&[0], &bytes).unwrap();
        assert_eq!(
            value,
            json!({
                "id": "o1", "amount": -1, "status": "PAID", "qty": [3, 4],
                "customer": {"name": "a", "last": "PAID"}, "counts": {"k": 7},
                "cash": true, "delta": -2, "price": 1.5, "lines": [{"sku": "x"}],
                "at": "1970-01-01T00:00:05Z", "blob": "/wA=", "31": 9
            })
        );
        assert_eq!(
            schema.message_name(&[0, 0]).as_deref(),
            Some("shop.v1.Order.Line")
        );
        assert_eq!(
            schema.message_name(&[1]).as_deref(),
            Some("shop.v1.Customer")
        );
        assert_eq!(
            schema.decode(&[1], &[0x0a, 0x01, b'z']).unwrap(),
            json!({"name": "z"})
        );
        assert!(schema.decode(&[5], &[]).is_err());
    }

    #[test]
    fn encodes_json_that_decodes_back() {
        let schema = ProtoSchema::parse(SCHEMA).unwrap();
        let value = json!({
            "id": "o1", "amount": -1, "status": "PAID", "qty": [3, 4],
            "customer": {"name": "a", "last": 1}, "counts": {"k": 7},
            "cash": true, "delta": -2, "price": 1.5, "lines": [{"sku": "x"}],
            "blob": "/wA="
        });
        let bytes = schema.encode_first(&value).unwrap();
        let (indexes, body) = read_message_indexes(&bytes).unwrap();
        assert_eq!(indexes, vec![0]);
        let mut expected = value.clone();
        expected["customer"]["last"] = json!("PAID");
        assert_eq!(schema.decode(&indexes, body).unwrap(), expected);
        assert!(schema.encode_first(&json!({"nope": 1})).is_err());
        assert!(schema.encode_first(&json!({"status": "LOST"})).is_err());
        assert!(schema.encode_first(&json!({"at": {"seconds": 1}})).is_err());
    }

    #[test]
    fn well_known_types_use_their_json_forms() {
        let schema = ProtoSchema::parse(
            r#"syntax = "proto3";
            import "google/protobuf/timestamp.proto";
            import "google/protobuf/wrappers.proto";
            message Event {
              google.protobuf.Timestamp at = 1;
              google.protobuf.Duration took = 2;
              google.protobuf.StringValue note = 3;
              .google.protobuf.Int64Value count = 4;
            }"#,
        )
        .unwrap();
        let value = json!({
            "at": "2026-10-12T05:00:00.123Z",
            "took": "1.5s",
            "note": "hi",
            "count": 7
        });
        let bytes = schema.encode_first(&value).unwrap();
        let (indexes, body) = read_message_indexes(&bytes).unwrap();
        assert_eq!(schema.decode(&indexes, body).unwrap(), value);
        assert_eq!(duration_text(-1, -500_000_000), "-1.5s");
        assert_eq!(parse_duration("-1.5s"), Some((-1, -500_000_000)));
        assert_eq!(parse_duration("--1s"), None);
        assert_eq!(parse_duration("+1s"), None);
        assert!(schema.encode_first(&json!({"at": "yesterday"})).is_err());
    }

    #[test]
    fn nested_packages_resolve_like_protoc() {
        let common = r#"syntax = "proto3"; package shop.common;
            message Money { int64 cents = 1; }"#;
        let main = r#"syntax = "proto3"; package shop;
            message Order { common.Money a = 1; .shop.common.Money b = 2; shop.common.Money c = 3; }"#;
        let schema = ProtoSchema::parse_with_imports(main, &[common.to_string()]).unwrap();
        let value = json!({"a": {"cents": 1}, "b": {"cents": 2}, "c": {"cents": 3}});
        let bytes = schema.encode_first(&value).unwrap();
        let (indexes, body) = read_message_indexes(&bytes).unwrap();
        assert_eq!(schema.decode(&indexes, body).unwrap(), value);
    }

    #[test]
    fn imported_files_resolve_across_packages() {
        let common = r#"syntax = "proto3"; package common;
            message Money { int64 cents = 1; string currency = 2; }
            enum Region { NONE = 0; EU = 1; }"#;
        let main = r#"syntax = "proto3"; package shop;
            import "common.proto";
            message Order { common.Money total = 1; .common.Region region = 2; }"#;
        let schema = ProtoSchema::parse_with_imports(main, &[common.to_string()]).unwrap();
        let value = json!({"total": {"cents": 1250, "currency": "EUR"}, "region": "EU"});
        let bytes = schema.encode_first(&value).unwrap();
        let (indexes, body) = read_message_indexes(&bytes).unwrap();
        assert_eq!(schema.decode(&indexes, body).unwrap(), value);
        // Without the import the type is unknown.
        assert!(ProtoSchema::parse(main)
            .unwrap()
            .encode_first(&value)
            .is_err());
    }

    #[test]
    fn message_indexes_and_bad_schemas() {
        assert_eq!(
            read_message_indexes(&[0x00, 0x08]).unwrap(),
            (vec![0], &[0x08][..])
        );
        // count 2 (zig-zag 4), indexes 1 (2) and 0 (0)
        assert_eq!(
            read_message_indexes(&[0x04, 0x02, 0x00, 0x08]).unwrap(),
            (vec![1, 0], &[0x08][..])
        );
        assert!(ProtoSchema::parse("syntax = \"proto3\";").is_err());
        assert!(ProtoSchema::parse("message M { int32 a = ; }").is_err());
    }

    #[test]
    fn unknown_repeated_fields_keep_every_value() {
        let schema = ProtoSchema::parse(SCHEMA).unwrap();
        // Field 40 (unknown, varint) three times, field 41 once.
        let bytes = [
            0xc0, 0x02, 0x01, 0xc0, 0x02, 0x02, 0xc0, 0x02, 0x03, 0xc8, 0x02, 0x09,
        ];
        let value = schema.decode(&[0], &bytes).unwrap();
        assert_eq!(value["40"], json!([1, 2, 3]));
        assert_eq!(value["41"], json!(9));
    }

    #[test]
    fn singular_message_seen_twice_is_merged() {
        let schema = ProtoSchema::parse(SCHEMA).unwrap();
        let bytes = [
            0x2a, 0x03, 0x0a, 0x01, b'a', // customer {name "a"}
            0x2a, 0x02, 0x10, 0x01, // customer {last PAID}
        ];
        let value = schema.decode(&[0], &bytes).unwrap();
        assert_eq!(value["customer"]["name"], json!("a"));
        assert_eq!(value["customer"]["last"], json!("PAID"));
    }

    #[test]
    fn map_entry_without_key_uses_key_type_default() {
        let schema = ProtoSchema::parse(
            r#"syntax = "proto3";
            message M { map<int32, string> by_id = 1; map<bool, int32> flags = 2; }"#,
        )
        .unwrap();
        let bytes = [
            0x0a, 0x03, 0x12, 0x01, b'x', // by_id entry with only value "x"
            0x12, 0x02, 0x10, 0x05, // flags entry with only value 5
        ];
        let value = schema.decode(&[0], &bytes).unwrap();
        assert_eq!(value["by_id"], json!({"0": "x"}));
        assert_eq!(value["flags"], json!({"false": 5}));
    }
}
