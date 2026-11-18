//! The field paths a registry schema gives its records, as filter
//! expressions address them (`$.customer.tier`, `$.items[0].sku`), with each
//! field's type and enum symbols. Used for filter autocomplete and
//! `ferrika schema fields`. No IO: the caller fetches the schema.
//!
//! Paths follow what the decoders show, not the schema's own layout: an Avro
//! union is its branch's value (no branch step), Protobuf `oneof` fields sit
//! in the enclosing message, and field names are used as declared.

use std::collections::HashMap;

use serde::Serialize;
use serde_json::Value;

use crate::codec::avro::Schema as Avro;
use crate::codec::schema::SchemaDefinition;
use crate::error::{KafkaError, KafkaResult};

/// One step of a field path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PathStep {
    /// A named field, `.name` or `["odd name"]`.
    Field(String),
    /// Any element of an array, written `[0]` (expressions take numeric indexes).
    AnyIndex,
    /// Any value of a map, whose keys are data rather than schema.
    AnyKey,
}

/// What a field holds, as the decoded JSON shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldKind {
    String,
    Integer,
    Number,
    Boolean,
    /// Bytes or fixed, shown as text.
    Bytes,
    Enum,
    Record,
    Array(Box<FieldKind>),
    Map(Box<FieldKind>),
    Null,
    /// Several types (a union or `type` list).
    Mixed,
    /// A type the schema does not define (e.g. an unread reference).
    Unknown,
}

impl FieldKind {
    /// Short type name for display: `string`, `array<record>`, `map<long>`.
    pub fn label(&self) -> String {
        match self {
            Self::String => "string".into(),
            Self::Integer => "integer".into(),
            Self::Number => "number".into(),
            Self::Boolean => "boolean".into(),
            Self::Bytes => "bytes".into(),
            Self::Enum => "enum".into(),
            Self::Record => "record".into(),
            Self::Array(of) => format!("array<{}>", of.label()),
            Self::Map(of) => format!("map<{}>", of.label()),
            Self::Null => "null".into(),
            Self::Mixed => "mixed".into(),
            Self::Unknown => "unknown".into(),
        }
    }

    /// One kind for several alternatives: the common one, else `Mixed`.
    pub(crate) fn combine(kinds: Vec<FieldKind>, only_null: bool) -> FieldKind {
        let mut distinct: Vec<FieldKind> = Vec::new();
        for kind in kinds {
            if !distinct.contains(&kind) {
                distinct.push(kind);
            }
        }
        // An integer that may also be fractional reads as a number.
        if distinct.len() == 2
            && distinct.contains(&FieldKind::Integer)
            && distinct.contains(&FieldKind::Number)
        {
            return FieldKind::Number;
        }
        match distinct.len() {
            0 if only_null => FieldKind::Null,
            0 => FieldKind::Unknown,
            1 => distinct.remove(0),
            _ => FieldKind::Mixed,
        }
    }
}

/// One field path of a schema.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FieldEntry {
    pub steps: Vec<PathStep>,
    pub kind: FieldKind,
    /// May be absent or null: a union with `null`, a `oneof` member, a JSON
    /// Schema property outside `required`.
    pub optional: bool,
    /// Enum symbols or `enum`/`const` values from the schema (never from records).
    pub enum_values: Vec<String>,
    /// Expansion stopped here because the type contains itself.
    pub recursive: bool,
    /// A grouping to show, e.g. the Protobuf `oneof` name.
    pub group: Option<String>,
}

impl FieldEntry {
    /// The path as an expression writes it.
    pub fn path(&self) -> String {
        render_path(&self.steps)
    }
}

/// The fields of one schema, in schema order (parents before children).
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct FieldCatalog {
    pub entries: Vec<FieldEntry>,
    /// Some fields were left out (depth, entry or effort limit).
    pub truncated: bool,
    /// Things worth telling the user, e.g. a type that could not be resolved.
    pub notes: Vec<String>,
}

/// Bounds on what a catalog holds.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Path steps below the root.
    pub max_depth: usize,
    pub max_entries: usize,
    /// Enum symbols kept per field.
    pub max_enum_values: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_depth: 8,
            max_entries: 2000,
            max_enum_values: 200,
        }
    }
}

/// The fields of `def`'s records. `message` picks a Protobuf message (full
/// name with or without the package, or the short name); the first
/// top-level message is used without it, as when producing.
pub fn from_definition(
    def: &SchemaDefinition,
    message: Option<&str>,
    limits: &Limits,
) -> KafkaResult<FieldCatalog> {
    match def.schema_type.to_ascii_uppercase().as_str() {
        "AVRO" | "" => {
            let (schema, refs) = crate::codec::schema::parse_avro_with_references(def)?;
            Ok(avro_catalog(&schema, &refs, limits))
        }
        "PROTOBUF" => crate::codec::proto_schema::ProtoSchema::parse_with_imports(
            &def.schema,
            &def.resolved_references,
        )?
        .field_entries(message, limits),
        "JSON" => {
            let schema: Value = serde_json::from_str(&def.schema)
                .map_err(|e| KafkaError::schema(format!("The JSON schema is not JSON: {e}")))?;
            // Referenced subjects that are not JSON are left out, as when producing.
            let external: Vec<(String, Value)> = def
                .resolved_reference_names
                .iter()
                .zip(&def.resolved_references)
                .filter_map(|(name, text)| Some((name.clone(), serde_json::from_str(text).ok()?)))
                .collect();
            Ok(json_catalog(&schema, &external, limits))
        }
        other => Err(KafkaError::schema(format!(
            "Cannot list the fields of a {other} schema"
        ))),
    }
}

/// Whether `name` can follow a `.` in a path; other names need `["…"]`.
fn is_plain_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(crate::model::expr::is_name_char)
}

/// Quotes `text` as an expression string literal (`"a \"b\""`).
pub fn quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        if c == '"' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
    out
}

/// One path step as an expression writes it.
pub fn render_step(step: &PathStep) -> String {
    match step {
        PathStep::Field(name) if is_plain_name(name) => format!(".{name}"),
        PathStep::Field(name) => format!("[{}]", quote(name)),
        PathStep::AnyIndex => "[0]".into(),
        PathStep::AnyKey => "[\"…\"]".into(),
    }
}

/// The path as an expression writes it: `$.a.b[0]["odd key"]`.
pub fn render_path(steps: &[PathStep]) -> String {
    let mut out = String::from("$");
    for step in steps {
        out.push_str(&render_step(step));
    }
    out
}

/// Collects entries within [`Limits`].
pub(crate) struct Builder {
    catalog: FieldCatalog,
    pub(crate) limits: Limits,
}

impl Builder {
    pub(crate) fn new(limits: &Limits) -> Self {
        Self {
            catalog: FieldCatalog::default(),
            limits: *limits,
        }
    }

    /// Adds `entry`; false once the catalog is full.
    pub(crate) fn push(&mut self, mut entry: FieldEntry) -> bool {
        if self.catalog.entries.len() >= self.limits.max_entries {
            self.catalog.truncated = true;
            return false;
        }
        let mut seen = std::collections::HashSet::new();
        entry.enum_values.retain(|v| seen.insert(v.clone()));
        if entry.enum_values.len() > self.limits.max_enum_values {
            entry.enum_values.truncate(self.limits.max_enum_values);
        }
        self.catalog.entries.push(entry);
        true
    }

    /// Whether children at `depth` steps may be added; marks the catalog
    /// truncated when they may not.
    pub(crate) fn may_descend(&mut self, depth: usize) -> bool {
        if depth >= self.limits.max_depth {
            self.catalog.truncated = true;
            return false;
        }
        self.catalog.entries.len() < self.limits.max_entries
    }

    pub(crate) fn truncate(&mut self) {
        self.catalog.truncated = true;
    }

    pub(crate) fn note(&mut self, note: String) {
        if !self.catalog.notes.contains(&note) && self.catalog.notes.len() < 20 {
            self.catalog.notes.push(note);
        }
    }

    pub(crate) fn finish(self) -> FieldCatalog {
        self.catalog
    }
}

/// The fields of an Avro schema whose named types may come from `references`.
pub fn avro_catalog(schema: &Avro, references: &[Avro], limits: &Limits) -> FieldCatalog {
    let mut named = HashMap::new();
    for r in references {
        r.collect_named(&mut named);
    }
    schema.collect_named(&mut named);
    let mut walk = AvroWalk {
        named,
        out: Builder::new(limits),
    };
    walk.walk(vec![schema], &mut Vec::new(), false, &mut Vec::new());
    walk.out.finish()
}

/// What the alternatives at one path (union branches, merged record fields)
/// add up to.
#[derive(Default)]
struct AvroShape<'a> {
    kinds: Vec<FieldKind>,
    optional: bool,
    enums: Vec<String>,
    /// Record branches: name and fields.
    records: Vec<(&'a str, &'a [(String, Avro)])>,
    items: Vec<&'a Avro>,
    values: Vec<&'a Avro>,
}

struct AvroWalk<'a> {
    named: HashMap<String, &'a Avro>,
    out: Builder,
}

impl<'a> AvroWalk<'a> {
    /// Resolves a `Ref` like the decoder: full name, else the short name.
    fn lookup(&self, name: &str) -> Option<&'a Avro> {
        self.named
            .get(name)
            .or_else(|| name.rsplit('.').next().and_then(|s| self.named.get(s)))
            .copied()
    }

    fn expand(&mut self, schema: &'a Avro, shape: &mut AvroShape<'a>, hops: usize) {
        match schema {
            Avro::Null => shape.optional = true,
            Avro::Boolean => shape.kinds.push(FieldKind::Boolean),
            Avro::Int | Avro::Long => shape.kinds.push(FieldKind::Integer),
            Avro::Float | Avro::Double => shape.kinds.push(FieldKind::Number),
            Avro::Bytes | Avro::Fixed { .. } => shape.kinds.push(FieldKind::Bytes),
            Avro::String => shape.kinds.push(FieldKind::String),
            Avro::Enum { symbols, .. } => {
                shape.kinds.push(FieldKind::Enum);
                shape.enums.extend(symbols.iter().cloned());
            }
            Avro::Record { name, fields, .. } => {
                shape.kinds.push(FieldKind::Record);
                shape.records.push((name, fields));
            }
            Avro::Array(items) => {
                // The element kind is filled in by `kind`.
                shape
                    .kinds
                    .push(FieldKind::Array(Box::new(FieldKind::Unknown)));
                shape.items.push(items);
            }
            Avro::Map(values) => {
                shape
                    .kinds
                    .push(FieldKind::Map(Box::new(FieldKind::Unknown)));
                shape.values.push(values);
            }
            // The decoder keeps only the branch's value.
            Avro::Union(branches) => {
                for b in branches {
                    self.expand(b, shape, hops);
                }
            }
            Avro::Ref(name) => match self.lookup(name) {
                Some(target) if hops < 16 => self.expand(target, shape, hops + 1),
                _ => {
                    shape.kinds.push(FieldKind::Unknown);
                    self.out
                        .note(format!("Avro type '{name}' is not defined in the schema"));
                }
            },
        }
    }

    fn shape(&mut self, alternatives: &[&'a Avro]) -> AvroShape<'a> {
        let mut shape = AvroShape::default();
        for s in alternatives {
            self.expand(s, &mut shape, 0);
        }
        shape
    }

    /// The shape's kind, with array elements and map values filled in.
    fn kind(&mut self, shape: &AvroShape<'a>, depth: usize) -> FieldKind {
        let inner = |walk: &mut Self, alts: &[&'a Avro]| {
            if depth > 8 {
                return FieldKind::Unknown;
            }
            let s = walk.shape(alts);
            walk.kind(&s, depth + 1)
        };
        let kinds = shape
            .kinds
            .iter()
            .map(|k| match k {
                FieldKind::Array(_) => FieldKind::Array(Box::new(inner(self, &shape.items))),
                FieldKind::Map(_) => FieldKind::Map(Box::new(inner(self, &shape.values))),
                other => other.clone(),
            })
            .collect();
        FieldKind::combine(kinds, shape.optional)
    }

    fn walk(
        &mut self,
        alternatives: Vec<&'a Avro>,
        steps: &mut Vec<PathStep>,
        optional: bool,
        stack: &mut Vec<&'a str>,
    ) {
        let shape = self.shape(&alternatives);
        let kind = self.kind(&shape, 0);
        let recursive = shape.records.iter().any(|(name, _)| stack.contains(name));
        if !steps.is_empty()
            && !self.out.push(FieldEntry {
                steps: steps.clone(),
                kind,
                optional: optional || shape.optional,
                enum_values: shape.enums.clone(),
                recursive,
                group: None,
            })
        {
            return;
        }
        let has_children =
            !shape.records.is_empty() || !shape.items.is_empty() || !shape.values.is_empty();
        if recursive || !has_children || !self.out.may_descend(steps.len()) {
            return;
        }
        if !shape.records.is_empty() {
            // Fields of every record branch, merged by name in first-seen order.
            let mut merged: Vec<(&'a str, Vec<&'a Avro>, usize)> = Vec::new();
            for (_, fields) in &shape.records {
                for (name, schema) in fields.iter() {
                    match merged.iter_mut().find(|(n, _, _)| *n == name.as_str()) {
                        Some((_, alts, count)) => {
                            alts.push(schema);
                            *count += 1;
                        }
                        None => merged.push((name, vec![schema], 1)),
                    }
                }
            }
            let depth = stack.len();
            stack.extend(shape.records.iter().map(|(name, _)| *name));
            let branches = shape.records.len();
            for (name, alts, count) in merged {
                steps.push(PathStep::Field(name.to_string()));
                self.walk(alts, steps, count < branches, stack);
                steps.pop();
            }
            stack.truncate(depth);
        }
        if !shape.items.is_empty() {
            steps.push(PathStep::AnyIndex);
            self.walk(shape.items.clone(), steps, false, stack);
            steps.pop();
        }
        if !shape.values.is_empty() {
            steps.push(PathStep::AnyKey);
            self.walk(shape.values.clone(), steps, false, stack);
            steps.pop();
        }
    }
}

/// The fields of a JSON Schema whose external `$ref`s may point at
/// `references` (reference name, document).
pub fn json_catalog(
    schema: &Value,
    references: &[(String, Value)],
    limits: &Limits,
) -> FieldCatalog {
    let mut walk = JsonWalk {
        references,
        out: Builder::new(limits),
        visits: 0,
    };
    // The root counts as being expanded, so `{"$ref": "#"}` stops.
    let root = (schema as *const Value as usize, "#".to_string());
    walk.walk(
        vec![(schema, schema)],
        &mut Vec::new(),
        false,
        &mut vec![root],
    );
    walk.out.finish()
}

/// The most schema nodes one JSON Schema catalog visits, like the validator's
/// budget: branching keywords with recursive `$ref`s can otherwise explode.
const MAX_JSON_VISITS: usize = 200_000;

/// A schema node and the document its local `$ref`s resolve in.
type JsonNode<'a> = (&'a Value, &'a Value);

/// What the schemas at one path (through `$ref`, `allOf`, `oneOf`, ...) add up to.
#[derive(Default)]
struct JsonShape<'a> {
    types: Vec<&'a str>,
    optional: bool,
    /// Properties in first-seen order: name, schemas, whether only seen in a
    /// `oneOf`/`anyOf` branch.
    properties: Vec<(&'a str, Vec<JsonNode<'a>>, bool)>,
    required: Vec<&'a str>,
    items: Vec<JsonNode<'a>>,
    /// `additionalProperties`/`patternProperties` schemas (map values).
    values: Vec<JsonNode<'a>>,
    enums: Vec<String>,
    /// Some `enum`/`const` value is not a string.
    enum_non_string: bool,
    /// `$ref` targets followed here, as `(document address, reference)`.
    followed: Vec<(usize, String)>,
    recursive: bool,
}

struct JsonWalk<'a> {
    references: &'a [(String, Value)],
    out: Builder,
    visits: usize,
}

/// An `enum`/`const` value as a candidate: strings as they are, other
/// values as JSON.
fn literal_text(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(_) | Value::Bool(_) | Value::Null => Some(value.to_string()),
        _ => None,
    }
}

impl<'a> JsonWalk<'a> {
    fn collect(
        &mut self,
        node: JsonNode<'a>,
        shape: &mut JsonShape<'a>,
        branch: bool,
        stack: &[(usize, String)],
    ) {
        self.visits += 1;
        if self.visits > MAX_JSON_VISITS {
            self.out.truncate();
            return;
        }
        let (root, schema) = node;
        let Some(s) = schema.as_object() else {
            return;
        };
        if let Some(reference) = s.get("$ref").and_then(Value::as_str) {
            let target = if reference == "#" || reference.starts_with("#/") {
                crate::codec::json_schema::resolve(root, reference).map(|t| (root, t))
            } else if reference.starts_with('#') {
                None
            } else {
                crate::codec::json_schema::resolve_external(self.references, reference)
                    .and_then(|(doc, target)| target.map(|t| (doc, t)))
            };
            match target {
                Some(target) => {
                    let key = (target.0 as *const Value as usize, reference.to_string());
                    if stack.contains(&key) || shape.followed.contains(&key) {
                        shape.recursive = true;
                    } else {
                        shape.followed.push(key);
                        self.collect(target, shape, branch, stack);
                    }
                }
                None => self.out.note(format!(
                    "JSON Schema $ref '{reference}' could not be resolved"
                )),
            }
        }
        match s.get("type") {
            Some(Value::String(t)) => shape.types.push(t),
            Some(Value::Array(ts)) => shape.types.extend(ts.iter().filter_map(Value::as_str)),
            _ => {}
        }
        if let Some(props) = s.get("properties").and_then(Value::as_object) {
            for (name, prop) in props {
                match shape.properties.iter_mut().find(|(n, _, _)| *n == name) {
                    Some((_, nodes, only_branch)) => {
                        nodes.push((root, prop));
                        *only_branch &= branch;
                    }
                    None => shape.properties.push((name, vec![(root, prop)], branch)),
                }
            }
        }
        if !branch {
            if let Some(required) = s.get("required").and_then(Value::as_array) {
                shape
                    .required
                    .extend(required.iter().filter_map(Value::as_str));
            }
        }
        match s.get("items") {
            Some(Value::Array(tuple)) => shape.items.extend(tuple.iter().map(|i| (root, i))),
            Some(items @ Value::Object(_)) => shape.items.push((root, items)),
            _ => {}
        }
        if let Some(Value::Array(tuple)) = s.get("prefixItems") {
            shape.items.extend(tuple.iter().map(|i| (root, i)));
        }
        if let Some(extra @ Value::Object(_)) = s.get("additionalProperties") {
            shape.values.push((root, extra));
        }
        if let Some(patterns) = s.get("patternProperties").and_then(Value::as_object) {
            shape.values.extend(patterns.values().map(|p| (root, p)));
        }
        let options = s
            .get("enum")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .chain(s.get("const"));
        for option in options {
            if let Some(text) = literal_text(option) {
                shape.enum_non_string |= !option.is_string();
                shape.enums.push(text);
            }
        }
        if let Some(all) = s.get("allOf").and_then(Value::as_array) {
            for sub in all {
                self.collect((root, sub), shape, branch, stack);
            }
        }
        for keyword in ["oneOf", "anyOf"] {
            if let Some(branches) = s.get(keyword).and_then(Value::as_array) {
                for sub in branches {
                    self.collect((root, sub), shape, true, stack);
                }
            }
        }
    }

    fn shape(&mut self, nodes: &[JsonNode<'a>], stack: &[(usize, String)]) -> JsonShape<'a> {
        let mut shape = JsonShape::default();
        for node in nodes {
            self.collect(*node, &mut shape, false, stack);
        }
        if shape.types.contains(&"null") {
            shape.optional = true;
        }
        shape.types.retain(|t| *t != "null");
        shape
    }

    fn kind(
        &mut self,
        shape: &JsonShape<'a>,
        stack: &[(usize, String)],
        depth: usize,
    ) -> FieldKind {
        let mut types = shape.types.clone();
        if types.is_empty() {
            if !shape.properties.is_empty() || !shape.values.is_empty() {
                types.push("object");
            } else if !shape.items.is_empty() {
                types.push("array");
            }
        }
        let inner = |walk: &mut Self, nodes: &[JsonNode<'a>]| {
            if depth > 8 || nodes.is_empty() {
                return FieldKind::Unknown;
            }
            let s = walk.shape(nodes, stack);
            walk.kind(&s, stack, depth + 1)
        };
        let all_strings = !shape.enums.is_empty() && !shape.enum_non_string;
        let mut kinds = Vec::new();
        for t in types {
            kinds.push(match t {
                "string" if all_strings => FieldKind::Enum,
                "string" => FieldKind::String,
                "integer" => FieldKind::Integer,
                "number" => FieldKind::Number,
                "boolean" => FieldKind::Boolean,
                "object" if shape.properties.is_empty() && !shape.values.is_empty() => {
                    FieldKind::Map(Box::new(inner(self, &shape.values)))
                }
                "object" => FieldKind::Record,
                "array" => FieldKind::Array(Box::new(inner(self, &shape.items))),
                _ => FieldKind::Unknown,
            });
        }
        if kinds.is_empty() && all_strings {
            kinds.push(FieldKind::Enum);
        }
        FieldKind::combine(kinds, shape.optional)
    }

    fn walk(
        &mut self,
        nodes: Vec<JsonNode<'a>>,
        steps: &mut Vec<PathStep>,
        optional: bool,
        stack: &mut Vec<(usize, String)>,
    ) {
        let shape = self.shape(&nodes, stack);
        let kind = self.kind(&shape, stack, 0);
        if !steps.is_empty()
            && !self.out.push(FieldEntry {
                steps: steps.clone(),
                kind,
                optional: optional || shape.optional,
                enum_values: shape.enums.clone(),
                recursive: shape.recursive,
                group: None,
            })
        {
            return;
        }
        let has_children =
            !shape.properties.is_empty() || !shape.items.is_empty() || !shape.values.is_empty();
        if shape.recursive || !has_children || !self.out.may_descend(steps.len()) {
            return;
        }
        let depth = stack.len();
        stack.extend(shape.followed.iter().cloned());
        for (name, prop_nodes, only_branch) in &shape.properties {
            let optional = *only_branch || !shape.required.contains(name);
            steps.push(PathStep::Field(name.to_string()));
            self.walk(prop_nodes.clone(), steps, optional, stack);
            steps.pop();
        }
        if !shape.items.is_empty() {
            steps.push(PathStep::AnyIndex);
            self.walk(shape.items.clone(), steps, false, stack);
            steps.pop();
        }
        // Known properties only when the object is a record with extras allowed.
        if !shape.values.is_empty() && shape.properties.is_empty() {
            steps.push(PathStep::AnyKey);
            self.walk(shape.values.clone(), steps, false, stack);
            steps.pop();
        }
        stack.truncate(depth);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::expr::{Expr, RecordView};

    fn avro(text: &str) -> FieldCatalog {
        avro_catalog(&Avro::parse(text).unwrap(), &[], &Limits::default())
    }

    fn entry<'a>(catalog: &'a FieldCatalog, path: &str) -> &'a FieldEntry {
        catalog
            .entries
            .iter()
            .find(|e| e.path() == path)
            .unwrap_or_else(|| panic!("{path} not in {:?}", paths(catalog)))
    }

    fn paths(catalog: &FieldCatalog) -> Vec<String> {
        catalog.entries.iter().map(FieldEntry::path).collect()
    }

    /// Every rendered path must parse as an expression.
    fn assert_paths_parse(catalog: &FieldCatalog) {
        for p in paths(catalog) {
            assert!(Expr::parse(&format!("{p} exists")).is_ok(), "{p}");
        }
    }

    /// Whether `<path> exists` is true for the JSON record `value`.
    pub(crate) fn exists_in(path: &str, value: &serde_json::Value) -> bool {
        let text = value.to_string();
        Expr::parse(&format!("{path} exists"))
            .unwrap()
            .matches(&RecordView {
                key: None,
                value: Some(&text),
                headers: &[],
                partition: 0,
                offset: 0,
                timestamp_ms: 0,
            })
    }

    /// Paths of every leaf in `value`, arrays as `[0]`.
    pub(crate) fn leaf_paths(value: &serde_json::Value, prefix: String, out: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(map) if !map.is_empty() => {
                for (k, v) in map {
                    let step = render_step(&PathStep::Field(k.clone()));
                    leaf_paths(v, format!("{prefix}{step}"), out);
                }
            }
            serde_json::Value::Array(items) if !items.is_empty() => {
                leaf_paths(&items[0], format!("{prefix}[0]"), out);
            }
            _ => out.push(prefix),
        }
    }

    const ORDER: &str = r#"{
        "type": "record", "name": "Order", "namespace": "shop",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["PENDING", "PAID", "REFUNDED"]}},
            {"name": "customer", "type": ["null", {"type": "record", "name": "Customer", "fields": [
                {"name": "id", "type": "long"},
                {"name": "email", "type": ["null", "string"]},
                {"name": "tier", "type": "Status"}
            ]}]},
            {"name": "items", "type": {"type": "array", "items": {"type": "record", "name": "Line", "fields": [
                {"name": "sku", "type": "string"}, {"name": "qty", "type": "int"}
            ]}}},
            {"name": "attrs", "type": {"type": "map", "values": "string"}},
            {"name": "previous", "type": ["null", "shop.Customer"]},
            {"name": "code", "type": ["int", "string"]},
            {"name": "hash", "type": {"type": "fixed", "name": "Hash", "size": 4}}
        ]
    }"#;

    #[test]
    fn avro_nested_records_arrays_maps_unions_and_enums() {
        let c = avro(ORDER);
        assert_eq!(
            paths(&c),
            [
                "$.id",
                "$.amount",
                "$.status",
                "$.customer",
                "$.customer.id",
                "$.customer.email",
                "$.customer.tier",
                "$.items",
                "$.items[0]",
                "$.items[0].sku",
                "$.items[0].qty",
                "$.attrs",
                "$.attrs[\"…\"]",
                "$.previous",
                "$.previous.id",
                "$.previous.email",
                "$.previous.tier",
                "$.code",
                "$.hash",
            ]
        );
        assert_paths_parse(&c);
        let status = entry(&c, "$.status");
        assert_eq!(status.kind, FieldKind::Enum);
        assert_eq!(status.enum_values, ["PENDING", "PAID", "REFUNDED"]);
        // A named type used again by name keeps its symbols.
        assert_eq!(entry(&c, "$.customer.tier").enum_values.len(), 3);
        let customer = entry(&c, "$.customer");
        assert_eq!(
            (&customer.kind, customer.optional),
            (&FieldKind::Record, true)
        );
        assert!(entry(&c, "$.customer.email").optional);
        assert!(!entry(&c, "$.customer.id").optional);
        assert_eq!(
            entry(&c, "$.items").kind,
            FieldKind::Array(Box::new(FieldKind::Record))
        );
        assert_eq!(
            entry(&c, "$.attrs").kind,
            FieldKind::Map(Box::new(FieldKind::String))
        );
        assert_eq!(entry(&c, "$.code").kind, FieldKind::Mixed);
        assert_eq!(entry(&c, "$.hash").kind, FieldKind::Bytes);
        assert_eq!(entry(&c, "$.amount").kind.label(), "number");
        assert!(c.notes.is_empty() && !c.truncated);
    }

    #[test]
    fn avro_record_unions_merge_and_recursion_stops() {
        let c = avro(
            r#"{"type": "record", "name": "Event", "fields": [
                {"name": "body", "type": [
                    {"type": "record", "name": "A", "fields": [{"name": "x", "type": "int"}, {"name": "both", "type": "int"}]},
                    {"type": "record", "name": "B", "fields": [{"name": "y", "type": "string"}, {"name": "both", "type": "string"}]}
                ]},
                {"name": "list", "type": {"type": "record", "name": "LinkedList", "fields": [
                    {"name": "value", "type": "int"},
                    {"name": "next", "type": ["null", "LinkedList"]}
                ]}},
                {"name": "lost", "type": "com.example.Missing"}
            ]}"#,
        );
        assert!(entry(&c, "$.body.x").optional);
        assert!(entry(&c, "$.body.y").optional);
        let both = entry(&c, "$.body.both");
        assert_eq!((&both.kind, both.optional), (&FieldKind::Mixed, false));
        let next = entry(&c, "$.list.next");
        assert!(next.recursive && next.optional);
        assert!(!paths(&c).iter().any(|p| p.starts_with("$.list.next.")));
        assert_eq!(entry(&c, "$.lost").kind, FieldKind::Unknown);
        assert!(c.notes[0].contains("com.example.Missing"), "{:?}", c.notes);
    }

    #[test]
    fn avro_types_from_references_resolve() {
        let money = Avro::parse(
            r#"{"type": "record", "name": "Money", "namespace": "common",
                "fields": [{"name": "units", "type": "long"}, {"name": "currency", "type": "string"}]}"#,
        )
        .unwrap();
        let order = Avro::parse(
            r#"{"type": "record", "name": "Order", "fields": [{"name": "total", "type": "common.Money"}]}"#,
        )
        .unwrap();
        let c = avro_catalog(&order, &[money], &Limits::default());
        assert_eq!(paths(&c), ["$.total", "$.total.units", "$.total.currency"]);
    }

    #[test]
    fn limits_truncate() {
        let c = avro_catalog(
            &Avro::parse(ORDER).unwrap(),
            &[],
            &Limits {
                max_entries: 5,
                ..Limits::default()
            },
        );
        assert_eq!(c.entries.len(), 5);
        assert!(c.truncated);
        let c = avro_catalog(
            &Avro::parse(ORDER).unwrap(),
            &[],
            &Limits {
                max_depth: 1,
                ..Limits::default()
            },
        );
        assert!(c.truncated);
        assert!(c.entries.iter().all(|e| e.steps.len() == 1));
    }

    #[test]
    fn paths_quote_names_expressions_cannot_read_bare() {
        let steps = |names: &[&str]| -> Vec<PathStep> {
            names
                .iter()
                .map(|n| PathStep::Field(n.to_string()))
                .collect()
        };
        assert_eq!(render_path(&steps(&["a", "b-c", "2fa"])), "$.a.b-c.2fa");
        assert_eq!(render_path(&steps(&["주문", "금액"])), "$.주문.금액");
        assert_eq!(
            render_path(&steps(&[
                "odd key",
                "a.b",
                "say \"hi\"",
                "",
                "$x",
                "back\\slash"
            ])),
            r#"$["odd key"]["a.b"]["say \"hi\""][""]["$x"]["back\\slash"]"#
        );
        for name in [
            "odd key",
            "a.b",
            "say \"hi\"",
            "",
            "$x",
            "back\\slash",
            "주문",
        ] {
            let path = render_path(&steps(&[name]));
            let doc = serde_json::json!({ name: 1 });
            assert!(exists_in(&path, &doc), "{path}");
        }
        assert_eq!(
            render_path(&[
                PathStep::Field("items".into()),
                PathStep::AnyIndex,
                PathStep::Field("tags".into()),
                PathStep::AnyKey
            ]),
            "$.items[0].tags[\"…\"]"
        );
    }

    #[test]
    fn avro_round_trip_paths_match_the_decoder() {
        let def = SchemaDefinition {
            id: 7,
            schema: ORDER.to_string(),
            schema_type: "AVRO".into(),
            version: None,
            subject: None,
            references: Vec::new(),
            resolved_references: Vec::new(),
            resolved_reference_names: Vec::new(),
        };
        let record = serde_json::json!({
            "id": "o1", "amount": 1.5, "status": "PAID",
            "customer": {"id": 3, "email": "a@b.c", "tier": "PENDING"},
            "items": [{"sku": "s", "qty": 2}], "attrs": {"k": "v"},
            "previous": null, "code": 4, "hash": "abcd"
        });
        let bytes = crate::codec::schema::encode_with_schema(&def, &record.to_string()).unwrap();
        let schema = Avro::parse(ORDER).unwrap();
        let decoded = crate::codec::avro::decode(&schema, &bytes[5..]).unwrap();
        let catalog = from_definition(&def, None, &Limits::default()).unwrap();
        assert_round_trip(&catalog, &decoded);
    }

    /// Every leaf of `decoded` is in the catalog, and every catalog path whose
    /// parent exists in `decoded` reads as present there.
    pub(crate) fn assert_round_trip(catalog: &FieldCatalog, decoded: &serde_json::Value) {
        let known = paths(catalog);
        let mut leaves = Vec::new();
        leaf_paths(decoded, "$".into(), &mut leaves);
        for leaf in &leaves {
            // A map key in the data is `["…"]` in the catalog.
            let found = known.iter().any(|k| {
                k == leaf || {
                    let pattern = k.replace("[\"…\"]", "\u{0}");
                    matches_with_wildcards(&pattern, leaf)
                }
            });
            assert!(found, "{leaf} not in {known:?}");
        }
        for path in &known {
            if path.contains('…') {
                continue;
            }
            let entry = entry(catalog, path);
            let parent = render_path(&entry.steps[..entry.steps.len() - 1]);
            let parent_present = parent == "$" || exists_in(&parent, decoded);
            let parent_null = parent != "$"
                && Expr::parse(&format!("{parent} == null"))
                    .unwrap()
                    .matches(&RecordView {
                        key: None,
                        value: Some(&decoded.to_string()),
                        headers: &[],
                        partition: 0,
                        offset: 0,
                        timestamp_ms: 0,
                    });
            if parent_present && !parent_null && !entry.optional {
                assert!(exists_in(path, decoded), "{path} missing in {decoded}");
            }
        }
    }

    /// `pattern` with `\0` standing for one `["key"]` step matches `text`.
    fn matches_with_wildcards(pattern: &str, text: &str) -> bool {
        match pattern.split_once('\u{0}') {
            None => pattern == text,
            Some((head, tail)) => {
                let Some(rest) = text.strip_prefix(head) else {
                    return false;
                };
                // A map key step: `.name` or `["name"]`.
                let ends = rest
                    .char_indices()
                    .skip(1)
                    .map(|(i, _)| i)
                    .chain(std::iter::once(rest.len()));
                ends.into_iter().any(|end| {
                    let step = &rest[..end];
                    (step.starts_with('.') || step.starts_with("[\""))
                        && matches_with_wildcards(tail, &rest[end..])
                })
            }
        }
    }

    const PROTO: &str = r#"
        syntax = "proto3";
        package shop.v1;
        import "google/protobuf/timestamp.proto";
        import "google/protobuf/wrappers.proto";
        import "common/money.proto";

        message Order {
          string id = 1;
          int64 amount = 2;
          Status status = 3;
          repeated Line lines = 4;
          map<string, Line> by_sku = 5;
          oneof payment { string card = 6; bool cash = 7; }
          google.protobuf.Timestamp at = 8;
          google.protobuf.Int64Value tip = 9;
          Node tree = 10;
          common.Money total = 11;
          bytes blob = 12;
          repeated string tags = 13;
          enum Status { UNKNOWN = 0; PAID = 2; NEW = 1; }
          message Line { string sku = 1; int32 qty = 2; }
        }
        message Node { string label = 1; repeated Node children = 2; Node parent = 3; }
    "#;

    fn proto(text: &str, message: Option<&str>) -> FieldCatalog {
        crate::codec::proto_schema::ProtoSchema::parse(text)
            .unwrap()
            .field_entries(message, &Limits::default())
            .unwrap()
    }

    #[test]
    fn protobuf_messages_repeated_maps_oneofs_and_enums() {
        let c = proto(PROTO, None);
        assert_paths_parse(&c);
        let p = paths(&c);
        for want in [
            "$.id",
            "$.lines",
            "$.lines[0]",
            "$.lines[0].sku",
            "$.by_sku[\"…\"].qty",
            "$.card",
            "$.cash",
            "$.at",
            "$.tip",
            "$.tree.label",
            "$.tree.children[0]",
            "$.tags[0]",
        ] {
            assert!(p.contains(&want.to_string()), "{want} in {p:?}");
        }
        let status = entry(&c, "$.status");
        assert_eq!(status.kind, FieldKind::Enum);
        assert_eq!(status.enum_values, ["UNKNOWN", "NEW", "PAID"]);
        let card = entry(&c, "$.card");
        assert!(card.optional);
        assert_eq!(card.group.as_deref(), Some("payment"));
        assert!(!entry(&c, "$.id").optional);
        assert_eq!(entry(&c, "$.at").kind, FieldKind::String);
        assert_eq!(entry(&c, "$.tip").kind, FieldKind::Integer);
        assert_eq!(
            entry(&c, "$.lines").kind,
            FieldKind::Array(Box::new(FieldKind::Record))
        );
        assert_eq!(
            entry(&c, "$.by_sku").kind,
            FieldKind::Map(Box::new(FieldKind::Record))
        );
        // Recursion stops at the message already being expanded.
        assert!(entry(&c, "$.tree.parent").recursive);
        assert!(entry(&c, "$.tree.children[0]").recursive);
        assert!(!p.iter().any(|x| x.starts_with("$.tree.parent.")));
        // The import was not given: unknown, with a note.
        assert_eq!(entry(&c, "$.total").kind, FieldKind::Unknown);
        assert!(
            c.notes.iter().any(|n| n.contains("common.Money")),
            "{:?}",
            c.notes
        );
        // Another message by short or full name.
        let node = proto(PROTO, Some("Node"));
        assert_eq!(paths(&node)[0], "$.label");
        assert_eq!(
            paths(&proto(PROTO, Some("shop.v1.Order.Line"))),
            ["$.sku", "$.qty"]
        );
        assert!(crate::codec::proto_schema::ProtoSchema::parse(PROTO)
            .unwrap()
            .field_entries(Some("Nope"), &Limits::default())
            .is_err());
        assert_eq!(
            crate::codec::proto_schema::ProtoSchema::parse(PROTO)
                .unwrap()
                .top_message_names(),
            ["shop.v1.Order", "shop.v1.Node"]
        );
    }

    #[test]
    fn protobuf_round_trip_paths_match_the_decoder() {
        let text = r#"
            syntax = "proto3";
            package shop;
            message Order {
              string id = 1;
              Status status = 2;
              repeated Line lines = 3;
              map<string, int32> counts = 4;
              oneof payment { string card = 5; bool cash = 6; }
              Customer customer = 7;
              enum Status { UNKNOWN = 0; PAID = 1; }
              message Line { string sku = 1; }
            }
            message Customer { string name = 1; }
        "#;
        let def = SchemaDefinition {
            id: 9,
            schema: text.to_string(),
            schema_type: "PROTOBUF".into(),
            version: None,
            subject: None,
            references: Vec::new(),
            resolved_references: Vec::new(),
            resolved_reference_names: Vec::new(),
        };
        let record = serde_json::json!({
            "id": "o1", "status": "PAID", "lines": [{"sku": "s"}],
            "counts": {"k": 2}, "cash": true, "customer": {"name": "n"}
        });
        let bytes = crate::codec::schema::encode_with_schema(&def, &record.to_string()).unwrap();
        let schema = crate::codec::proto_schema::ProtoSchema::parse(text).unwrap();
        let (indexes, body) =
            crate::codec::proto_schema::read_message_indexes(&bytes[5..]).unwrap();
        let decoded = schema.decode(&indexes, body).unwrap();
        let catalog = from_definition(&def, None, &Limits::default()).unwrap();
        assert_round_trip(&catalog, &decoded);
    }

    fn json(text: &str) -> FieldCatalog {
        json_catalog(
            &serde_json::from_str(text).unwrap(),
            &[],
            &Limits::default(),
        )
    }

    #[test]
    fn json_schema_properties_refs_combinators_and_enums() {
        let c = json(
            r##"{
                "type": "object",
                "required": ["id", "status"],
                "properties": {
                    "id": {"type": "string"},
                    "status": {"enum": ["PAID", "NEW"]},
                    "level": {"type": "integer", "enum": [1, 2, 3]},
                    "kind": {"const": "order"},
                    "amount": {"type": ["number", "null"]},
                    "both": {"type": ["string", "integer"]},
                    "customer": {"$ref": "#/$defs/Customer"},
                    "items": {"type": "array", "items": {"$ref": "#/definitions/Line"}},
                    "pair": {"type": "array", "prefixItems": [{"type": "string"}, {"type": "integer"}]},
                    "attrs": {"type": "object", "additionalProperties": {"type": "string"}},
                    "extra": {"allOf": [{"properties": {"a": {"type": "string"}}, "required": ["a"]}, {"properties": {"b": {"type": "boolean"}}}]},
                    "pay": {"oneOf": [{"properties": {"card": {"type": "string"}}, "required": ["card"]}, {"properties": {"cash": {"type": "boolean"}}}]},
                    "tree": {"$ref": "#/$defs/Tree"},
                    "odd key": {"type": "string"}
                },
                "$defs": {
                    "Customer": {"type": "object", "properties": {"tier": {"type": "string"}}},
                    "Tree": {"type": "object", "properties": {"next": {"$ref": "#/$defs/Tree"}, "v": {"type": "integer"}}}
                },
                "definitions": {"Line": {"type": "object", "properties": {"sku": {"type": "string"}}}}
            }"##,
        );
        assert_paths_parse(&c);
        let p = paths(&c);
        for want in [
            "$.customer.tier",
            "$.items[0].sku",
            "$.pair[0]",
            "$.attrs[\"…\"]",
            "$.extra.a",
            "$.extra.b",
            "$.pay.card",
            "$.pay.cash",
            "$.tree.next",
            "$.tree.v",
            "$[\"odd key\"]",
        ] {
            assert!(p.contains(&want.to_string()), "{want} in {p:?}");
        }
        assert!(!entry(&c, "$.id").optional);
        assert!(entry(&c, "$.customer").optional);
        let status = entry(&c, "$.status");
        assert_eq!(
            (&status.kind, status.enum_values.len()),
            (&FieldKind::Enum, 2)
        );
        let level = entry(&c, "$.level");
        assert_eq!(level.kind, FieldKind::Integer);
        assert_eq!(level.enum_values, ["1", "2", "3"]);
        assert_eq!(entry(&c, "$.kind").enum_values, ["order"]);
        let amount = entry(&c, "$.amount");
        assert_eq!((&amount.kind, amount.optional), (&FieldKind::Number, true));
        assert_eq!(entry(&c, "$.both").kind, FieldKind::Mixed);
        assert_eq!(
            entry(&c, "$.attrs").kind,
            FieldKind::Map(Box::new(FieldKind::String))
        );
        assert_eq!(
            entry(&c, "$.pair").kind,
            FieldKind::Array(Box::new(FieldKind::Mixed))
        );
        assert!(!entry(&c, "$.extra.a").optional);
        assert!(entry(&c, "$.pay.card").optional);
        assert!(entry(&c, "$.tree.next").recursive);
        assert!(!p.iter().any(|x| x.starts_with("$.tree.next.")));
    }

    #[test]
    fn json_schema_external_refs_root_recursion_and_budget() {
        let money = serde_json::json!({
            "$id": "https://example.com/money.json",
            "type": "object",
            "properties": {"units": {"type": "integer"}, "code": {"$ref": "#/$defs/Code"}},
            "$defs": {"Code": {"type": "string", "enum": ["KRW", "USD"]}}
        });
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "total": {"$ref": "money.json"},
                "self": {"$ref": "#"},
                "lost": {"$ref": "other.json"}
            }
        });
        let c = json_catalog(&schema, &[("money.json".into(), money)], &Limits::default());
        assert_eq!(entry(&c, "$.total.code").enum_values, ["KRW", "USD"]);
        assert!(entry(&c, "$.self").recursive);
        assert!(
            c.notes.iter().any(|n| n.contains("other.json")),
            "{:?}",
            c.notes
        );
        // Branches over a recursive ref stop within the visit budget.
        let explosive = serde_json::json!({
            "$defs": {"N": {"anyOf": [
                {"properties": {"a": {"$ref": "#/$defs/N"}, "b": {"$ref": "#/$defs/N"}}},
                {"properties": {"c": {"$ref": "#/$defs/N"}, "d": {"$ref": "#/$defs/N"}}}
            ]}},
            "$ref": "#/$defs/N"
        });
        let c = json_catalog(&explosive, &[], &Limits::default());
        assert!(c.entries.len() <= Limits::default().max_entries);
    }

    #[test]
    fn json_schema_round_trip_paths_match_the_payload() {
        let schema = r#"{
            "type": "object", "required": ["id", "lines"],
            "properties": {
                "id": {"type": "string"},
                "lines": {"type": "array", "items": {"type": "object", "required": ["sku"], "properties": {"sku": {"type": "string"}}}},
                "meta": {"type": "object", "additionalProperties": {"type": "integer"}}
            }
        }"#;
        let def = SchemaDefinition {
            id: 3,
            schema: schema.to_string(),
            schema_type: "JSON".into(),
            version: None,
            subject: None,
            references: Vec::new(),
            resolved_references: Vec::new(),
            resolved_reference_names: Vec::new(),
        };
        let record = serde_json::json!({"id": "o1", "lines": [{"sku": "s"}], "meta": {"k": 1}});
        let bytes = crate::codec::schema::encode_with_schema(&def, &record.to_string()).unwrap();
        let decoded: serde_json::Value = serde_json::from_slice(&bytes[5..]).unwrap();
        let catalog = from_definition(&def, None, &Limits::default()).unwrap();
        assert_round_trip(&catalog, &decoded);
        assert!(from_definition(
            &SchemaDefinition {
                schema_type: "XML".into(),
                ..def
            },
            None,
            &Limits::default()
        )
        .is_err());
    }
}
