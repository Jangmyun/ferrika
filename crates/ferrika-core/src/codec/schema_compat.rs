use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Why a schema could not be compared.
#[derive(Debug, thiserror::Error)]
pub enum SchemaCompatError {
    #[error("Invalid JSON in schema definition: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error(
        "Schema format not recognized (expected Avro 'fields' array or JSON Schema 'properties' object)"
    )]
    UnrecognizedFormat,
}

/// Schema evolution compatibility enforcement modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompatibilityMode {
    /// Consumer using new schema can process messages produced with old schema.
    #[default]
    Backward,
    /// Consumer using old schema can process messages produced with new schema.
    Forward,
    /// Both Backward and Forward compatible (bidirectional compatibility).
    Full,
    /// No compatibility checks enforced.
    None,
}

impl CompatibilityMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Backward => "BACKWARD",
            Self::Forward => "FORWARD",
            Self::Full => "FULL",
            Self::None => "NONE",
        }
    }
}

/// Represents a parsed schema field definition (supporting Avro and JSON Schema).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaField {
    /// Field name.
    pub name: String,
    /// Field type descriptor (string, int, union, etc.).
    pub type_def: Value,
    /// Default value if specified.
    pub default_val: Option<Value>,
    /// True if field is nullable or optional.
    pub is_optional: bool,
}

impl SchemaField {
    /// Returns true if this field has a default value or is optional.
    pub fn has_default_or_optional(&self) -> bool {
        self.default_val.is_some() || self.is_optional
    }

    /// Checks if the field type change from `self` (old) to `new_field` is
    /// compatible under `mode`: BACKWARD needs the new type to read data
    /// written with the old one, FORWARD the reverse, FULL both.
    pub fn is_type_compatible(&self, new_field: &SchemaField, mode: CompatibilityMode) -> bool {
        let (old, new) = (&self.type_def, &new_field.type_def);
        match mode {
            CompatibilityMode::None => true,
            CompatibilityMode::Backward => can_read(new, old),
            CompatibilityMode::Forward => can_read(old, new),
            CompatibilityMode::Full => can_read(new, old) && can_read(old, new),
        }
    }
}

/// `{"type": "int"}` is the same Avro type as `"int"`.
fn normalize_type(t: &Value) -> Value {
    match t {
        Value::Object(obj) if obj.len() == 1 => match obj.get("type") {
            Some(Value::String(s)) => Value::String(s.clone()),
            _ => t.clone(),
        },
        Value::Array(items) => Value::Array(items.iter().map(normalize_type).collect()),
        _ => t.clone(),
    }
}

/// True if a reader with type `reader` can resolve data written with
/// type `writer` (equal types or an Avro primitive promotion).
fn can_read(reader: &Value, writer: &Value) -> bool {
    let (reader, writer) = (normalize_type(reader), normalize_type(writer));
    if reader == writer {
        return true;
    }
    matches!(
        (writer.as_str(), reader.as_str()),
        (Some("int"), Some("long" | "float" | "double"))
            | (Some("long"), Some("float" | "double"))
            | (Some("float"), Some("double"))
            | (Some("string"), Some("bytes"))
            | (Some("bytes"), Some("string"))
    )
}

/// A specific compatibility rule violation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilityViolation {
    /// Field where the breaking change was detected.
    pub field_name: String,
    /// Specific compatibility rule violated.
    pub rule: String,
    /// Detailed diagnostic description of the breaking change.
    pub description: String,
}

/// Comprehensive compatibility verdict and drift breakdown.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompatibilityReport {
    /// Target subject name if known.
    pub subject: Option<String>,
    /// Evaluation compatibility mode.
    pub mode: CompatibilityMode,
    /// True if candidate schema is fully compatible.
    pub is_compatible: bool,
    /// List of breaking violations.
    pub violations: Vec<CompatibilityViolation>,
    /// Fields added in candidate schema.
    pub added_fields: Vec<String>,
    /// Fields deleted in candidate schema.
    pub deleted_fields: Vec<String>,
    /// Fields with modified types or defaults.
    pub modified_fields: Vec<String>,
}

/// Extracts fields from an Avro or JSON schema definition string.
pub fn parse_schema_fields(
    schema_str: &str,
) -> Result<HashMap<String, SchemaField>, SchemaCompatError> {
    let json_val: Value = serde_json::from_str(schema_str)?;

    let mut fields_map = HashMap::new();

    // 1. Avro Schema Format: {"type": "record", "fields": [ {"name": "...", "type": "...", "default": ...} ]}
    if let Some(fields_arr) = json_val.get("fields").and_then(|f| f.as_array()) {
        for f in fields_arr {
            if let Some(name) = f.get("name").and_then(|n| n.as_str()) {
                let type_def = f.get("type").cloned().unwrap_or(Value::Null);
                let default_val = f.get("default").cloned();

                // Avro resolution only falls back to the field's `default`;
                // a null union without one is still a resolution error.
                let is_optional = false;

                fields_map.insert(
                    name.to_string(),
                    SchemaField {
                        name: name.to_string(),
                        type_def,
                        default_val,
                        is_optional,
                    },
                );
            }
        }
        return Ok(fields_map);
    }

    // 2. JSON Schema Format: {"type": "object", "properties": { "field": { ... } }, "required": ["..."]}
    if let Some(props_obj) = json_val.get("properties").and_then(|p| p.as_object()) {
        let required_set: HashSet<String> = json_val
            .get("required")
            .and_then(|r| r.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        for (name, prop) in props_obj {
            let type_def = prop.get("type").cloned().unwrap_or(Value::Null);
            let default_val = prop.get("default").cloned();
            let is_optional = !required_set.contains(name);

            fields_map.insert(
                name.clone(),
                SchemaField {
                    name: name.clone(),
                    type_def,
                    default_val,
                    is_optional,
                },
            );
        }
        return Ok(fields_map);
    }

    Err(SchemaCompatError::UnrecognizedFormat)
}

/// Evaluates compatibility of a candidate schema against an existing base schema.
pub fn evaluate_compatibility(
    subject: Option<&str>,
    old_schema_str: &str,
    new_schema_str: &str,
    mode: CompatibilityMode,
) -> Result<CompatibilityReport, SchemaCompatError> {
    if mode == CompatibilityMode::None {
        return Ok(CompatibilityReport {
            subject: subject.map(|s| s.to_string()),
            mode,
            is_compatible: true,
            violations: Vec::new(),
            added_fields: Vec::new(),
            deleted_fields: Vec::new(),
            modified_fields: Vec::new(),
        });
    }

    let old_fields = parse_schema_fields(old_schema_str)?;
    let new_fields = parse_schema_fields(new_schema_str)?;

    let mut violations = Vec::new();
    let mut added_fields = Vec::new();
    let mut deleted_fields = Vec::new();
    let mut modified_fields = Vec::new();

    // Check for newly added fields
    for (name, new_field) in &new_fields {
        if !old_fields.contains_key(name) {
            added_fields.push(name.clone());

            // BACKWARD violation: added field must have default or be optional
            if (mode == CompatibilityMode::Backward || mode == CompatibilityMode::Full)
                && !new_field.has_default_or_optional()
            {
                violations.push(CompatibilityViolation {
                    field_name: name.clone(),
                    rule: "BACKWARD_INCOMPATIBLE_ADDED_REQUIRED_FIELD".to_string(),
                    description: format!(
                        "Field '{name}' added in candidate schema without default value or optional null union. Consumers with new schema cannot read old data missing this field."
                    ),
                });
            }
        }
    }

    // Check for deleted fields and modified fields
    for (name, old_field) in &old_fields {
        if let Some(new_field) = new_fields.get(name) {
            // Field present in both: check type compatibility
            if !old_field.is_type_compatible(new_field, mode) {
                modified_fields.push(name.clone());
                violations.push(CompatibilityViolation {
                    field_name: name.clone(),
                    rule: "INCOMPATIBLE_TYPE_CHANGE".to_string(),
                    description: format!(
                        "Field '{name}' type mutated from {:?} to {:?}. Incompatible data format.",
                        old_field.type_def, new_field.type_def
                    ),
                });
            }
        } else {
            deleted_fields.push(name.clone());

            // FORWARD violation: deleted field must have had a default in old schema
            if (mode == CompatibilityMode::Forward || mode == CompatibilityMode::Full)
                && !old_field.has_default_or_optional()
            {
                violations.push(CompatibilityViolation {
                    field_name: name.clone(),
                    rule: "FORWARD_INCOMPATIBLE_DELETED_REQUIRED_FIELD".to_string(),
                    description: format!(
                        "Field '{name}' was deleted in candidate schema but lacked default value in old schema. Consumers with old schema will fail when reading new records."
                    ),
                });
            }
        }
    }

    let is_compatible = violations.is_empty();

    Ok(CompatibilityReport {
        subject: subject.map(|s| s.to_string()),
        mode,
        is_compatible,
        violations,
        added_fields,
        deleted_fields,
        modified_fields,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const OLD_AVRO_SCHEMA: &str = r#"{
        "type": "record",
        "name": "UserEvent",
        "fields": [
            {"name": "user_id", "type": "string"},
            {"name": "signup_timestamp", "type": "long"},
            {"name": "email", "type": ["null", "string"], "default": null}
        ]
    }"#;

    #[test]
    fn test_backward_compatible_added_field_with_default() {
        let candidate = r#"{
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "signup_timestamp", "type": "long"},
                {"name": "email", "type": ["null", "string"], "default": null},
                {"name": "loyalty_tier", "type": "string", "default": "BRONZE"}
            ]
        }"#;

        let report = evaluate_compatibility(
            Some("users-value"),
            OLD_AVRO_SCHEMA,
            candidate,
            CompatibilityMode::Backward,
        )
        .unwrap();

        assert!(report.is_compatible);
        assert_eq!(report.violations.len(), 0);
        assert_eq!(report.added_fields, vec!["loyalty_tier"]);
    }

    #[test]
    fn test_backward_incompatible_added_required_field() {
        let candidate = r#"{
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "signup_timestamp", "type": "long"},
                {"name": "email", "type": ["null", "string"], "default": null},
                {"name": "phone_number", "type": "string"}
            ]
        }"#;

        let report = evaluate_compatibility(
            Some("users-value"),
            OLD_AVRO_SCHEMA,
            candidate,
            CompatibilityMode::Backward,
        )
        .unwrap();

        assert!(!report.is_compatible);
        assert_eq!(report.violations.len(), 1);
        assert_eq!(
            report.violations[0].rule,
            "BACKWARD_INCOMPATIBLE_ADDED_REQUIRED_FIELD"
        );
        assert_eq!(report.violations[0].field_name, "phone_number");
    }

    #[test]
    fn test_incompatible_field_type_mutation() {
        let candidate = r#"{
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {"name": "user_id", "type": "int"},
                {"name": "signup_timestamp", "type": "long"},
                {"name": "email", "type": ["null", "string"], "default": null}
            ]
        }"#;

        let report = evaluate_compatibility(
            Some("users-value"),
            OLD_AVRO_SCHEMA,
            candidate,
            CompatibilityMode::Backward,
        )
        .unwrap();

        assert!(!report.is_compatible);
        assert_eq!(report.violations.len(), 1);
        assert_eq!(report.violations[0].rule, "INCOMPATIBLE_TYPE_CHANGE");
        assert_eq!(report.violations[0].field_name, "user_id");
    }

    #[test]
    fn test_forward_compatibility_rules() {
        // Deleting a required field without default breaks FORWARD
        let candidate_deleted_required = r#"{
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "email", "type": ["null", "string"], "default": null}
            ]
        }"#;

        let report = evaluate_compatibility(
            Some("users-value"),
            OLD_AVRO_SCHEMA,
            candidate_deleted_required,
            CompatibilityMode::Forward,
        )
        .unwrap();

        assert!(!report.is_compatible);
        assert_eq!(
            report.violations[0].rule,
            "FORWARD_INCOMPATIBLE_DELETED_REQUIRED_FIELD"
        );
        assert_eq!(report.violations[0].field_name, "signup_timestamp");
    }

    #[test]
    fn test_full_compatibility_evaluation() {
        let candidate = r#"{
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "signup_timestamp", "type": "long"},
                {"name": "email", "type": ["null", "string"], "default": null},
                {"name": "vip_status", "type": "boolean", "default": false}
            ]
        }"#;

        let report = evaluate_compatibility(
            Some("users-value"),
            OLD_AVRO_SCHEMA,
            candidate,
            CompatibilityMode::Full,
        )
        .unwrap();

        assert!(report.is_compatible);
    }

    fn record(fields: &str) -> String {
        format!(r#"{{"type":"record","name":"R","fields":[{fields}]}}"#)
    }

    fn compatible(old: &str, new: &str, mode: CompatibilityMode) -> bool {
        evaluate_compatibility(None, &record(old), &record(new), mode)
            .unwrap()
            .is_compatible
    }

    #[test]
    fn test_null_union_without_default_is_not_optional() {
        let base = r#"{"name":"id","type":"string"}"#;
        let with_x = r#"{"name":"id","type":"string"},{"name":"x","type":["null","string"]}"#;
        // BACKWARD: added nullable field without default.
        assert!(!compatible(base, with_x, CompatibilityMode::Backward));
        // FORWARD: deleted nullable field without default.
        assert!(!compatible(with_x, base, CompatibilityMode::Forward));
        // With an explicit null default both are fine.
        let with_default =
            r#"{"name":"id","type":"string"},{"name":"x","type":["null","string"],"default":null}"#;
        assert!(compatible(base, with_default, CompatibilityMode::Full));
        assert!(compatible(with_default, base, CompatibilityMode::Full));
    }

    #[test]
    fn test_type_promotion_respects_direction() {
        let int = r#"{"name":"n","type":"int"}"#;
        let long = r#"{"name":"n","type":"long"}"#;
        // New reader (long) reads old int data.
        assert!(compatible(int, long, CompatibilityMode::Backward));
        // Old reader (int) cannot read new long data.
        assert!(!compatible(int, long, CompatibilityMode::Forward));
        assert!(!compatible(int, long, CompatibilityMode::Full));
        // Narrowing is the reverse.
        assert!(!compatible(long, int, CompatibilityMode::Backward));
        assert!(compatible(long, int, CompatibilityMode::Forward));
    }

    #[test]
    fn test_full_avro_promotion_table() {
        let field = |t: &str| format!(r#"{{"name":"n","type":"{t}"}}"#);
        for (writer, reader) in [
            ("int", "long"),
            ("int", "float"),
            ("int", "double"),
            ("long", "float"),
            ("long", "double"),
            ("float", "double"),
            ("string", "bytes"),
            ("bytes", "string"),
        ] {
            assert!(
                compatible(&field(writer), &field(reader), CompatibilityMode::Backward),
                "{writer} -> {reader}"
            );
        }
        assert!(!compatible(
            &field("double"),
            &field("float"),
            CompatibilityMode::Backward
        ));
        assert!(compatible(
            &field("string"),
            &field("bytes"),
            CompatibilityMode::Full
        ));
    }

    #[test]
    fn test_object_wrapped_primitive_equals_name() {
        let plain = r#"{"name":"n","type":"int"}"#;
        let wrapped = r#"{"name":"n","type":{"type":"int"}}"#;
        assert!(compatible(plain, wrapped, CompatibilityMode::Full));
        assert!(compatible(wrapped, plain, CompatibilityMode::Full));
    }
}
