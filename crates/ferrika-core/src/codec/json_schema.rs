//! A JSON Schema validator for producing with a registry JSON schema. Covers
//! the common keywords: type, enum, const, properties, required,
//! additionalProperties, items, min/max(Length|imum|Items), pattern, $ref
//! (local `#/...` pointers and other registry subjects passed as schema
//! references), allOf, anyOf, oneOf, not.

use std::cell::Cell;

use serde_json::Value;

/// Problems found in `value`, as `"<path>: <problem>"`; empty when valid.
pub fn validate(schema: &Value, value: &Value) -> Vec<String> {
    validate_with_references(schema, &[], value)
}

/// Like [`validate`], with the schemas that `$ref`s to other documents point
/// at, keyed by reference name (`customer.json`,
/// `https://example.com/customer.json`).
pub fn validate_with_references(
    schema: &Value,
    references: &[(String, Value)],
    value: &Value,
) -> Vec<String> {
    let mut problems = Vec::new();
    let references = Refs {
        docs: references,
        budget: Cell::new(MAX_VISITS),
    };
    check(schema, &references, schema, value, "$", &mut problems, 0);
    if references.budget.get() == 0 {
        // Partial results are unreliable once checks were skipped (a skipped
        // `not` or `anyOf` branch could read as a match), so report only this.
        return vec![format!(
            "$: the schema is too complex to validate (over {MAX_VISITS} checks)"
        )];
    }
    problems
}

/// The most schema/value checks one validation may make. Branching keywords
/// (`anyOf`, `oneOf`, `not`) combined with recursive `$ref`s can otherwise
/// take exponential time within the depth limit.
const MAX_VISITS: usize = 200_000;

/// The referenced documents, plus the checks left in this validation.
struct Refs<'a> {
    docs: &'a [(String, Value)],
    budget: Cell<usize>,
}

/// The last path segment of a URI, for matching `$ref`s to reference names.
fn file_name(uri: &str) -> &str {
    uri.rsplit('/').next().unwrap_or(uri)
}

/// Finds the document an external `$ref` such as `customer.json#/$defs/Id`
/// names, by reference name or `$id`, and the schema inside it. `None` when
/// no such document was given.
pub(crate) fn resolve_external<'a>(
    references: &'a [(String, Value)],
    reference: &str,
) -> Option<(&'a Value, Option<&'a Value>)> {
    let (uri, fragment) = reference.split_once('#').unwrap_or((reference, ""));
    let doc = references
        .iter()
        .find(|(name, _)| name == uri)
        .or_else(|| {
            references
                .iter()
                .find(|(_, doc)| doc.get("$id").and_then(Value::as_str) == Some(uri))
        })
        .or_else(|| {
            references.iter().find(|(name, doc)| {
                file_name(name) == file_name(uri)
                    || doc
                        .get("$id")
                        .and_then(Value::as_str)
                        .is_some_and(|id| file_name(id) == file_name(uri))
            })
        })
        .map(|(_, doc)| doc)?;
    // `$anchor` names (`money.json#Amount`) are not followed, like other
    // refs this validator cannot resolve.
    if !fragment.is_empty() && !fragment.starts_with('/') {
        return None;
    }
    Some((doc, resolve(doc, &format!("#{fragment}"))))
}

fn type_matches(kind: &str, value: &Value) -> bool {
    match kind {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => {
            value.as_i64().is_some()
                || value.as_u64().is_some()
                || value.as_f64().is_some_and(|f| f.fract() == 0.0)
        }
        "array" => value.is_array(),
        "object" => value.is_object(),
        _ => true,
    }
}

/// Resolves a local `$ref` such as `#/definitions/Money` or `#/$defs/Money`.
pub(crate) fn resolve<'a>(root: &'a Value, reference: &str) -> Option<&'a Value> {
    let pointer = reference.strip_prefix('#')?;
    if pointer.is_empty() {
        return Some(root);
    }
    root.pointer(pointer)
}

fn check(
    root: &Value,
    references: &Refs,
    schema: &Value,
    value: &Value,
    path: &str,
    out: &mut Vec<String>,
    depth: usize,
) {
    let Some(left) = references.budget.get().checked_sub(1) else {
        // Out of budget: the caller reports it once at the top.
        out.push(format!("{path}: not checked"));
        return;
    };
    references.budget.set(left);
    if depth > 64 {
        out.push(format!("{path}: schema nests too deeply"));
        return;
    }
    let Some(s) = schema.as_object() else {
        // `true` accepts everything, `false` nothing.
        if schema == &Value::Bool(false) {
            out.push(format!("{path}: not allowed"));
        }
        return;
    };
    // Local refs and refs to the given references are followed; other
    // files or URLs are skipped.
    if let Some(reference) = s.get("$ref").and_then(Value::as_str) {
        // `#Name` is a `$anchor`, which is not followed (like unknown files).
        if reference == "#" || reference.starts_with("#/") {
            match resolve(root, reference) {
                Some(target) => check(root, references, target, value, path, out, depth + 1),
                None => out.push(format!("{path}: cannot resolve $ref '{reference}'")),
            }
        } else if let Some((doc, target)) = (!reference.starts_with('#'))
            .then(|| resolve_external(references.docs, reference))
            .flatten()
        {
            match target {
                Some(target) => check(doc, references, target, value, path, out, depth + 1),
                None => out.push(format!("{path}: cannot resolve $ref '{reference}'")),
            }
        }
    }
    if let Some(t) = s.get("type") {
        let kinds: Vec<&str> = match t {
            Value::String(k) => vec![k.as_str()],
            Value::Array(ks) => ks.iter().filter_map(Value::as_str).collect(),
            _ => Vec::new(),
        };
        if !kinds.is_empty() && !kinds.iter().any(|k| type_matches(k, value)) {
            out.push(format!(
                "{path}: expected {}, found {value}",
                kinds.join(" or ")
            ));
            return;
        }
    }
    if let Some(options) = s.get("enum").and_then(Value::as_array) {
        if !options.contains(value) {
            out.push(format!(
                "{path}: {value} is not one of {}",
                Value::Array(options.clone())
            ));
        }
    }
    if let Some(expected) = s.get("const") {
        if expected != value {
            out.push(format!("{path}: must be {expected}"));
        }
    }
    let num = |k: &str| s.get(k).and_then(Value::as_f64);
    if let Some(n) = value.as_f64() {
        if num("minimum").is_some_and(|m| n < m) {
            out.push(format!("{path}: {n} is below the minimum {}", s["minimum"]));
        }
        if num("maximum").is_some_and(|m| n > m) {
            out.push(format!("{path}: {n} is above the maximum {}", s["maximum"]));
        }
        if num("exclusiveMinimum").is_some_and(|m| n <= m) {
            out.push(format!(
                "{path}: {n} must be above {}",
                s["exclusiveMinimum"]
            ));
        }
        if num("exclusiveMaximum").is_some_and(|m| n >= m) {
            out.push(format!(
                "{path}: {n} must be below {}",
                s["exclusiveMaximum"]
            ));
        }
    }
    if let Some(text) = value.as_str() {
        let len = text.chars().count() as f64;
        if num("minLength").is_some_and(|m| len < m) {
            out.push(format!("{path}: shorter than {}", s["minLength"]));
        }
        if num("maxLength").is_some_and(|m| len > m) {
            out.push(format!("{path}: longer than {}", s["maxLength"]));
        }
        if let Some(pattern) = s.get("pattern").and_then(Value::as_str) {
            match regex::Regex::new(pattern) {
                Ok(re) if !re.is_match(text) => {
                    out.push(format!("{path}: does not match /{pattern}/"))
                }
                Ok(_) => {}
                Err(_) => out.push(format!("{path}: invalid pattern /{pattern}/ in the schema")),
            }
        }
    }
    if let Some(items) = value.as_array() {
        let len = items.len() as f64;
        if num("minItems").is_some_and(|m| len < m) {
            out.push(format!("{path}: fewer than {} items", s["minItems"]));
        }
        if num("maxItems").is_some_and(|m| len > m) {
            out.push(format!("{path}: more than {} items", s["maxItems"]));
        }
        if let Some(item_schema) = s.get("items") {
            for (i, item) in items.iter().enumerate() {
                check(
                    root,
                    references,
                    item_schema,
                    item,
                    &format!("{path}[{i}]"),
                    out,
                    depth + 1,
                );
            }
        }
    }
    if let Some(obj) = value.as_object() {
        for name in s
            .get("required")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(name) = name.as_str() {
                if !obj.contains_key(name) {
                    out.push(format!("{path}: missing required '{name}'"));
                }
            }
        }
        let props = s.get("properties").and_then(Value::as_object);
        let patterns: Vec<(regex::Regex, &Value)> = s
            .get("patternProperties")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
            .filter_map(|(p, schema)| regex::Regex::new(p).ok().map(|re| (re, schema)))
            .collect();
        for (key, v) in obj {
            let child = format!("{path}.{key}");
            let pattern_hits: Vec<&Value> = patterns
                .iter()
                .filter(|(re, _)| re.is_match(key))
                .map(|(_, schema)| *schema)
                .collect();
            for schema in &pattern_hits {
                check(root, references, schema, v, &child, out, depth + 1);
            }
            match props.and_then(|p| p.get(key)) {
                Some(prop) => check(root, references, prop, v, &child, out, depth + 1),
                None if !pattern_hits.is_empty() => {}
                None => match s.get("additionalProperties") {
                    Some(Value::Bool(false)) => out.push(format!("{path}: '{key}' is not allowed")),
                    Some(extra @ Value::Object(_)) => {
                        check(root, references, extra, v, &child, out, depth + 1)
                    }
                    _ => {}
                },
            }
        }
    }
    let sub = |schema: &Value| {
        let mut problems = Vec::new();
        check(
            root,
            references,
            schema,
            value,
            path,
            &mut problems,
            depth + 1,
        );
        problems
    };
    if let Some(all) = s.get("allOf").and_then(Value::as_array) {
        for a in all {
            out.extend(sub(a));
        }
    }
    if let Some(any) = s.get("anyOf").and_then(Value::as_array) {
        if !any.iter().any(|a| sub(a).is_empty()) {
            out.push(format!("{path}: matches none of anyOf"));
        }
    }
    if let Some(one) = s.get("oneOf").and_then(Value::as_array) {
        let matching = one.iter().filter(|a| sub(a).is_empty()).count();
        if matching != 1 {
            out.push(format!(
                "{path}: matches {matching} of oneOf (needs exactly 1)"
            ));
        }
    }
    if let Some(not) = s.get("not") {
        if sub(not).is_empty() {
            out.push(format!("{path}: must not match the 'not' schema"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validates_common_keywords() {
        let schema = json!({
            "type": "object",
            "required": ["id", "total"],
            "additionalProperties": false,
            "properties": {
                "id": {"type": "string", "pattern": "^inv-[0-9]+$"},
                "total": {"$ref": "#/definitions/Money"},
                "tags": {"type": "array", "items": {"type": "string"}, "maxItems": 2},
                "status": {"enum": ["OPEN", "PAID"]},
                "note": {"type": ["string", "null"]}
            },
            "definitions": {
                "Money": {
                    "type": "object",
                    "required": ["cents"],
                    "properties": {"cents": {"type": "integer", "minimum": 0}}
                }
            }
        });
        let ok = json!({"id": "inv-1", "total": {"cents": 5}, "tags": ["a"], "status": "PAID", "note": null});
        assert!(
            validate(&schema, &ok).is_empty(),
            "{:?}",
            validate(&schema, &ok)
        );
        let bad = json!({"id": "x", "total": {"cents": -1}, "tags": ["a", 1, "c"], "status": "LOST", "extra": 1});
        let problems = validate(&schema, &bad);
        for needle in [
            "$.id: does not match",
            "$.total.cents: -1 is below the minimum",
            "$.tags[1]: expected string",
            "$.tags: more than 2 items",
            "$.status: \"LOST\" is not one of",
            "$: 'extra' is not allowed",
        ] {
            assert!(
                problems.iter().any(|p| p.starts_with(needle)),
                "{needle} in {problems:?}"
            );
        }
        assert!(validate(&schema, &json!({"id": "inv-2"}))
            .iter()
            .any(|p| p == "$: missing required 'total'"));
        let patterned = json!({"patternProperties": {"^x-": {"type": "integer"}}, "additionalProperties": false});
        assert!(validate(&patterned, &json!({"x-a": 1})).is_empty());
        assert!(!validate(&patterned, &json!({"x-a": "no"})).is_empty());
        assert!(!validate(&patterned, &json!({"y": 1})).is_empty());
        assert!(validate(&json!({"$ref": "money.json"}), &json!(5)).is_empty());
        // A $ref to another registry subject, by reference name or $id.
        let money = json!({
            "$id": "https://example.com/money.json",
            "type": "object",
            "required": ["amount"],
            "properties": {"amount": {"$ref": "#/$defs/Amount"}},
            "$defs": {"Amount": {"type": "number", "minimum": 0}}
        });
        let refs = vec![("money.json".to_string(), money)];
        let order = json!({"properties": {"total": {"$ref": "money.json"}}});
        assert!(
            validate_with_references(&order, &refs, &json!({"total": {"amount": 3}})).is_empty()
        );
        assert_eq!(
            validate_with_references(&order, &refs, &json!({"total": {"amount": -1}})),
            vec!["$.total.amount: -1 is below the minimum 0".to_string()]
        );
        let by_id = json!({"$ref": "https://example.com/money.json#/$defs/Amount"});
        assert!(!validate_with_references(&by_id, &refs, &json!("x")).is_empty());
        // $anchor fragments are not followed, so they do not block.
        let anchored = json!({"$ref": "money.json#Amount"});
        assert!(validate_with_references(&anchored, &refs, &json!("x")).is_empty());
        assert!(validate(&json!({"$ref": "#Amount"}), &json!("x")).is_empty());
        let missing = json!({"$ref": "money.json#/$defs/Nope"});
        assert!(!validate_with_references(&missing, &refs, &json!(1)).is_empty());
        let choice = json!({"oneOf": [{"type": "integer"}, {"type": "number", "minimum": 10}]});
        assert!(validate(&choice, &json!(3)).is_empty());
        assert!(!validate(&choice, &json!(12)).is_empty());
    }

    #[test]
    fn branching_recursive_refs_stop_at_the_budget() {
        // Each level doubles the work: 2^64 checks without a budget.
        let start = std::time::Instant::now();
        for schema in [
            json!({"anyOf": [{"$ref": "#"}, {"$ref": "#"}]}),
            json!({"oneOf": [{"$ref": "#"}, {"$ref": "#"}]}),
            json!({"allOf": [{"$ref": "#"}, {"$ref": "#"}]}),
            json!({"not": {"allOf": [{"$ref": "#"}, {"$ref": "#"}]}}),
        ] {
            let problems = validate(&schema, &json!(1));
            assert_eq!(problems.len(), 1, "{problems:?}");
            assert!(problems[0].contains("too complex"), "{problems:?}");
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(30));
    }

    fn problems(schema: Value, value: Value) -> Vec<String> {
        validate(&schema, &value)
    }

    #[test]
    fn integer_accepts_whole_floats_and_rejects_fractions() {
        let s = json!({"type": "integer"});
        assert!(problems(s.clone(), json!(3)).is_empty());
        assert!(problems(s.clone(), json!(3.0)).is_empty());
        assert!(problems(s.clone(), json!(u64::MAX)).is_empty());
        assert!(problems(s.clone(), json!(-0.0)).is_empty());
        assert!(!problems(s.clone(), json!(3.5)).is_empty());
        assert!(!problems(s.clone(), json!("3")).is_empty());
        // An unknown type name constrains nothing.
        assert!(problems(json!({"type": "mystery"}), json!(1)).is_empty());
    }

    #[test]
    fn numeric_bounds_are_inclusive_and_exclusive_as_named() {
        let s = json!({"minimum": 1, "maximum": 3, "exclusiveMinimum": 0, "exclusiveMaximum": 4});
        assert!(problems(s.clone(), json!(1)).is_empty());
        assert!(problems(s.clone(), json!(3)).is_empty());
        assert!(!problems(s.clone(), json!(0)).is_empty());
        assert!(!problems(s.clone(), json!(3.1)).is_empty());
        assert!(!problems(json!({"exclusiveMinimum": 1}), json!(1)).is_empty());
        assert!(!problems(json!({"exclusiveMaximum": 1}), json!(1)).is_empty());
        // Bounds do not apply to non-numbers.
        assert!(problems(s, json!("text")).is_empty());
    }

    #[test]
    fn string_length_counts_characters_not_bytes() {
        let s = json!({"minLength": 2, "maxLength": 2});
        assert!(problems(s.clone(), json!("가나")).is_empty());
        assert!(problems(s.clone(), json!("\u{1F600}\u{1F600}")).is_empty());
        assert!(!problems(s.clone(), json!("가")).is_empty());
        assert!(!problems(s.clone(), json!("가나다")).is_empty());
        assert!(!problems(json!({"minLength": 1}), json!("")).is_empty());
    }

    #[test]
    fn invalid_schema_pattern_is_reported_not_panicked() {
        let out = problems(json!({"pattern": "("}), json!("x"));
        assert!(out[0].contains("invalid pattern"), "{out:?}");
        // An invalid patternProperties regex is skipped.
        assert!(problems(
            json!({"patternProperties": {"(": {"type": "string"}}}),
            json!({"a": 1})
        )
        .is_empty());
    }

    #[test]
    fn boolean_schemas_and_combinators() {
        assert!(problems(json!(true), json!(1)).is_empty());
        assert_eq!(problems(json!(false), json!(1)), vec!["$: not allowed"]);
        let one = json!({"oneOf": [{"type": "integer"}, {"minimum": 0}]});
        assert!(problems(one.clone(), json!(-1)).is_empty());
        assert!(!problems(one.clone(), json!(5)).is_empty(), "matches both");
        assert!(!problems(json!({"anyOf": []}), json!(1)).is_empty());
        assert!(problems(json!({"allOf": []}), json!(1)).is_empty());
        assert!(!problems(json!({"not": {}}), json!(1)).is_empty());
    }

    #[test]
    fn additional_properties_schema_and_pattern_properties() {
        let s = json!({
            "properties": {"a": {"type": "string"}},
            "patternProperties": {"^x-": {"type": "integer"}},
            "additionalProperties": {"type": "boolean"}
        });
        assert!(problems(s.clone(), json!({"a": "s", "x-1": 1, "other": true})).is_empty());
        assert!(!problems(s.clone(), json!({"x-1": "no"})).is_empty());
        assert!(!problems(s.clone(), json!({"other": 1})).is_empty());
        assert!(problems(s, json!({})).is_empty());
    }

    #[test]
    fn unresolvable_and_external_refs() {
        let out = problems(json!({"$ref": "#/definitions/Missing"}), json!(1));
        assert!(out[0].contains("cannot resolve"), "{out:?}");
        // Unknown files and anchors are skipped, not failed.
        assert!(problems(json!({"$ref": "other.json#/x"}), json!(1)).is_empty());
        assert!(problems(json!({"$ref": "#Anchor"}), json!(1)).is_empty());
        let docs = vec![(
            "https://example.com/schemas/money.json".to_string(),
            json!({"$defs": {"Cents": {"type": "integer", "minimum": 0}}}),
        )];
        let schema = json!({"$ref": "money.json#/$defs/Cents"});
        assert!(validate_with_references(&schema, &docs, &json!(5)).is_empty());
        assert!(!validate_with_references(&schema, &docs, &json!(-5)).is_empty());
        let bad = json!({"$ref": "money.json#/$defs/Nope"});
        assert!(validate_with_references(&bad, &docs, &json!(5))[0].contains("cannot resolve"));
        assert!(resolve_external(&docs, "money.json#Cents").is_none());
        assert!(resolve_external(&[], "money.json").is_none());
    }

    #[test]
    fn self_referencing_ref_terminates() {
        let s = json!({"$ref": "#"});
        let out = problems(s, json!(1));
        assert!(!out.is_empty());
    }
}
