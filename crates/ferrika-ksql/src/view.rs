//! Query rows as a front end shows them: PII-masked per column on request,
//! as JSON objects or as table cells.

use serde_json::{Map, Value};

use ferrika_core::codec::pii::{mask_json_value, MaskStrategy};

use crate::model::{Column, QueryResult};

/// `row` with PII masked: each value is judged with its column name, the
/// way JSON fields are judged by their key (a `PASSWORD` column is hidden
/// whole, an `EMAIL` column's address and any card number or SSN inside a
/// text value are masked), and structs, arrays and maps field by field.
pub fn mask_row(columns: &[Column], row: &[Value]) -> Vec<Value> {
    row.iter()
        .enumerate()
        .map(|(i, value)| {
            let name = columns.get(i).map_or("", |c| c.name.as_str());
            let mut field = Value::Object(Map::from_iter([(name.to_string(), value.clone())]));
            mask_json_value(&mut field, MaskStrategy::Partial);
            // A name that looks like PII is masked too; take the one value.
            field
                .as_object()
                .and_then(|m| m.values().next().cloned())
                .unwrap_or(Value::Null)
        })
        .collect()
}

impl QueryResult {
    /// The result with every row masked (see [`mask_row`]).
    pub fn masked(mut self) -> Self {
        for row in &mut self.rows {
            *row = mask_row(&self.columns, row);
        }
        self
    }

    /// The rows as objects keyed by column name, in column order.
    pub fn row_objects(&self) -> Vec<Value> {
        self.rows
            .iter()
            .map(|row| row_object(&self.columns, row))
            .collect()
    }
}

/// `row` as an object keyed by column name.
pub fn row_object(columns: &[Column], row: &[Value]) -> Value {
    Value::Object(
        columns
            .iter()
            .zip(row)
            .map(|(c, v)| (c.name.clone(), v.clone()))
            .collect(),
    )
}

/// A value as a table cell: text without quotes, `null` for SQL NULL,
/// anything else as compact JSON.
pub fn cell_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::QueryEnd;
    use serde_json::json;

    fn columns(names: &[&str]) -> Vec<Column> {
        names
            .iter()
            .map(|n| Column {
                name: n.to_string(),
                type_name: "STRING".into(),
            })
            .collect()
    }

    #[test]
    fn rows_are_masked_by_column_name_and_content() {
        let cols = columns(&["ID", "EMAIL", "PASSWORD", "NOTE", "ADDR"]);
        let row = vec![
            json!(7),
            json!("jane.doe@example.com"),
            json!("hunter2"),
            json!("card 4111 1111 1111 1111 on file"),
            json!({"CITY": "Seoul", "SSN": "123-45-6789"}),
        ];
        let masked = mask_row(&cols, &row);
        assert_eq!(masked[0], json!(7));
        let text = serde_json::to_string(&masked).unwrap();
        for raw in [
            "jane.doe@example.com",
            "hunter2",
            "4111 1111 1111 1111",
            "123-45-6789",
        ] {
            assert!(!text.contains(raw), "{raw} in {text}");
        }
        assert_eq!(masked[4]["CITY"], "Seoul");
        assert_eq!(masked.len(), row.len());
    }

    #[test]
    fn results_render_as_objects_and_cells() {
        let r = QueryResult {
            query_id: "q".into(),
            push: false,
            columns: columns(&["A", "B"]),
            rows: vec![vec![json!("x"), Value::Null]],
            end: QueryEnd::Completed,
        };
        assert_eq!(r.row_objects(), [json!({"A": "x", "B": null})]);
        assert_eq!(cell_text(&json!("x")), "x");
        assert_eq!(cell_text(&Value::Null), "null");
        assert_eq!(cell_text(&json!([1, 2])), "[1,2]");
        let masked = r.masked();
        assert_eq!(masked.rows[0][0], json!("x"));
    }
}
