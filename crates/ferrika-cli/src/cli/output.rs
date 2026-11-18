//! Shared console output for the CLI commands: one table style and one way
//! to print JSON, so every subcommand looks and serializes the same.

use comfy_table::presets::UTF8_FULL;
use comfy_table::{ContentArrangement, Row, Table};
use serde::Serialize;

use crate::error::{KafkaError, KafkaResult};

/// A headerless table in the standard style: UTF-8 borders with rounded
/// corners, columns wrapped to the terminal width.
pub fn untitled_table() -> Table {
    let mut table = Table::new();
    table
        .load_style(UTF8_FULL.with_rounded_corners())
        .set_content_arrangement(ContentArrangement::Dynamic);
    table
}

/// A table in the standard style with the given header cells (plain strings
/// or styled `Cell`s).
pub fn table(headers: impl Into<Row>) -> Table {
    let mut table = untitled_table();
    table.set_header(headers);
    table
}

/// Like [`table`], but columns keep their natural width instead of wrapping
/// to the terminal, for wide reports that read better unbroken.
pub fn fixed_table(headers: impl Into<Row>) -> Table {
    let mut table = table(headers);
    table.set_content_arrangement(ContentArrangement::Disabled);
    table
}

/// Prints `value` as pretty JSON followed by a newline.
pub fn print_json<T: Serialize + ?Sized>(value: &T) -> KafkaResult<()> {
    let json = serde_json::to_string_pretty(value)
        .map_err(|e| KafkaError::Serialization(e.to_string()))?;
    println!("{json}");
    Ok(())
}

/// Prints `value` as JSON when `json` is set, otherwise runs `render` to draw
/// the human-readable view.
pub fn emit<T: Serialize + ?Sized>(
    json: bool,
    value: &T,
    render: impl FnOnce(&T),
) -> KafkaResult<()> {
    if json {
        print_json(value)
    } else {
        render(value);
        Ok(())
    }
}
