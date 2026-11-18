//! Schema Registry references: a schema's imports and the schema a record
//! was decoded with.

use serde::{Deserialize, Serialize};

/// One entry of a schema's `references`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaReference {
    #[serde(default)]
    pub name: String,
    pub subject: String,
    /// `-1` means the latest version.
    #[serde(default = "latest_version")]
    pub version: i32,
}

/// The schema a record was decoded with: its registry id and, for
/// Protobuf, the message.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaRef {
    pub id: u32,
    pub message: Option<String>,
}

impl SchemaRef {
    /// Schema `id` with no message named.
    pub fn id(id: u32) -> Self {
        Self { id, message: None }
    }
}

fn latest_version() -> i32 {
    -1
}
