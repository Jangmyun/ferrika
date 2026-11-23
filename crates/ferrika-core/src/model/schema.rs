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

impl SchemaReference {
    /// Reads `NAME=SUBJECT:VERSION` as the command line writes a reference:
    /// `NAME` up to the first `=`, `VERSION` after the last `:` (subjects
    /// may contain `:`, as in context-qualified `:.ctx:orders`). `VERSION`
    /// is a positive number or `latest` (kept as `-1`, to be resolved to a
    /// number before the reference is sent).
    pub fn parse_spec(spec: &str) -> Result<Self, String> {
        let usage = || format!("Reference '{spec}': expected NAME=SUBJECT:VERSION");
        let (name, rest) = spec.split_once('=').ok_or_else(usage)?;
        let (subject, version) = rest.rsplit_once(':').ok_or_else(usage)?;
        let (name, subject, version) = (name.trim(), subject.trim(), version.trim());
        if name.is_empty() || subject.is_empty() || version.is_empty() {
            return Err(usage());
        }
        let version = if version.eq_ignore_ascii_case("latest") {
            -1
        } else {
            match version.parse::<i32>() {
                Ok(v) if v > 0 => v,
                _ => {
                    return Err(format!(
                        "Reference '{spec}': version must be a positive number or 'latest'"
                    ))
                }
            }
        };
        Ok(Self {
            name: name.to_string(),
            subject: subject.to_string(),
            version,
        })
    }

    /// Whether the version is still `latest` (not resolved to a number).
    pub fn is_latest(&self) -> bool {
        self.version < 0
    }
}

impl std::fmt::Display for SchemaReference {
    /// `NAME=SUBJECT:VERSION`, the form [`SchemaReference::parse_spec`] reads.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_latest() {
            write!(f, "{}={}:latest", self.name, self.subject)
        } else {
            write!(f, "{}={}:{}", self.name, self.subject, self.version)
        }
    }
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

#[cfg(test)]
mod tests {
    use super::SchemaReference;

    #[test]
    fn reference_specs_read_name_subject_and_version() {
        let r = SchemaReference::parse_spec("common/money.proto=money-value:3").unwrap();
        assert_eq!(
            (r.name.as_str(), r.subject.as_str(), r.version),
            ("common/money.proto", "money-value", 3)
        );
        assert_eq!(r.to_string(), "common/money.proto=money-value:3");
        // `latest` stays -1 until resolved; subjects may contain ':'.
        let r = SchemaReference::parse_spec("com.acme.Money=:.ctx:money:latest").unwrap();
        assert_eq!(r.subject, ":.ctx:money");
        assert!(r.is_latest());
        assert_eq!(r.to_string(), "com.acme.Money=:.ctx:money:latest");
        for bad in [
            "money",
            "money=subject",
            "=s:1",
            "n=:1",
            "n=s:",
            "n=s:0",
            "n=s:-1",
            "n=s:x",
        ] {
            assert!(SchemaReference::parse_spec(bad).is_err(), "{bad}");
        }
    }
}
