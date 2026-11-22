use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::codec::schema::{
    encode_with_schema, parse_avro_with_references, parse_confluent_wire_format, parse_definition,
    DecodedPayload, SchemaDefinition,
};
use crate::error::{KafkaError, KafkaResult};
use crate::model::schema::SchemaRef;

/// Asynchronous Schema Registry REST API client with thread-safe memory caching.
#[derive(Clone)]
pub struct SchemaRegistryClient {
    base_url: String,
    cache: Arc<RwLock<HashMap<u32, SchemaDefinition>>>,
    /// Per-request limit; `None` keeps the HTTP client's default (30 s).
    timeout: Option<std::time::Duration>,
}

impl SchemaRegistryClient {
    /// Creates a new Schema Registry client pointing to specified base URL (e.g. "http://localhost:8081").
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            timeout: None,
        }
    }

    /// Sets the time each request may take (see
    /// [`crate::kafka::http::Endpoint::with_timeout`]).
    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Pre-populates the schema cache with a known schema definition.
    pub fn with_cached_schema(self, def: SchemaDefinition) -> Self {
        let mut map = HashMap::new();
        map.insert(def.id, def);
        Self {
            base_url: self.base_url,
            cache: Arc::new(RwLock::new(map)),
            timeout: self.timeout,
        }
    }

    /// Returns base URL of this Schema Registry client.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Fetches schema definition by its global ID, checking memory cache first.
    pub async fn get_schema_by_id(&self, id: u32) -> KafkaResult<SchemaDefinition> {
        // 1. Check local cache
        {
            let cache = self.cache.read().await;
            if let Some(def) = cache.get(&id) {
                return Ok(def.clone());
            }
        }

        // 2. Fetch over HTTP
        let path = format!("/schemas/ids/{}", id);
        let body = self.http_get(&path).await?;

        let mut def = parse_definition(&body, false).map_err(|e| {
            KafkaError::schema(format!("Schema Registry response for ID {id}: {e}"))
        })?;
        def.id = id;
        self.resolve_references(&mut def).await?;

        // 3. Save to cache
        {
            let mut cache = self.cache.write().await;
            cache.insert(id, def.clone());
        }

        Ok(def)
    }

    /// Fills `resolved_references` with the text of every schema `def`
    /// references, directly or through other references.
    async fn resolve_references(&self, def: &mut SchemaDefinition) -> KafkaResult<()> {
        let mut pending = def.references.clone();
        let mut seen = std::collections::HashSet::new();
        let mut texts = Vec::new();
        let mut names = Vec::new();
        while let Some(r) = pending.pop() {
            if !seen.insert((r.subject.clone(), r.version)) {
                continue;
            }
            if seen.len() > 64 {
                return Err(KafkaError::schema("Too many schema references"));
            }
            let path = if r.version < 0 {
                format!("/subjects/{}/versions/latest", path_segment(&r.subject))
            } else {
                format!(
                    "/subjects/{}/versions/{}",
                    path_segment(&r.subject),
                    r.version
                )
            };
            // A reference that cannot be read leaves its types unresolved
            // (shown by field number) instead of failing the whole schema.
            let referenced = match self.http_get(&path).await {
                Ok(body) => serde_json::from_str::<SchemaDefinition>(&body).ok(),
                Err(e) => {
                    tracing::debug!(subject = r.subject, %e, "Schema reference unavailable");
                    None
                }
            };
            if let Some(referenced) = referenced {
                pending.extend(referenced.references);
                texts.push(referenced.schema);
                names.push(r.name);
            }
        }
        def.resolved_references = texts;
        def.resolved_reference_names = names;
        Ok(())
    }

    /// Fetches all registered subject names in Schema Registry.
    pub async fn list_subjects(&self) -> KafkaResult<Vec<String>> {
        let body = self.http_get("/subjects").await?;
        let subjects: Vec<String> = serde_json::from_str(&body).map_err(|e| {
            KafkaError::schema(format!(
                "Failed to parse subjects list from Schema Registry: {}",
                e
            ))
        })?;
        Ok(subjects)
    }

    /// Fetches latest schema version for a specific subject.
    pub async fn get_latest_subject_schema(&self, subject: &str) -> KafkaResult<SchemaDefinition> {
        let path = format!("/subjects/{}/versions/latest", path_segment(subject));
        let body = self.http_get(&path).await?;
        let mut def = parse_definition(&body, true).map_err(|e| {
            KafkaError::schema(format!("Latest schema for subject '{subject}': {e}"))
        })?;
        self.resolve_references(&mut def).await?;

        // Cache by ID if available
        if def.id > 0 {
            let mut cache = self.cache.write().await;
            cache.insert(def.id, def.clone());
        }

        Ok(def)
    }

    /// Performs an asynchronous lightweight HTTP/1.1 GET request over Tokio TCP.
    /// The compatibility level that applies to `subject`: its own, else the
    /// registry-wide default.
    pub async fn compatibility(&self, subject: &str) -> KafkaResult<String> {
        let level = |body: &str| {
            serde_json::from_str::<serde_json::Value>(body)
                .ok()
                .and_then(|v| {
                    v.get("compatibilityLevel")
                        .or_else(|| v.get("compatibility"))
                        .and_then(|l| l.as_str())
                        .map(str::to_string)
                })
                .ok_or_else(|| KafkaError::schema("Unexpected compatibility response"))
        };
        match self
            .http_get(&format!("/config/{}", path_segment(subject)))
            .await
        {
            Ok(body) => level(&body),
            Err(_) => level(&self.http_get("/config").await?),
        }
    }

    /// Sets `subject`'s compatibility level (BACKWARD, FORWARD, FULL, NONE, ...).
    pub async fn set_compatibility(&self, subject: &str, level: &str) -> KafkaResult<()> {
        let body = serde_json::json!({ "compatibility": level }).to_string();
        self.http(
            "PUT",
            &format!("/config/{}", path_segment(subject)),
            Some(&body),
        )
        .await
        .map(|_| ())
    }

    /// Whether `schema` is compatible with every registered version of
    /// `subject` under its compatibility level, as registering checks a
    /// `*_TRANSITIVE` level (`POST /compatibility/subjects/{s}/versions`).
    pub async fn is_compatible_with_all_versions(
        &self,
        subject: &str,
        schema: &str,
        schema_type: &str,
    ) -> KafkaResult<bool> {
        let body = schema_body(schema, schema_type);
        let response = self
            .http(
                "POST",
                &format!("/compatibility/subjects/{}/versions", path_segment(subject)),
                Some(&body),
            )
            .await?;
        serde_json::from_str::<serde_json::Value>(&response)
            .ok()
            .and_then(|v| v.get("is_compatible").and_then(|b| b.as_bool()))
            .ok_or_else(|| KafkaError::schema("Unexpected compatibility check response"))
    }

    /// Whether `schema` could be registered under `subject` (checked against
    /// the latest version with the subject's compatibility level).
    pub async fn is_compatible(
        &self,
        subject: &str,
        schema: &str,
        schema_type: &str,
    ) -> KafkaResult<bool> {
        let body = schema_body(schema, schema_type);
        let response = self
            .http(
                "POST",
                &format!(
                    "/compatibility/subjects/{}/versions/latest",
                    path_segment(subject)
                ),
                Some(&body),
            )
            .await?;
        serde_json::from_str::<serde_json::Value>(&response)
            .ok()
            .and_then(|v| v.get("is_compatible").and_then(|b| b.as_bool()))
            .ok_or_else(|| KafkaError::schema("Unexpected compatibility check response"))
    }

    /// Registers `schema` as a new version of `subject`; returns its id.
    pub async fn register(
        &self,
        subject: &str,
        schema: &str,
        schema_type: &str,
    ) -> KafkaResult<u32> {
        let body = schema_body(schema, schema_type);
        let response = self
            .http(
                "POST",
                &format!("/subjects/{}/versions", path_segment(subject)),
                Some(&body),
            )
            .await?;
        serde_json::from_str::<serde_json::Value>(&response)
            .ok()
            .and_then(|v| v.get("id").and_then(|id| id.as_u64()))
            .map(|id| id as u32)
            .ok_or_else(|| KafkaError::schema("Unexpected register response"))
    }

    /// Soft-deletes `subject` (all versions); returns the deleted versions.
    pub async fn delete_subject(&self, subject: &str) -> KafkaResult<Vec<u32>> {
        let response = self
            .http(
                "DELETE",
                &format!("/subjects/{}", path_segment(subject)),
                None,
            )
            .await?;
        Ok(serde_json::from_str(&response).unwrap_or_default())
    }

    /// Registered versions of `subject`, oldest first.
    pub async fn list_versions(&self, subject: &str) -> KafkaResult<Vec<u32>> {
        let body = self
            .http_get(&format!("/subjects/{}/versions", path_segment(subject)))
            .await?;
        serde_json::from_str(&body).map_err(|e| {
            KafkaError::schema(format!("Failed to parse versions of '{subject}': {e}"))
        })
    }

    /// One version of `subject`.
    pub async fn get_subject_version(
        &self,
        subject: &str,
        version: u32,
    ) -> KafkaResult<SchemaDefinition> {
        let body = self
            .http_get(&format!(
                "/subjects/{}/versions/{version}",
                path_segment(subject)
            ))
            .await?;
        serde_json::from_str(&body).map_err(|e| {
            KafkaError::schema(format!(
                "Failed to parse version {version} of '{subject}': {e}"
            ))
        })
    }

    /// One version of `subject` with its references resolved, or the latest
    /// version without `version`.
    pub async fn subject_schema_resolved(
        &self,
        subject: &str,
        version: Option<u32>,
    ) -> KafkaResult<SchemaDefinition> {
        let Some(version) = version else {
            return self.get_latest_subject_schema(subject).await;
        };
        let mut def = self.get_subject_version(subject, version).await?;
        self.resolve_references(&mut def).await?;
        Ok(def)
    }

    /// `subject`'s own compatibility level, `None` when it has none and the
    /// registry-wide default applies (`GET /config/{subject}` answers 404;
    /// `defaultToGlobal=false` keeps newer registries from filling it in).
    pub async fn subject_compatibility_own(&self, subject: &str) -> KafkaResult<Option<String>> {
        let response = self
            .http_response(
                "GET",
                &format!("/config/{}?defaultToGlobal=false", path_segment(subject)),
                None,
            )
            .await?;
        if response.status == 404 {
            return Ok(None);
        }
        if !response.is_success() {
            return Err(registry_error(&response));
        }
        serde_json::from_str::<serde_json::Value>(&response.body)
            .ok()
            .and_then(|v| {
                v.get("compatibilityLevel")
                    .or_else(|| v.get("compatibility"))
                    .and_then(|l| l.as_str())
                    .map(|l| Some(l.to_string()))
            })
            .ok_or_else(|| KafkaError::schema("Unexpected compatibility response"))
    }

    /// Removes `subject`'s own compatibility level, so the registry-wide
    /// default applies again. A subject without one is not an error.
    pub async fn delete_compatibility(&self, subject: &str) -> KafkaResult<()> {
        let response = self
            .http_response(
                "DELETE",
                &format!("/config/{}", path_segment(subject)),
                None,
            )
            .await?;
        if response.is_success() || response.status == 404 {
            Ok(())
        } else {
            Err(registry_error(&response))
        }
    }

    /// Soft-deletes one version of `subject`.
    pub async fn delete_version(&self, subject: &str, version: u32) -> KafkaResult<()> {
        self.http(
            "DELETE",
            &format!("/subjects/{}/versions/{version}", path_segment(subject)),
            None,
        )
        .await
        .map(|_| ())
    }

    /// Every live version of `subject` with its schema text, type and
    /// references, oldest first; empty when the subject does not exist.
    pub async fn subject_versions_with_schemas(
        &self,
        subject: &str,
    ) -> KafkaResult<Vec<SchemaDefinition>> {
        let response = self
            .http_response(
                "GET",
                &format!("/subjects/{}/versions", path_segment(subject)),
                None,
            )
            .await?;
        if response.status == 404 {
            return Ok(Vec::new());
        }
        if !response.is_success() {
            return Err(registry_error(&response));
        }
        let versions: Vec<u32> = serde_json::from_str(&response.body).map_err(|e| {
            KafkaError::schema(format!("Failed to parse versions of '{subject}': {e}"))
        })?;
        let mut out = Vec::with_capacity(versions.len());
        for version in versions {
            out.push(self.get_subject_version(subject, version).await?);
        }
        Ok(out)
    }

    /// Registers `def` (its text, type and references) under `subject`;
    /// returns the schema id.
    pub async fn register_definition(
        &self,
        subject: &str,
        def: &SchemaDefinition,
    ) -> KafkaResult<u32> {
        let response = self
            .http(
                "POST",
                &format!("/subjects/{}/versions", path_segment(subject)),
                Some(&definition_body(def)),
            )
            .await?;
        serde_json::from_str::<serde_json::Value>(&response)
            .ok()
            .and_then(|v| v.get("id").and_then(|id| id.as_u64()))
            .map(|id| id as u32)
            .ok_or_else(|| KafkaError::schema("Unexpected register response"))
    }

    async fn http_get(&self, path: &str) -> KafkaResult<String> {
        self.http("GET", path, None).await
    }

    /// Sends one request; non-2xx responses become errors carrying the
    /// registry's `message`.
    async fn http(&self, method: &str, path: &str, body: Option<&str>) -> KafkaResult<String> {
        let response = self.http_response(method, path, body).await?;
        if !response.is_success() {
            return Err(registry_error(&response));
        }
        Ok(response.body)
    }

    /// Sends one request and returns the response whatever its status.
    async fn http_response(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> KafkaResult<crate::kafka::http::Response> {
        let mut endpoint = crate::kafka::http::Endpoint::parse(&self.base_url, 8081)
            .map_err(|e| KafkaError::schema(format!("Schema Registry URL: {e}")))?;
        if let Some(timeout) = self.timeout {
            endpoint = endpoint.with_timeout(timeout);
        }
        endpoint
            .send(method, path, "application/vnd.schemaregistry.v1+json", body)
            .await
            .map_err(|e| {
                KafkaError::schema(format!(
                    "Schema Registry at {} is unreachable: {e}",
                    endpoint.target()
                ))
            })
    }
}

/// The error for a non-2xx registry response.
fn registry_error(response: &crate::kafka::http::Response) -> KafkaError {
    KafkaError::schema(format!(
        "Schema Registry returned {}: {}",
        response.status,
        response.error_message()
    ))
}

/// Request body registering `def` again: its text, its type (AVRO left out,
/// as in [`schema_body`]) and its references.
fn definition_body(def: &SchemaDefinition) -> String {
    let mut body = serde_json::json!({ "schema": def.schema });
    if !def.schema_type.eq_ignore_ascii_case("AVRO") && !def.schema_type.is_empty() {
        body["schemaType"] = def.schema_type.to_uppercase().into();
    }
    if !def.references.is_empty() {
        body["references"] = serde_json::json!(def
            .references
            .iter()
            .map(|r| serde_json::json!({
                "name": r.name,
                "subject": r.subject,
                "version": r.version,
            }))
            .collect::<Vec<_>>());
    }
    body.to_string()
}

/// Percent-encodes a subject for use as one URL path segment (subjects
/// such as `common/money.proto` contain slashes). A name that is only dots
/// (`.`, `..`) is encoded too, or the URL would point at a parent path.
pub(crate) fn path_segment(text: &str) -> String {
    if !text.is_empty() && text.bytes().all(|b| b == b'.') {
        return "%2E".repeat(text.len());
    }
    text.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Request body for registering or checking a schema. AVRO is the
/// registry's default type and is left out.
fn schema_body(schema: &str, schema_type: &str) -> String {
    if schema_type.eq_ignore_ascii_case("AVRO") || schema_type.is_empty() {
        serde_json::json!({ "schema": schema }).to_string()
    } else {
        serde_json::json!({ "schema": schema, "schemaType": schema_type.to_uppercase() })
            .to_string()
    }
}

impl SchemaRegistryClient {
    /// Serializes a record key with the latest schema of `subject`, or
    /// `None` when the subject does not exist (keys are often plain
    /// strings). Text that is not JSON is taken as a JSON string.
    pub async fn encode_key(&self, subject: &str, key: &str) -> KafkaResult<Option<Vec<u8>>> {
        let def = match self.get_latest_subject_schema(subject).await {
            Ok(def) => def,
            Err(e) if e.to_string().contains("returned 404") => return Ok(None),
            Err(e) => return Err(e),
        };
        let json = if serde_json::from_str::<serde_json::Value>(key).is_ok() {
            key.to_string()
        } else {
            serde_json::Value::String(key.to_string()).to_string()
        };
        encode_with_schema(&def, &json).map(Some)
    }

    /// Serializes JSON text with the latest schema of `subject`; returns the
    /// bytes and the schema used.
    pub async fn encode_latest(
        &self,
        subject: &str,
        json_text: &str,
    ) -> KafkaResult<(Vec<u8>, SchemaDefinition)> {
        let def = self.get_latest_subject_schema(subject).await?;
        let bytes = encode_with_schema(&def, json_text)?;
        Ok((bytes, def))
    }
}

/// A parsed Avro schema with its referenced schemas, or why it is unusable.
type AvroWithReferences =
    KafkaResult<(crate::codec::avro::Schema, Vec<crate::codec::avro::Schema>)>;

/// Decodes Confluent wire-format payloads (magic byte 0, schema id), fetching
/// and caching writer schemas from a Schema Registry when one is configured.
pub struct PayloadDecoder {
    registry: Option<SchemaRegistryClient>,
    /// Parsed Avro schemas (or why one could not be used), by schema id.
    avro: HashMap<u32, AvroWithReferences>,
    /// Parsed Protobuf schemas (or why one could not be used), by schema id.
    proto: HashMap<u32, KafkaResult<crate::codec::proto_schema::ProtoSchema>>,
}

impl PayloadDecoder {
    pub fn new(registry_url: Option<&str>) -> Self {
        Self {
            registry: registry_url
                .filter(|u| !u.trim().is_empty())
                .map(SchemaRegistryClient::new),
            avro: HashMap::new(),
            proto: HashMap::new(),
        }
    }

    /// `None` when `bytes` has no wire-format header.
    pub async fn decode(&mut self, bytes: &[u8]) -> Option<DecodedPayload> {
        let wire = parse_confluent_wire_format(bytes)?;
        let id = wire.schema_id;
        let text_body = || String::from_utf8_lossy(&wire.payload).into_owned();
        let Some(registry) = &self.registry else {
            return Some(DecodedPayload {
                text: text_body(),
                label: format!("schema {id} (no Schema Registry in this profile)"),
                schema_ref: None,
            });
        };
        let def = match registry.get_schema_by_id(id).await {
            Ok(def) => def,
            Err(e) => {
                return Some(DecodedPayload {
                    text: text_body(),
                    label: format!("schema {id} (lookup failed: {e})"),
                    schema_ref: None,
                })
            }
        };
        match def.schema_type.to_ascii_uppercase().as_str() {
            "AVRO" => {
                let parsed = self
                    .avro
                    .entry(id)
                    .or_insert_with(|| parse_avro_with_references(&def));
                Some(match parsed {
                    Ok((schema, refs)) => {
                        match crate::codec::avro::decode_with(schema, refs, &wire.payload) {
                            Ok(value) => DecodedPayload {
                                text: value.to_string(),
                                label: format!("AVRO schema {id}"),
                                schema_ref: Some(SchemaRef::id(id)),
                            },
                            Err(e) => DecodedPayload {
                                text: text_body(),
                                label: format!("AVRO schema {id} (decode failed: {e})"),
                                schema_ref: Some(SchemaRef::id(id)),
                            },
                        }
                    }
                    Err(e) => DecodedPayload {
                        text: text_body(),
                        label: format!("AVRO schema {id} (unusable schema: {e})"),
                        schema_ref: Some(SchemaRef::id(id)),
                    },
                })
            }
            "JSON" => Some(DecodedPayload {
                text: text_body(),
                label: format!("JSON schema {id}"),
                schema_ref: Some(SchemaRef::id(id)),
            }),
            "PROTOBUF" => {
                let parsed = self.proto.entry(id).or_insert_with(|| {
                    crate::codec::proto_schema::ProtoSchema::parse_with_imports(
                        &def.schema,
                        &def.resolved_references,
                    )
                });
                let typed = match parsed {
                    Ok(schema) => crate::codec::proto_schema::read_message_indexes(&wire.payload)
                        .and_then(|(indexes, body)| {
                            let value = schema.decode(&indexes, body)?;
                            let name = schema.message_name(&indexes).unwrap_or_default();
                            Ok((value, name))
                        })
                        .map_err(|e| e.to_string()),
                    Err(e) => Err(format!("unusable schema: {e}")),
                };
                Some(match typed {
                    Ok((value, name)) => DecodedPayload {
                        text: value.to_string(),
                        label: format!("PROTOBUF schema {id} ({name})"),
                        schema_ref: Some(SchemaRef {
                            id,
                            message: Some(name).filter(|n| !n.is_empty()),
                        }),
                    },
                    // Fall back to field numbers.
                    Err(reason) => {
                        match crate::codec::protobuf::skip_message_indexes(&wire.payload)
                            .and_then(crate::codec::protobuf::decode_raw)
                        {
                            Ok(value) => DecodedPayload {
                                text: value.to_string(),
                                label: format!("PROTOBUF schema {id} (fields by number: {reason})"),
                                schema_ref: Some(SchemaRef::id(id)),
                            },
                            Err(e) => DecodedPayload {
                                text: text_body(),
                                label: format!("PROTOBUF schema {id} (decode failed: {e})"),
                                schema_ref: Some(SchemaRef::id(id)),
                            },
                        }
                    }
                })
            }
            other => Some(DecodedPayload {
                text: text_body(),
                label: format!("{other} schema {id} (not decoded)"),
                schema_ref: Some(SchemaRef::id(id)),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::schema::SchemaReference;

    /// A Schema Registry stand-in answering GETs from `routes` (path, body).
    async fn mock_registry(routes: Vec<(&'static str, String)>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let text = String::from_utf8_lossy(&request);
                let path = text.split_whitespace().nth(1).unwrap_or("");
                let (status, body) = match routes.iter().find(|(p, _)| *p == path) {
                    Some((_, body)) => ("200 OK", body.clone()),
                    None => (
                        "404 Not Found",
                        r#"{"error_code":40401,"message":"not found"}"#.to_string(),
                    ),
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn json_schema_ref_to_another_subject_is_validated() {
        let money = r#"{"type":"object","required":["amount"],"properties":{"amount":{"type":"number","minimum":0}}}"#;
        let order = r#"{"type":"object","properties":{"total":{"$ref":"money.json"}}}"#;
        let url = mock_registry(vec![
            (
                "/subjects/orders-value/versions/latest",
                serde_json::json!({
                    "subject": "orders-value", "version": 1, "id": 7,
                    "schemaType": "JSON", "schema": order,
                    "references": [{"name": "money.json", "subject": "money", "version": 2}]
                })
                .to_string(),
            ),
            (
                "/subjects/money/versions/2",
                serde_json::json!({"subject": "money", "version": 2, "id": 6, "schemaType": "JSON", "schema": money})
                    .to_string(),
            ),
        ])
        .await;
        let client = SchemaRegistryClient::new(url);
        let def = client
            .get_latest_subject_schema("orders-value")
            .await
            .unwrap();
        assert_eq!(def.resolved_reference_names, vec!["money.json".to_string()]);
        assert!(encode_with_schema(&def, r#"{"total":{"amount":5}}"#).is_ok());
        let err = encode_with_schema(&def, r#"{"total":{"amount":-5}}"#).unwrap_err();
        assert!(err.to_string().contains("$.total.amount"), "{err}");
    }

    #[tokio::test]
    async fn own_compatibility_tells_inherited_from_set() {
        let url = mock_registry(vec![
            (
                "/config/orders-value?defaultToGlobal=false",
                r#"{"compatibilityLevel":"FULL"}"#.to_string(),
            ),
            (
                "/subjects/orders-value/versions",
                "[1,2]".to_string(),
            ),
            (
                "/subjects/orders-value/versions/1",
                r#"{"subject":"orders-value","version":1,"id":5,"schema":"\"string\""}"#
                    .to_string(),
            ),
            (
                "/subjects/orders-value/versions/2",
                r#"{"subject":"orders-value","version":2,"id":9,"schemaType":"JSON","schema":"{}"}"#
                    .to_string(),
            ),
        ])
        .await;
        let client = SchemaRegistryClient::new(url);
        assert_eq!(
            client
                .subject_compatibility_own("orders-value")
                .await
                .unwrap(),
            Some("FULL".to_string())
        );
        // 404: no level of its own.
        assert_eq!(
            client.subject_compatibility_own("other").await.unwrap(),
            None
        );
        let versions = client
            .subject_versions_with_schemas("orders-value")
            .await
            .unwrap();
        let ids: Vec<u32> = versions.iter().map(|v| v.id).collect();
        assert_eq!(ids, vec![5, 9]);
        assert!(client
            .subject_versions_with_schemas("missing")
            .await
            .unwrap()
            .is_empty());
    }

    #[test]
    fn re_registration_body_keeps_type_and_references() {
        let def = SchemaDefinition {
            id: 3,
            schema: "{}".into(),
            schema_type: "JSON".into(),
            version: Some(1),
            subject: Some("s".into()),
            references: vec![SchemaReference {
                name: "m.json".into(),
                subject: "m".into(),
                version: 2,
            }],
            resolved_references: vec![],
            resolved_reference_names: vec![],
        };
        let body: serde_json::Value = serde_json::from_str(&definition_body(&def)).unwrap();
        assert_eq!(body["schemaType"], "JSON");
        assert_eq!(body["references"][0]["subject"], "m");
        let avro = SchemaDefinition {
            schema_type: "AVRO".into(),
            references: vec![],
            ..def
        };
        let body: serde_json::Value = serde_json::from_str(&definition_body(&avro)).unwrap();
        assert!(body.get("schemaType").is_none());
        assert!(body.get("references").is_none());
    }

    #[tokio::test]
    async fn test_schema_registry_client_cache() {
        let client = SchemaRegistryClient::new("http://127.0.0.1:8081").with_cached_schema(
            SchemaDefinition {
                id: 100,
                schema: r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"}]}"#
                    .to_string(),
                schema_type: "AVRO".to_string(),
                version: Some(1),
                subject: Some("users-value".to_string()),
                references: Vec::new(),
                resolved_references: Vec::new(),
                resolved_reference_names: Vec::new(),
            },
        );

        let def = client
            .get_schema_by_id(100)
            .await
            .expect("Cache hit expected");
        assert_eq!(def.id, 100);
        assert_eq!(def.schema_type, "AVRO");
        assert!(def.schema.contains("User"));
    }

    #[test]
    fn subjects_are_encoded_as_one_path_segment() {
        assert_eq!(path_segment("orders-value"), "orders-value");
        assert_eq!(path_segment("common/money.proto"), "common%2Fmoney.proto");
        assert_eq!(path_segment(".."), "%2E%2E");
        assert_eq!(path_segment("."), "%2E");
        assert_eq!(path_segment("a..b"), "a..b");
    }
}

#[cfg(test)]
mod decoder_tests {
    use super::*;
    use crate::codec::schema::encode_confluent_wire_format;

    #[tokio::test]
    async fn payload_decoder_renders_avro_from_a_cached_schema() {
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
        let mut decoder = PayloadDecoder {
            registry: Some(SchemaRegistryClient::new("http://unused").with_cached_schema(def)),
            avro: HashMap::new(),
            proto: HashMap::new(),
        };
        // zig-zag 21 = 42
        let bytes = encode_confluent_wire_format(7, &[84]);
        let decoded = decoder.decode(&bytes).await.unwrap();
        assert_eq!(decoded.text, r#"{"n":42}"#);
        assert_eq!(decoded.label, "AVRO schema 7");
        assert_eq!(decoded.schema_ref, Some(SchemaRef::id(7)));
        assert!(decoder.decode(b"plain text").await.is_none());

        let proto = SchemaDefinition {
            id: 8,
            schema: "message M { int32 n = 1; }".into(),
            schema_type: "PROTOBUF".into(),
            version: None,
            subject: None,
            references: Vec::new(),
            resolved_references: Vec::new(),
            resolved_reference_names: Vec::new(),
        };
        let mut decoder = PayloadDecoder {
            registry: Some(SchemaRegistryClient::new("http://unused").with_cached_schema(proto)),
            avro: HashMap::new(),
            proto: HashMap::new(),
        };
        // Message index list [0], then field 1 = 42.
        let bytes = encode_confluent_wire_format(8, &[0x00, 0x08, 42]);
        let decoded = decoder.decode(&bytes).await.unwrap();
        assert_eq!(decoded.text, r#"{"n":42}"#);
        assert_eq!(decoded.label, "PROTOBUF schema 8 (M)");
        assert_eq!(
            decoded.schema_ref,
            Some(SchemaRef {
                id: 8,
                message: Some("M".into())
            })
        );

        let mut offline = PayloadDecoder::new(None);
        let json = encode_confluent_wire_format(3, br#"{"a":1}"#);
        let decoded = offline.decode(&json).await.unwrap();
        assert_eq!(decoded.text, r#"{"a":1}"#);
        assert!(decoded.label.contains("no Schema Registry"));
        assert_eq!(decoded.schema_ref, None);
    }
}
