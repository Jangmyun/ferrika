//! Kafka Connect REST client: list connectors with their status and config,
//! and pause, resume or restart them. Plain HTTP (no TLS), like the Schema
//! Registry client.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::schema::path_segment;

/// One connector with its state and tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorView {
    pub name: String,
    /// `source` or `sink`.
    pub kind: String,
    /// RUNNING, PAUSED, FAILED, UNASSIGNED, RESTARTING, STOPPED.
    pub state: String,
    pub worker: String,
    /// The `connector.class` config.
    pub class: String,
    pub config: BTreeMap<String, String>,
    pub tasks: Vec<TaskView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskView {
    pub id: i64,
    pub state: String,
    pub worker: String,
    /// The stack trace of a failed task.
    pub trace: Option<String>,
}

impl ConnectorView {
    pub fn running_tasks(&self) -> usize {
        self.tasks.iter().filter(|t| t.state == "RUNNING").count()
    }
}

/// What to do to a connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorAction {
    Pause,
    Resume,
    /// Restart the connector and its tasks.
    Restart,
    /// Restart one task (e.g. a failed one), leaving the others running.
    RestartTask(i64),
}

impl ConnectorAction {
    pub fn label(self) -> &'static str {
        match self {
            Self::Pause => "Pause",
            Self::Resume => "Resume",
            Self::Restart => "Restart",
            Self::RestartTask(_) => "Task restart",
        }
    }
}

/// `/connectors/{name}` with `name` percent-encoded, so a name holding `/`,
/// `..`, `?` or CR/LF cannot change the request path or inject headers.
fn connector_path(name: &str) -> String {
    format!("/connectors/{}", path_segment(name))
}

/// The method and path for `action` on connector `name`.
fn action_request(name: &str, action: ConnectorAction) -> (&'static str, String) {
    let base = connector_path(name);
    match action {
        ConnectorAction::Pause => ("PUT", format!("{base}/pause")),
        ConnectorAction::Resume => ("PUT", format!("{base}/resume")),
        ConnectorAction::Restart => (
            "POST",
            format!("{base}/restart?includeTasks=true&onlyFailed=false"),
        ),
        ConnectorAction::RestartTask(id) => ("POST", format!("{base}/tasks/{id}/restart")),
    }
}

pub struct ConnectClient {
    endpoint: crate::kafka::http::Endpoint,
}

fn err(msg: impl Into<String>) -> KafkaError {
    KafkaError::Admin(msg.into())
}

impl ConnectClient {
    /// `url` like `http://connect:8083` or `https://user:pass@connect`.
    pub fn new(url: &str) -> KafkaResult<Self> {
        Ok(Self {
            endpoint: crate::kafka::http::Endpoint::parse(url, 8083)?,
        })
    }

    /// Sets the time each request may take (see
    /// [`crate::kafka::http::Endpoint::with_timeout`]).
    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.endpoint = self.endpoint.with_timeout(timeout);
        self
    }

    /// Every connector with status and config (`?expand=status&expand=info`).
    pub async fn list(&self) -> KafkaResult<Vec<ConnectorView>> {
        let body = self
            .request("GET", "/connectors?expand=status&expand=info")
            .await?;
        parse_connectors(&body)
    }

    /// Creates connector `name` with `config`, or replaces the config of an
    /// existing one (PUT /connectors/{name}/config does both).
    pub async fn put_config(
        &self,
        name: &str,
        config: &BTreeMap<String, String>,
    ) -> KafkaResult<()> {
        let body = serde_json::to_string(config)
            .map_err(|e| err(format!("Invalid connector config: {e}")))?;
        self.request_with_body(
            "PUT",
            &format!("{}/config", connector_path(name)),
            Some(&body),
        )
        .await
        .map(|_| ())
    }

    /// Connector classes installed on the workers, as `(class, type, version)`.
    pub async fn plugins(&self) -> KafkaResult<Vec<(String, String, String)>> {
        let body = self.request("GET", "/connector-plugins").await?;
        parse_plugins(&body)
    }

    /// Asks the workers to check `config` against its connector class without
    /// creating anything. Returns one `"key: problem"` line per error (empty
    /// when the config is valid).
    pub async fn validate(&self, config: &BTreeMap<String, String>) -> KafkaResult<Vec<String>> {
        let class = config
            .get("connector.class")
            .ok_or_else(|| KafkaError::config("The config needs a connector.class"))?;
        let short = class.rsplit('.').next().unwrap_or(class);
        let body = serde_json::to_string(config)
            .map_err(|e| err(format!("Invalid connector config: {e}")))?;
        let response = self
            .request_with_body(
                "PUT",
                &format!("/connector-plugins/{}/config/validate", path_segment(short)),
                Some(&body),
            )
            .await?;
        parse_validation(&response)
    }

    pub async fn delete(&self, name: &str) -> KafkaResult<()> {
        self.request("DELETE", &connector_path(name))
            .await
            .map(|_| ())
    }

    pub async fn act(&self, name: &str, action: ConnectorAction) -> KafkaResult<()> {
        let (method, path) = action_request(name, action);
        self.request(method, &path).await.map(|_| ())
    }

    /// The config of connector `name`, `None` when there is no such connector.
    pub async fn get_config(&self, name: &str) -> KafkaResult<Option<BTreeMap<String, String>>> {
        let response = self
            .send("GET", &format!("{}/config", connector_path(name)), None)
            .await?;
        if response.status == 404 {
            return Ok(None);
        }
        parse_config_object(&success_body(response)?).map(Some)
    }

    /// The state of connector `name` (RUNNING, PAUSED, ...), `None` when
    /// there is no such connector.
    pub async fn status(&self, name: &str) -> KafkaResult<Option<String>> {
        let response = self
            .send("GET", &format!("{}/status", connector_path(name)), None)
            .await?;
        if response.status == 404 {
            return Ok(None);
        }
        parse_status_state(&success_body(response)?).map(Some)
    }

    async fn request(&self, method: &str, path: &str) -> KafkaResult<String> {
        self.request_with_body(method, path, None).await
    }

    async fn request_with_body(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> KafkaResult<String> {
        success_body(self.send(method, path, body).await?)
    }

    /// Sends one request and returns the response whatever its status.
    async fn send(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> KafkaResult<crate::kafka::http::Response> {
        self.endpoint
            .send(method, path, "application/json", body)
            .await
            .map_err(|e| {
                err(format!(
                    "Cannot reach Kafka Connect at {}: {e}",
                    self.endpoint.target()
                ))
            })
    }
}

/// The body of a 2xx response; anything else becomes an error.
fn success_body(response: crate::kafka::http::Response) -> KafkaResult<String> {
    if response.is_success() {
        Ok(response.body)
    } else {
        Err(err(format!(
            "Kafka Connect returned {}: {}",
            response.status,
            response.error_message()
        )))
    }
}

/// A `GET /connectors/{name}/config` answer, values stringified.
fn parse_config_object(body: &str) -> KafkaResult<BTreeMap<String, String>> {
    let json: Value = serde_json::from_str(body)
        .map_err(|e| err(format!("Unexpected connector config answer: {e}")))?;
    let object = json
        .as_object()
        .ok_or_else(|| err("Unexpected connector config answer: not an object"))?;
    Ok(object
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                v.as_str().map_or_else(|| v.to_string(), str::to_string),
            )
        })
        .collect())
}

/// The connector's state from a `GET /connectors/{name}/status` answer.
fn parse_status_state(body: &str) -> KafkaResult<String> {
    let json: Value = serde_json::from_str(body)
        .map_err(|e| err(format!("Unexpected connector status answer: {e}")))?;
    Ok(json
        .pointer("/connector/state")
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN")
        .to_string())
}

fn parse_plugins(body: &str) -> KafkaResult<Vec<(String, String, String)>> {
    let json: Value = serde_json::from_str(body)
        .map_err(|e| err(format!("Unexpected connector-plugins answer: {e}")))?;
    let field = |p: &Value, k: &str| p.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    Ok(json
        .as_array()
        .map(|plugins| {
            plugins
                .iter()
                .map(|p| (field(p, "class"), field(p, "type"), field(p, "version")))
                .collect()
        })
        .unwrap_or_default())
}

/// `"key: problem"` lines from a `config/validate` answer.
fn parse_validation(body: &str) -> KafkaResult<Vec<String>> {
    let json: Value = serde_json::from_str(body)
        .map_err(|e| err(format!("Unexpected validation answer: {e}")))?;
    let mut problems = Vec::new();
    for entry in json
        .get("configs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let value = entry.get("value").unwrap_or(&Value::Null);
        let name = value.get("name").and_then(Value::as_str).unwrap_or("?");
        for e in value
            .get("errors")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(e) = e.as_str() {
                problems.push(format!("{name}: {e}"));
            }
        }
    }
    let count = json.get("error_count").and_then(Value::as_u64).unwrap_or(0);
    if count > 0 && problems.is_empty() {
        problems.push(format!("{count} error(s) without details"));
    }
    Ok(problems)
}

/// Whether a connector config key likely holds a secret (passwords, tokens,
/// API keys); such values are masked when shown.
pub fn is_sensitive_key(key: &str) -> bool {
    // Connector keys are dotted (`api.key`, `ssl.key.password`); the shared
    // rule reads `.` as `_`. `sasl.jaas.config` embeds credentials inline.
    crate::codec::pii::is_sensitive_key(key) || key.to_ascii_lowercase().contains("jaas")
}

/// `value`, or a mask when `key` is sensitive.
pub fn display_value(key: &str, value: &str) -> String {
    if is_sensitive_key(key) && !value.is_empty() && !value.starts_with("${") {
        "********".to_string()
    } else {
        value.to_string()
    }
}

/// Parses a connector config from JSON: either the flat config object, or
/// `{"name": ..., "config": {...}}` as `POST /connectors` takes. Values are
/// stringified. Returns the name when the JSON carries one.
pub fn parse_config_json(text: &str) -> KafkaResult<(Option<String>, BTreeMap<String, String>)> {
    let json: Value = serde_json::from_str(text)
        .map_err(|e| KafkaError::config(format!("Connector config is not valid JSON: {e}")))?;
    let name = json.get("name").and_then(Value::as_str).map(str::to_string);
    let object = json
        .get("config")
        .and_then(Value::as_object)
        .or_else(|| json.as_object())
        .ok_or_else(|| KafkaError::config("Connector config must be a JSON object"))?;
    let config: BTreeMap<String, String> = object
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                v.as_str().map_or_else(|| v.to_string(), str::to_string),
            )
        })
        .collect();
    if !config.contains_key("connector.class") {
        return Err(KafkaError::config("The config needs a connector.class"));
    }
    Ok((name, config))
}

/// Parses the `expand=status&expand=info` listing.
fn parse_connectors(body: &str) -> KafkaResult<Vec<ConnectorView>> {
    let json: Value = serde_json::from_str(body)
        .map_err(|e| err(format!("Unexpected Kafka Connect response: {e}")))?;
    let Value::Object(map) = json else {
        return Err(err("Unexpected Kafka Connect response: not an object"));
    };
    let text = |v: Option<&Value>| v.and_then(Value::as_str).unwrap_or_default().to_string();
    let mut connectors: Vec<ConnectorView> = map
        .into_iter()
        .map(|(name, entry)| {
            let info = entry.get("info");
            let status = entry.get("status");
            let config: BTreeMap<String, String> = info
                .and_then(|i| i.get("config"))
                .and_then(Value::as_object)
                .map(|c| {
                    c.iter()
                        .map(|(k, v)| {
                            let value = v.as_str().map_or_else(|| v.to_string(), str::to_string);
                            (k.clone(), value)
                        })
                        .collect()
                })
                .unwrap_or_default();
            let tasks = status
                .and_then(|s| s.get("tasks"))
                .and_then(Value::as_array)
                .map(|tasks| {
                    tasks
                        .iter()
                        .map(|t| TaskView {
                            id: t.get("id").and_then(Value::as_i64).unwrap_or(-1),
                            state: text(t.get("state")),
                            worker: text(t.get("worker_id")),
                            trace: t.get("trace").and_then(Value::as_str).map(str::to_string),
                        })
                        .collect()
                })
                .unwrap_or_default();
            let connector_status = status.and_then(|s| s.get("connector"));
            ConnectorView {
                kind: text(info.and_then(|i| i.get("type")))
                    .chars()
                    .collect::<String>()
                    .to_lowercase(),
                state: text(connector_status.and_then(|c| c.get("state"))),
                worker: text(connector_status.and_then(|c| c.get("worker_id"))),
                class: config.get("connector.class").cloned().unwrap_or_default(),
                config,
                tasks,
                name,
            }
        })
        .collect();
    connectors.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(connectors)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sensitive_keys_union_shared_rule_and_connect_rules() {
        for k in [
            "connection.password",
            "aws.secret.access.key",
            "sasl.jaas.config",
            "api.key",
            "ssl.key.password",
            "my.auth-token",
            "db.pwd",
            "http.authorization",
        ] {
            assert!(is_sensitive_key(k), "{k}");
            assert_eq!(display_value(k, "v"), "********", "{k}");
        }
        for k in ["connector.class", "tasks.max", "topics", "passport.id"] {
            assert!(!is_sensitive_key(k), "{k}");
        }
    }

    #[test]
    fn connector_names_are_encoded_in_paths() {
        assert_eq!(connector_path("orders-sink"), "/connectors/orders-sink");
        assert_eq!(connector_path("a/../b"), "/connectors/a%2F..%2Fb");
        assert_eq!(
            connector_path("x\r\nHost: evil"),
            "/connectors/x%0D%0AHost%3A%20evil"
        );
        let (method, path) = action_request("a/b?c", ConnectorAction::RestartTask(2));
        assert_eq!(method, "POST");
        assert_eq!(path, "/connectors/a%2Fb%3Fc/tasks/2/restart");
    }

    #[test]
    fn parses_expanded_connector_listing() {
        let body = r#"{
          "orders-sink": {
            "info": {"name": "orders-sink", "type": "sink",
                     "config": {"connector.class": "io.confluent.JdbcSink", "tasks.max": "2", "topics": "orders"}},
            "status": {"name": "orders-sink",
                       "connector": {"state": "RUNNING", "worker_id": "w1:8083"},
                       "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "w1:8083"},
                                 {"id": 1, "state": "FAILED", "worker_id": "w2:8083", "trace": "boom"}],
                       "type": "sink"}
          }
        }"#;
        let list = parse_connectors(body).unwrap();
        assert_eq!(list.len(), 1);
        let c = &list[0];
        assert_eq!(c.kind, "sink");
        assert_eq!(c.state, "RUNNING");
        assert_eq!(c.class, "io.confluent.JdbcSink");
        assert_eq!(c.running_tasks(), 1);
        assert_eq!(c.tasks[1].trace.as_deref(), Some("boom"));
        assert_eq!(c.config.get("topics").map(String::as_str), Some("orders"));
    }

    #[test]
    fn parses_plugins_and_validation_answers() {
        let plugins =
            parse_plugins(r#"[{"class":"io.x.S3SinkConnector","type":"sink","version":"10.5"}]"#)
                .unwrap();
        assert_eq!(
            plugins,
            vec![(
                "io.x.S3SinkConnector".to_string(),
                "sink".to_string(),
                "10.5".to_string()
            )]
        );
        let problems = parse_validation(
            r#"{"name":"S3SinkConnector","error_count":1,"configs":[
                {"definition":{"name":"topics"},"value":{"name":"topics","errors":[]}},
                {"value":{"name":"s3.bucket.name","errors":["Missing required configuration"]}}
            ]}"#,
        )
        .unwrap();
        assert_eq!(
            problems,
            vec!["s3.bucket.name: Missing required configuration"]
        );
        assert!(parse_validation(r#"{"error_count":0,"configs":[]}"#)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn single_connector_answers_parse() {
        let config = parse_config_object(r#"{"connector.class":"X","tasks.max":2}"#).unwrap();
        assert_eq!(config.get("connector.class").map(String::as_str), Some("X"));
        assert_eq!(config.get("tasks.max").map(String::as_str), Some("2"));
        assert!(parse_config_object("[]").is_err());
        let state = parse_status_state(
            r#"{"name":"c","connector":{"state":"PAUSED","worker_id":"w"},"tasks":[]}"#,
        )
        .unwrap();
        assert_eq!(state, "PAUSED");
    }

    #[test]
    fn sensitive_values_are_masked_but_references_shown() {
        assert_eq!(display_value("connection.password", "hunter2"), "********");
        assert_eq!(display_value("aws.secret.access.key", "x"), "********");
        // Config-provider references are not secrets themselves.
        assert_eq!(
            display_value("connection.password", "${file:/secrets.properties:pw}"),
            "${file:/secrets.properties:pw}"
        );
        assert_eq!(
            display_value("key.converter", "StringConverter"),
            "StringConverter"
        );
        assert_eq!(display_value("topics", "a"), "a");
    }

    #[test]
    fn connector_config_json_accepts_flat_and_wrapped_forms() {
        let (name, config) = parse_config_json(
            r#"{"name": "s3", "config": {"connector.class": "S3Sink", "tasks.max": 2}}"#,
        )
        .unwrap();
        assert_eq!(name.as_deref(), Some("s3"));
        assert_eq!(config.get("tasks.max").map(String::as_str), Some("2"));
        let (name, config) =
            parse_config_json(r#"{"connector.class": "S3Sink", "topics": "a"}"#).unwrap();
        assert_eq!(name, None);
        assert_eq!(config.len(), 2);
        assert!(parse_config_json(r#"{"topics": "a"}"#).is_err());
        assert!(parse_config_json("not json").is_err());
    }

    #[test]
    fn connect_urls_parse_host_and_port() {
        let c = ConnectClient::new("http://connect:8084/").unwrap();
        assert_eq!(c.endpoint.target(), "connect:8084");
        let c = ConnectClient::new("localhost").unwrap();
        assert_eq!(c.endpoint.port, 8083);
        assert!(ConnectClient::new("https://secure").unwrap().endpoint.tls);
    }
}
