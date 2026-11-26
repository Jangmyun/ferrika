//! Kafka Connect REST client: list connectors with their status and config,
//! pause, resume, restart or stop them, and read, alter or reset their
//! offsets (KIP-875). Stopping and reading offsets need Kafka Connect 3.5 or
//! later, altering and resetting offsets 3.6 or later; older workers answer
//! with an error that says so.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::schema::path_segment;
pub use crate::model::connect::{all_sink, parse_offsets_json, ConnectorOffset, SinkOffset};

/// What an older Connect worker lacks, for the error it causes.
const NEEDS_STOP: &str = "Stopping a connector needs Kafka Connect 3.5 or later";
const NEEDS_OFFSETS: &str = "Reading connector offsets needs Kafka Connect 3.5 or later";
const NEEDS_ALTER: &str =
    "Altering or resetting connector offsets needs Kafka Connect 3.6 or later";

/// One connector with its state and tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorView {
    pub name: String,
    /// `source` or `sink`.
    pub kind: String,
    /// RUNNING, PAUSED, FAILED, UNASSIGNED, RESTARTING, STOPPED.
    pub state: String,
    pub worker: String,
    /// The stack trace of a failed connector (its tasks have their own).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<String>,
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

    /// Stops connector `name` (`PUT /connectors/{name}/stop`, Kafka
    /// Connect 3.5+): its tasks shut down and release their resources, but
    /// the config stays. `resume` starts it again; offsets can be altered
    /// or reset only while it is stopped.
    pub async fn stop(&self, name: &str) -> KafkaResult<()> {
        let response = self
            .send("PUT", &format!("{}/stop", connector_path(name)), None)
            .await?;
        feature_body(response, name, NEEDS_STOP).map(|_| ())
    }

    /// The offsets of connector `name` (`GET /connectors/{name}/offsets`,
    /// Kafka Connect 3.5+). A sink connector's are its consumer group's
    /// (see [`ConnectorOffset::as_sink`]); a source connector's are the
    /// maps it stores. Empty when it has none yet.
    pub async fn offsets(&self, name: &str) -> KafkaResult<Vec<ConnectorOffset>> {
        let response = self
            .send("GET", &format!("{}/offsets", connector_path(name)), None)
            .await?;
        parse_offsets_json(&feature_body(response, name, NEEDS_OFFSETS)?)
    }

    /// Sets the offsets of the partitions in `offsets` (`PATCH
    /// /connectors/{name}/offsets`, Kafka Connect 3.6+); an entry without
    /// an offset resets its partition. The connector must be STOPPED.
    /// Returns the worker's message.
    pub async fn alter_offsets(
        &self,
        name: &str,
        offsets: &[ConnectorOffset],
    ) -> KafkaResult<String> {
        let body = serde_json::to_string(&serde_json::json!({ "offsets": offsets }))
            .map_err(|e| err(format!("Invalid connector offsets: {e}")))?;
        let response = self
            .send(
                "PATCH",
                &format!("{}/offsets", connector_path(name)),
                Some(&body),
            )
            .await?;
        feature_body(response, name, NEEDS_ALTER).map(|body| response_message(&body))
    }

    /// Deletes every offset of connector `name` (`DELETE
    /// /connectors/{name}/offsets`, Kafka Connect 3.6+), so it starts over
    /// as if new. The connector must be STOPPED. Returns the worker's
    /// message.
    pub async fn reset_offsets(&self, name: &str) -> KafkaResult<String> {
        let response = self
            .send("DELETE", &format!("{}/offsets", connector_path(name)), None)
            .await?;
        feature_body(response, name, NEEDS_ALTER).map(|body| response_message(&body))
    }

    /// The state of connector `name` (RUNNING, PAUSED, STOPPED, ...), `None` when
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

/// The body of a 2xx answer to a request older workers do not know.
/// Connect answers 404 both for an unknown connector ("Connector x not
/// found") and for a path it does not have ("HTTP 404 Not Found"), and some
/// versions 405 for a method it does not take: those say the worker is too
/// old (`needs`). A 400 about the STOPPED state gets the command that stops
/// the connector.
fn feature_body(
    response: crate::kafka::http::Response,
    name: &str,
    needs: &str,
) -> KafkaResult<String> {
    if response.is_success() {
        return Ok(response.body);
    }
    let message = response.error_message();
    match response.status {
        404 if unknown_connector(&message, name) => {
            Err(err(format!("No connector named '{name}'")))
        }
        404 | 405 => Err(err(format!(
            "{needs} (the worker answered {}: {message})",
            response.status
        ))),
        400 | 409 if message.contains("STOPPED") => Err(err(format!(
            "Connector '{name}' must be STOPPED before its offsets can be changed; stop it with `ferrika connect stop {name}` (Kafka Connect said: {message})"
        ))),
        _ => success_body(response),
    }
}

/// Whether a 404 message names connector `name` as missing (rather than
/// the path being unknown to an older worker).
fn unknown_connector(message: &str, name: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("not found") && message.contains(name) && !lower.starts_with("http 404")
}

/// The `message` of a Connect answer, or the body itself.
fn response_message(body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("message").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_else(|| body.trim().to_string())
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
                trace: connector_status
                    .and_then(|c| c.get("trace"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
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
        assert_eq!(c.trace, None);
        let failed = parse_connectors(
            r#"{"c": {"info": {"type": "source", "config": {}},
                      "status": {"connector": {"state": "FAILED", "worker_id": "w", "trace": "oops"}, "tasks": []}}}"#,
        )
        .unwrap();
        assert_eq!(failed[0].trace.as_deref(), Some("oops"));
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

    /// One request the fake Connect worker received: method, path, body.
    type Seen = std::sync::Arc<std::sync::Mutex<Vec<(String, String, String)>>>;

    /// A Kafka Connect stand-in answering `(method, path)` from `routes`
    /// with `(status, body)`, anything else 404 "HTTP 404 Not Found" as a
    /// worker without the endpoint does. Returns its URL and the requests.
    async fn fake_connect(
        routes: Vec<(&'static str, &'static str, u16, String)>,
    ) -> (String, Seen) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen: Seen = Default::default();
        let log = seen.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = Vec::new();
                let mut buf = [0u8; 4096];
                let header_end = loop {
                    if let Some(i) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i + 4;
                    }
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => break request.len(),
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                };
                let head = String::from_utf8_lossy(&request[..header_end]).to_string();
                let length: usize = head
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse().unwrap_or(0))
                    })
                    .unwrap_or(0);
                while request.len() < header_end + length {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let body = String::from_utf8_lossy(&request[header_end..]).to_string();
                let mut words = head.split_whitespace();
                let method = words.next().unwrap_or("").to_string();
                let path = words.next().unwrap_or("").to_string();
                log.lock()
                    .unwrap()
                    .push((method.clone(), path.clone(), body));
                let (status, body) = routes
                    .iter()
                    .find(|(m, p, _, _)| *m == method && *p == path)
                    .map(|(_, _, s, b)| (*s, b.clone()))
                    .unwrap_or((
                        404,
                        r#"{"error_code":404,"message":"HTTP 404 Not Found"}"#.to_string(),
                    ));
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        (format!("http://{addr}"), seen)
    }

    #[tokio::test]
    async fn stop_and_offsets_use_the_kip_875_endpoints() {
        let offsets = r#"{"offsets":[
            {"partition":{"kafka_topic":"orders","kafka_partition":0},"offset":{"kafka_offset":7}},
            {"partition":{"kafka_topic":"orders","kafka_partition":1},"offset":{"kafka_offset":3}}]}"#;
        let ok = r#"{"message":"The offsets for this connector have been altered successfully"}"#;
        let (url, seen) = fake_connect(vec![
            ("PUT", "/connectors/sink%2F1/stop", 202, String::new()),
            (
                "GET",
                "/connectors/sink%2F1/offsets",
                200,
                offsets.to_string(),
            ),
            ("PATCH", "/connectors/sink%2F1/offsets", 200, ok.to_string()),
            (
                "DELETE",
                "/connectors/sink%2F1/offsets",
                200,
                r#"{"message":"reset"}"#.to_string(),
            ),
        ])
        .await;
        let client = ConnectClient::new(&url).unwrap();
        client.stop("sink/1").await.unwrap();
        let read = client.offsets("sink/1").await.unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(
            read[0].as_sink(),
            Some(SinkOffset {
                topic: "orders".into(),
                partition: 0,
                offset: Some(7)
            })
        );
        let message = client
            .alter_offsets(
                "sink/1",
                &[
                    ConnectorOffset::sink("orders", 0, Some(1)),
                    ConnectorOffset::sink("orders", 1, None),
                ],
            )
            .await
            .unwrap();
        assert!(message.contains("altered successfully"), "{message}");
        assert_eq!(client.reset_offsets("sink/1").await.unwrap(), "reset");
        let seen = seen.lock().unwrap();
        let patch = &seen[2];
        assert_eq!(patch.0, "PATCH");
        let body: Value = serde_json::from_str(&patch.2).unwrap();
        assert_eq!(body["offsets"][0]["offset"]["kafka_offset"], 1);
        assert_eq!(body["offsets"][1]["offset"], Value::Null);
        assert_eq!(body["offsets"][1]["partition"]["kafka_partition"], 1);
    }

    #[tokio::test]
    async fn older_workers_and_unknown_connectors_get_clear_errors() {
        let (url, _) = fake_connect(vec![
            (
                "GET",
                "/connectors/ghost/offsets",
                404,
                r#"{"error_code":404,"message":"Connector ghost not found"}"#.to_string(),
            ),
            (
                "PATCH",
                "/connectors/old/offsets",
                405,
                r#"{"error_code":405,"message":"HTTP 405 Method Not Allowed"}"#.to_string(),
            ),
            (
                "DELETE",
                "/connectors/running/offsets",
                400,
                r#"{"error_code":400,"message":"Connectors must be in the STOPPED state before their offsets can be modified."}"#.to_string(),
            ),
        ])
        .await;
        let client = ConnectClient::new(&url).unwrap();
        // A 3.4 worker has no /stop and no /offsets: 404 "HTTP 404 Not Found".
        let e = client.stop("old").await.unwrap_err().to_string();
        assert!(e.contains("needs Kafka Connect 3.5"), "{e}");
        let e = client.offsets("old").await.unwrap_err().to_string();
        assert!(e.contains("needs Kafka Connect 3.5"), "{e}");
        let e = client.reset_offsets("old").await.unwrap_err().to_string();
        assert!(e.contains("needs Kafka Connect 3.6"), "{e}");
        let e = client
            .alter_offsets("old", &[])
            .await
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("needs Kafka Connect 3.6") && e.contains("405"),
            "{e}"
        );
        let e = client.offsets("ghost").await.unwrap_err().to_string();
        assert!(e.contains("No connector named 'ghost'"), "{e}");
        let e = client
            .reset_offsets("running")
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("ferrika connect stop running"), "{e}");
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
