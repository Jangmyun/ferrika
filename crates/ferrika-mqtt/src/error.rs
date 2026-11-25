//! Errors of the MQTT client.

/// Why an MQTT operation failed. Messages never contain credentials or
/// message payloads.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MqttError {
    /// A bad URL, topic, name or argument, found before any connection.
    #[error("MQTT configuration error: {0}")]
    Config(String),
    /// The broker could not be reached (connect, TLS, network).
    #[error("Cannot reach the MQTT broker at {target}: {reason}")]
    Unreachable { target: String, reason: String },
    /// The broker refused the connection (CONNACK with an error code).
    #[error("The MQTT broker at {target} refused the connection: {reason}")]
    Refused { target: String, reason: String },
    /// The broker refused a subscription or a publish (SUBACK or PUBACK
    /// with an error code).
    #[error("The MQTT broker refused {what}: {reason}")]
    Rejected { what: String, reason: String },
    /// No answer from the broker in time.
    #[error("No answer from the MQTT broker within {seconds} s while {waiting_for}")]
    Timeout { seconds: u64, waiting_for: String },
    /// The connection broke or the broker closed it.
    #[error("The MQTT connection was lost: {0}")]
    Connection(String),
}

impl MqttError {
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }
}

pub type MqttResult<T> = Result<T, MqttError>;
