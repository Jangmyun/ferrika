//! Errors of the RabbitMQ management client.

/// Why a management API call failed. Messages never contain credentials.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RabbitError {
    /// A bad URL, name or argument, found before any request.
    #[error("RabbitMQ configuration error: {0}")]
    Config(String),
    /// The management API could not be reached (connect, TLS, timeout).
    #[error("Cannot reach the RabbitMQ management API at {target}: {reason}")]
    Unreachable { target: String, reason: String },
    /// The API answered with an error status.
    #[error("RabbitMQ management API error ({status}): {message}")]
    Api { status: u16, message: String },
    /// The API answered with a body this client does not understand.
    #[error("Unexpected RabbitMQ management API response: {0}")]
    Response(String),
}

impl RabbitError {
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }
}

pub type RabbitResult<T> = Result<T, RabbitError>;
