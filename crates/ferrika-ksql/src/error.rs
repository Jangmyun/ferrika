//! Errors of the ksqlDB client.

/// Why a ksqlDB call failed. Messages never contain credentials.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KsqlError {
    /// A bad URL, TLS file or argument, found before any request.
    #[error("ksqlDB configuration error: {0}")]
    Config(String),
    /// A statement the client cannot split or classify (an unterminated
    /// string literal or comment, a script with no statement).
    #[error("ksqlDB statement error: {0}")]
    Statement(String),
    /// The server could not be reached (connect, TLS, timeout).
    #[error("Cannot reach the ksqlDB server at {target}: {reason}")]
    Unreachable { target: String, reason: String },
    /// The server answered with an error (`statement_error`,
    /// `generic_error`): HTTP status, ksqlDB error code, message.
    #[error("{}", api_message(*status, *code, message))]
    Api {
        status: u16,
        code: Option<i64>,
        message: String,
    },
    /// The server answered with something this client does not understand.
    #[error("Unexpected ksqlDB response: {0}")]
    Response(String),
}

fn api_message(status: u16, code: Option<i64>, message: &str) -> String {
    match code {
        Some(code) => format!("ksqlDB error {code} (HTTP {status}): {message}"),
        None => format!("ksqlDB error (HTTP {status}): {message}"),
    }
}

impl KsqlError {
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }

    /// A short, fixed description for audit entries: the kind of failure
    /// and the ksqlDB error code, never the server's message (it can quote
    /// the statement, and with it record values) or a credential.
    pub fn audit_reason(&self) -> String {
        match self {
            Self::Config(_) => "failed: configuration error".to_string(),
            Self::Statement(_) => "failed: statement error".to_string(),
            Self::Unreachable { .. } => "failed: ksqlDB server unreachable".to_string(),
            Self::Api {
                status,
                code: Some(code),
                ..
            } => format!("failed: ksqlDB error {code} (HTTP {status})"),
            Self::Api {
                status, code: None, ..
            } => format!("failed: ksqlDB error (HTTP {status})"),
            Self::Response(_) => "failed: unexpected ksqlDB response".to_string(),
        }
    }
}

pub type KsqlResult<T> = Result<T, KsqlError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_reasons_never_quote_the_server_message() {
        let err = KsqlError::Api {
            status: 400,
            code: Some(40001),
            message: "Failed to prepare statement: 'jane@example.com'".into(),
        };
        assert!(err.to_string().contains("jane@example.com"));
        assert_eq!(err.audit_reason(), "failed: ksqlDB error 40001 (HTTP 400)");
        assert!(!KsqlError::Response("x secret".into())
            .audit_reason()
            .contains("secret"));
    }
}
