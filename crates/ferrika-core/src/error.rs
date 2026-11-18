use thiserror::Error;

/// Domain error types for Kafka operations in Ferrika.
#[derive(Error, Debug)]
pub enum KafkaError {
    #[error("Kafka configuration error: {0}")]
    Config(String),

    #[error("Kafka driver error: {0}")]
    Driver(#[from] DriverError),

    #[error("Kafka admin operation failed: {0}")]
    Admin(String),

    #[error("Topic '{0}' not found")]
    TopicNotFound(String),

    #[error("Partition {partition} not found in topic '{topic}'")]
    PartitionNotFound { topic: String, partition: i32 },

    #[error(
        "Failed to produce message to topic '{topic}'{}: {reason}",
        .partition.map(|p| format!(" (partition {p})")).unwrap_or_default()
    )]
    DeliveryFailed {
        topic: String,
        partition: Option<i32>,
        reason: String,
    },

    #[error("Consumer operation error: {0}")]
    Consumer(String),

    #[error("Kafka operation timed out: {0}")]
    Timeout(String),

    #[error("Offset manipulation error: {0}")]
    Offset(String),

    #[error("Consumer lag calculation error: {0}")]
    Lag(String),

    #[error("Serialization / Deserialization error: {0}")]
    Serialization(String),

    #[error("Profile management error: {0}")]
    Profile(String),

    #[error("Schema Registry error: {0}")]
    Schema(String),

    #[error("ACL / Security authorization error: {0}")]
    Acl(String),

    #[error("I/O error: {0}")]
    Io(String),

    /// A local file could not be read or written.
    #[error("File error: {0}")]
    File(String),

    /// A check the user asked for failed (SLA, PII gate, schema
    /// compatibility) or a batch partly failed; the message says what.
    #[error("{0}")]
    Check(String),

    /// The OS credential store (Windows Credential Manager, macOS
    /// Keychain, Linux Secret Service) refused a request; the message is
    /// the store's own.
    #[error("{0}")]
    SecretStore(String),

    #[error("Unexpected Kafka error: {0}")]
    Other(String),
}

/// What a broker or client error means for retrying it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// Kafka documents it as retriable: a leader election, a topic that
    /// was just created, a timeout or a dropped connection.
    Retriable,
    /// Not allowed (authorization or authentication): retrying cannot help.
    Denied,
    /// The topic or partition does not exist.
    Gone,
    Other,
}

impl ErrorClass {
    /// The class of an error code.
    pub fn of_code(code: ErrorCode) -> Self {
        use ErrorCode as Code;
        match code {
            Code::NotLeaderForPartition
            | Code::LeaderNotAvailable
            | Code::UnknownTopicOrPartition
            | Code::UnknownLeaderEpoch
            | Code::FencedLeaderEpoch
            | Code::OffsetNotAvailable
            | Code::RequestTimedOut
            | Code::NetworkException
            | Code::OperationTimedOut
            | Code::BrokerTransportFailure
            | Code::AllBrokersDown => Self::Retriable,
            Code::TopicAuthorizationFailed
            | Code::ClusterAuthorizationFailed
            | Code::GroupAuthorizationFailed
            | Code::SaslAuthenticationFailed
            | Code::Authentication => Self::Denied,
            Code::UnknownTopic | Code::UnknownPartition => Self::Gone,
            _ => Self::Other,
        }
    }

    /// The class of a librdkafka error. Admin operation results do not
    /// count (see [`DriverError::code`]).
    pub(crate) fn of_driver(error: &rdkafka::error::KafkaError) -> Self {
        error.rdkafka_error_code().map_or(Self::Other, |code| {
            Self::of_code(ErrorCode::from_driver(code))
        })
    }
}

macro_rules! error_codes {
    ($($name:ident),* $(,)?) => {
        /// A broker or librdkafka error code this crate tells apart. Codes
        /// it has no use for are [`ErrorCode::Other`]; the error's message
        /// still names them.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum ErrorCode {
            $($name,)*
            Other,
        }

        impl ErrorCode {
            fn from_driver(code: rdkafka::types::RDKafkaErrorCode) -> Self {
                use rdkafka::types::RDKafkaErrorCode as Code;
                match code {
                    $(Code::$name => Self::$name,)*
                    _ => Self::Other,
                }
            }
        }
    };
}

error_codes!(
    AllBrokersDown,
    Authentication,
    BrokerTransportFailure,
    ClusterAuthorizationFailed,
    FencedLeaderEpoch,
    GroupAuthorizationFailed,
    GroupIdNotFound,
    InvalidConfig,
    InvalidPartitions,
    InvalidReplicationFactor,
    LeaderNotAvailable,
    MessageSizeTooLarge,
    NetworkException,
    NonEmptyGroup,
    NotLeaderForPartition,
    OffsetNotAvailable,
    OperationTimedOut,
    PolicyViolation,
    RequestTimedOut,
    SaslAuthenticationFailed,
    TopicAlreadyExists,
    TopicAuthorizationFailed,
    TransactionalIdAuthorizationFailed,
    UnknownLeaderEpoch,
    UnknownPartition,
    UnknownTopic,
    UnknownTopicOrPartition,
);

/// An error reported by librdkafka. The driver's own error type is not part
/// of this crate's API: [`DriverError::code`] and
/// [`DriverError::is_client_config`] tell the cases apart, and `Display`
/// gives librdkafka's message.
#[derive(Debug)]
pub struct DriverError(rdkafka::error::KafkaError);

impl DriverError {
    /// The broker or librdkafka error code, including an admin operation's
    /// result; `None` when the error has none (a cancelled call, a bad
    /// client setting).
    pub fn code(&self) -> Option<ErrorCode> {
        match &self.0 {
            rdkafka::error::KafkaError::AdminOp(code) => Some(*code),
            other => other.rdkafka_error_code(),
        }
        .map(ErrorCode::from_driver)
    }

    /// librdkafka rejected a client setting (an unknown property or a bad
    /// value) before talking to a broker.
    pub fn is_client_config(&self) -> bool {
        matches!(self.0, rdkafka::error::KafkaError::ClientConfig(..))
    }
}

impl std::fmt::Display for DriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for DriverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl From<rdkafka::error::KafkaError> for KafkaError {
    fn from(e: rdkafka::error::KafkaError) -> Self {
        Self::Driver(DriverError(e))
    }
}

impl KafkaError {
    /// The class of this error (see [`ErrorClass`]); only driver errors
    /// carry a code.
    pub fn class(&self) -> ErrorClass {
        match self {
            Self::Driver(e) => ErrorClass::of_driver(&e.0),
            Self::TopicNotFound(_) | Self::PartitionNotFound { .. } => ErrorClass::Gone,
            Self::Acl(_) => ErrorClass::Denied,
            _ => ErrorClass::Other,
        }
    }

    /// Convenience helper to create a schema error.
    pub fn schema(msg: impl Into<String>) -> Self {
        Self::Schema(msg.into())
    }

    /// Convenience helper to create an ACL error.
    pub fn acl(msg: impl Into<String>) -> Self {
        Self::Acl(msg.into())
    }

    /// Convenience helper to create an I/O error.
    pub fn io(msg: impl Into<String>) -> Self {
        Self::Io(msg.into())
    }

    /// Convenience helper to create a profile error.
    pub fn profile(msg: impl Into<String>) -> Self {
        Self::Profile(msg.into())
    }

    /// Convenience helper to create a config error.
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Convenience helper to create an admin error.
    pub fn admin(msg: impl Into<String>) -> Self {
        Self::Admin(msg.into())
    }

    /// Convenience helper to create a consumer error.
    pub fn consumer(msg: impl Into<String>) -> Self {
        Self::Consumer(msg.into())
    }

    /// Convenience helper to create a lag error.
    pub fn lag(msg: impl Into<String>) -> Self {
        Self::Lag(msg.into())
    }

    /// Convenience helper to create a timeout error.
    pub fn timeout(msg: impl Into<String>) -> Self {
        Self::Timeout(msg.into())
    }
}

pub type KafkaResult<T> = Result<T, KafkaError>;

impl From<crate::codec::base64::DecodeError> for KafkaError {
    fn from(e: crate::codec::base64::DecodeError) -> Self {
        KafkaError::Serialization(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_constructors_and_display() {
        let err = KafkaError::config("invalid port");
        assert_eq!(err.to_string(), "Kafka configuration error: invalid port");

        let err = KafkaError::admin("broker unavailable");
        assert_eq!(
            err.to_string(),
            "Kafka admin operation failed: broker unavailable"
        );

        let err = KafkaError::consumer("rebalance in progress");
        assert_eq!(
            err.to_string(),
            "Consumer operation error: rebalance in progress"
        );

        let err = KafkaError::lag("negative watermark");
        assert_eq!(
            err.to_string(),
            "Consumer lag calculation error: negative watermark"
        );

        let err = KafkaError::timeout("deadline exceeded");
        assert_eq!(
            err.to_string(),
            "Kafka operation timed out: deadline exceeded"
        );

        let err = KafkaError::TopicNotFound("my-topic".into());
        assert_eq!(err.to_string(), "Topic 'my-topic' not found");

        let err = KafkaError::PartitionNotFound {
            topic: "events".into(),
            partition: 3,
        };
        assert_eq!(err.to_string(), "Partition 3 not found in topic 'events'");

        let err = KafkaError::DeliveryFailed {
            topic: "orders".into(),
            partition: Some(1),
            reason: "leader not available".into(),
        };
        assert!(err.to_string().contains("orders"));
        assert!(err.to_string().contains("leader not available"));
    }
}
