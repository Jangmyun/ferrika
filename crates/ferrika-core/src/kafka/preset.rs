//! Connection presets: the security settings that Kafka-compatible brokers
//! and managed Kafka services expect, for any front end that creates
//! profiles (the CLI's `profile add --preset`, a GUI form, a library user).
//!
//! A preset only fills in what the user leaves out; explicit settings always
//! win (see [`resolve`]). Credentials are never part of a preset, but
//! [`Preset::credential_fields`] says which ones the service expects and
//! what it calls them, and [`missing_settings`] says which are still missing.
//! The texts here name no command-line flags, so every front end can show
//! them.
//!
//! ```
//! use ferrika_core::kafka::preset::{resolve, Preset};
//!
//! let preset = Preset::parse("confluent-cloud").unwrap();
//! assert_eq!(preset.display_name(), "Confluent Cloud");
//! let resolved = resolve(Some(preset), None, None);
//! assert_eq!(resolved.security_protocol, "sasl_ssl");
//! assert_eq!(resolved.sasl_mechanism.as_deref(), Some("PLAIN"));
//! assert_eq!(preset.credential_fields()[0].label, "API key");
//! ```

/// A known broker or managed service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Confluent Cloud: SASL_SSL + PLAIN with an API key and secret.
    ConfluentCloud,
    /// Amazon MSK with IAM access control: SASL_SSL + AWS-MSK-IAM.
    MskIam,
    /// Amazon MSK with SASL/SCRAM: SASL_SSL + SCRAM-SHA-512.
    MskScram,
    /// Redpanda Cloud: SASL_SSL + SCRAM-SHA-256.
    RedpandaCloud,
    /// WarpStream agents: plaintext unless the agents are set up for TLS/SASL.
    WarpStream,
    /// AutoMQ: the same settings as Apache Kafka (plaintext by default).
    AutoMq,
}

/// A preset name that is not one of [`Preset::ALL`]. Its message lists the
/// valid names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownPreset(pub String);

impl std::fmt::Display for UnknownPreset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Unknown preset '{}'. Valid presets: {}",
            self.0,
            Preset::ALL.map(Preset::name).join(", ")
        )
    }
}

impl std::error::Error for UnknownPreset {}

/// Which profile setting a credential goes into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CredentialKind {
    /// The SASL username.
    SaslUsername,
    /// The SASL password.
    SaslPassword,
    /// The Schema Registry basic-auth user.
    SchemaRegistryUser,
    /// The Schema Registry basic-auth password.
    SchemaRegistryPassword,
}

/// A credential a preset's service expects, named the way the service
/// names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialField {
    pub kind: CredentialKind,
    /// The service's name for it, e.g. `API key`.
    pub label: &'static str,
    /// Whether the value is a secret (show it masked, keep it in the OS
    /// credential store).
    pub secret: bool,
    /// Whether the connection needs it. Schema Registry credentials are
    /// needed only when a Schema Registry is configured.
    pub required: bool,
}

const fn field(
    kind: CredentialKind,
    label: &'static str,
    secret: bool,
    required: bool,
) -> CredentialField {
    CredentialField {
        kind,
        label,
        secret,
        required,
    }
}

static CONFLUENT_FIELDS: [CredentialField; 4] = [
    field(CredentialKind::SaslUsername, "API key", false, true),
    field(CredentialKind::SaslPassword, "API secret", true, true),
    field(
        CredentialKind::SchemaRegistryUser,
        "Schema Registry API key",
        false,
        false,
    ),
    field(
        CredentialKind::SchemaRegistryPassword,
        "Schema Registry API secret",
        true,
        false,
    ),
];

static SCRAM_FIELDS: [CredentialField; 2] = [
    field(CredentialKind::SaslUsername, "SCRAM username", false, true),
    field(CredentialKind::SaslPassword, "SCRAM password", true, true),
];

impl Preset {
    /// Every preset, in the order help and errors list them.
    pub const ALL: [Preset; 6] = [
        Preset::ConfluentCloud,
        Preset::MskIam,
        Preset::MskScram,
        Preset::RedpandaCloud,
        Preset::WarpStream,
        Preset::AutoMq,
    ];

    /// The preset's identifier, e.g. `confluent-cloud` (what the CLI's
    /// `--preset` takes and [`Self::parse`] reads).
    pub fn name(self) -> &'static str {
        match self {
            Preset::ConfluentCloud => "confluent-cloud",
            Preset::MskIam => "msk-iam",
            Preset::MskScram => "msk-scram",
            Preset::RedpandaCloud => "redpanda-cloud",
            Preset::WarpStream => "warpstream",
            Preset::AutoMq => "automq",
        }
    }

    /// The name to show people, e.g. `Confluent Cloud`.
    pub fn display_name(self) -> &'static str {
        match self {
            Preset::ConfluentCloud => "Confluent Cloud",
            Preset::MskIam => "Amazon MSK (IAM)",
            Preset::MskScram => "Amazon MSK (SASL/SCRAM)",
            Preset::RedpandaCloud => "Redpanda Cloud",
            Preset::WarpStream => "WarpStream",
            Preset::AutoMq => "AutoMQ",
        }
    }

    /// Parses a preset name (case-insensitive; `_` is read as `-`).
    pub fn parse(text: &str) -> Result<Self, UnknownPreset> {
        let wanted = text.trim().to_lowercase().replace('_', "-");
        Self::ALL
            .into_iter()
            .find(|p| p.name() == wanted)
            .ok_or_else(|| UnknownPreset(text.to_string()))
    }

    /// The security protocol this preset implies (`sasl_ssl`, `plaintext`).
    pub fn security_protocol(self) -> &'static str {
        match self {
            Preset::ConfluentCloud | Preset::MskIam | Preset::MskScram | Preset::RedpandaCloud => {
                "sasl_ssl"
            }
            Preset::WarpStream | Preset::AutoMq => "plaintext",
        }
    }

    /// The SASL mechanism this preset implies, if any.
    pub fn sasl_mechanism(self) -> Option<&'static str> {
        match self {
            Preset::ConfluentCloud => Some("PLAIN"),
            Preset::MskIam => Some("AWS-MSK-IAM"),
            Preset::MskScram => Some("SCRAM-SHA-512"),
            Preset::RedpandaCloud => Some("SCRAM-SHA-256"),
            Preset::WarpStream | Preset::AutoMq => None,
        }
    }

    /// The credentials the service expects, with the service's names for
    /// them: a form can label its fields with these. Empty when the preset
    /// needs none (MSK IAM takes AWS credentials from the environment).
    pub fn credential_fields(self) -> &'static [CredentialField] {
        match self {
            Preset::ConfluentCloud => &CONFLUENT_FIELDS,
            Preset::MskScram | Preset::RedpandaCloud => &SCRAM_FIELDS,
            Preset::MskIam | Preset::WarpStream | Preset::AutoMq => &[],
        }
    }

    /// Whether the preset needs a SASL username and password.
    pub fn needs_sasl_credentials(self) -> bool {
        self.credential_fields()
            .iter()
            .any(|f| f.required && f.kind == CredentialKind::SaslUsername)
    }

    /// What the service expects as SASL credentials, in words that name no
    /// command-line flag; `None` when it needs none.
    pub fn credentials_hint(self) -> Option<&'static str> {
        match self {
            Preset::ConfluentCloud => Some(
                "Confluent Cloud expects an API key as the SASL username and its secret as the password.",
            ),
            Preset::MskScram => Some(
                "MSK SCRAM expects the user and password stored in AWS Secrets Manager.",
            ),
            Preset::RedpandaCloud => Some("Redpanda Cloud expects a SCRAM user and its password."),
            Preset::MskIam | Preset::WarpStream | Preset::AutoMq => None,
        }
    }

    /// Broker-specific settings a preset cannot fill in, in words that name
    /// no command-line flag (for any front end).
    pub fn notes(self) -> &'static [&'static str] {
        match self {
            Preset::ConfluentCloud => &[
                "Use the bootstrap endpoint from the cluster settings (port 9092).",
                "Confluent Cloud manages broker settings itself, so broker-level config changes are refused by the service.",
            ],
            Preset::MskIam => &[
                "MSK serves IAM clients on port 9098 (public access: 9198). AWS credentials come from the default AWS credential chain; an AWS region and profile can be set to override it.",
                "IAM authentication goes through librdkafka's OAUTHBEARER, which needs a Ferrika build with TLS support.",
            ],
            Preset::MskScram => &[
                "MSK serves SASL/SCRAM clients on port 9096 (public access: 9196).",
                "MSK keeps SCRAM users in AWS Secrets Manager, so they cannot be changed through Kafka's SCRAM user API.",
            ],
            Preset::RedpandaCloud => &[
                "Use the Kafka API bootstrap URL from the cluster overview.",
            ],
            Preset::WarpStream => &[
                "WarpStream agents listen on port 9092. Set a security protocol and SASL mechanism when the agents require TLS or SASL.",
                "WarpStream has no partition replicas on brokers: reassignment, leader election and replication changes do not apply.",
            ],
            Preset::AutoMq => &["AutoMQ uses the same client settings as Apache Kafka."],
        }
    }
}

/// The security settings to use after applying a preset, and which of them
/// the preset filled in (`(label, value)`, for a success message).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub filled: Vec<(&'static str, String)>,
}

/// Applies `preset` under the explicit security protocol and SASL mechanism:
/// a given value wins over the preset's. Without either, the protocol is
/// `plaintext` and there is no mechanism.
pub fn resolve(
    preset: Option<Preset>,
    security_protocol: Option<&str>,
    sasl_mechanism: Option<&str>,
) -> Resolved {
    let mut filled = Vec::new();
    let security_protocol = match (security_protocol, preset) {
        (Some(given), _) => given.to_string(),
        (None, Some(p)) => {
            filled.push(("security protocol", p.security_protocol().to_string()));
            p.security_protocol().to_string()
        }
        (None, None) => "plaintext".to_string(),
    };
    let sasl_mechanism = match (sasl_mechanism, preset.and_then(Preset::sasl_mechanism)) {
        (Some(given), _) => Some(given.to_string()),
        (None, Some(m)) => {
            filled.push(("SASL mechanism", m.to_string()));
            Some(m.to_string())
        }
        (None, None) => None,
    };
    Resolved {
        security_protocol,
        sasl_mechanism,
        filled,
    }
}

/// A setting a preset's service needs that the profile lacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MissingSetting {
    /// The SASL username or password (see [`Preset::credential_fields`]).
    SaslCredentials,
    /// Credentials for the configured Schema Registry (Confluent Cloud
    /// Schema Registry needs its own API key).
    SchemaRegistryCredentials,
}

/// The settings `preset`'s service needs that are missing: SASL credentials
/// when the preset needs them and either part is absent, and for Confluent
/// Cloud, Schema Registry credentials when `schema_registry` (the URL, which
/// may carry `user:password@`) has none.
pub fn missing_settings(
    preset: Preset,
    has_username: bool,
    has_password: bool,
    schema_registry: Option<&str>,
) -> Vec<MissingSetting> {
    let mut out = Vec::new();
    if preset.needs_sasl_credentials() && (!has_username || !has_password) {
        out.push(MissingSetting::SaslCredentials);
    }
    if preset == Preset::ConfluentCloud {
        if let Some(url) = schema_registry {
            if !crate::kafka::profile::url_has_credentials(url) {
                out.push(MissingSetting::SchemaRegistryCredentials);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_preset_parses_by_its_name() {
        for preset in Preset::ALL {
            assert_eq!(Preset::parse(preset.name()).unwrap(), preset);
        }
        assert_eq!(
            Preset::parse("Confluent_Cloud").unwrap(),
            Preset::ConfluentCloud
        );
    }

    #[test]
    fn unknown_preset_lists_the_valid_ones() {
        let err = Preset::parse("kinesis").unwrap_err().to_string();
        assert!(err.contains("'kinesis'"), "{err}");
        for preset in Preset::ALL {
            assert!(err.contains(preset.name()), "{err}");
        }
    }

    #[test]
    fn display_names_are_distinct() {
        let names: std::collections::HashSet<_> =
            Preset::ALL.iter().map(|p| p.display_name()).collect();
        assert_eq!(names.len(), Preset::ALL.len());
        assert_eq!(Preset::MskIam.display_name(), "Amazon MSK (IAM)");
    }

    #[test]
    fn credential_fields_follow_the_mechanism() {
        for preset in Preset::ALL {
            let fields = preset.credential_fields();
            // Presets without a password-based mechanism ask for nothing.
            let password_based = matches!(
                preset.sasl_mechanism(),
                Some("PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512")
            );
            assert_eq!(
                preset.needs_sasl_credentials(),
                password_based,
                "{preset:?}"
            );
            assert_eq!(preset.credentials_hint().is_some(), password_based);
            assert!(fields.iter().all(|f| f.secret
                == matches!(
                    f.kind,
                    CredentialKind::SaslPassword | CredentialKind::SchemaRegistryPassword
                )));
        }
        let confluent = Preset::ConfluentCloud.credential_fields();
        assert_eq!(confluent[0].kind, CredentialKind::SaslUsername);
        assert_eq!(confluent[1].label, "API secret");
    }

    #[test]
    fn notes_name_no_flags() {
        for preset in Preset::ALL {
            for text in preset
                .notes()
                .iter()
                .copied()
                .chain(preset.credentials_hint())
            {
                assert!(!text.contains("--"), "{preset:?}: {text}");
                assert!(!text.contains('`'), "{preset:?}: {text}");
            }
        }
    }

    #[test]
    fn preset_fills_only_what_is_not_given() {
        let r = resolve(Some(Preset::ConfluentCloud), None, None);
        assert_eq!(r.security_protocol, "sasl_ssl");
        assert_eq!(r.sasl_mechanism.as_deref(), Some("PLAIN"));
        assert_eq!(r.filled.len(), 2);

        // Explicit values win.
        let r = resolve(
            Some(Preset::MskScram),
            Some("sasl_plaintext"),
            Some("PLAIN"),
        );
        assert_eq!(r.security_protocol, "sasl_plaintext");
        assert_eq!(r.sasl_mechanism.as_deref(), Some("PLAIN"));
        assert!(r.filled.is_empty());

        // No preset: the old default.
        let r = resolve(None, None, None);
        assert_eq!(r.security_protocol, "plaintext");
        assert_eq!(r.sasl_mechanism, None);
        assert!(r.filled.is_empty());

        // A preset without a mechanism leaves an explicit one alone.
        let r = resolve(Some(Preset::WarpStream), Some("sasl_ssl"), Some("PLAIN"));
        assert_eq!(r.sasl_mechanism.as_deref(), Some("PLAIN"));
    }

    #[test]
    fn presets_map_to_their_security_settings() {
        let expect = [
            (Preset::ConfluentCloud, "sasl_ssl", Some("PLAIN")),
            (Preset::MskIam, "sasl_ssl", Some("AWS-MSK-IAM")),
            (Preset::MskScram, "sasl_ssl", Some("SCRAM-SHA-512")),
            (Preset::RedpandaCloud, "sasl_ssl", Some("SCRAM-SHA-256")),
            (Preset::WarpStream, "plaintext", None),
            (Preset::AutoMq, "plaintext", None),
        ];
        for (preset, protocol, mechanism) in expect {
            assert_eq!(preset.security_protocol(), protocol, "{preset:?}");
            assert_eq!(preset.sasl_mechanism(), mechanism, "{preset:?}");
        }
    }

    #[test]
    fn missing_credentials_are_named() {
        assert_eq!(
            missing_settings(Preset::ConfluentCloud, false, false, None),
            vec![MissingSetting::SaslCredentials]
        );
        assert!(missing_settings(Preset::ConfluentCloud, true, true, None).is_empty());
        assert!(missing_settings(Preset::MskIam, false, false, None).is_empty());
        assert_eq!(
            missing_settings(
                Preset::ConfluentCloud,
                true,
                true,
                Some("https://psrc-1.us-east-1.aws.confluent.cloud"),
            ),
            vec![MissingSetting::SchemaRegistryCredentials]
        );
        assert!(missing_settings(
            Preset::ConfluentCloud,
            true,
            true,
            Some("https://KEY:SECRET@psrc-1.us-east-1.aws.confluent.cloud")
        )
        .is_empty());
    }
}
