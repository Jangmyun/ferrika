//! Connection presets for `ferrika profile add --preset`: the security
//! settings that Kafka-compatible brokers and managed Kafka services expect.
//! A preset only fills in what the command line leaves out; explicit flags
//! always win. Credentials are never part of a preset.

/// A known broker or managed service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    /// The name `--preset` takes.
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

    /// Parses a preset name (case-insensitive; `_` is read as `-`).
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let wanted = text.trim().to_lowercase().replace('_', "-");
        Self::ALL
            .into_iter()
            .find(|p| p.name() == wanted)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Unknown preset '{text}'. Valid presets: {}",
                    Self::ALL.map(Preset::name).join(", ")
                )
            })
    }

    /// The `--security-protocol` value this preset implies.
    pub fn security_protocol(self) -> &'static str {
        match self {
            Preset::ConfluentCloud | Preset::MskIam | Preset::MskScram | Preset::RedpandaCloud => {
                "sasl_ssl"
            }
            Preset::WarpStream | Preset::AutoMq => "plaintext",
        }
    }

    /// The `--sasl-mechanism` value this preset implies, if any.
    pub fn sasl_mechanism(self) -> Option<&'static str> {
        match self {
            Preset::ConfluentCloud => Some("PLAIN"),
            Preset::MskIam => Some("AWS-MSK-IAM"),
            Preset::MskScram => Some("SCRAM-SHA-512"),
            Preset::RedpandaCloud => Some("SCRAM-SHA-256"),
            Preset::WarpStream | Preset::AutoMq => None,
        }
    }

    /// What the user still has to give for the preset's own settings: the
    /// SASL username and password, named the way the service names them.
    fn credentials_hint(self) -> Option<&'static str> {
        match self {
            Preset::ConfluentCloud => Some(
                "Confluent Cloud expects an API key as --sasl-username and its secret as the password (--sasl-password-stdin)",
            ),
            Preset::MskScram => Some(
                "MSK SCRAM expects the user and password stored in AWS Secrets Manager: --sasl-username and --sasl-password-stdin",
            ),
            Preset::RedpandaCloud => Some(
                "Redpanda Cloud expects a SCRAM user: --sasl-username and --sasl-password-stdin",
            ),
            Preset::MskIam | Preset::WarpStream | Preset::AutoMq => None,
        }
    }

    /// Notes printed after the profile is saved: broker-specific settings a
    /// preset cannot fill in.
    pub fn notes(self) -> &'static [&'static str] {
        match self {
            Preset::ConfluentCloud => &[
                "Use the bootstrap endpoint from the cluster settings (port 9092).",
                "Confluent Cloud manages broker settings itself, so broker-level changes (such as `lint --fix` on broker configs) are refused by the service.",
            ],
            Preset::MskIam => &[
                "MSK serves IAM clients on port 9098 (public access: 9198). AWS credentials come from the usual AWS chain; --aws-region / --aws-profile override it.",
                "IAM authentication goes through librdkafka's OAUTHBEARER, which needs a Ferrika build with TLS support (`--features ssl`).",
            ],
            Preset::MskScram => &[
                "MSK serves SASL/SCRAM clients on port 9096 (public access: 9196).",
                "MSK keeps SCRAM users in AWS Secrets Manager, so `ferrika user` cannot change them.",
            ],
            Preset::RedpandaCloud => &[
                "Use the Kafka API bootstrap URL from the cluster overview.",
            ],
            Preset::WarpStream => &[
                "WarpStream agents listen on port 9092. Add --security-protocol and --sasl-mechanism when the agents require TLS or SASL.",
                "WarpStream has no partition replicas on brokers: reassignment, leader election and replication changes do not apply.",
            ],
            Preset::AutoMq => &["AutoMQ uses the same client settings as Apache Kafka."],
        }
    }
}

/// The security settings to use after applying `preset`, and which of them
/// the preset filled in (`(label, value)`, for the success output).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub filled: Vec<(&'static str, String)>,
}

/// Applies `preset` under the explicit `--security-protocol` and
/// `--sasl-mechanism` values: a given flag wins over the preset.
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

/// Warnings for settings the preset's service needs that are missing.
pub fn missing_settings(
    preset: Preset,
    has_username: bool,
    has_password: bool,
    schema_registry: Option<&str>,
) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(hint) = preset.credentials_hint() {
        if !has_username || !has_password {
            out.push(hint.to_string());
        }
    }
    if preset == Preset::ConfluentCloud {
        if let Some(url) = schema_registry {
            if !url_has_credentials(url) {
                out.push(
                    "Confluent Cloud Schema Registry needs its own API key: --schema-registry-user and --schema-registry-password-stdin".to_string(),
                );
            }
        }
    }
    out
}

/// Whether `url` carries `user@` or `user:password@` before its host.
fn url_has_credentials(url: &str) -> bool {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority_end = rest.find('/').unwrap_or(rest.len());
    rest[..authority_end].contains('@')
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
    fn preset_fills_only_what_is_not_given() {
        let r = resolve(Some(Preset::ConfluentCloud), None, None);
        assert_eq!(r.security_protocol, "sasl_ssl");
        assert_eq!(r.sasl_mechanism.as_deref(), Some("PLAIN"));
        assert_eq!(r.filled.len(), 2);

        // Explicit flags win.
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
            missing_settings(Preset::ConfluentCloud, false, false, None).len(),
            1
        );
        assert!(missing_settings(Preset::ConfluentCloud, true, true, None).is_empty());
        assert!(missing_settings(Preset::MskIam, false, false, None).is_empty());
        let sr = missing_settings(
            Preset::ConfluentCloud,
            true,
            true,
            Some("https://psrc-1.us-east-1.aws.confluent.cloud"),
        );
        assert_eq!(sr.len(), 1);
        assert!(sr[0].contains("Schema Registry"), "{sr:?}");
        assert!(missing_settings(
            Preset::ConfluentCloud,
            true,
            true,
            Some("https://KEY:SECRET@psrc-1.us-east-1.aws.confluent.cloud")
        )
        .is_empty());
    }
}
