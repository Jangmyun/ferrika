//! Connection presets for `ferrika profile add --preset`. The presets and
//! their settings live in `ferrika_core::kafka::preset`, shared with other
//! front ends; this module keeps the texts that name the CLI's flags.

pub use crate::kafka::preset::{resolve, MissingSetting, Preset, Resolved};

/// What the user still has to give for the preset's own settings, named the
/// way the service names them and with the flags that take them.
fn credentials_hint(preset: Preset) -> Option<&'static str> {
    match preset {
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
/// preset cannot fill in, with the CLI's flags and commands
/// (`Preset::notes` has the same without them).
pub fn cli_notes(preset: Preset) -> &'static [&'static str] {
    match preset {
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

/// Warnings for settings the preset's service needs that are missing
/// (`ferrika_core::kafka::preset::missing_settings`), naming the flags.
pub fn missing_settings(
    preset: Preset,
    has_username: bool,
    has_password: bool,
    schema_registry: Option<&str>,
) -> Vec<String> {
    crate::kafka::preset::missing_settings(preset, has_username, has_password, schema_registry)
        .into_iter()
        .filter_map(|missing| match missing {
            MissingSetting::SaslCredentials => credentials_hint(preset).map(str::to_string),
            MissingSetting::SchemaRegistryCredentials => Some(
                "Confluent Cloud Schema Registry needs its own API key: --schema-registry-user and --schema-registry-password-stdin".to_string(),
            ),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_credentials_are_named_with_flags() {
        let warnings = missing_settings(Preset::ConfluentCloud, false, false, None);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("--sasl-username"), "{warnings:?}");
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

    #[test]
    fn every_preset_with_credentials_has_a_cli_hint() {
        for preset in Preset::ALL {
            assert_eq!(
                credentials_hint(preset).is_some(),
                preset.needs_sasl_credentials(),
                "{preset:?}"
            );
            assert_eq!(cli_notes(preset).len(), preset.notes().len(), "{preset:?}");
        }
    }
}
