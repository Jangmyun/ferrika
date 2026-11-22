//! `ferrika quota`: client quotas (byte rates, request share) per user,
//! client id or IP.

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::{Cell, CellAlignment};

use crate::cli::record::{self, Change};
use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::wire;
use crate::model::quota::{quota_entity_label, QuotaEntity};

#[derive(Args, Debug, Clone)]
pub struct QuotaArgs {
    #[command(subcommand)]
    pub command: QuotaSubcommands,
}

/// Who a quota applies to. Give a name, or the `--*-default` flag for the
/// default quota of that kind (shown as `<default>` in listings).
#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct EntityArgs {
    /// User principal name (`--user default` is the user named "default";
    /// the default quota of all users is --user-default)
    #[arg(long, conflicts_with = "user_default")]
    pub user: Option<String>,
    /// The default user quota: applies to every user without its own
    #[arg(long)]
    pub user_default: bool,
    /// Client id (`--client-id default` is the client id "default"; the
    /// default quota of all client ids is --client-id-default)
    #[arg(long = "client-id", conflicts_with = "client_id_default")]
    pub client_id: Option<String>,
    /// The default client-id quota: applies to every client id without its
    /// own
    #[arg(long = "client-id-default")]
    pub client_id_default: bool,
    /// Client IP address (or `default`, same as --ip-default), for
    /// connection_creation_rate
    #[arg(long, conflicts_with = "ip_default")]
    pub ip: Option<String>,
    /// The default IP quota: applies to every IP address without its own
    #[arg(long)]
    pub ip_default: bool,
}

impl EntityArgs {
    pub fn entity(&self) -> KafkaResult<QuotaEntity> {
        // A named entity, or with `default` set the type's default (`None`).
        let part = |kind: &str, name: Option<String>, default: bool| {
            if default {
                Some((kind.to_string(), None))
            } else {
                name.map(|n| (kind.to_string(), Some(n)))
            }
        };
        // "default" is no IP address, so `--ip default` keeps naming the
        // default entity.
        let ip = self.ip.clone().filter(|ip| ip != "default");
        let ip_default = self.ip_default || self.ip.as_deref() == Some("default");
        let entity: QuotaEntity = [
            part("user", self.user.clone(), self.user_default),
            part("client-id", self.client_id.clone(), self.client_id_default),
            part("ip", ip, ip_default),
        ]
        .into_iter()
        .flatten()
        .collect();
        if entity.is_empty() {
            return Err(KafkaError::config(
                "Name who the quota is for: --user, --client-id and/or --ip (or \
                 --user-default, --client-id-default, --ip-default for the default quotas)",
            ));
        }
        Ok(entity)
    }

    /// A note for `--user default` / `--client-id default`, which named the
    /// default entity in earlier releases and now name a user or client id
    /// called "default".
    pub fn default_name_hint(&self) -> Option<String> {
        let mut hints = Vec::new();
        if self.user.as_deref() == Some("default") {
            hints.push(
                "--user default now means the user named 'default'; \
                 for the default quota of all users use --user-default",
            );
        }
        if self.client_id.as_deref() == Some("default") {
            hints.push(
                "--client-id default now means the client id 'default'; \
                 for the default quota of all client ids use --client-id-default",
            );
        }
        (!hints.is_empty()).then(|| hints.join(". "))
    }
}

/// Prints the [`EntityArgs::default_name_hint`] note on stderr, if any.
fn print_default_name_hint(entity: &EntityArgs) {
    if let Some(hint) = entity.default_name_hint() {
        eprintln!("{} {hint}.", "Note:".yellow().bold());
    }
}

#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum QuotaSubcommands {
    /// List every client quota
    List,
    /// Set a quota, e.g. --user alice --key producer_byte_rate --value 1048576
    Set {
        #[command(flatten)]
        entity: EntityArgs,
        /// producer_byte_rate, consumer_byte_rate, request_percentage,
        /// controller_mutation_rate or connection_creation_rate
        #[arg(long)]
        key: String,
        /// A finite number, 0 or more (0 throttles the entity to nothing)
        #[arg(long, value_parser = parse_quota_value)]
        value: f64,
    },
    /// Remove a quota
    Remove {
        #[command(flatten)]
        entity: EntityArgs,
        #[arg(long)]
        key: String,
    },
}

/// Parses a quota value: finite and not negative. NaN and infinity would be
/// stored by the broker as-is, and a negative rate has no meaning.
fn parse_quota_value(raw: &str) -> Result<f64, String> {
    let value: f64 = raw
        .trim()
        .parse()
        .map_err(|_| format!("'{raw}' is not a number"))?;
    if value.is_finite() && value >= 0.0 {
        Ok(value)
    } else {
        Err(format!("'{raw}' must be a finite number, 0 or more"))
    }
}

pub fn run(args: QuotaArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let config = global.to_kafka_config();
    match args.command {
        QuotaSubcommands::List => {
            let quotas = wire::describe_client_quotas(&config)?;
            if global.format == OutputFormat::Json {
                let items: Vec<_> = quotas
                    .iter()
                    .map(|q| {
                        serde_json::json!({
                            "entity": quota_entity_label(&q.entity),
                            "key": q.key,
                            "value": q.value,
                        })
                    })
                    .collect();
                output::print_json(&items)?;
                return Ok(());
            }
            if quotas.is_empty() {
                println!("No client quotas are set.");
                return Ok(());
            }
            let mut table = output::table(vec!["ENTITY", "QUOTA", "VALUE"]);
            for q in &quotas {
                table.add_row(vec![
                    Cell::new(quota_entity_label(&q.entity)),
                    Cell::new(&q.key),
                    Cell::new(q.value).set_alignment(CellAlignment::Right),
                ]);
            }
            println!("{table}");
            Ok(())
        }
        QuotaSubcommands::Set { entity, key, value } => {
            print_default_name_hint(&entity);
            let entity = entity.entity()?;
            let pending = record::start(
                global,
                record::cluster_scope(global, None),
                quota_change(&config, &entity, &key, Some(value)),
            );
            let result = wire::alter_client_quota(&config, &entity, &key, Some(value));
            record::finish(pending, &result, &format!("Set {key} = {value}"));
            result?;
            println!(
                "{} Set {key} = {value} for {}",
                "✔".green().bold(),
                quota_entity_label(&entity).cyan()
            );
            Ok(())
        }
        QuotaSubcommands::Remove { entity, key } => {
            print_default_name_hint(&entity);
            let entity = entity.entity()?;
            let pending = record::start(
                global,
                record::cluster_scope(global, None),
                quota_change(&config, &entity, &key, None),
            );
            let result = wire::alter_client_quota(&config, &entity, &key, None);
            record::finish(pending, &result, &format!("Removed {key}"));
            result?;
            println!(
                "{} Removed {key} for {}",
                "✔".green().bold(),
                quota_entity_label(&entity).cyan()
            );
            Ok(())
        }
    }
}

/// The record of setting (`Some`) or removing (`None`) one quota: its value
/// before and after.
fn quota_change(
    config: &crate::kafka::config::KafkaClientConfig,
    entity: &QuotaEntity,
    key: &str,
    value: Option<f64>,
) -> Change {
    let state = |value| crate::model::history::State::Quota {
        entity: entity.clone(),
        key: key.to_string(),
        value,
    };
    let before = wire::describe_client_quotas(config).map(|quotas| {
        quotas
            .into_iter()
            .find(|q| crate::model::quota::same_quota_entity(&q.entity, entity) && q.key == key)
            .map(|q| q.value)
    });
    record::change_from_capture(
        crate::model::history::Reversibility::Reversible,
        before.map(|old| (vec![state(old)], vec![state(value)])),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_value_must_be_finite_and_not_negative() {
        assert_eq!(parse_quota_value("1048576"), Ok(1_048_576.0));
        assert_eq!(parse_quota_value("0"), Ok(0.0));
        assert_eq!(parse_quota_value("12.5"), Ok(12.5));
        for bad in ["NaN", "nan", "inf", "-inf", "infinity", "-1", "abc", ""] {
            assert!(parse_quota_value(bad).is_err(), "{bad}");
        }
    }

    fn parse_entity(args: &[&str]) -> Result<EntityArgs, clap::Error> {
        use clap::Parser;
        #[derive(Parser)]
        struct Wrapper {
            #[command(flatten)]
            entity: EntityArgs,
        }
        Wrapper::try_parse_from(std::iter::once("quota").chain(args.iter().copied()))
            .map(|w| w.entity)
    }

    #[test]
    fn user_default_names_a_real_user_and_the_flag_the_default_entity() {
        let named = parse_entity(&["--user", "default"]).unwrap();
        assert_eq!(
            named.entity().unwrap(),
            vec![("user".to_string(), Some("default".to_string()))]
        );
        assert!(named.default_name_hint().is_some());

        let default = parse_entity(&["--user-default"]).unwrap();
        assert_eq!(default.entity().unwrap(), vec![("user".to_string(), None)]);
        assert!(default.default_name_hint().is_none());
        assert_eq!(
            quota_entity_label(&default.entity().unwrap()),
            "user=<default>"
        );

        let client = parse_entity(&["--user", "alice", "--client-id-default"]).unwrap();
        assert_eq!(
            client.entity().unwrap(),
            vec![
                ("user".to_string(), Some("alice".to_string())),
                ("client-id".to_string(), None)
            ]
        );
        let named_client = parse_entity(&["--client-id", "default"]).unwrap();
        assert_eq!(
            named_client.entity().unwrap(),
            vec![("client-id".to_string(), Some("default".to_string()))]
        );
        assert!(named_client.default_name_hint().is_some());

        // "default" is no IP address: both spellings name the default entity.
        for args in [&["--ip", "default"][..], &["--ip-default"][..]] {
            let ip = parse_entity(args).unwrap();
            assert_eq!(ip.entity().unwrap(), vec![("ip".to_string(), None)]);
        }

        assert!(parse_entity(&["--user", "a", "--user-default"]).is_err());
        assert!(parse_entity(&["--client-id", "a", "--client-id-default"]).is_err());
        assert!(parse_entity(&[]).unwrap().entity().is_err());
    }
}
