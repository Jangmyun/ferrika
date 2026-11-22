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

/// Who a quota applies to. Give a name, or `default` for the default
/// quota of that kind.
#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct EntityArgs {
    /// User principal name (or `default`)
    #[arg(long)]
    pub user: Option<String>,
    /// Client id (or `default`)
    #[arg(long = "client-id")]
    pub client_id: Option<String>,
    /// Client IP address (or `default`), for connection_creation_rate
    #[arg(long)]
    pub ip: Option<String>,
}

impl EntityArgs {
    pub fn entity(&self) -> KafkaResult<QuotaEntity> {
        let part = |kind: &str, name: &Option<String>| {
            name.as_ref()
                .map(|n| (kind.to_string(), (n != "default").then(|| n.clone())))
        };
        let entity: QuotaEntity = [
            part("user", &self.user),
            part("client-id", &self.client_id),
            part("ip", &self.ip),
        ]
        .into_iter()
        .flatten()
        .collect();
        if entity.is_empty() {
            return Err(KafkaError::config(
                "Name who the quota is for: --user, --client-id and/or --ip",
            ));
        }
        Ok(entity)
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
}
