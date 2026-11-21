//! `ferrika user`: SASL/SCRAM users (credentials stored by the brokers).

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::Cell;

use crate::cli::record::{self, Change, CANNOT_UNDO};
use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::scram::ScramHash;
use crate::kafka::wire;
use crate::model::history::{Reversibility, State};

#[derive(Args, Debug, Clone)]
pub struct UserArgs {
    #[command(subcommand)]
    pub command: UserSubcommands,
}

/// `Debug` is written by hand to redact the password.
#[derive(Subcommand, Clone, PartialEq, Eq)]
pub enum UserSubcommands {
    /// List users with SCRAM credentials and their mechanisms
    List,
    /// Create a SCRAM user, or change its password
    Create {
        name: String,
        /// The password (salted locally; only the salted form is sent).
        /// Defaults to the FERRIKA_SCRAM_PASSWORD environment variable.
        #[arg(
            long,
            env = "FERRIKA_SCRAM_PASSWORD",
            hide_env_values = true,
            required_unless_present = "password_stdin"
        )]
        password: Option<String>,
        /// Read the password from the first line of stdin instead, keeping
        /// it out of the process list and shell history (wins over the
        /// environment variable)
        #[arg(long)]
        password_stdin: bool,
        /// SCRAM-SHA-256 or SCRAM-SHA-512
        #[arg(long, default_value = "SCRAM-SHA-256")]
        mechanism: String,
        /// PBKDF2 iterations (at least 4096)
        #[arg(long, default_value_t = 8192)]
        iterations: u32,
    },
    /// Delete one SCRAM credential of a user
    Delete {
        name: String,
        #[arg(long, default_value = "SCRAM-SHA-256")]
        mechanism: String,
    },
}

impl std::fmt::Debug for UserSubcommands {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::List => f.write_str("List"),
            Self::Create {
                name,
                password,
                password_stdin,
                mechanism,
                iterations,
            } => f
                .debug_struct("Create")
                .field("name", name)
                .field("password", &password.as_ref().map(|_| "<redacted>"))
                .field("password_stdin", password_stdin)
                .field("mechanism", mechanism)
                .field("iterations", iterations)
                .finish(),
            Self::Delete { name, mechanism } => f
                .debug_struct("Delete")
                .field("name", name)
                .field("mechanism", mechanism)
                .finish(),
        }
    }
}

fn mechanism(name: &str) -> KafkaResult<ScramHash> {
    ScramHash::parse(name).ok_or_else(|| {
        KafkaError::config(format!(
            "Unknown mechanism '{name}': use SCRAM-SHA-256 or SCRAM-SHA-512"
        ))
    })
}

pub fn run(args: UserArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let config = global.to_kafka_config();
    match args.command {
        UserSubcommands::List => {
            let users = wire::describe_scram_users(&config)?;
            if global.format == OutputFormat::Json {
                let items: Vec<_> = users
                    .iter()
                    .map(|u| {
                        let creds: Vec<_> = u
                            .credentials
                            .iter()
                            .map(|(h, i)| serde_json::json!({"mechanism": h.mechanism(), "iterations": i}))
                            .collect();
                        serde_json::json!({"user": u.name, "credentials": creds})
                    })
                    .collect();
                output::print_json(&items)?;
                return Ok(());
            }
            if users.is_empty() {
                println!("No SCRAM users.");
                return Ok(());
            }
            let mut table = output::table(vec!["USER", "CREDENTIALS"]);
            for u in &users {
                let creds: Vec<String> = u
                    .credentials
                    .iter()
                    .map(|(h, i)| format!("{} ({i} iterations)", h.mechanism()))
                    .collect();
                table.add_row(vec![Cell::new(&u.name), Cell::new(creds.join(", "))]);
            }
            println!("{table}");
            Ok(())
        }
        UserSubcommands::Create {
            name,
            password,
            password_stdin,
            mechanism: mech,
            iterations,
        } => {
            let hash = mechanism(&mech)?;
            let password = match password {
                Some(p) if !password_stdin => p,
                _ => crate::cli::read_password_stdin().map_err(KafkaError::config)?,
            };
            if password.is_empty() {
                return Err(KafkaError::config("The password is empty"));
            }
            let pending = record::start(
                global,
                record::cluster_scope(global, None),
                credential_change(&config, &name, hash),
            );
            let result = wire::alter_scram_user(&config, &name, hash, Some(&password), iterations);
            record::finish(
                pending,
                &result,
                &format!("Set the {} credential of {name}", hash.mechanism()),
            );
            result?;
            println!(
                "{} Set the {} credential of '{}'",
                "✔".green().bold(),
                hash.mechanism(),
                name.cyan()
            );
            Ok(())
        }
        UserSubcommands::Delete {
            name,
            mechanism: mech,
        } => {
            let hash = mechanism(&mech)?;
            let pending = record::start(
                global,
                record::cluster_scope(global, None),
                Change::irreversible(
                    Reversibility::Irreversible,
                    "a deleted credential's password is not known",
                ),
            );
            let result = wire::alter_scram_user(&config, &name, hash, None, 0);
            record::finish(
                pending,
                &result,
                &format!("Deleted the {} credential of {name}", hash.mechanism()),
            );
            result?;
            println!(
                "{} Deleted the {} credential of '{}'",
                "✔".green().bold(),
                hash.mechanism(),
                name.cyan()
            );
            Ok(())
        }
    }
}

/// The record of setting `user`'s credential: undoable (by deleting it)
/// when it is new; replacing a password cannot be undone.
fn credential_change(
    config: &crate::kafka::config::KafkaClientConfig,
    user: &str,
    hash: ScramHash,
) -> Change {
    let existed = wire::describe_scram_users(config).map(|users| {
        users
            .iter()
            .filter(|u| u.name == user)
            .flat_map(|u| &u.credentials)
            .any(|(h, _)| *h == hash)
    });
    let state = |exists| State::Scram {
        user: user.to_string(),
        mechanism: hash.mechanism().to_string(),
        exists,
    };
    match existed {
        Ok(true) => {
            eprintln!(
                "{} '{user}' already has a {} credential; replacing it. {CANNOT_UNDO}",
                "note:".yellow().bold(),
                hash.mechanism()
            );
            Change::irreversible(
                Reversibility::Irreversible,
                "the replaced password is not known",
            )
        }
        captured => record::change_from_capture(
            Reversibility::Reversible,
            captured.map(|_| (vec![state(false)], vec![state(true)])),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        command: UserSubcommands,
    }

    #[test]
    fn debug_output_redacts_the_password() {
        let cli =
            TestCli::try_parse_from(["user", "create", "bob", "--password", "hunter2-pw"]).unwrap();
        let shown = format!("{:?}", cli.command);
        assert!(
            shown.contains("bob") && shown.contains("<redacted>"),
            "{shown}"
        );
        assert!(!shown.contains("hunter2-pw"), "{shown}");
    }

    #[test]
    fn create_accepts_password_stdin_instead_of_password() {
        let cli = TestCli::try_parse_from(["user", "create", "bob", "--password-stdin"]).unwrap();
        assert!(matches!(
            cli.command,
            UserSubcommands::Create {
                password_stdin: true,
                ..
            }
        ));
        let cli = TestCli::try_parse_from(["user", "create", "bob", "--password", "pw"]).unwrap();
        assert!(matches!(
            cli.command,
            UserSubcommands::Create { password: Some(ref p), password_stdin: false, .. } if p == "pw"
        ));
    }
}
