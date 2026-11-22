//! `ferrika mcp`: the command-line options of the MCP server in the
//! `ferrika-mcp` crate.

use clap::{Args, ValueEnum};

use crate::cli::GlobalOpts;
use crate::error::KafkaResult;

/// `ferrika mcp` options. Without `--allow-write` the server only reads.
#[derive(Args, Debug, Clone, Default)]
pub struct McpArgs {
    /// Offer the write tools: produce_messages, create_topic, alter_topic_config,
    /// reset_consumer_group_offsets, transform_execute, and for RabbitMQ
    /// profiles rabbit_peek, rabbit_dlq_inspect, rabbit_publish,
    /// rabbit_dlq_redrive. Each write is planned, confirmed and audited;
    /// company policy and a read-only profile still apply
    #[arg(long)]
    pub allow_write: bool,

    /// Offer only these write tools (comma-separated)
    #[arg(
        long,
        value_delimiter = ',',
        value_name = "TOOL",
        requires = "allow_write",
        value_parser = clap::builder::PossibleValuesParser::new(ferrika_mcp::WRITE_TOOLS)
    )]
    pub allow_tools: Vec<String>,

    /// Topics the write tools may change (glob, repeatable). Default: every
    /// topic except internal ones (`__*`)
    #[arg(long = "topics", value_name = "GLOB", requires = "allow_write")]
    pub topics: Vec<String>,

    /// RabbitMQ queues the rabbit_* write tools may read from, move out of
    /// or publish to through the default exchange (glob, repeatable).
    /// Default: every queue
    #[arg(long = "queues", value_name = "GLOB", requires = "allow_write")]
    pub queues: Vec<String>,

    /// RabbitMQ exchanges rabbit_publish and rabbit_dlq_redrive may publish
    /// to (glob, repeatable; amq.default is the default exchange). Default:
    /// every exchange except amq.rabbitmq.*
    #[arg(long = "exchanges", value_name = "GLOB", requires = "allow_write")]
    pub exchanges: Vec<String>,

    /// How a write is confirmed before it runs: elicit asks the user through
    /// the client (MCP elicitation), token takes the token from the plan,
    /// auto asks the user when the client can and uses the token otherwise
    #[arg(long, value_enum, default_value_t = ConfirmArg::Auto, requires = "allow_write")]
    pub confirm: ConfirmArg,

    /// Most write executions per minute
    #[arg(
        long,
        default_value_t = 10,
        value_parser = clap::value_parser!(u32).range(1..),
        requires = "allow_write"
    )]
    pub max_writes_per_minute: u32,

    /// Most records produce_messages and transform_execute may write in this
    /// session
    #[arg(long, default_value_t = 1000, requires = "allow_write")]
    pub max_produce_records: u64,

    /// Most records one offset reset may skip or re-read without a person
    /// confirming it
    #[arg(long, default_value_t = 1_000_000, requires = "allow_write")]
    pub max_offset_delta: u64,
}

/// How write tools are confirmed. There is deliberately no "none".
#[derive(ValueEnum, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ConfirmArg {
    /// Elicitation when the client supports it, else the plan's token.
    #[default]
    Auto,
    /// Always ask the user; clients without elicitation get no write tools.
    Elicit,
    /// The token from the plan (the agent can confirm on its own).
    Token,
}

impl From<ConfirmArg> for ferrika_mcp::ConfirmMode {
    fn from(arg: ConfirmArg) -> Self {
        match arg {
            ConfirmArg::Auto => Self::Auto,
            ConfirmArg::Elicit => Self::Elicit,
            ConfirmArg::Token => Self::Token,
        }
    }
}

impl McpArgs {
    /// The write tools asked for; `None` without `--allow-write`.
    fn write_options(&self) -> Option<ferrika_mcp::WriteOptions> {
        self.allow_write.then(|| ferrika_mcp::WriteOptions {
            allow_tools: self.allow_tools.clone(),
            topics: self.topics.clone(),
            queues: self.queues.clone(),
            exchanges: self.exchanges.clone(),
            confirm: self.confirm.into(),
            max_writes_per_minute: self.max_writes_per_minute,
            max_produce_records: self.max_produce_records,
            max_offset_delta: self.max_offset_delta,
        })
    }
}

pub async fn run(args: McpArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let context = global.context();
    let writes = args.write_options();
    let server = ferrika_mcp::McpServer::new(&context, writes.as_ref());
    for warning in server.startup_warnings() {
        eprintln!("{warning}");
    }
    server.serve_stdio().await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<McpArgs, clap::Error> {
        use clap::Parser;
        #[derive(Parser)]
        struct Wrap {
            #[command(flatten)]
            a: McpArgs,
        }
        Wrap::try_parse_from(std::iter::once("mcp").chain(args.iter().copied())).map(|w| w.a)
    }

    #[test]
    fn write_flags_need_allow_write() {
        let plain = parse(&[]).unwrap();
        assert!(!plain.allow_write);
        assert_eq!(
            (
                plain.max_writes_per_minute,
                plain.max_produce_records,
                plain.max_offset_delta
            ),
            (10, 1000, 1_000_000)
        );
        assert!(parse(&["--topics", "test-*"]).is_err());
        assert!(parse(&["--queues", "test-*"]).is_err());
        assert!(parse(&["--exchanges", "test.*"]).is_err());
        let rabbit = parse(&[
            "--allow-write",
            "--allow-tools",
            "rabbit_peek,rabbit_dlq_redrive",
            "--queues",
            "test-*",
            "--exchanges",
            "test.*",
        ])
        .unwrap();
        assert_eq!(rabbit.queues, ["test-*"]);
        assert_eq!(rabbit.exchanges, ["test.*"]);
        let options = rabbit.write_options().unwrap();
        assert_eq!(options.queues, ["test-*"]);
        assert_eq!(options.allow_tools, ["rabbit_peek", "rabbit_dlq_redrive"]);
        assert!(parse(&["--allow-tools", "create_topic"]).is_err());
        assert!(parse(&["--max-writes-per-minute", "3"]).is_err());
        assert!(parse(&["--confirm", "token"]).is_err());
        assert_eq!(plain.confirm, ConfirmArg::Auto);
        assert!(parse(&["--allow-write", "--confirm", "none"]).is_err());
        let args = parse(&[
            "--allow-write",
            "--allow-tools",
            "create_topic,produce_messages",
            "--topics",
            "test-*",
            "--topics",
            "dev-*",
        ])
        .unwrap();
        assert_eq!(args.allow_tools, ["create_topic", "produce_messages"]);
        assert_eq!(args.topics, ["test-*", "dev-*"]);
        assert!(parse(&["--allow-write", "--allow-tools", "delete_topic"]).is_err());
        assert!(parse(&["--allow-write", "--max-writes-per-minute", "0"]).is_err());
    }
}
