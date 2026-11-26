# Agent Skills for Ferrika

Step-by-step workflows that teach a coding agent (Claude Code and other
tools that read the [Agent Skills](https://agentskills.io) format) how to
use the `ferrika` command line for common Kafka and RabbitMQ operations
tasks: which commands to run, how to read their JSON output, and where to
stop and ask a person.

| Skill | Use it for |
| --- | --- |
| [kafka-lag-triage](kafka-lag-triage/SKILL.md) | Why a consumer group lags: stopped or slow consumers, a hot or stuck partition, an input burst (read-only) |
| [kafka-why-no-data](kafka-why-no-data/SKILL.md) | A topic or consumer that shows no data: wrong cluster, wrong name, retention, timestamps, group position, decoding (read-only) |
| [kafka-safe-offset-reset](kafka-safe-offset-reset/SKILL.md) | Rewind, skip or seek a consumer group: dry-run plan, approval, execute, verify, undo |
| [kafka-dlq-redrive](kafka-dlq-redrive/SKILL.md) | Inspect a dead letter topic by cause and redrive it in small, resumable batches |
| [kafka-incident-report](kafka-incident-report/SKILL.md) | Collect health, lag trend and recent changes into one incident document (read-only) |
| [kafka-config-lint](kafka-config-lint/SKILL.md) | Review topic, broker and group configs; apply fixes through a validated dry run and undo |
| [kafka-topic-replay](kafka-topic-replay/SKILL.md) | Replay a time window or filtered subset into another topic, masked, preview first |
| [rabbitmq-dlq](rabbitmq-dlq/SKILL.md) | Inspect a RabbitMQ dead-letter queue by `x-death` and redrive it (preview feature) |

Every skill follows the same rules: read-only commands first, `-f json`
for output the agent parses, a dry run or preview before any change, the
user's explicit approval before `--execute`, `--mask-pii` when records are
read, and a stop when Ferrika answers `Refused` (company policy or a
read-only profile). Changes that Ferrika records can be reverted with
`ferrika undo`; the skills say when a step cannot be undone.

## Requirements

- `ferrika` on `PATH` ([install](../README.md#install)), the same release as
  these files or newer.
- A saved profile for the cluster (`ferrika profile add`), or for RabbitMQ a
  RabbitMQ profile (`ferrika rabbit profile add`). The skills never ask for
  passwords; profiles keep them in the OS credential store.
- For production clusters, consider `ferrika profile read-only NAME` for the
  agent's sessions: every change is then refused, and the read-only skills
  still work.

## Install

A skill is a directory with a `SKILL.md`. Copy the ones you want to where
your agent looks for skills.

Claude Code, for every project of your user:

```bash
git clone --depth 1 https://github.com/Jangmyun/ferrika.git
mkdir -p ~/.claude/skills
cp -r ferrika/skills/kafka-* ferrika/skills/rabbitmq-* ~/.claude/skills/
```

Claude Code, for one project (commit them so the team shares them):

```bash
mkdir -p .claude/skills
cp -r path/to/ferrika/skills/kafka-lag-triage .claude/skills/
```

On Windows (PowerShell), the user directory is
`$HOME\.claude\skills`:

```powershell
New-Item -ItemType Directory -Force "$HOME\.claude\skills"
Copy-Item -Recurse ferrika\skills\kafka-* "$HOME\.claude\skills\"
```

Claude Code picks skills up by their `description`, so asking "why is the
billing consumer lagging?" loads `kafka-lag-triage`. Other agents that read
Agent Skills take the same directories; see their documentation for the
location.

## Keeping them correct

Every `ferrika` command line in a `bash` block of these files is parsed
against the current command line by a test
(`crates/ferrika-cli/tests/skill_commands.rs`, part of `cargo test`), so a
renamed command or flag fails CI. When you change a skill, keep commands in
`bash` blocks, use placeholders such as `TOPIC` and `GROUP` for names and
real numbers for numeric options, and run:

```bash
cargo test -p ferrika-cli --test skill_commands
```

Output field names come from the JSON the commands print; check them with
a test cluster when you change one.
