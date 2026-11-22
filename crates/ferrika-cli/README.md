# ferrika-cli

The `ferrika` command line: a single-binary Kafka client written in Rust, with
no JVM. Topics, producing and consuming, consumer group lag, ACLs, Schema
Registry, Kafka Connect, health and config checks, backup and restore, and an
MCP server for AI assistants.

```bash
cargo install ferrika-cli          # installs the `ferrika` binary
ferrika profile add local -b localhost:9092 --set-active
ferrika topics list
```

Building needs a C compiler and CMake (librdkafka is built from source).
`cargo install ferrika-cli --features ssl` adds TLS and SASL SCRAM for
producing and consuming; it builds OpenSSL and needs Perl.

Documentation: <https://github.com/Jangmyun/ferrika>.

## As a library

This crate is the command line. Since 0.2.0 its command modules, argument
structs and command enum are crate-internal; the public `cli` items are the
entry points (`cli::app::{Cli, run, init_tracing,
exit_quietly_on_broken_pipe}`, `cli::GlobalOpts`) and a few RabbitMQ and
transform helpers for front ends. Use `ferrika-core`, `ferrika-governance`,
`ferrika-ops`, `ferrika-mcp` and `ferrika-rabbitmq` as libraries.

## License

Apache-2.0
