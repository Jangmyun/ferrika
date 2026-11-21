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

This crate is the command line. Its Rust `cli` module (argument structs,
the command enum) is not a stable API: options added to commands change it
in patch versions, and 0.2.0 makes it crate-internal. Use `ferrika-core`,
`ferrika-governance`, `ferrika-ops` and `ferrika-mcp` as libraries.

## License

Apache-2.0
