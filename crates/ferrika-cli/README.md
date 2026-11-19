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

## License

Apache-2.0
