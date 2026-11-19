# ferrika-ops

Kafka operations of [Ferrika](https://github.com/Jangmyun/ferrika), built on
the [`ferrika-core`](https://crates.io/crates/ferrika-core) clients and the
[`ferrika-governance`](https://crates.io/crates/ferrika-governance) policy:

- backup and restore of topics to JSON Lines, mirroring between topics and
  clusters
- dead-letter queue inspection and redrive, topic diffs, compaction analysis
- configuration lint, health checks, topology and consumer group lag
- Markdown incident reports
- the change history behind `ferrika undo`
- benchmarks, latency probes, synthetic data generators and tracing header
  checks

## Stability

Pre-1.0: public items can change between minor versions.

## License

Apache-2.0
