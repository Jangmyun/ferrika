# ferrika-rabbitmq

**Preview.** RabbitMQ support of [Ferrika](https://github.com/Jangmyun/ferrika),
behind the `ferrika rabbit` command. Its API, file formats and behaviour may
change in any release until it leaves preview.

- `client`: a typed client for the RabbitMQ management HTTP API (plugin
  `rabbitmq_management`): queues, exchanges and bindings of a vhost, peeking
  at messages (taken and requeued: they are marked redelivered) and
  publishing to an exchange
- `model`: the API's responses and request bodies, and the `x-death`
  history of dead-lettered messages
- `profile`: named connections in `rabbit-profiles.json` next to Ferrika's
  `profiles.json`, passwords in the OS credential store
- `amqp` (feature `amqp`, on by default): moving dead-lettered messages back
  to their original exchange over AMQP 0-9-1 with
  [lapin](https://crates.io/crates/lapin): one message at a time, published
  with publisher confirms and acknowledged only after the confirm

The crate talks to the broker only. Ferrika's company policy, audit log and
PII masking are applied by the caller (the `ferrika` command line).

## Stability

Preview, pre-1.0: public items can change in any version.

## License

Apache-2.0
