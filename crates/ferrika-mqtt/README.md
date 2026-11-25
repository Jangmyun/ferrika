# ferrika-mqtt

**Preview.** MQTT support of [Ferrika](https://github.com/Jangmyun/ferrika),
behind the `ferrika mqtt` command. Its API, file formats and behaviour may
change in any release until it leaves preview.

For operators of MQTT 3.1.1 and 5 brokers (Mosquitto, EMQX, HiveMQ, brokers
that bridge MQTT to Kafka):

- `client`: one clean MQTT session over [rumqttc](https://crates.io/crates/rumqttc)
  (TLS through rustls with the ring backend, a CA file or the OS trust
  store). Every session is clean, with a fresh client id, so it never takes
  over another client's session and leaves nothing on the broker
- `collect`: bounded loops only: a subscription up to a count or a time
  limit, the retained messages under a topic filter, a publish until its
  acknowledgement
- `topic`: topic and filter checks; shared subscriptions (`$share/`,
  `$queue/`) are refused for reads, since they take messages from the
  group's consumers
- `profile`: named connections in `mqtt-profiles.json` next to Ferrika's
  `profiles.json`, passwords in the OS credential store
- `connection`: a profile with the user's overrides applied
- `view`: messages as a front end shows them (PII-masked on request)

Ferrika's company policy and audit log are applied by the caller (the
`ferrika` command line or another front end) through `ferrika-governance`.

## Stability

Preview, pre-1.0: public items can change in any version.

## License

Apache-2.0
