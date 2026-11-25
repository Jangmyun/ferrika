# MQTT support (preview)

Status: preview implementation for the release after 0.2.0 (`ferrika mqtt`,
crate `ferrika-mqtt`, cargo feature `mqtt` of `ferrika-cli`, on by default).
It follows the RabbitMQ preview ([multi-broker.md](multi-broker.md)): the
same policy gate, audit log, export gate and PII masking, with MQTT
profiles of its own. Kafka and RabbitMQ behaviour is unchanged.

## Goal and scope

An operator of an MQTT broker (Mosquitto, EMQX, HiveMQ, or a broker that
bridges MQTT to Kafka) needs to look at what flows and what is retained,
and to act on it safely, under the company's policy and with an audit
trail:

| Need | Command |
| --- | --- |
| See what is published under a topic filter, for a while | `ferrika mqtt sub FILTER [-n N] [--duration S] [--qos Q]` |
| Send a test or command message | `ferrika mqtt pub TOPIC -m TEXT [--qos Q] [--retain]` |
| See the retained messages (devices' last state, configuration) | `ferrika mqtt retained FILTER [--wait S] [-n N]` |
| Remove a stale or wrong retained message | `ferrika mqtt retained clear TOPIC [--yes]` |
| Keep broker connections | `ferrika mqtt profile add\|list\|use\|remove\|show` |

Out of scope for the preview: broker administration APIs (EMQX REST,
HiveMQ Control Center, Mosquitto dynamic security), `$SYS` dashboards,
persistent sessions, shared subscriptions, last wills, client certificates
(mutual TLS), WebSocket transport, MQTT tools for the MCP server and the
desktop app. See [Later](#later).

## Library choice

[rumqttc](https://crates.io/crates/rumqttc) 0.25.1 (Apache-2.0, Bytebeam),
**its packet codec only** (`rumqttc::mqttbytes` for MQTT 3.1.1 and
`rumqttc::v5::mqttbytes` for MQTT 5), with no cargo features.

- License: Apache-2.0, like Ferrika's crates. Its own dependencies without
  features (`bytes`, `flume`, `fixedbitset`, `futures-util`, `log`,
  `thiserror`, `tokio`, `tokio-stream`, `tokio-util`) are MIT or
  MIT/Apache-2.0; `cargo deny` allows all of them.
- Why not its event loop and TLS: rumqttc's TLS features
  (`use-rustls`, `use-rustls-no-provider`) pull `rustls-webpki` 0.102.8,
  which has five open RustSec advisories (RUSTSEC-2026-0049, -0098, -0099,
  -0104, -0382; fixed only in 0.103), and `rustls-pemfile`, which is
  unmaintained (RUSTSEC-2025-0134). rumqttc uses them only for an error
  type and PEM parsing that Ferrika does not call, but they would fail
  `cargo deny` and show up in every customer's SBOM scan. Its event loop
  also reconnects and resends on its own, which a bounded, audited command
  does not want.
- So `ferrika-mqtt::client::Session` is a small connection of its own:
  TCP or TLS through the workspace's `tokio-rustls`
  with the ring backend (no aws-lc, no OpenSSL; the OS trust store or a CA
  file), CONNECT/CONNACK, SUBSCRIBE/SUBACK, PUBLISH with QoS 0, 1 and 2
  (PUBACK, PUBREC/PUBREL/PUBCOMP both ways), PINGREQ every 15 s while idle
  (keep-alive 30 s) and DISCONNECT. No retry, no resend: a broken
  connection ends the command with an error, which the audit result
  records.
- MQTT 3.1.1 and 5 both: 3.1.1 is the default (every broker and MQTT
  bridge speaks it); 5 adds reason codes on acknowledgements (a broker can
  say "not authorized" for a publish, which 3.1.1 cannot), user properties
  and content types.

Alternatives looked at: `paho-mqtt` (wraps the Eclipse Paho C library, EPL
or EDL licensed, needs a C build and OpenSSL: no), `ntex-mqtt` (its own
runtime, large), `mqttrs` / `mqttbytes` (3.1.1 or old releases only).

## Decisions

1. **A separate crate, behind a default-on feature.** `ferrika-mqtt`
   depends on `ferrika-core` only (secrets, config directory, masking),
   like `ferrika-rabbitmq`. `ferrika-cli` takes it with the feature `mqtt`
   (default), so `cargo build -p ferrika-cli --no-default-features` builds
   a CLI without MQTT and without its dependencies. Reason: MQTT users are
   a different audience from Kafka users; a distributor or a security
   review can leave the code out.

2. **Profiles in their own store**, `mqtt-profiles.json` next to
   `profiles.json` (so `FERRIKA_CONFIG_DIR` and `FERRIKA_CONFIG_PATH` move
   it too), managed with `ferrika mqtt profile add|list|use|remove|show`.
   The password goes to the OS credential store under
   `<profile>/mqtt_password` (no Kafka or RabbitMQ field has that name, so a
   profile of another broker with the same name never shares it); with
   `FERRIKA_SECRET_STORE=file` or without a store it stays in the
   owner-only file. Same reasons as RabbitMQ decision 3: an MQTT broker is
   not a Kafka cluster, and the Kafka `ClusterProfile` keeps its public
   shape.

   ```json
   {
     "active_profile": "plant-a",
     "profiles": {
       "plant-a": {
         "url": "mqtts://broker.plant-a.corp:8883",
         "version": "5",
         "user": "ops",
         "password_in_store": true,
         "ca_file": "C:\\certs\\plant-a-ca.pem",
         "read_only": true
       }
     }
   }
   ```

3. **No credentials in URLs.** `mqtt://` and `mqtts://` URLs with a user or
   password are refused everywhere (unlike `--rabbit-url`, which accepts
   them with a warning). The user comes from `--mqtt-user` or the profile,
   the password from `FERRIKA_MQTT_PASSWORD`, `--mqtt-password-stdin` or
   the profile. A new command family has no old habit to keep, so the safe
   form is the only one. `--mqtt-url` without `--mqtt-profile` uses no
   profile: a stored password never goes to a broker typed by hand, and a
   stored password is used only for its profile's own user (as for
   RabbitMQ).

4. **A subscription is a read, under conditions that make it one.**
   `mqtt sub` and the `mqtt retained` listing pass the export gate (they
   show message data: stdout to a terminal is viewing; redirected stdout
   is an export, refused under `export: forbidden`, recorded under
   `export: audited`) and mask PII when the policy enforces masking (or
   with `--mask-pii`), but not the write gate. That holds because:
   - every session is **clean** (MQTT 3.1.1 `clean_session = 1`; MQTT 5
     `clean_start = 1` with session expiry 0): the broker keeps no
     session, subscription or queued messages after the command ends,
     whatever the QoS;
   - the **client id is fresh** each time (`ferrika` + 12 random hex
     digits): it never takes over a device's or a service's session (a
     broker disconnects the older of two clients with the same id);
   - **shared subscriptions are refused** (`$share/GROUP/...`, EMQX's
     `$queue/...`): joining a group would take messages away from the
     group's real consumers, the MQTT form of a consumer-group commit;
   - **no last will** is set, and nothing is published.

   Unlike a RabbitMQ peek (decision 1 there), nothing about the messages
   changes on the broker, so a read-only profile or policy still allows
   `sub` and `retained`. Persistent sessions (`clean_session = 0`, or a
   session expiry) would leave a subscription that queues messages on the
   broker until someone resumes it: a write. The preview does not offer
   them; if they come, they come as a write (category to be decided, with
   the same compatibility note as below) and with an explicit session
   expiry. **Recommendation for operators**: use clean sessions for
   inspection (Ferrika always does) and give the inspection user ACLs that
   allow subscribe on the topics needed and nothing on `$share/`.

5. **Every subscription is bounded.** `sub` stops after `--count` messages
   (default 10, at most 100 000) or `--duration` seconds (default 30, at
   most a day), whichever comes first; `retained` stops when no retained
   message came for a second, after `--wait` seconds (default 5, at most
   600), or at `--count` (default 500, at most 10 000), and says on stderr
   when there may be more. A script can never hang on a quiet topic, and a
   forgotten terminal does not hold a connection open.

6. **`mqtt pub` is a write in the category `produce`**, like Kafka's
   `produce` and `rabbit publish`: one `forbid: ["produce"]` covers all
   three brokers. Default QoS 1 (the broker acknowledges; QoS 0 gives no
   confirmation, QoS 2 is accepted). `--retain` is explicit and off by
   default; the audit target says `retained` when it is set. An empty
   retained message would clear the topic's retained message, so `pub
   --retain -m ''` is refused and points to `retained clear` (the clear
   must not slip past its own category). MQTT 5 fields (`-H` user
   properties, `--content-type`) need `--mqtt-version 5` and are refused on
   3.1.1 rather than dropped. Audit: request entry before, result entry
   after (`acknowledged by the broker`, or `sent (QoS 0: no
   acknowledgement)`), target `topic T (QoS q[, retained])`, never the
   payload. On MQTT 3.1.1 a broker that drops a message its ACL forbids
   still acknowledges it; MQTT 5 says so with a reason code, which the
   command reports as an error.

7. **Clearing a retained message is a write in a new category,
   `messages.delete`, which also needs `produce`.** `retained clear TOPIC`
   publishes an empty retained message (QoS 1), which deletes what the
   broker keeps for new subscribers: often a device's last known state or
   its configuration, which devices read when they connect. That is
   different in kind from sending one more message: an operator may want
   to allow test publishes and still forbid wiping retained state, so it
   gets its own category (`resource.verb`, as `messages.peek` and
   `messages.move`). It also needs `produce` allowed
   (`PolicyAction::also_requires`, like `messages.move`), since it
   publishes: `forbid: ["produce"]` keeps refusing it, as an operator
   would expect. Before it publishes, the command looks for the retained
   message (a clean subscription to the exact topic, two seconds): if there
   is none it changes nothing and says so; else the confirmation names the
   size (`--yes` skips it). The audit result names the cleared size, never
   the payload.

   Compatibility: builds before this one do not know `messages.delete`, so
   a signed policy that forbids it is invalid there and those builds run in
   the strictest mode. The policy format number is not raised (a new
   `forbid` value is not a new field); `docs/POLICY.md` and the CHANGELOG
   say to roll such a policy out only once every client runs this release
   or later. The desktop app and other front ends that match
   `PolicyAction` exhaustively must add the variant when they move to this
   release.

8. **The gate matches the broker's `host:port`** (from the URL, default
   port 1883 or 8883) and the MQTT profile name, never a Kafka or RabbitMQ
   profile. `read_only`, `read_only_brokers` and `export_clusters` globs
   cover MQTT without a policy format change: `read_only_brokers:
   ["*.plant-a.corp:*"]` makes `broker.plant-a.corp:8883` read-only. A
   profile's own `read_only` flag refuses `pub` and `retained clear`.

9. **Bad arguments stop before the gates.** Invalid topics or filters,
   shared subscriptions, out-of-range limits, an empty retained publish
   and MQTT 5 fields on 3.1.1 are refused while the connection is
   resolved, so a mistyped command leaves no audit entry.

10. **Masking**: payloads through `mask_bytes` (JSON field by field, text
    by pattern, binary hidden whole), MQTT 5 user properties through
    `mask_header_value` (judged by their name, like Kafka record headers),
    and the topic as text, since device topics often carry identifiers
    such as e-mail addresses (`users/kim@example.com/state`).

11. **TLS** with `mqtts://` (port 8883 by default): the OS trust store, or
    only the certificates of `--mqtt-ca-file` / the profile's `ca_file`
    (stored as an absolute path), for brokers with a private CA. A CA file
    with `mqtt://` is refused. No client certificates in the preview.

## Governance mapping

| Command | Gate | Policy category | Audit |
| --- | --- | --- | --- |
| `mqtt sub` | export gate (channel `stdout`) | none (a read, decision 4) | `record.export` when exports are audited and stdout is redirected, with the message count |
| `mqtt retained FILTER` | export gate (channel `stdout`) | none | as `sub` |
| `mqtt pub` | write gate | `produce` | request entry, result entry (`mqtt pub`, target `topic T (QoS q[, retained])`) |
| `mqtt retained clear` | write gate, confirmation unless `--yes` | `messages.delete`, and `produce` must be allowed too | request entry, result entry with the cleared size or "nothing cleared" |
| `mqtt profile ...` | none (local configuration) | | none |

The classification sits in `crates/ferrika-cli/src/cli/mqtt.rs`
(`write_action`, `write_target`, `export`) and in `write_action` /
`export_gate` in `cli/app.rs`, with no wildcard arms.

## Crate layout

```
ferrika-cli ──(feature mqtt)──> ferrika-mqtt ──> ferrika-core
```

| Module | Contents |
| --- | --- |
| `profile` | `MqttProfile`, `MqttProfileStore` (`mqtt-profiles.json`, OS credential store), `BrokerUrl`, `MqttVersion` |
| `connection` | `ResolvedMqtt`: a profile with the user's overrides, the login, the connect target |
| `client` | `Session` (connection, TLS, packets), `Event`, the `EventSource` trait, `client_id` |
| `collect` | the bounded loops over an `EventSource`: `await_subscription`, `read_messages`, `collect_retained`, `await_publish` (tested with a scripted event stream under tokio's paused clock) |
| `topic` | topic and filter checks, shared-subscription refusal, filter matching |
| `view` | `ShownMessage` (masking), `KEY:VALUE` user properties |

The crate does no policy, audit or masking decisions itself; the CLI
applies them, as for RabbitMQ.

## Verification

- Unit tests: profile store and secrets, URL parsing, resolution and
  password scoping, topic rules, the bounded loops (count, duration,
  quiet time, wait, maximum, refusals, timeouts), packet handling
  (acknowledgements by QoS, MQTT 5 reason codes, partial packets), masking,
  the CLI's gate classification and argument checks.
- `tools/e2e/mqtt_smoke.py` against `eclipse-mosquitto:2` (2.1.4) set up
  by `tools/e2e/mosquitto-setup.sh` (password file, anonymous access off,
  TLS listener with a test CA): 41 checks, also in CI (`e2e-mqtt`).

## Later

1. MCP tools: `mqtt_retained` (read, export-gated) and `mqtt_publish`
   (write tool, `produce`), once the neutral gate target of
   multi-broker.md exists.
2. Desktop views for retained messages and a live topic tree.
3. Client certificates (mutual TLS), WebSocket transport (`wss://`).
4. A client id prefix per profile, for brokers whose ACLs key on it
   (AWS IoT Core policies, HiveMQ).
5. Persistent sessions as an explicit write (decision 4).
6. Broker administration through EMQX's REST API or Mosquitto's dynamic
   security plugin (clients, ACLs), with history and undo.
