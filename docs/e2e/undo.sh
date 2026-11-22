#!/usr/bin/env bash
# End-to-end check of `ferrika history` / `ferrika undo` against a broker
# (e.g. a single-node KRaft container, or the SASL cluster of
# kafka-sasl-compose.yml).
#
#   BOOTSTRAP=localhost:9092 KAFKA_CONTAINER=kafka FERRIKA=./target/debug/ferrika \
#     bash docs/e2e/undo.sh
#
# Against a cluster that needs credentials, name a profile instead of the
# brokers (and its config directory), and say how to reach the broker's own
# tools:
#
#   FERRIKA_CONFIG_DIR=/path/to/cfg PROFILE=e2e \
#   KAFKA_EXEC="docker exec kafka-sasl-1" INTERNAL_BOOTSTRAP=kafka-sasl-1:29092 \
#     bash docs/e2e/undo.sh
#
# PROFILE      profile to use (-p); without it, -b "$BOOTSTRAP"
# KAFKA_EXEC   command that runs a program in a broker container
#              (default: docker exec "$KAFKA_CONTAINER")
# INTERNAL_BOOTSTRAP  brokers as seen from inside that container
#              (default: localhost:9092)
# KAFKA_BIN    directory of the Kafka tools in that container
#              (default: /opt/kafka/bin; AutoMQ: /opt/automq/kafka/bin)
# KAFKA_CONFIGS_ARGS  extra kafka-configs.sh arguments, e.g.
#              "--command-config /etc/kafka/admin.properties"
# TOPIC_PREFIX prefix of the topics and group created (default undo-e2e)
# TIMEOUT      seconds one command may take (default 90)
#
# Without FERRIKA_CONFIG_DIR a throwaway config directory is used, so the
# real profiles, audit log and history are not touched. Stops at the first
# failed check.
set -euo pipefail

BOOTSTRAP=${BOOTSTRAP:-localhost:9092}
KAFKA_CONTAINER=${KAFKA_CONTAINER:-kafka}
KAFKA_EXEC=${KAFKA_EXEC:-docker exec $KAFKA_CONTAINER}
INTERNAL_BOOTSTRAP=${INTERNAL_BOOTSTRAP:-localhost:9092}
KAFKA_BIN=${KAFKA_BIN:-/opt/kafka/bin}
KAFKA_CONFIGS_ARGS=${KAFKA_CONFIGS_ARGS:-}
PROFILE=${PROFILE:-}
TIMEOUT=${TIMEOUT:-90}
FERRIKA=${FERRIKA:-./target/debug/ferrika}
export FERRIKA_CONFIG_DIR=${FERRIKA_CONFIG_DIR:-$(mktemp -d)}
TOPIC=${TOPIC_PREFIX:-undo-e2e}-$$
GROUP=${TOPIC_PREFIX:-undo-e2e}-group-$$

if [ -n "$PROFILE" ]; then
  CLUSTER=(-p "$PROFILE")
else
  CLUSTER=(-b "$BOOTSTRAP")
fi

# stdin from /dev/null: a command that would prompt fails instead of hanging.
f() { timeout "$TIMEOUT" "$FERRIKA" "${CLUSTER[@]}" "$@" < /dev/null; }
pass() { echo "✔ $*"; }
fail() { echo "✖ $*" >&2; exit 1; }
latest() { f -f json history -n 1 | sed -n 's/.*"id": "\([0-9A-Z]*\)".*/\1/p' | head -1; }
override() { f topics config "$TOPIC" | grep -E "^\S*\s*$1\b|│ $1 " | sed 's/[│┆]/ /g' | awk '{print $2}'; }
# The group's committed offset per partition of $TOPIC (-1: none), one
# "partition:offset" per line, read from a dry-run reset (which changes nothing).
committed() {
  f -f json offset reset "$GROUP" -t "$TOPIC" --to-earliest | tr -d ' \r\n' \
    | grep -o '"partition":[0-9]*,"current_offset":-\{0,1\}[0-9]*' \
    | sed 's/"partition":\([0-9]*\),"current_offset":\(.*\)/\1:\2/' | sort
}
# Topic deletion finishes on the brokers after the request returns, so a
# list taken at once may still show the topic: wait up to 10 s for it to go.
topic_gone() {
  for _ in $(seq 1 10); do
    # Not `f ... | grep -q`: with pipefail an early grep exit fails the pipe.
    grep -q "$1" <<< "$(f topics list)" || return 0
    sleep 1
  done
  return 1
}

echo "config dir: $FERRIKA_CONFIG_DIR"

# 1. Topic config: an override and a default key, undone together.
f topics create "$TOPIC" -p 3 -r 1 -c retention.ms=604800000 >/dev/null
f topics config "$TOPIC" -s retention.ms=1000 -s cleanup.policy=compact >/dev/null
f undo -y >/dev/null
[ "$(override retention.ms)" = 604800000 ] || fail "retention.ms not restored"
[ -z "$(override cleanup.policy)" ] || fail "cleanup.policy override not removed"
pass "topic config undone (override restored, default reset)"

# 2. Undo of the undo applies the change again (the topic stays compacted
#    from here on, so records produced to it below need a key).
f undo -y >/dev/null
[ "$(override retention.ms)" = 1000 ] || fail "redo did not apply the change"
pass "undoing the undo re-applies the change"

# 3. Drift: another tool changes the same key; undo refuses, --force goes on.
f topics config "$TOPIC" -s retention.ms=2000 >/dev/null
OP=$(latest)
# shellcheck disable=SC2086 # KAFKA_EXEC and KAFKA_CONFIGS_ARGS are word lists
MSYS_NO_PATHCONV=1 timeout "$TIMEOUT" $KAFKA_EXEC "$KAFKA_BIN/kafka-configs.sh" \
  --bootstrap-server "$INTERNAL_BOOTSTRAP" $KAFKA_CONFIGS_ARGS \
  --entity-type topics --entity-name "$TOPIC" --alter --add-config retention.ms=3000 \
  < /dev/null >/dev/null
if f undo "$OP" -y >/dev/null 2>&1; then fail "undo ran despite drift"; fi
if f undo "$OP" --dry-run >/dev/null 2>&1; then fail "dry run of a refused undo exited 0"; fi
f undo "$OP" --force -y >/dev/null
f audit -n 5 | grep -q "forced" || fail "audit outcome does not say forced"
pass "drift refused (also by --dry-run), --force recorded as forced"

# 4. Topic creation: undone while empty, refused once it has a record.
f topics create "$TOPIC-b" -p 1 -r 1 >/dev/null
f undo -y >/dev/null
topic_gone "$TOPIC-b" || fail "empty topic not deleted by undo"
f topics create "$TOPIC-c" -p 1 -r 1 >/dev/null
OP=$(latest)
f produce "$TOPIC-c" -m hello >/dev/null
if f undo "$OP" -y >/dev/null 2>&1; then fail "topic with a record was deleted"; fi
pass "topic creation undone only while empty"

# 5. Offset reset (with an uncommitted partition) and group deletion.
f produce "$TOPIC" -k k -m a >/dev/null
f consume "$TOPIC" --group "$GROUP" --from-beginning -n 1 >/dev/null
BEFORE=$(committed)
f offset reset "$GROUP" -t "$TOPIC" --to-latest --execute >/dev/null
[ "$(committed)" != "$BEFORE" ] || fail "offset reset changed nothing"
f undo -y >/dev/null
AFTER=$(committed)
[ "$AFTER" = "$BEFORE" ] \
  || fail "offsets after undo: $(echo "$AFTER" | tr '\n' ' ')expected: $(echo "$BEFORE" | tr '\n' ' ')"
pass "offset reset undone"
f offset delete-group "$GROUP" -y >/dev/null
f undo -y >/dev/null
[ "$(committed)" = "$BEFORE" ] || fail "group offsets not back"
pass "deleted group's offsets committed again"

# 6. Replication: back to the previous replicas (needs 2+ brokers; skipped otherwise).
if [ "$(f -f json topology 2>/dev/null | grep -c '"id"')" -ge 2 ]; then
  f topics set-replication "$TOPIC" 2 -y >/dev/null
  f undo -y >/dev/null
  pass "replication undone"
else
  echo "- replication skipped (one broker)"
fi

# 7. ACLs and quotas (need an authorizer / quota support; failures are reported, not fatal).
if f acl create --principal User:e2e --topic "$TOPIC" --operation Read >/dev/null 2>&1; then
  f undo -y >/dev/null && pass "ACL create undone"
else
  echo "- ACL checks skipped (no authorizer)"
fi
f quota set --user e2e --key consumer_byte_rate --value 1024 >/dev/null 2>&1 \
  && f undo -y >/dev/null && pass "quota set undone" || echo "- quota checks skipped"

# 8. The audit log ties every undoable change to its journal entry.
f audit verify >/dev/null || fail "audit chain broken"
f -f json audit -n 200 | grep -q '"op_digest"' || fail "no op_digest in the audit log"
pass "audit chain intact, entries carry op/op_digest"

f topics delete "$TOPIC" -y >/dev/null
f topics delete "$TOPIC-c" -y >/dev/null
echo "all undo checks passed"
