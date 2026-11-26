#!/usr/bin/env python3
"""End-to-end smoke test of `ferrika connect` against a real Kafka Connect.

Checks what unit tests cannot: the Connect REST API (KIP-875 stop and
offsets) as a live worker answers it. The script makes a topic with a few
records, then through the CLI a FileStreamSink connector reading it and a
FileStreamSource connector reading a file on the worker, and checks listing
and status, `connect stop`, `connect offsets` (show, preview, refusal while
running, --set and --reset with --execute), history and undo of each, and
that the audit log names the changes without their offsets.

Start a throw-away broker and a Connect worker (Kafka 3.6 or later) with the
file connectors on its plugin path first, e.g. with the apache/kafka image
(host networking so `localhost:9092` reaches the broker):

    docker run -d --name ferrika-kafka-test -p 9092:9092 apache/kafka:3.9.0
    docker run -d --name ferrika-connect-test --network host apache/kafka:3.9.0 \\
        sh -c "$(python3 tools/e2e/connect_smoke.py --print-worker-script)"

Run (needs only the Python 3 standard library):

    cargo build -p ferrika-cli
    python3 tools/e2e/connect_smoke.py target/debug/ferrika --broker localhost:9092 \\
        --url http://localhost:8083

Isolation: the topic, connectors and profile are named <prefix><run id>-...,
and the CLI runs with its own FERRIKA_CONFIG_DIR (HOME and USERPROFILE too),
so the user's profiles, history and audit log are not touched. The
connectors and the topic are deleted at the end, even after a failure. Exit
code 0 when every check passes, 1 when any fails.

Use a test cluster only: the script creates and deletes topics and
connectors.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

# The worker the docstring starts: distributed mode on localhost:9092 with
# the file connectors (not on Kafka's default plugin path since 3.2).
WORKER_SCRIPT = r"""
mkdir -p /tmp/plugins/file && cp /opt/kafka/libs/connect-file-*.jar /tmp/plugins/file/ &&
cat > /tmp/worker.properties <<EOF
bootstrap.servers=localhost:9092
group.id=ferrika-connect-e2e
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
offset.storage.topic=ferrika-connect-e2e-offsets
config.storage.topic=ferrika-connect-e2e-configs
status.storage.topic=ferrika-connect-e2e-status
offset.storage.replication.factor=1
config.storage.replication.factor=1
status.storage.replication.factor=1
offset.flush.interval.ms=1000
listeners=http://0.0.0.0:8083
plugin.path=/tmp/plugins
EOF
exec /opt/kafka/bin/connect-distributed.sh /tmp/worker.properties
"""

if "--print-worker-script" in sys.argv:
    print(WORKER_SCRIPT.strip())
    sys.exit(0)

ROOT = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
parser.add_argument("binary", nargs="?", default=str(ROOT / "target" / "release" / "ferrika"))
parser.add_argument("--broker", default="localhost:9092")
parser.add_argument("--url", default="http://localhost:8083", help="Connect REST URL (default http://localhost:8083)")
parser.add_argument("--source-file", default="/opt/kafka/LICENSE",
                    help="a file on the worker for the source connector to read (default /opt/kafka/LICENSE)")
parser.add_argument("--prefix", default="zz-", help="name prefix of everything the run creates (default zz-)")
ARGS = parser.parse_args()

F = ARGS.binary
API = ARGS.url.rstrip("/")
RUN = f"{ARGS.prefix}{uuid.uuid4().hex[:6]}-"
TOPIC, SINK, SOURCE, SOURCE_TOPIC = f"{RUN}in", f"{RUN}sink", f"{RUN}source", f"{RUN}lines"
PROFILE = f"{RUN}p"
RECORDS = 5
results = []

tmp = Path(tempfile.mkdtemp(prefix="ferrika-connect-smoke-"))
HOME = tmp / "home"
HOME.mkdir()
CONFIG = HOME / ".ferrika"
ENV = dict(os.environ)
ENV.update({"HOME": str(HOME), "USERPROFILE": str(HOME), "FERRIKA_CONFIG_DIR": str(CONFIG),
            "NO_COLOR": "1", "FERRIKA_SECRET_STORE": "file"})
ENV.pop("FERRIKA_PROFILE", None)


def ferrika(args, timeout=60, stdin=None):
    try:
        p = subprocess.run([F] + args, capture_output=True, text=True, encoding="utf-8", errors="replace",
                           timeout=timeout, env=ENV, input=stdin)
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired as ex:
        return "timeout", str(ex.stdout or ""), str(ex.stderr or "")


def cj(args, timeout=60):
    """Runs `ferrika -f json connect ...`; returns (rc, parsed JSON, output)."""
    rc, out, err = ferrika(["-f", "json", "connect"] + args, timeout=timeout)
    return rc, parse_json(out), out + err


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"   [{detail}]" if detail and not ok else ""), flush=True)


def tail(s, n=300):
    return str(s)[-n:].replace("\n", " | ")


def parse_json(text):
    try:
        return json.loads(text)
    except ValueError:
        return None


def api(method, path, ok_missing=False):
    req = urllib.request.Request(f"{API}{path}", method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        if ok_missing and e.code == 404:
            return None
        raise RuntimeError(f"{method} {path}: HTTP {e.code} {e.read()[:200]!r}") from e


def q(name):
    return urllib.parse.quote(name, safe="")


def state(name):
    status = api("GET", f"/connectors/{q(name)}/status", ok_missing=True) or {}
    return status.get("connector", {}).get("state")


def wait(what, predicate, timeout=60):
    deadline = time.time() + timeout
    while True:
        value = predicate()
        if value or time.time() > deadline:
            return value
        time.sleep(1)


def offsets(name):
    return (api("GET", f"/connectors/{q(name)}/offsets") or {}).get("offsets", [])


def sink_offsets(name):
    """{partition: offset} of a sink connector."""
    return {o["partition"]["kafka_partition"]: (o.get("offset") or {}).get("kafka_offset")
            for o in offsets(name)}


def op_of(action):
    """The newest recorded operation `action` (from `ferrika history`)."""
    rc, out, err = ferrika(["-f", "json", "history", "-n", "200"])
    for op in parse_json(out) or []:
        if op.get("action") == action:
            return op
    return {}


def apply(name, config):
    path = tmp / f"{name}.json"
    path.write_text(json.dumps({"name": name, "config": config}))
    return ferrika(["connect", "apply", "-f", str(path)])


def setup():
    rc, out, err = ferrika(["profile", "add", PROFILE, "-b", ARGS.broker, "--connect", API])
    check("profile add with a Connect URL", rc == 0, f"rc={rc} {tail(out + err)}")
    rc, out, err = ferrika(["profile", "use", PROFILE])
    check("profile use", rc == 0, f"rc={rc} {tail(out + err)}")
    rc, out, err = ferrika(["topics", "create", TOPIC, "--partitions", "1"])
    check("topic created", rc == 0, f"rc={rc} {tail(out + err)}")
    for i in range(RECORDS):
        ferrika(["produce", TOPIC, "-m", f"line {i}"])
    rc, out, err = apply(SINK, {
        "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
        "tasks.max": "1", "topics": TOPIC, "file": f"/tmp/{SINK}.txt"})
    check("connect apply: file sink", rc == 0, f"rc={rc} {tail(out + err)}")
    rc, out, err = apply(SOURCE, {
        "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
        "tasks.max": "1", "topic": SOURCE_TOPIC, "file": ARGS.source_file})
    check("connect apply: file source", rc == 0, f"rc={rc} {tail(out + err)}")


def scenarios():
    running = wait("sink running", lambda: state(SINK) == "RUNNING" and state(SOURCE) == "RUNNING", 90)
    check("both connectors are RUNNING", running, f"sink={state(SINK)} source={state(SOURCE)}")
    consumed = wait("sink offsets", lambda: sink_offsets(SINK).get(0) == RECORDS, 60)
    check("the sink connector consumed the records (Connect shows its offsets)", consumed, str(sink_offsets(SINK)))

    rc, out, text = cj(["list"])
    names = {c.get("name"): c for c in out or []}
    check("connect list shows both connectors", rc == 0 and SINK in names and SOURCE in names, tail(text))
    rc, out, err = ferrika(["connect", "status", SINK])
    check("connect status shows the connector and its task", rc == 0 and "task 0" in out, tail(out + err))

    # --- offsets: show ----------------------------------------------------
    rc, out, text = cj(["offsets", SINK])
    shown = {o["partition"]["kafka_partition"]: o["offset"]["kafka_offset"] for o in (out or {}).get("offsets", [])}
    check("connect offsets -f json shows the sink's topic, partition and offset",
          rc == 0 and shown.get(0) == RECORDS
          and (out or {}).get("offsets", [{}])[0].get("partition", {}).get("kafka_topic") == TOPIC, tail(text))
    rc, out, err = ferrika(["connect", "offsets", SINK])
    check("connect offsets table has TOPIC, PARTITION, OFFSET",
          rc == 0 and "TOPIC" in out and TOPIC in out and str(RECORDS) in out, tail(out + err))
    source_ok = wait("source offsets", lambda: offsets(SOURCE), 30)
    rc, out, text = cj(["offsets", SOURCE])
    first = ((out or {}).get("offsets") or [{}])[0]
    check("a source connector's offsets are its own maps (filename, position)",
          rc == 0 and source_ok and first.get("partition", {}).get("filename") == ARGS.source_file
          and isinstance(first.get("offset", {}).get("position"), int), tail(text))

    # --- changes need a stopped connector -----------------------------------
    rc, out, text = cj(["offsets", SINK, "--reset"])
    check("--reset without --execute is a preview",
          rc == 0 and out and out.get("executed") is False and out.get("state") == "RUNNING"
          and sink_offsets(SINK).get(0) == RECORDS, tail(text))
    rc, out, err = ferrika(["connect", "offsets", SINK, "--reset", "--execute"])
    check("--reset --execute is refused while the connector runs, pointing to connect stop",
          rc not in (0, "timeout") and "connect stop" in err and sink_offsets(SINK).get(0) == RECORDS,
          f"rc={rc} {tail(out + err)}")

    rc, out, text = cj(["stop", SINK])
    stopped = wait("stopped", lambda: state(SINK) == "STOPPED", 30)
    check("connect stop stops the connector", rc == 0 and stopped, f"rc={rc} state={state(SINK)} {tail(text)}")

    # --- set, then undo -----------------------------------------------------
    new = {"offsets": [{"partition": {"kafka_topic": TOPIC, "kafka_partition": 0}, "offset": {"kafka_offset": 1}}]}
    rc, out, text = cj(["offsets", SINK, "--set", json.dumps(new), "--execute"])
    check("--set --execute sets the offset of a stopped sink",
          rc == 0 and out and out.get("executed") is True and sink_offsets(SINK).get(0) == 1,
          f"rc={rc} {sink_offsets(SINK)} {tail(text)}")
    op = op_of("connect offsets --set")
    check("history records the offset change as reversible", op.get("reversibility") == "reversible", tail(op))
    rc, out, err = ferrika(["undo", op.get("id", "none"), "--yes"])
    check("undo sets the previous offset back", rc == 0 and sink_offsets(SINK).get(0) == RECORDS,
          f"rc={rc} {sink_offsets(SINK)} {tail(out + err)}")

    # --- reset, then undo ---------------------------------------------------
    rc, out, text = cj(["offsets", SINK, "--reset", "--execute"])
    check("--reset --execute deletes every offset", rc == 0 and not sink_offsets(SINK),
          f"rc={rc} {sink_offsets(SINK)} {tail(text)}")
    op = op_of("connect offsets --reset")
    rc, out, err = ferrika(["undo", op.get("id", "none"), "--yes"])
    check("undo of the reset sets the offsets back", rc == 0 and sink_offsets(SINK).get(0) == RECORDS,
          f"rc={rc} {sink_offsets(SINK)} {tail(out + err)}")

    # --- undo of the stop ---------------------------------------------------
    op = op_of("connect stop")
    rc, out, err = ferrika(["undo", op.get("id", "none"), "--yes"])
    running = wait("running again", lambda: state(SINK) == "RUNNING", 30)
    check("undo of the stop resumes the connector", rc == 0 and running,
          f"rc={rc} state={state(SINK)} {tail(out + err)}")

    # --- audit --------------------------------------------------------------
    rc, out, err = ferrika(["-f", "json", "audit", "-n", "200"])
    entries = parse_json(out) or []
    if isinstance(entries, dict):
        entries = entries.get("entries", [])
    actions = {e.get("action") for e in entries}
    want = {"connect stop", "connect offsets --set", "connect offsets --reset"}
    check("audit log names the stop and the offset changes", rc == 0 and want <= actions,
          f"missing={sorted(want - actions)}")
    log = CONFIG / "audit.log"
    audit_text = log.read_text(encoding="utf-8") if log.exists() else ""
    check("audit log holds no offsets", "connect offsets --set" in audit_text and "kafka_offset" not in audit_text,
          f"exists={log.exists()}")


def cleanup():
    for name in (SINK, SOURCE):
        try:
            api("DELETE", f"/connectors/{q(name)}", ok_missing=True)
        except Exception as e:  # noqa: BLE001 - best effort
            print(f"cleanup: {name}: {e}")
    for topic in (TOPIC, SOURCE_TOPIC):
        ferrika(["topics", "delete", topic, "-y"])
    shutil.rmtree(tmp, ignore_errors=True)


def main():
    try:
        api("GET", "/")
    except Exception as e:  # noqa: BLE001
        print(f"Kafka Connect at {API} does not answer: {e}")
        return 1
    try:
        setup()
        scenarios()
    finally:
        cleanup()
    failed = results.count(False)
    print(f"\n{len(results) - failed}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
