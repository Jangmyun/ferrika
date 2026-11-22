#!/usr/bin/env python3
"""End-to-end smoke test of `ferrika mcp` against a real Kafka broker.

Drives the MCP server over stdio JSON-RPC as an AI client would
(initialize, notifications/initialized, tools/list, tools/call) with write
tools on and limited to this run's topics: read tools, transform_preview
(masked records), transform_execute (plan, confirmation token, execute), a
destination outside --topics, and the audit entries the session leaves.

Start a throw-away broker first (KRaft, single node, no auth):

    docker run -d --name ferrika-kafka-test -p 9092:9092 apache/kafka:3.9.0

Run (needs only the Python 3 standard library):

    cargo build -p ferrika-cli
    python3 tools/e2e/mcp_smoke.py target/debug/ferrika --broker localhost:9092

With --rabbit-url (and --rabbit-user/--rabbit-password) it also adds a
RabbitMQ profile in the isolated config directory and calls the RabbitMQ
read tools with it.

Isolation: every topic is named <prefix><run id>-... (one more, outside the
write scope, <prefix>x<run id>-outside), and the CLI runs with its own
FERRIKA_CONFIG_DIR (HOME and USERPROFILE too), so the user's profiles and
audit log are not touched. Everything the run created is deleted at the
end, even after a failure. Exit code 0 when every check passes, 1 when any
fails.

Use a test broker only: the script creates and deletes topics.
"""
import argparse
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
parser.add_argument("binary", nargs="?", default=str(ROOT / "target" / "release" / "ferrika"))
parser.add_argument("--broker", default="localhost:9092", help="bootstrap servers (default localhost:9092)")
parser.add_argument("--prefix", default="zz-", help="name prefix of everything the run creates (default zz-)")
parser.add_argument("--rabbit-url", help="also check the RabbitMQ read tools against this management API")
parser.add_argument("--rabbit-user", default="guest")
parser.add_argument("--rabbit-password", default="guest")
ARGS = parser.parse_args()

F = ARGS.binary
BROKER = ARGS.broker
HEX = uuid.uuid4().hex[:6]
RUN = f"{ARGS.prefix}{HEX}-"  # unique per run
SRC, DST = f"{RUN}src", f"{RUN}dst"
OUTSIDE = f"{ARGS.prefix}x{HEX}-outside"  # exists, but outside --topics
N = 5
EMAILS = [f"user{i}-{HEX}@example.com" for i in range(N)]
results = []

tmp = Path(tempfile.mkdtemp(prefix="ferrika-mcp-smoke-"))
HOME = tmp / "home"
HOME.mkdir()
CONFIG = HOME / ".ferrika"
ENV = dict(os.environ)
ENV.update({"HOME": str(HOME), "USERPROFILE": str(HOME), "FERRIKA_CONFIG_DIR": str(CONFIG),
            "FERRIKA_SECRET_STORE": "file", "NO_COLOR": "1"})
for key in ("FERRIKA_PROFILE", "FERRIKA_RABBIT_PROFILE", "FERRIKA_RABBIT_URL", "FERRIKA_RABBIT_PASSWORD"):
    ENV.pop(key, None)


def run(args, timeout=30, stdin=None, broker=True):
    cmd = [F] + (["-b", BROKER] if broker else []) + args
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
                           timeout=timeout, env=ENV, input=stdin)
        return p.returncode, p.stdout + p.stderr
    except subprocess.TimeoutExpired as ex:
        return "timeout", str(ex.stdout or "") + str(ex.stderr or "")


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"   [{detail}]" if detail and not ok else ""), flush=True)


def tail(s, n=300):
    return str(s)[-n:].replace("\n", " | ")


class Mcp:
    """One `ferrika mcp` process spoken to over newline-delimited JSON-RPC."""

    def __init__(self, args):
        self.proc = subprocess.Popen([F, "-b", BROKER, "mcp", *args], stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=ENV,
                                     text=True, encoding="utf-8", errors="replace", bufsize=1)
        self.lines = queue.Queue()
        self.stderr = []
        self.next_id = 0
        threading.Thread(target=self._read, args=(self.proc.stdout, self.lines.put), daemon=True).start()
        threading.Thread(target=self._read, args=(self.proc.stderr, self.stderr.append), daemon=True).start()

    @staticmethod
    def _read(stream, sink):
        for line in stream:
            sink(line)

    def send(self, message):
        self.proc.stdin.write(json.dumps(message) + "\n")
        self.proc.stdin.flush()

    def request(self, method, params=None, timeout=60):
        self.next_id += 1
        rid = self.next_id
        self.send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params or {}})
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                line = self.lines.get(timeout=max(0.1, deadline - time.time()))
            except queue.Empty:
                break
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            # Notifications and server requests (no matching id) are skipped.
            if msg.get("id") == rid and ("result" in msg or "error" in msg):
                return msg
        raise TimeoutError(f"no answer to {method} in {timeout}s; stderr: {tail(''.join(self.stderr))}")

    def call(self, name, arguments, timeout=60):
        """tools/call; returns (is_error, structured or parsed text, raw text)."""
        msg = self.request("tools/call", {"name": name, "arguments": arguments}, timeout)
        if "error" in msg:
            return True, msg["error"], json.dumps(msg["error"])
        result = msg["result"]
        text = "".join(c.get("text", "") for c in result.get("content", []))
        data = result.get("structuredContent")
        if data is None:
            try:
                data = json.loads(text)
            except ValueError:
                data = text
        return bool(result.get("isError")), data, text

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=15)
        except (OSError, subprocess.TimeoutExpired):
            self.proc.kill()


def find(data, key):
    """The first value of `key` anywhere in a nested result."""
    if isinstance(data, dict):
        if key in data:
            return data[key]
        data = list(data.values())
    if isinstance(data, list):
        for item in data:
            found = find(item, key)
            if found is not None:
                return found
    return None


def records_in(data):
    """The records list of a peek or preview result, whatever it is called."""
    if isinstance(data, dict):
        for key in ("records", "messages"):
            if isinstance(data.get(key), list):
                return data[key]
    return data if isinstance(data, list) else []


def scenarios():
    for t in (SRC, DST, OUTSIDE):
        rc, out = run(["topics", "create", t])
        check(f"create topic {t}", rc == 0, f"rc={rc} {tail(out)}")
    for i, email in enumerate(EMAILS):
        run(["produce", SRC, "-k", f"k{i}", "-m", json.dumps({"id": i, "email": email, "amount": i * 10})])

    if ARGS.rabbit_url:
        rc, out = run(["rabbit", "profile", "add", f"{RUN}rabbit", "--use", "--rabbit-url", ARGS.rabbit_url,
                       "--rabbit-user", ARGS.rabbit_user, "--rabbit-password-stdin"],
                      stdin=ARGS.rabbit_password + "\n", broker=False)
        check("rabbit profile for the RabbitMQ tools", rc == 0, f"rc={rc} {tail(out)}")

    mcp = Mcp(["--allow-write", "--topics", f"{RUN}*"])
    try:
        init = mcp.request("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                          "clientInfo": {"name": "ferrika-e2e", "version": "1.0"}})
        info = init.get("result", {})
        check("initialize answers with server info and tools capability",
              "tools" in info.get("capabilities", {}) and info.get("serverInfo"), tail(init))
        mcp.send({"jsonrpc": "2.0", "method": "notifications/initialized"})

        listed = mcp.request("tools/list")
        names = {t.get("name") for t in listed.get("result", {}).get("tools", [])}
        want = {"list_topics", "describe_topic", "peek_messages", "transform_preview",
                "produce_messages", "create_topic", "transform_execute"}
        check("tools/list has the read and write tools", want <= names, f"missing={sorted(want - names)}")

        err, data, text = mcp.call("list_topics", {"filter": RUN})
        check("list_topics finds this run's topics", not err and SRC in text and DST in text, tail(text))

        err, data, text = mcp.call("peek_messages", {"topic": SRC, "from": "earliest", "limit": 10})
        recs = records_in(data)
        check("peek_messages reads the records", not err and len(recs) == N, f"got={len(recs)} {tail(text)}")
        check("peek_messages masks emails by default", not any(e in text for e in EMAILS), tail(text))

        steps = [{"filter": "$.amount >= 20"}, {"drop": "amount"}, {"set": 'env="e2e"'}]
        err, data, text = mcp.call("transform_preview", {"topic": SRC, "steps": steps, "max_records": 10})
        recs = records_in(data)
        check("transform_preview returns the transformed records (3 of 5 kept)",
              not err and len(recs) == 3 and "e2e" in text, f"got={len(recs)} {tail(text)}")
        check("transform_preview masks the records", not err and not any(e in text for e in EMAILS), tail(text))

        args = {"topic": SRC, "to": DST, "steps": steps, "max_records": 10}
        err, plan, text = mcp.call("transform_execute", {**args, "mode": "plan"})
        token = (plan.get("confirmation") or {}).get("token") if isinstance(plan, dict) else None
        check("transform_execute plan returns a confirmation token", not err and token, tail(text))
        err, data, text = mcp.call("transform_execute", {**args, "mode": "execute"})
        check("transform_execute without the token is refused", err, tail(text))
        err, done, text = mcp.call("transform_execute", {**args, "mode": "execute", "confirmation": token}, timeout=90)
        written = find(done, "written")
        check("transform_execute with the token writes 3 records", not err and written == 3, tail(text))

        err, data, text = mcp.call("peek_messages", {"topic": DST, "from": "earliest", "limit": 10,
                                                     "mask_pii": False})
        recs = records_in(data)
        check("the destination topic holds the 3 transformed records",
              not err and len(recs) == 3 and "e2e" in text and '"amount"' not in text
              and all(e in text for e in EMAILS[2:]), f"got={len(recs)} {tail(text)}")

        err, data, text = mcp.call("transform_execute", {**args, "to": OUTSIDE, "mode": "plan"})
        check("a destination outside --topics is refused", err and "outside" in text, tail(text))
        err, data, text = mcp.call("produce_messages", {"topic": OUTSIDE, "records": [{"value": "x"}],
                                                        "mode": "plan"})
        check("produce_messages to a topic outside --topics is refused", err and "outside" in text, tail(text))

        if ARGS.rabbit_url:
            err, data, text = mcp.call("rabbit_queues", {})
            check("rabbit_queues answers with the active RabbitMQ profile", not err and "queues" in text, tail(text))
    finally:
        mcp.close()

    rc, out = run(["-f", "json", "audit", "--source", "mcp", "-n", "500"], broker=False)
    try:
        entries = json.loads(out)
    except ValueError:
        entries = []
    mine = [e for e in entries if SRC in (e.get("target") or "") or OUTSIDE in (e.get("target") or "")]
    actions = {e.get("action") for e in mine}
    check("audit log has the MCP transform_execute entries with the client as actor",
          any("transform_execute" in (e.get("action") or "") and e.get("ok") for e in mine)
          and all((e.get("actor") or "").startswith("mcp:ferrika-e2e") for e in mine),
          f"rc={rc} actions={sorted(map(str, actions))} {tail(out)}")
    leaked = [p.name for p in CONFIG.rglob("*") if p.is_file()
              and any(e in p.read_text(encoding="utf-8", errors="replace") for e in EMAILS)]
    check("no record value in the audit log or any config file", not leaked, f"files={leaked}")


def topic_names():
    rc, out = run(["-f", "json", "topics", "list"], timeout=30)
    try:
        return [t["name"] for t in json.loads(out)]
    except (ValueError, KeyError, TypeError):
        return re.findall(re.escape(ARGS.prefix) + r"[\w.\-]+", out)


def cleanup():
    mine = lambda n: n.startswith(RUN) or n == OUTSIDE  # noqa: E731
    names = [n for n in topic_names() if mine(n)]
    for name in names:
        run(["topics", "delete", name, "-y"])
    for _ in range(10):
        left = [n for n in topic_names() if mine(n)]
        if not left:
            break
        time.sleep(1)
    print(f"\ncleanup: deleted {len(names)} topics, topics left: {left}")
    return not left


if not Path(F).exists():
    sys.exit(f"binary not found: {F} (build with `cargo build -p ferrika-cli`)")
print(f"ferrika mcp smoke: binary={F} broker={BROKER} run={RUN}\n")
rc, out = run(["topics", "list"], timeout=30)
if rc != 0:
    sys.exit(f"cannot reach the broker at {BROKER}: {tail(out)}")
clean = False
try:
    scenarios()
finally:
    try:
        clean = cleanup()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
check("cleanup leaves no topics of this run", clean)
print(f"{sum(results)}/{len(results)} passed")
sys.exit(0 if all(results) else 1)
