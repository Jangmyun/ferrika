#!/usr/bin/env python3
"""End-to-end smoke test of `ferrika ksql` against a real ksqlDB server.

Checks what unit tests cannot: the ksqlDB REST API (/info, /ksql,
/query-stream over HTTP/1.1, /close-query) as a live server answers it. The
script runs the CLI for a profile with a ksqlDB server, listings, DESCRIBE,
statements (CREATE, INSERT, CREATE TABLE AS SELECT, TERMINATE, DROP ...
DELETE TOPIC), pull and push queries (row limit, time limit, masking), a
read-only profile, a failing script, the audit log, and the MCP tools
ksql_list, ksql_describe and ksql_query.

With --tls-url (and --tls-user, --tls-password, --tls-ca, --tls-cert,
--tls-key) it also checks a second server that takes HTTPS only, with HTTP
basic authentication and a required client certificate (mutual TLS): a
profile with the login, CA, certificate and key; info, statements and
queries through it; refusals without the client certificate, without the
CA and with a wrong password; a hand-typed URL with credentials; the MCP
tools on it. Every output of those runs and the audit log are checked for
the password. tools/e2e/ksqldb-tls-setup.sh writes such a server's
configuration (certificates, keystores, JAAS file).

Start a throw-away Kafka and ksqlDB first, e.g.:

    docker run -d --name ferrika-kafka-test -p 9092:9092 apache/kafka:3.9.0
    docker run -d --name ferrika-ksql-test --network host \\
        -e KSQL_BOOTSTRAP_SERVERS=localhost:9092 -e KSQL_LISTENERS=http://0.0.0.0:8088 \\
        confluentinc/ksqldb-server:0.29.0

Run (needs only the Python 3 standard library):

    cargo build -p ferrika-cli
    python3 tools/e2e/ksql_smoke.py target/debug/ferrika --url http://localhost:8088 \\
        --broker localhost:9092

Isolation: every stream, table and topic is named <prefix><run id>_..., and
the CLI runs with its own FERRIKA_CONFIG_DIR (HOME and USERPROFILE too), so
the user's profiles and audit log are not touched. Everything the run created
is dropped (with its topic) at the end, even after a failure. Exit code 0
when every check passes, 1 when any fails.

Use a test server only: the script creates and drops streams and topics.
"""
import argparse
import json
import os
import queue
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import base64
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
parser.add_argument("binary", nargs="?", default=str(ROOT / "target" / "release" / "ferrika"))
parser.add_argument("--url", default="http://localhost:8088", help="ksqlDB server (default http://localhost:8088)")
parser.add_argument("--broker", default="localhost:9092", help="the server's Kafka cluster (default localhost:9092)")
parser.add_argument("--prefix", default="kq_", help="name prefix of everything the run creates (default kq_)")
parser.add_argument("--tls-url", help="also check this HTTPS server with basic auth and mutual TLS")
parser.add_argument("--tls-user", default="ferrika-e2e")
parser.add_argument("--tls-password", default="")
parser.add_argument("--tls-ca", help="CA of the --tls-url server (PEM)")
parser.add_argument("--tls-cert", help="client certificate for --tls-url (PEM)")
parser.add_argument("--tls-key", help="client key for --tls-url (PEM, unencrypted)")
ARGS = parser.parse_args()
if ARGS.tls_url and not (ARGS.tls_password and ARGS.tls_ca and ARGS.tls_cert and ARGS.tls_key):
    parser.error("--tls-url needs --tls-password, --tls-ca, --tls-cert and --tls-key")

F = ARGS.binary
URL = ARGS.url.rstrip("/")
RUN = f"{ARGS.prefix}{uuid.uuid4().hex[:6]}_"  # unique per run
ORDERS, BIG, TOTALS, FAIL = f"{RUN}orders", f"{RUN}big", f"{RUN}totals", f"{RUN}fail"
SECURE = f"{RUN}secure"  # the stream made on the --tls-url server
PROFILE, RO_PROFILE = f"{RUN}p", f"{RUN}ro"
# Record values: must never show up in the audit log, nor unmasked when masked.
EMAIL = f"jane.{uuid.uuid4().hex[:6]}@example.com"
results = []

tmp = Path(tempfile.mkdtemp(prefix="ferrika-ksql-smoke-"))
HOME = tmp / "home"
HOME.mkdir()
CONFIG = HOME / ".ferrika"
ENV = dict(os.environ)
ENV.update({"HOME": str(HOME), "USERPROFILE": str(HOME), "FERRIKA_CONFIG_DIR": str(CONFIG),
            "FERRIKA_SECRET_STORE": "file", "NO_COLOR": "1"})
for key in ("FERRIKA_PROFILE", "FERRIKA_KSQL_URL", "FERRIKA_KSQL_USER", "FERRIKA_KSQL_PASSWORD",
            "KAFKA_BOOTSTRAP_SERVERS"):
    ENV.pop(key, None)


def ferrika(args, stdin=None, timeout=60):
    """Runs `ferrika ...`; returns (rc, stdout, stderr)."""
    try:
        p = subprocess.run([F] + args, capture_output=True, text=True, encoding="utf-8", errors="replace",
                           timeout=timeout, env=ENV, input=stdin)
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired as ex:
        return "timeout", str(ex.stdout or ""), str(ex.stderr or "")


def ksql(args, profile=PROFILE, json_out=True, **kw):
    head = (["-f", "json"] if json_out else []) + (["-p", profile] if profile else [])
    return ferrika(head + ["ksql"] + args, **kw)


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


def tls_context():
    """Trusts the --tls-ca and presents the client certificate."""
    ctx = ssl.create_default_context(cafile=ARGS.tls_ca)
    ctx.load_cert_chain(ARGS.tls_cert, ARGS.tls_key)
    return ctx


def server(statement, secure=False):
    """Runs a statement on the server directly (set-up checks, clean-up);
    `secure`: on the --tls-url server."""
    body = json.dumps({"ksql": statement, "streamsProperties": {}}).encode()
    headers = {"Content-Type": "application/json", "Accept": "application/vnd.ksql.v1+json"}
    base, ctx = URL, None
    if secure:
        base, ctx = ARGS.tls_url.rstrip("/"), tls_context()
        login = base64.b64encode(f"{ARGS.tls_user}:{ARGS.tls_password}".encode()).decode()
        headers["Authorization"] = f"Basic {login}"
    req = urllib.request.Request(f"{base}/ksql", data=body, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"error": e.read().decode(errors="replace")}


def close_query(query_id):
    """Asks the server to close `query_id`: (HTTP status, body)."""
    body = json.dumps({"queryId": query_id}).encode()
    req = urllib.request.Request(f"{URL}/close-query", data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode(errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")


def names(kind, secure=False):
    """Stream or table names the server has now."""
    out = server(f"SHOW {kind.upper()};", secure)
    if not isinstance(out, list) or not out:
        return []
    return [s["name"] for s in out[0].get(kind, [])]


def audit_entries():
    path = CONFIG / "audit.log"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def wait_rows(sql, n, timeout=30, profile=PROFILE, extra=()):
    """Pull-query rows, polled until there are `n` (the server materializes
    asynchronously)."""
    deadline = time.time() + timeout
    rows = []
    while time.time() < deadline:
        rc, out, _ = ksql(["query", sql, *extra], profile=profile)
        data = parse_json(out) if rc == 0 else None
        rows = data["rows"] if data else []
        if len(rows) >= n:
            return rows
        time.sleep(1)
    return rows


class Mcp:
    """One `ferrika mcp` process spoken to over newline-delimited JSON-RPC."""

    def __init__(self, profile, args=()):
        self.proc = subprocess.Popen([F, "-p", profile, "mcp", *args], stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=ENV,
                                     text=True, encoding="utf-8", errors="replace", bufsize=1)
        self.lines = queue.Queue()
        self.next_id = 0
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.proc.stdout:
            self.lines.put(line)

    def request(self, method, params=None, timeout=60):
        self.next_id += 1
        rid = self.next_id
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": rid, "method": method,
                                          "params": params or {}}) + "\n")
        self.proc.stdin.flush()
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                msg = json.loads(self.lines.get(timeout=max(0.1, deadline - time.time())))
            except (queue.Empty, ValueError):
                continue
            if msg.get("id") == rid:
                return msg
        raise TimeoutError(f"no answer to {method}")

    def call(self, name, arguments):
        msg = self.request("tools/call", {"name": name, "arguments": arguments})
        result = msg.get("result", {})
        text = "".join(c.get("text", "") for c in result.get("content", []))
        return bool(result.get("isError")), result.get("structuredContent"), text

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=15)
        except (OSError, subprocess.TimeoutExpired):
            self.proc.kill()


def scenarios():
    # --- profiles ---------------------------------------------------------
    rc, out, err = ferrika(["profile", "add", PROFILE, "-b", ARGS.broker, "--ksql-url", URL])
    check("profile add --ksql-url", rc == 0, f"rc={rc} {tail(out + err)}")
    rc, out, err = ferrika(["profile", "add", RO_PROFILE, "-b", ARGS.broker, "--ksql-url", URL, "--read-only"])
    check("read-only profile add", rc == 0, tail(out + err))
    rc, out, err = ferrika(["-f", "json", "profile", "show", PROFILE])
    shown = parse_json(out) or {}
    check("profile show names the ksqlDB URL", shown.get("ksql_url") == URL, tail(out + err))

    # --- metadata ---------------------------------------------------------
    rc, out, err = ksql(["info"])
    info = parse_json(out) or {}
    check("ksql info", rc == 0 and info.get("version") and info.get("server_status") == "RUNNING",
          tail(out + err))
    print(f"      ksqlDB {info.get('version')}")

    # --- statements -------------------------------------------------------
    script = (f"CREATE STREAM {ORDERS} (id INT KEY, email STRING, amount DOUBLE) "
              f"WITH (KAFKA_TOPIC='{ORDERS}', PARTITIONS=1, VALUE_FORMAT='JSON');\n"
              f"-- two orders; the comment's ; does not split\n"
              f"INSERT INTO {ORDERS} (id, email, amount) VALUES (1, '{EMAIL}', 25.0);\n"
              f"INSERT INTO {ORDERS} (id, email, amount) VALUES (2, 'bob@example.com', 3.5);\n")
    rc, out, err = ksql(["exec", script, "--dry-run"])
    plan = parse_json(out) or {}
    cats = [s.get("categories") for s in plan.get("statements", [])]
    check("exec --dry-run classifies each statement",
          rc == 0 and cats == [["ksql.create", "topics.create"], ["ksql.insert", "produce"],
                               ["ksql.insert", "produce"]], tail(out + err))
    check("a dry run sends nothing", ORDERS.upper() not in names("streams"))

    # A file, read from disk.
    sql_file = tmp / "orders.sql"
    sql_file.write_text(script, encoding="utf-8")
    rc, out, err = ksql(["exec", "-f", str(sql_file), "--yes"])
    done = parse_json(out) or {}
    check("exec -f creates the stream and inserts", rc == 0 and done.get("ok") is True
          and [s.get("status") for s in done.get("statements", [])] == ["SUCCESS"] * 3, tail(out + err))

    rc, out, err = ksql(["streams"])
    check("ksql streams lists it", rc == 0 and any(s["name"] == ORDERS.upper() for s in parse_json(out) or []),
          tail(out + err))
    rc, out, err = ksql(["describe", ORDERS])
    d = parse_json(out) or {}
    check("ksql describe shows columns and topic",
          rc == 0 and [f["name"] for f in d.get("fields", [])] == ["ID", "EMAIL", "AMOUNT"]
          and d.get("topic") == ORDERS, tail(out + err))

    # --- queries ----------------------------------------------------------
    rows = wait_rows(f"SELECT * FROM {ORDERS};", 2)
    check("pull query returns the rows with columns", len(rows) == 2 and {r["ID"] for r in rows} == {1, 2},
          f"rows={rows}")
    rc, out, err = ksql(["query", f"SELECT id, email FROM {ORDERS} EMIT CHANGES;", "--from-beginning",
                         "-n", "2", "--mask-pii"], timeout=90)
    data = parse_json(out) or {}
    check("push query stops at the row limit", rc == 0 and data.get("end") == "row_limit"
          and len(data.get("rows", [])) == 2 and data.get("push") is True, tail(out + err))
    check("push query rows are masked", data.get("masked") is True and EMAIL not in out
          and "example.com" in out, tail(out))
    code, body = close_query(data.get("query_id") or "none")
    check("the push query was closed on the server", code == 400 and "No query with id" in body,
          f"{code} {tail(body)}")
    started = time.time()
    rc, out, err = ksql(["query", f"SELECT * FROM {ORDERS} EMIT CHANGES;", "--timeout", "3"], timeout=60)
    data = parse_json(out) or {}
    check("push query stops at the time limit", rc == 0 and data.get("end") == "time_limit"
          and time.time() - started < 30, tail(out + err))
    rc, out, err = ksql(["query", f"SELECT * FROM {ORDERS} EMIT CHANGES LIMIT 1;", "--from-beginning"],
                        timeout=60)
    data = parse_json(out) or {}
    check("a query's own LIMIT ends it on the server", rc == 0 and data.get("end") == "completed"
          and len(data.get("rows", [])) == 1, tail(out + err))
    rc, out, err = ksql(["query", f"SELECT * FROM {ORDERS};"], json_out=False)
    check("table output", rc == 0 and "EMAIL" in out and "AMOUNT" in out, tail(out + err))
    rc, out, err = ksql(["query", f"DROP STREAM {ORDERS};"])
    check("query refuses a statement", rc != 0 and "ksql exec" in err and ORDERS.upper() in names("streams"),
          tail(err))
    rc, out, err = ksql(["exec", f"SELECT * FROM {ORDERS};", "--yes"])
    check("exec refuses a query", rc != 0 and "ksql query" in err, tail(err))
    rc, out, err = ksql(["query", "SELECT * FROM no_such_stream_xyz;"])
    check("a server error is reported", rc != 0 and "NO_SUCH_STREAM_XYZ" in err.upper(), tail(err))

    # --- persistent query, terminate, drop --------------------------------
    rc, out, err = ksql(["exec", f"CREATE TABLE {TOTALS} AS SELECT id, SUM(amount) AS total "
                                 f"FROM {ORDERS} GROUP BY id EMIT CHANGES;", "--yes"])
    done = parse_json(out) or {}
    query_id = (done.get("statements") or [{}])[0].get("query_id")
    check("CREATE TABLE AS SELECT starts a query", rc == 0 and bool(query_id), tail(out + err))
    rc, out, err = ksql(["queries"])
    check("ksql queries lists it", rc == 0 and any(q["id"] == query_id for q in parse_json(out) or []),
          tail(out + err))
    rc, out, err = ksql(["exec", f"TERMINATE {query_id}; DROP TABLE {TOTALS} DELETE TOPIC;", "--yes"])
    check("TERMINATE and DROP TABLE ... DELETE TOPIC", rc == 0 and TOTALS.upper() not in names("tables"),
          tail(out + err))

    # --- governance -------------------------------------------------------
    rc, out, err = ksql(["exec", f"CREATE STREAM {BIG} AS SELECT * FROM {ORDERS} WHERE amount > 10;", "--yes"],
                        profile=RO_PROFILE)
    check("a read-only profile refuses writes and sends nothing",
          rc != 0 and "read-only" in err and BIG.upper() not in names("streams"), tail(err))
    rc, out, err = ksql(["streams"], profile=RO_PROFILE)
    check("a read-only profile still reads", rc == 0, tail(err))
    rc, out, err = ksql(["exec", f"CREATE STREAM {FAIL} (v STRING) WITH (KAFKA_TOPIC='{FAIL}', PARTITIONS=1, "
                                 f"VALUE_FORMAT='JSON'); INSERT INTO no_such_stream_xyz (v) VALUES ('x'); "
                                 f"DROP STREAM {FAIL} DELETE TOPIC;", "--yes"])
    done = parse_json(out) or {}
    statuses = [s.get("status") for s in done.get("statements", [])]
    check("a failing statement stops the script", rc != 0 and statuses == ["SUCCESS", "ERROR", "NOT RUN"]
          and FAIL.upper() in names("streams"), f"{statuses} {tail(err)}")
    rc, out, err = ksql(["exec", "--yes", "-f", "-"], stdin=f"DROP STREAM {FAIL} DELETE TOPIC;\n")
    check("exec -f - reads stdin", rc == 0 and FAIL.upper() not in names("streams"), tail(out + err))

    entries = [e for e in audit_entries() if e.get("action") == "ksql exec"]
    targets = [e["target"] for e in entries]
    check("writes are audited before and after", any(t == f"CREATE TABLE {TOTALS.upper()} AS SELECT"
                                                    for t in targets)
          and any(e["outcome"].startswith("ok: SUCCESS, query ") for e in entries)
          and any(e["outcome"] == "requested" for e in entries), f"{targets[:6]}")
    check("a refused read-only write is audited", any(not e["ok"] and e["profile"] == RO_PROFILE for e in entries))
    check("failures and skipped statements are audited",
          any(e["outcome"].startswith("failed: ksqlDB error") for e in entries)
          and any(e["outcome"].startswith("not run") for e in entries))
    log = (CONFIG / "audit.log").read_text(encoding="utf-8") if (CONFIG / "audit.log").exists() else ""
    check("the audit log holds no record values", EMAIL not in log and "bob@example.com" not in log)

    # --- MCP --------------------------------------------------------------
    mcp = Mcp(PROFILE)
    try:
        mcp.request("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                   "clientInfo": {"name": "ksql-smoke", "version": "1"}})
        tools = [t["name"] for t in mcp.request("tools/list")["result"]["tools"]]
        check("MCP lists the ksqlDB tools", {"ksql_list", "ksql_describe", "ksql_query"} <= set(tools), tools)
        err_, data, text = mcp.call("ksql_list", {"kind": "streams", "filter": ORDERS.upper()})
        check("MCP ksql_list", not err_ and data and data.get("count") == 1, tail(text))
        err_, data, text = mcp.call("ksql_describe", {"name": ORDERS})
        check("MCP ksql_describe", not err_ and data and data["description"]["topic"] == ORDERS, tail(text))
        err_, data, text = mcp.call("ksql_query", {"sql": f"SELECT * FROM {ORDERS} EMIT CHANGES;",
                                                   "from_beginning": True, "limit": 2, "timeout_secs": 20})
        check("MCP ksql_query returns masked rows", not err_ and data and data.get("count") == 2
              and data.get("masked") is True and EMAIL not in text, tail(text))
        err_, data, text = mcp.call("ksql_query", {"sql": f"DROP STREAM {ORDERS};"})
        check("MCP ksql_query refuses statements", err_ and ORDERS.upper() in names("streams"), tail(text))
    finally:
        mcp.close()


def secure_scenarios():
    """The --tls-url server: HTTPS, basic auth and a required client
    certificate. No output and no audit entry may show the password."""
    pw, user = ARGS.tls_password, ARGS.tls_user
    url = ARGS.tls_url.rstrip("/")
    host = urllib.parse.urlsplit(url).netloc
    sec, no_cert, no_ca, bad_pw = (f"{RUN}sec", f"{RUN}nocert", f"{RUN}noca", f"{RUN}badpw")
    outputs = []

    def add(name, password, ca=True, cert=True):
        args = ["profile", "add", name, "-b", ARGS.broker, "--ksql-url", url, "--ksql-user", user,
                "--ksql-password-stdin"]
        if ca:
            args += ["--ksql-ca-location", ARGS.tls_ca]
        if cert:
            args += ["--ksql-certificate-location", ARGS.tls_cert, "--ksql-key-location", ARGS.tls_key]
        rc, out, err = ferrika(args, stdin=password + "\n")
        outputs.append(out + err)
        return rc, out + err

    def run(args, profile=sec, **kw):
        rc, out, err = ksql(args, profile=profile, **kw)
        outputs.append(out + err)
        return rc, out, err

    rc, out = add(sec, pw)
    check("TLS: profile add with basic auth, CA, client certificate and key", rc == 0, tail(out))
    for name, password, ca, cert in ((no_cert, pw, True, False), (no_ca, pw, False, True),
                                     (bad_pw, pw + "-wrong", True, True)):
        add(name, password, ca, cert)
    rc, out, err = ferrika(["-f", "json", "profile", "show", sec])
    outputs.append(out + err)
    shown = (parse_json(out) or {}).get("ksql_url") or ""
    check("TLS: profile show names the server without the password",
          rc == 0 and host in shown and pw not in shown, tail(out + err))

    rc, out, err = run(["info"])
    info = parse_json(out) or {}
    check("TLS: ksql info over HTTPS with basic auth and a client certificate",
          rc == 0 and info.get("server_status") == "RUNNING", tail(out + err))
    script = (f"CREATE STREAM {SECURE} (id INT KEY, email STRING) WITH (KAFKA_TOPIC='{SECURE}', "
              f"PARTITIONS=1, VALUE_FORMAT='JSON');\n"
              f"INSERT INTO {SECURE} (id, email) VALUES (1, '{EMAIL}');\n")
    rc, out, err = run(["exec", script, "--yes"])
    done = parse_json(out) or {}
    check("TLS: ksql exec creates a stream and inserts", rc == 0 and done.get("ok") is True, tail(out + err))
    rows = wait_rows(f"SELECT * FROM {SECURE};", 1, profile=sec, extra=["--mask-pii"])
    outputs.append(json.dumps(rows))
    check("TLS: a pull query reads the row, masked", len(rows) == 1 and EMAIL not in json.dumps(rows),
          f"rows={rows}")
    rc, out, err = run(["query", f"SELECT * FROM {SECURE} EMIT CHANGES;", "--from-beginning", "-n", "1",
                        "--mask-pii"], timeout=90)
    data = parse_json(out) or {}
    check("TLS: a push query streams over HTTPS", rc == 0 and data.get("end") == "row_limit", tail(out + err))

    rc, out, err = run(["info"], profile=no_cert)
    check("TLS: without the client certificate the server refuses", rc != 0, tail(out + err))
    rc, out, err = run(["info"], profile=no_ca)
    check("TLS: without the CA the server certificate is not trusted",
          rc != 0 and "certificate" in (out + err).lower(), tail(out + err))
    rc, out, err = run(["info"], profile=bad_pw)
    check("TLS: a wrong password is refused (HTTP 401)", rc != 0 and "401" in out + err, tail(out + err))
    typed = url.replace("://", f"://{user}:{pw}@", 1)
    rc, out, err = ferrika(["ksql", "--ksql-url", typed, "info"])
    outputs.append(out + err)
    check("TLS: a hand-typed URL with credentials fails without the CA and prints no password",
          rc != 0 and pw not in out + err, tail(out + err))

    mcp = Mcp(sec, ["--allow-write"])
    try:
        mcp.request("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                   "clientInfo": {"name": "ksql-smoke", "version": "1"}})
        err_, data, text = mcp.call("ksql_list", {"kind": "streams", "filter": SECURE.upper()})
        outputs.append(text)
        check("TLS: MCP ksql_list on the HTTPS server", not err_ and data and data.get("count") == 1
              and data.get("server") == host, tail(text))
        args = {"sql": f"INSERT INTO {SECURE} (id, email) VALUES (2, 'x');"}
        err_, plan, text = mcp.call("ksql_exec", {**args, "mode": "plan"})
        outputs.append(text)
        token = ((plan or {}).get("confirmation") or {}).get("token")
        err_, data, text = mcp.call("ksql_exec", {**args, "mode": "execute", "confirmation": token})
        outputs.append(text)
        check("TLS: MCP ksql_exec plans and runs on the HTTPS server",
              token and not err_ and (data or {}).get("executed") is True, tail(text))
    finally:
        mcp.close()
        outputs.append("".join(mcp.proc.stderr.read() or "") if mcp.proc.stderr else "")

    rc, out, err = run(["exec", f"DROP STREAM {SECURE} DELETE TOPIC;", "--yes"])
    check("TLS: DROP STREAM ... DELETE TOPIC", rc == 0 and SECURE.upper() not in names("streams", True),
          tail(out + err))
    leaked = [i for i, o in enumerate(outputs) if pw in o]
    check("TLS: no output shows the password", not leaked, f"outputs={leaked}")
    log = (CONFIG / "audit.log").read_text(encoding="utf-8") if (CONFIG / "audit.log").exists() else ""
    entries = [e for e in audit_entries() if e.get("profile") == sec]
    check("TLS: writes are audited without the password or record values",
          entries and pw not in log and EMAIL not in log, f"{len(entries)} entries")


def cleanup():
    if ARGS.tls_url and SECURE.upper() in names("streams", True):
        server(f"DROP STREAM IF EXISTS {SECURE} DELETE TOPIC;", True)
    for name in (BIG, FAIL, ORDERS):
        if name.upper() in names("streams"):
            server(f"DROP STREAM IF EXISTS {name} DELETE TOPIC;")
    if TOTALS.upper() in names("tables"):
        server(f"DROP TABLE IF EXISTS {TOTALS} DELETE TOPIC;")
    left = [n for n in names("streams") + names("tables") if n.startswith(RUN.upper())]
    print(f"\ncleanup: left {left}")
    return not left


if not Path(F).exists():
    sys.exit(f"binary not found: {F} (build with `cargo build -p ferrika-cli`)")
print(f"ferrika ksql smoke: binary={F} url={URL} run={RUN}\n")
try:
    with urllib.request.urlopen(f"{URL}/info", timeout=10) as r:
        r.read()
except (urllib.error.URLError, OSError) as e:
    sys.exit(f"cannot reach the ksqlDB server at {URL}: {e}")
clean = False
try:
    scenarios()
    if ARGS.tls_url:
        secure_scenarios()
finally:
    try:
        clean = cleanup()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
check("cleanup leaves no streams or tables of this run", clean)
print(f"{sum(results)}/{len(results)} passed")
sys.exit(0 if all(results) else 1)
