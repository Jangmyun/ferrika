#!/usr/bin/env python3
"""End-to-end smoke test of `ferrika mqtt` against a real MQTT broker.

Checks what unit tests cannot: MQTT 3.1.1 and 5 as a live broker speaks
them. The script runs the CLI for profiles, bounded subscriptions, publish
(QoS 0, 1 and 2), retained messages (list and clear), read-only profiles,
shared-subscription refusal, masking, TLS (optional) and the audit log.

Start a throw-away Mosquitto first, with a user and TLS (needs openssl and
docker; see tools/e2e/mosquitto-setup.sh):

    sh tools/e2e/mosquitto-setup.sh /tmp/ferrika-mqtt zz zz-pw
    docker run -d --name ferrika-mqtt-test -p 1883:1883 -p 8883:8883 \
        -v /tmp/ferrika-mqtt:/mosquitto/config eclipse-mosquitto:2

Run (needs only the Python 3 standard library):

    cargo build -p ferrika-cli
    python3 tools/e2e/mqtt_smoke.py target/debug/ferrika --url mqtt://localhost:1883 \
        --user zz --password zz-pw [--tls-url mqtts://localhost:8883 --ca-file /tmp/ferrika-mqtt/ca.pem]

Isolation: every topic and profile is named <prefix><run id>..., and the CLI
runs with its own FERRIKA_CONFIG_DIR (HOME and USERPROFILE too, under
--work-dir or a new temporary directory), so the user's profiles and audit
log are not touched. The run's retained messages are cleared at the end,
even after a failure. With FERRIKA_SECRET_STORE=file (CI) the profile
password is kept in the profile file, which must then be private (mode
0600 on POSIX); otherwise it must go to the OS credential store and not be
in the file. Exit code 0 when every check passes, 1 when any fails.

Use a test broker only: the script publishes and clears retained messages.
"""
import argparse
import json
import os
import shutil
import stat
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
parser.add_argument("--url", default="mqtt://localhost:1883", help="broker (default mqtt://localhost:1883)")
parser.add_argument("--user", required=True)
parser.add_argument("--password", required=True)
parser.add_argument("--tls-url", help="the broker's TLS listener, e.g. mqtts://localhost:8883")
parser.add_argument("--ca-file", help="CA (PEM) of the TLS listener's certificate")
parser.add_argument("--prefix", default="zz-", help="name prefix of everything the run creates (default zz-)")
parser.add_argument("--work-dir", help="parent of the run's config directory (default: a new temp dir)")
ARGS = parser.parse_args()

F = ARGS.binary
RUN = f"{ARGS.prefix}{uuid.uuid4().hex[:6]}"  # unique per run; also the topic root
PROFILE, RO_PROFILE, V5_PROFILE = f"{RUN}-p", f"{RUN}-ro", f"{RUN}-v5"
# Message bodies: must never show up in the audit log.
BODY = f"body-{uuid.uuid4().hex[:10]}"
FILE_STORE = os.environ.get("FERRIKA_SECRET_STORE", "").lower() == "file"
results = []
retained_topics = set()

if ARGS.work_dir:
    Path(ARGS.work_dir).mkdir(parents=True, exist_ok=True)
tmp = Path(tempfile.mkdtemp(prefix="ferrika-mqtt-smoke-", dir=ARGS.work_dir))
HOME = tmp / "home"
HOME.mkdir()
CONFIG = HOME / ".ferrika"
ENV = dict(os.environ)
ENV.update({"HOME": str(HOME), "USERPROFILE": str(HOME), "FERRIKA_CONFIG_DIR": str(CONFIG),
            "NO_COLOR": "1"})
for key in ("FERRIKA_MQTT_PROFILE", "FERRIKA_MQTT_URL", "FERRIKA_MQTT_USER", "FERRIKA_MQTT_PASSWORD",
            "FERRIKA_MQTT_VERSION", "FERRIKA_MQTT_CA_FILE", "FERRIKA_PROFILE"):
    ENV.pop(key, None)


def topic(name):
    return f"{RUN}/{name}"


def ferrika(args, stdin=None, timeout=60, env=None):
    """Runs `ferrika ...`; returns (rc, stdout, stderr)."""
    try:
        p = subprocess.run([F] + args, capture_output=True, text=True, encoding="utf-8", errors="replace",
                           timeout=timeout, env=env or ENV, input=stdin)
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired as ex:
        return "timeout", str(ex.stdout or ""), str(ex.stderr or "")


def mqtt(args, profile=PROFILE, json_output=False, **kw):
    """Runs `ferrika [-f json] mqtt [--mqtt-profile P] ...`."""
    head = (["-f", "json"] if json_output else []) + ["mqtt"] + (["--mqtt-profile", profile] if profile else [])
    return ferrika(head + args, **kw)


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"   [{detail}]" if detail and not ok else ""), flush=True)


def tail(s, n=240):
    return str(s)[-n:].replace("\n", " | ")


def parse_json(text):
    try:
        return json.loads(text)
    except ValueError:
        return None


def json_lines(text):
    return [v for v in (parse_json(line) for line in text.splitlines() if line.strip()) if v is not None]


def background(args, **kw):
    """Starts `mqtt(args)` in a thread; returns a function giving its result."""
    box = {}
    thread = threading.Thread(target=lambda: box.update(result=mqtt(args, **kw)), daemon=True)
    thread.start()

    def result():
        thread.join(timeout=90)
        return box.get("result", ("timeout", "", ""))
    return result


def retained(filter_, profile=PROFILE, extra=()):
    rc, out, err = mqtt(["retained", filter_, "--wait", "5", *extra], profile=profile, json_output=True)
    doc = parse_json(out) or {}
    return rc, {m.get("topic"): m for m in doc.get("messages") or []}, out + err


def scenarios():
    # --- profiles ---------------------------------------------------------
    rc, out, err = mqtt(["profile", "add", PROFILE, "--mqtt-url", ARGS.url, "--mqtt-user", ARGS.user,
                         "--mqtt-password-stdin"], profile=None, stdin=ARGS.password + "\n")
    check("mqtt profile add", rc == 0, f"rc={rc} {tail(out + err)}")
    rc, out, err = mqtt(["profile", "add", RO_PROFILE, "--read-only", "--mqtt-url", ARGS.url, "--mqtt-user",
                         ARGS.user, "--mqtt-password-stdin"], profile=None, stdin=ARGS.password + "\n")
    check("mqtt profile add --read-only", rc == 0, f"rc={rc} {tail(out + err)}")
    rc, out, err = mqtt(["profile", "add", V5_PROFILE, "--mqtt-version", "5", "--mqtt-url", ARGS.url,
                         "--mqtt-user", ARGS.user, "--mqtt-password-stdin"], profile=None,
                        stdin=ARGS.password + "\n")
    check("mqtt profile add --mqtt-version 5", rc == 0, f"rc={rc} {tail(out + err)}")
    rc, out, err = mqtt(["profile", "list"], profile=None, json_output=True)
    views = {v.get("name"): v for v in (parse_json(out) or [])}
    mine = views.get(PROFILE, {})
    check("mqtt profile list --json: url, user, version, read-only flag, no password",
          rc == 0 and mine.get("user") == ARGS.user and mine.get("url") == ARGS.url
          and mine.get("version") == "3.1.1" and views.get(V5_PROFILE, {}).get("version") == "5"
          and views.get(RO_PROFILE, {}).get("read_only") is True and mine.get("read_only") is False
          and ARGS.password not in out, f"rc={rc} {tail(out + err)}")
    path = CONFIG / "mqtt-profiles.json"
    text = path.read_text(encoding="utf-8") if path.exists() else ""
    where = mine.get("password")
    if FILE_STORE:
        check("FERRIKA_SECRET_STORE=file: the password is kept in the profile file", where == "file",
              f"password={where}")
        if os.name == "posix":
            mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else None
            check("mqtt-profiles.json is private (0600)", mode == 0o600, f"mode={oct(mode or 0)}")
    elif where == "os-credential-store":
        check("password in the OS credential store, not in mqtt-profiles.json",
              path.exists() and ARGS.password not in text, "password found in the file")
    else:
        check("password in the OS credential store (or set FERRIKA_SECRET_STORE=file)", False, f"password={where}")

    # --- login and argument checks ----------------------------------------
    rc, out, err = mqtt(["--mqtt-url", ARGS.url, "--mqtt-user", ARGS.user, "retained", topic("x")], profile=None,
                        env={**ENV, "FERRIKA_MQTT_PASSWORD": f"wrong-{BODY}"})
    check("a wrong password is refused by the broker", rc not in (0, "timeout") and "refused" in err,
          f"rc={rc} {tail(out + err)}")
    rc, out, err = mqtt(["sub", f"$share/{RUN}/{RUN}/#"])
    check("a shared subscription is refused", rc not in (0, "timeout") and "shared subscription" in err,
          f"rc={rc} {tail(out + err)}")
    rc, out, err = mqtt(["pub", topic("empty"), "-m", "", "--retain"])
    check("an empty retained publish points to retained clear",
          rc not in (0, "timeout") and "retained clear" in err, f"rc={rc} {tail(out + err)}")

    # --- publish and retained ---------------------------------------------
    state = topic("dev/1/state")
    rc, out, err = mqtt(["pub", state, "-m", f'{{"body":"{BODY}","email":"kim@example.com"}}', "--retain"],
                        json_output=True)
    retained_topics.add(state)
    doc = parse_json(out) or {}
    check("pub --retain (QoS 1) is acknowledged",
          rc == 0 and doc.get("acknowledged") is True and doc.get("retain") is True, f"rc={rc} {tail(out + err)}")
    other = topic("dev/2/state")
    rc, out, err = mqtt(["pub", other, "-m", f"{BODY}-2", "--retain", "--qos", "2"])
    retained_topics.add(other)
    check("pub --retain --qos 2", rc == 0 and "Published" in out, f"rc={rc} {tail(out + err)}")
    rc, out, err = mqtt(["pub", topic("dev/3/live"), "-m", f"{BODY}-live", "--qos", "0"])
    check("pub --qos 0 (not retained)", rc == 0 and "Sent" in out, f"rc={rc} {tail(out + err)}")

    rc, found, detail = retained(topic("dev/+/state"))
    check("retained --json lists the two retained messages",
          rc == 0 and set(found) == {state, other} and BODY in (found.get(state, {}).get("payload") or ""),
          f"rc={rc} {tail(detail)}")
    check("retained leaves out topics without a retained message", topic("dev/3/live") not in found)
    rc, found, detail = retained(topic("dev/+/state"), extra=["--mask-pii"])
    check("retained --mask-pii masks the e-mail address",
          rc == 0 and "kim@example.com" not in detail and state in found, f"rc={rc} {tail(detail)}")
    rc, out, err = mqtt(["retained", topic("dev/#")])
    check("retained prints a table", rc == 0 and "TOPIC" in out and state in out, f"rc={rc} {tail(out + err)}")

    # --- bounded subscriptions --------------------------------------------
    live = topic("live/a")
    result = background(["sub", topic("live/#"), "-n", "2", "--duration", "30", "--qos", "2"], json_output=True)
    time.sleep(2)
    mqtt(["pub", live, "-m", f"{BODY}-s1", "--qos", "0"])
    mqtt(["pub", live, "-m", f"{BODY}-s2", "--qos", "2"])
    rc, out, err = result()
    lines = json_lines(out)
    check("sub -n 2 stops after two messages (JSON Lines)",
          rc == 0 and [m.get("payload") for m in lines] == [f"{BODY}-s1", f"{BODY}-s2"] and "at --count" in err,
          f"rc={rc} {tail(out + err)}")
    start = time.time()
    rc, out, err = mqtt(["sub", topic("quiet/#"), "--duration", "2"])
    check("sub --duration stops on time without messages",
          rc == 0 and "at --duration" in err and time.time() - start < 15, f"rc={rc} {tail(out + err)}")
    result = background(["sub", topic("dev/+/state"), "-n", "2", "--duration", "10"], json_output=True)
    rc, out, err = result()
    check("sub gets retained messages first, flagged retained",
          rc == 0 and len(json_lines(out)) == 2 and all(m.get("retain") for m in json_lines(out)),
          f"rc={rc} {tail(out + err)}")

    # --- MQTT 5: user properties and content type -------------------------
    v5_topic = topic("v5/a")
    result = background(["sub", topic("v5/#"), "-n", "1", "--duration", "30"], profile=V5_PROFILE, json_output=True)
    time.sleep(2)
    rc, out, err = mqtt(["pub", v5_topic, "-m", f"{BODY}-v5", "-H", "trace:t-1", "--content-type", "text/plain"],
                        profile=V5_PROFILE)
    check("MQTT 5 pub with a user property and a content type", rc == 0, f"rc={rc} {tail(out + err)}")
    rc, out, err = result()
    got = (json_lines(out) or [{}])[0]
    check("MQTT 5 sub shows the user property and the content type",
          rc == 0 and got.get("user_properties") == {"trace": "t-1"} and got.get("content_type") == "text/plain",
          f"rc={rc} {tail(out + err)}")
    rc, out, err = mqtt(["pub", v5_topic, "-m", "x", "-H", "k:v"])
    check("user properties need MQTT 5", rc not in (0, "timeout") and "MQTT 5" in err, f"rc={rc} {tail(out + err)}")

    # --- TLS --------------------------------------------------------------
    if ARGS.tls_url:
        ca = ["--mqtt-ca-file", ARGS.ca_file] if ARGS.ca_file else []
        tls_profile = f"{RUN}-tls"
        rc, out, err = mqtt(["profile", "add", tls_profile, "--mqtt-url", ARGS.tls_url, *ca, "--mqtt-user",
                             ARGS.user, "--mqtt-password-stdin"], profile=None, stdin=ARGS.password + "\n")
        check("mqtt profile add with mqtts:// and a CA file", rc == 0, f"rc={rc} {tail(out + err)}")
        rc, found, detail = retained(topic("dev/+/state"), profile=tls_profile)
        check("retained over TLS", rc == 0 and set(found) == {state, other}, f"rc={rc} {tail(detail)}")
        rc, out, err = mqtt(["--mqtt-url", ARGS.tls_url, "--mqtt-user", ARGS.user, "retained", topic("x")],
                            profile=None, env={**ENV, "FERRIKA_MQTT_PASSWORD": ARGS.password})
        check("TLS without the test CA is refused (certificate not trusted)",
              rc not in (0, "timeout") and "TLS" in err, f"rc={rc} {tail(out + err)}")
        mqtt(["profile", "remove", tls_profile], profile=None)

    # --- read-only profile ------------------------------------------------
    for label, args in (("pub", ["pub", topic("ro"), "-m", f"{BODY}-ro"]),
                        ("retained clear", ["retained", "clear", state, "--yes"])):
        rc, out, err = mqtt(args, profile=RO_PROFILE)
        check(f"read-only profile refuses {label}", rc not in (0, "timeout") and "read-only" in err,
              f"rc={rc} {tail(out + err)}")
    rc, found, detail = retained(topic("dev/+/state"), profile=RO_PROFILE)
    check("read-only profile still lists retained messages, and nothing was cleared",
          rc == 0 and set(found) == {state, other}, f"rc={rc} {tail(detail)}")

    # --- retained clear ---------------------------------------------------
    rc, out, err = mqtt(["retained", "clear", other], stdin="n\n")
    check("retained clear asks first; 'n' changes nothing",
          rc not in (0, "timeout") and "cancelled" in err and "bytes" in err, f"rc={rc} {tail(out + err)}")
    rc, out, err = mqtt(["retained", "clear", other, "--yes"], json_output=True)
    doc = parse_json(out) or {}
    check("retained clear --yes clears the message",
          rc == 0 and doc.get("cleared") is True and doc.get("previous_bytes") == len(f"{BODY}-2"),
          f"rc={rc} {tail(out + err)}")
    rc, found, detail = retained(topic("dev/+/state"))
    check("the cleared message is gone, the other stays", rc == 0 and set(found) == {state}, f"rc={rc} {tail(detail)}")
    rc, out, err = mqtt(["retained", "clear", other, "--yes"], json_output=True)
    doc = parse_json(out) or {}
    check("retained clear on a topic without one changes nothing", rc == 0 and doc.get("cleared") is False,
          f"rc={rc} {tail(out + err)}")

    # --- audit log --------------------------------------------------------
    rc, out, err = ferrika(["-f", "json", "audit", "--source", "cli", "-n", "500"])
    entries = parse_json(out) or []
    mine = [e for e in entries if RUN in (e.get("target") or "")]
    actions = {e.get("action") for e in mine}
    check("audit log has the mqtt writes of this run", rc == 0 and {"mqtt pub", "mqtt retained clear"} <= actions,
          f"rc={rc} actions={sorted(actions)} {tail(out + err)}")
    check("audit log records the read-only refusals", any(not e.get("ok") for e in mine), tail(out))
    check("audit log records the clear with the cleared size",
          any(e.get("action") == "mqtt retained clear" and "cleared the retained message" in (e.get("outcome") or "")
              for e in mine), tail(out))
    check("sub and retained listings are not audited as writes",
          not actions & {"mqtt sub", "mqtt retained"}, f"actions={sorted(actions)}")
    check("argument errors leave no audit entry",
          not any(topic("empty") in (e.get("target") or "") for e in entries), tail(out))
    leaked = [p.name for p in CONFIG.rglob("*") if p.is_file() and BODY in p.read_text(encoding="utf-8", errors="replace")]
    check("no message body in the audit log or any config file", rc == 0 and BODY not in out and not leaked,
          f"files={leaked}")

    # --- profile remove ---------------------------------------------------
    for name in (PROFILE, RO_PROFILE, V5_PROFILE):
        rc, out, err = mqtt(["profile", "remove", name], profile=None)
        check(f"mqtt profile remove {name}", rc == 0, f"rc={rc} {tail(out + err)}")


def cleanup():
    """Clears the run's retained messages through a temporary profile."""
    name = f"{RUN}-cleanup"
    mqtt(["profile", "add", name, "--mqtt-url", ARGS.url, "--mqtt-user", ARGS.user, "--mqtt-password-stdin"],
         profile=None, stdin=ARGS.password + "\n")
    for t in sorted(retained_topics):
        mqtt(["retained", "clear", t, "--yes"], profile=name)
    rc, found, _ = retained(f"{RUN}/#", profile=name)
    for p in (PROFILE, RO_PROFILE, V5_PROFILE, f"{RUN}-tls", name):
        mqtt(["profile", "remove", p], profile=None)
    print(f"\ncleanup: cleared {len(retained_topics)} retained topic(s), left: {sorted(found)}")
    return rc == 0 and not found


if not Path(F).exists():
    sys.exit(f"binary not found: {F} (build with `cargo build -p ferrika-cli`)")
print(f"ferrika mqtt smoke: binary={F} url={ARGS.url} run={RUN} secret store={'file' if FILE_STORE else 'os'}\n")
clean = False
try:
    scenarios()
finally:
    try:
        clean = cleanup()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
check("cleanup leaves no retained messages of this run", clean)
print(f"{sum(results)}/{len(results)} passed")
sys.exit(0 if all(results) else 1)
