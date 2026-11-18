#!/usr/bin/env python3
"""Generate a CycloneDX 1.5 SBOM and a dependency license summary.

Uses `cargo metadata` only, so no extra cargo tools are needed. The SBOM
lists every crate the build can pull in (all target platforms, normal and
build dependencies, no dev-dependencies) with its version, license and the
checksum recorded in Cargo.lock.

    python tools/sbom.py              # default features
    python tools/sbom.py --features ssl
    python tools/sbom.py --package ferrika-cli    # the open-source CLI

The SBOM is for one workspace package, --package (default ferrika-cli).
`--features ssl` means `<package>/ssl`. Only third-party crates are listed;
the workspace's own crates are not.

Writes docs/sbom/<package>.cdx.json (or <package>-ssl.cdx.json) and
docs/sbom/LICENSES-<package>.md (LICENSES-<package>-ssl.md).
"""

import argparse
import datetime
import json
import re
import subprocess
import sys
import uuid
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "sbom"

# Licenses that need a closer look before shipping a closed-source build.
REVIEW = re.compile(r"\b(A?GPL|LGPL|MPL|EPL|CDDL|OSL|SSPL|BUSL|CC-BY-SA)", re.I)

# Native libraries compiled into the binary by -sys crates; their crate
# license does not describe the bundled C code.
BUNDLED_NATIVE = {
    "rdkafka-sys": ("librdkafka", "BSD-2-Clause (bundled sources also carry other permissive notices, see LICENSES.librdkafka)"),
    "openssl-src": ("OpenSSL", "Apache-2.0"),
    "openssl-sys": ("OpenSSL (linked, ssl feature)", "Apache-2.0 for OpenSSL 3.x"),
    "libz-sys": ("zlib", "Zlib"),
    "zstd-sys": ("zstd (rdkafka `zstd` feature)", "BSD-3-Clause (zstd is dual BSD-3-Clause OR GPL-2.0-only; Ferrika uses the BSD-3-Clause option)"),
}


def cargo_metadata(package, features):
    cmd = ["cargo", "metadata", "--format-version", "1", "--locked"]
    if features:
        cmd += ["--features", ",".join(f"{package}/{f}" for f in features.split(","))]
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
    if result.returncode != 0:
        sys.exit(result.stderr)
    return json.loads(result.stdout)


def root_id(meta, package):
    """Package id of the workspace member the SBOM is for."""
    for p in meta["packages"]:
        if p["name"] == package and p["id"] in meta["workspace_members"]:
            return p["id"]
    sys.exit(f"no workspace package named {package}")


def lock_checksums():
    checksums = {}
    text = (ROOT / "Cargo.lock").read_text(encoding="utf-8")
    for block in text.split("[[package]]"):
        name = re.search(r'^name = "(.+)"', block, re.M)
        version = re.search(r'^version = "(.+)"', block, re.M)
        checksum = re.search(r'^checksum = "(.+)"', block, re.M)
        if name and version and checksum:
            checksums[(name.group(1), version.group(1))] = checksum.group(1)
    return checksums


def needs_review(license):
    """True when no `OR` alternative of the expression is permissive."""
    if not license:
        return True
    alternatives = re.split(r"\s+OR\s+|\s*/\s*", license.replace("(", "").replace(")", ""))
    return all(REVIEW.search(alt) for alt in alternatives)


def linked_packages(meta, root):
    """Package ids compiled into the binary: normal edges only, not through
    build scripts or proc-macros (those run at compile time). Third-party
    crates only."""
    packages = {p["id"]: p for p in meta["packages"]}
    nodes = {n["id"]: n for n in meta["resolve"]["nodes"]}
    seen, stack = set(), [root]
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        if pid != root and any("proc-macro" in t["kind"] for t in packages[pid]["targets"]):
            continue
        seen.add(pid)
        for dep in nodes[pid]["deps"]:
            if any(k["kind"] is None for k in dep["dep_kinds"]):
                stack.append(dep["pkg"])
    return seen - set(meta["workspace_members"])


def shipped_packages(meta, root):
    """Package ids reachable from the root through normal and build edges
    (third-party crates only)."""
    nodes = {n["id"]: n for n in meta["resolve"]["nodes"]}
    seen, stack = set(), [root]
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        for dep in nodes[pid]["deps"]:
            kinds = {k["kind"] for k in dep["dep_kinds"]}
            if kinds - {"dev"}:
                stack.append(dep["pkg"])
    return seen - set(meta["workspace_members"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", default="")
    parser.add_argument("--package", default="ferrika-cli")
    args = parser.parse_args()

    meta = cargo_metadata(args.package, args.features)
    packages = {p["id"]: p for p in meta["packages"]}
    root_pid = root_id(meta, args.package)
    root = packages[root_pid]
    checksums = lock_checksums()
    shipped = sorted((packages[i] for i in shipped_packages(meta, root_pid)), key=lambda p: (p["name"], p["version"]))
    linked = linked_packages(meta, root_pid)

    components = []
    for p in shipped:
        component = {
            "type": "library",
            "bom-ref": f"pkg:cargo/{p['name']}@{p['version']}",
            "name": p["name"],
            "version": p["version"],
            "purl": f"pkg:cargo/{p['name']}@{p['version']}",
        }
        if p.get("license"):
            component["licenses"] = [{"expression": p["license"]}]
        checksum = checksums.get((p["name"], p["version"]))
        if checksum:
            component["hashes"] = [{"alg": "SHA-256", "content": checksum}]
        if p.get("repository"):
            component["externalReferences"] = [{"type": "vcs", "url": p["repository"]}]
        components.append(component)

    suffix = f"-{args.features}" if args.features else ""
    pkg_tag = f"-{args.package}"
    bom = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": f"urn:uuid:{uuid.uuid4()}",
        "version": 1,
        "metadata": {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tools": [{"name": "tools/sbom.py (cargo metadata)"}],
            "component": {
                "type": "application",
                "bom-ref": f"pkg:cargo/{root['name']}@{root['version']}",
                "name": root["name"],
                "version": root["version"],
                "purl": f"pkg:cargo/{root['name']}@{root['version']}",
                **({"licenses": [{"expression": root["license"]}]} if root.get("license") else {}),
            },
            "properties": [{"name": "cargo:features", "value": args.features or "default"}],
        },
        "components": components,
    }
    OUT.mkdir(parents=True, exist_ok=True)
    sbom_path = OUT / f"{args.package}{suffix}.cdx.json"
    sbom_path.write_text(json.dumps(bom, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    by_license = Counter(p.get("license") or "(none: see license_file)" for p in shipped)
    review = [p for p in shipped if needs_review(p.get("license"))]
    native = [(p["name"], *BUNDLED_NATIVE[p["name"]]) for p in shipped if p["name"] in BUNDLED_NATIVE]

    lines = [
        f"# Dependency licenses ({args.features or 'default'} features)",
        "",
        f"Generated by `tools/sbom.py` from Cargo.lock on {bom['metadata']['timestamp'][:10]}.",
        f"{len(shipped)} crates (all target platforms, no dev-dependencies), {len(linked)} of them compiled into the binary; the rest run only at build time (build scripts, proc-macros). SBOM: `{sbom_path.name}`.",
        "",
        "## Licenses",
        "",
        "| License expression | Crates |",
        "| --- | ---: |",
    ]
    lines += [f"| `{lic}` | {n} |" for lic, n in by_license.most_common()]
    lines += ["", "## Needs review", ""]
    if review:
        lines += ["Copyleft or missing SPDX expression with no permissive `OR` alternative.", ""]
        lines += ["| Crate | Version | License | In the binary |", "| --- | --- | --- | --- |"]
        lines += [f"| {p['name']} | {p['version']} | `{p.get('license') or 'license_file: ' + str(p.get('license_file'))}` | {'yes' if p['id'] in linked else 'no (build time only)'} |" for p in review]
        lines += [
            "",
            "MPL-2.0 is file-level copyleft: linking unmodified MPL crates into a closed or",
            "differently licensed binary is allowed, as long as the notice is kept and the",
            "MPL source stays available (it is, on crates.io). Modified MPL files must be",
            "published under MPL-2.0. Not legal advice.",
        ]
    else:
        lines.append("None.")
    lines += ["", "## Bundled native code", ""]
    if native:
        lines += ["| Crate | Native library | License |", "| --- | --- | --- |"]
        lines += [f"| {crate} | {lib} | {lic} |" for crate, lib, lic in native]
    else:
        lines.append("None.")
    (OUT / f"LICENSES{pkg_tag}{suffix}.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"{len(shipped)} crates -> {sbom_path.relative_to(ROOT)}, {len(review)} need review")


if __name__ == "__main__":
    main()
