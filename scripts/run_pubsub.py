#!/usr/bin/env python3
"""Execute the actual Pub/Sub integration suite and report local-only coverage."""

import hashlib
import json
import os
import socket
import subprocess
from pathlib import Path


def main():
    if os.environ.get("GCP_LOCAL_ISOLATED") != "1" or {
        name for _, name in socket.if_nameindex()
    } != {"lo"}:
        raise SystemExit("Run scripts/check_pubsub.sh in moo/Linux isolation")
    root = Path(__file__).resolve().parents[1]
    command = [
        str(root / ".tools/go/bin/go"),
        "tool",
        "test2json",
        "-t",
        "-p",
        "pubsub",
        str(root / "bin/pubsub-integration"),
        "-test.v=test2json",
        "-test.timeout=100s",
    ]
    result = subprocess.run(
        command, cwd=root / "internal/providers/pubsub", capture_output=True, text=True
    )
    tests, output = {}, []
    for line in result.stdout.splitlines():
        event = json.loads(line)
        if "Test" in event and event["Action"] in ("run", "pass", "fail", "skip"):
            tests[event["Test"]] = {"status": event["Action"], "seconds": event.get("Elapsed")}
        if "Output" in event:
            output.append(event["Output"])
    manifest = json.loads((root / "toolchains/pubsub.json").read_text())
    source = hashlib.sha256()
    for directory in (
        "internal/providers/pubsub",
        "internal/backend",
        "internal/gateway",
        "internal/iam",
        "internal/identity",
        "api/generated",
    ):
        for path in sorted((root / directory).rglob("*.go")):
            source.update(str(path.relative_to(root)).encode())
            source.update(path.read_bytes())
    for path in sorted((root / "internal/providers/pubsub/testdata").glob("*.py")):
        source.update(path.read_bytes())
    for name in ("pubsub.py", "pubsub_scope.py"):
        source.update((root / "gcp_validation" / name).read_bytes())
    versions = subprocess.check_output(
        [
            os.environ["GCP_LOCAL_TEST_PYTHON"],
            "-c",
            "import importlib.metadata,json; "
            "print(json.dumps({n: importlib.metadata.version(n) "
            'for n in ("google-cloud-pubsub", "grpcio", "google-auth")}))',
        ],
        text=True,
    )
    report = {
        "kind": "local-provider",
        "service": "pubsub",
        "gcp_execution": False,
        "cloud_fidelity": "unverified",
        "network": "loopback-only",
        "race_detector": True,
        "backend_version": manifest["version"],
        "backend_sha256": manifest["jar_sha256"],
        "source_sha256": source.hexdigest(),
        "sdk_versions": json.loads(versions),
        "tests": tests,
        "passed": result.returncode == 0
        and bool(tests)
        and all(t["status"] == "pass" for t in tests.values()),
    }
    directory = root / ".artifacts/pubsub-provider"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    print("".join(output), end="")
    if result.stderr:
        print(result.stderr)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
