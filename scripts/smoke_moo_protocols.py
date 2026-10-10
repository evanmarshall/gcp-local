#!/usr/bin/env python3
"""Check actual Pub/Sub gRPC forwarding and distinguish TCP send-half-close."""

import argparse
import hashlib
import json
import re
import shlex
import shutil
import socket
import subprocess
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]


def run(command, timeout=60):
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--machine",
        required=True,
        help="An existing disposable task machine forwarding guest port 8080",
    )
    args = parser.parse_args()
    if not re.fullmatch("[a-zA-Z0-9_-]+", args.machine):
        parser.error("Select an explicit task machine")
    lines = [line.split() for line in run(["moo", "ls"]).splitlines()]
    matches = [
        line
        for line in lines
        if len(line) > 4 and line[0] == "gcp-local" and line[1] == args.machine
    ]
    if len(matches) != 1:
        parser.error("The selected gcp-local machine was not uniquely identified")
    ports = [int(value.split("->")[0]) for value in matches[0] if value.endswith("->8080")]
    if len(ports) != 1:
        parser.error("The task machine must forward guest port 8080")
    endpoint = f"127.0.0.1:{ports[0]}"
    run(
        [
            "sh",
            "scripts/go.sh",
            "build",
            "-tags=backendprobe",
            "-o",
            "bin/moo-pubsub-probe",
            "./tools/pubsub-probe",
        ],
        timeout=180,
    )

    def guest(action, identifier):
        command = shlex.join(
            ["python3", "/srv/app/scripts/moo_protocol_guest.py", action, "--run", identifier]
        )
        return run(["moo", "run", args.machine, "--", command])

    checks = []
    for restart in (False, True):
        identifier = uuid4().hex
        try:
            guest("start-grpc", identifier)
            command = ["bin/moo-pubsub-probe", "-endpoint", endpoint, "-moo-forwarded"]
            if restart:
                command.append("-expect-empty")
            checks.append(json.loads(run(command)))
        finally:
            guest("stop", identifier)
    identifier = uuid4().hex
    try:
        guest("start-tcp", identifier)
        payload = b"gcp-local-moo-half-close"
        with socket.create_connection(("127.0.0.1", ports[0]), timeout=3) as connection:
            connection.settimeout(3)
            connection.sendall(payload)
            connection.shutdown(socket.SHUT_WR)
            try:
                received = connection.recv(1024)
                tcp = (
                    "response-after-fin"
                    if received == b"ACK:" + payload
                    else "closed-before-response"
                )
            except TimeoutError:
                tcp = "timeout-waiting-for-response"
    finally:
        guest("stop", identifier)
    # Preserve the stronger loopback-only test as a distinct result.
    output = run(
        ["moo", "run", args.machine, "--", "cd /srv/app && sh scripts/check_pubsub_backend.sh"],
        timeout=180,
    )
    guest_report = json.loads(output[output.index("{") :])
    report = {
        "kind": "moo-protocol-probe",
        "gcp_execution": False,
        "cloud_fidelity": "unverified",
        "host_forwarded_grpc": checks,
        "tcp_send_half_close": tcp,
        "guest_isolated": guest_report,
        "cleanup": "passed",
        "moo_binary_sha256": hashlib.sha256(Path(shutil.which("moo")).read_bytes()).hexdigest(),
    }
    path = ROOT / ".artifacts/moo-runtime/protocol.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
