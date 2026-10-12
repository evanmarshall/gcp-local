#!/usr/bin/env python3
"""Check the built bin/gcp-local: listener, SIGTERM shutdown, and local identity, offline."""

import json
import queue
import select
import signal
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

from smoke_support import exchange

ROOT = Path(__file__).resolve().parents[1]
BINARY = str(ROOT / "bin/gcp-local")


def runtime() -> None:
    """Check startup, health, an unsupported API, and SIGTERM shutdown."""
    with subprocess.Popen(
        [BINARY, "-listen", "127.0.0.1:0", "-grpc-listen", "127.0.0.1:0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ) as process:
        try:
            if not select.select([process.stdout], [], [], 10)[0]:
                raise AssertionError("Runtime did not start within ten seconds")
            start = json.loads(process.stdout.readline())
            assert start["msg"] == "gcp-local listening", start
            endpoint = "http://" + start["address"]
            client = build_opener(ProxyHandler({}))
            with client.open(endpoint + "/_local/health", timeout=5) as response:
                body = json.load(response)
                assert response.status == 200
                assert body["ready"] is True
                assert body["providers"] == [], body
            try:
                client.open(endpoint + "/storage/v1/b", timeout=5)
                raise AssertionError("Unsupported service reported success")
            except HTTPError as response:
                with response:
                    assert response.code == 501
                    assert json.load(response)["error"]["status"] == "UNIMPLEMENTED"
            process.send_signal(signal.SIGTERM)
            tail, _ = process.communicate(timeout=12)
            assert process.returncode == 0, tail
            assert any(
                json.loads(line).get("msg") == "gcp-local stopped" for line in tail.splitlines()
            ), tail
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    print("Runtime startup, health, unsupported API, and SIGTERM checks passed.")


def identity() -> None:
    """Check private ADC setup, STS, identity inspection, and the metadata listener."""
    principal = "serviceAccount:app@synthetic.iam.gserviceaccount.com"
    with tempfile.TemporaryDirectory(prefix="gcp-local-identity-") as temporary:
        directory = Path(temporary) / "credentials"
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts/create_local_identity.py"),
                "--directory",
                str(directory),
                "--project",
                "synthetic",
                "--principal",
                principal,
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        subject = (directory / "subject.txt").read_text()
        process = subprocess.Popen(
            [
                BINARY,
                "-listen",
                "127.0.0.1:0",
                "-grpc-listen",
                "127.0.0.1:0",
                "-identity-config",
                str(directory / "server.json"),
                "-metadata-listen",
                "127.0.0.1:0",
                "-metadata-principal",
                principal,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        lines, events = [], queue.Queue()

        def read_logs() -> None:
            for line in process.stdout:
                lines.append(line)
                events.put(json.loads(line))

        reader = threading.Thread(target=read_logs, daemon=True)
        reader.start()
        try:
            started, metadata = events.get(timeout=10), events.get(timeout=10)
            assert started["msg"] == "gcp-local listening"
            assert metadata["msg"] == "workload metadata listening"
            endpoint = "http://" + started["address"]
            client = build_opener(ProxyHandler({}))
            access = exchange(endpoint, subject)
            request = Request(  # noqa: S310 -- loopback endpoint built above
                endpoint + "/_local/identity", headers={"Authorization": "Bearer " + access}
            )
            with client.open(request, timeout=5) as response:
                assert json.load(response)["subject"] == principal
            request = Request(
                "http://"
                + metadata["address"]
                + "/computeMetadata/v1/instance/service-accounts/default/email",
                headers={"Metadata-Flavor": "Google"},
            )
            with client.open(request, timeout=5) as response:
                assert response.read().decode() == principal.split(":", 1)[1]
            process.terminate()
            assert process.wait(timeout=12) == 0
            reader.join(timeout=2)
            assert subject not in "".join(lines)
            assert access not in "".join(lines)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            reader.join(timeout=2)
            process.stdout.close()
    print("Private ADC setup, STS, identity inspection, metadata binding, and shutdown passed.")


if __name__ == "__main__":
    runtime()
    identity()
