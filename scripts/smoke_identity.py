#!/usr/bin/env python3
"""Exercise private ADC setup and the built core's local identity listeners."""

import json
import queue
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import ProxyHandler, Request, build_opener


def main() -> int:
    """Exercise private ADC setup and the local identity listeners."""
    root = Path(__file__).resolve().parents[1]
    principal = "serviceAccount:app@synthetic.iam.gserviceaccount.com"
    with tempfile.TemporaryDirectory(prefix="gcp-local-identity-") as temporary:
        directory = Path(temporary) / "credentials"
        subprocess.run(
            [
                sys.executable,
                str(root / "scripts/create_local_identity.py"),
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
                str(root / "bin/gcp-local"),
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

        def read_logs() -> str:
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
            form = {
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "audience": "//gcp-local/identity",
                "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "subject_token": subject,
                "scope": "https://www.googleapis.com/auth/cloud-platform",
            }
            request = Request(  # noqa: S310 -- loopback endpoint built above
                endpoint + "/v1/token",
                data=urlencode(form).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            with client.open(request, timeout=5) as response:
                access = json.load(response)["access_token"]
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
    main()
