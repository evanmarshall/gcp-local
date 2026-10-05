#!/usr/bin/env python3
"""Verify the built process's listener and SIGTERM shutdown without cloud access."""

import json
from pathlib import Path
import select
import signal
import subprocess
from urllib.error import HTTPError
from urllib.request import ProxyHandler, build_opener


def main():
    root = Path(__file__).resolve().parents[1]
    with subprocess.Popen([str(root / "bin/gcp-local"), "-listen", "127.0.0.1:0", "-grpc-listen", "127.0.0.1:0"],
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as process:
        try:
            if not select.select([process.stdout], [], [], 10)[0]:
                raise AssertionError("Runtime did not start within ten seconds")
            start = json.loads(process.stdout.readline())
            assert start["msg"] == "gcp-local listening", start
            endpoint = "http://" + start["address"]
            client = build_opener(ProxyHandler({}))
            with client.open(endpoint + "/_local/health", timeout=5) as response:
                body = json.load(response)
                assert response.status == 200 and body["ready"] is True, body
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
            assert any(json.loads(line).get("msg") == "gcp-local stopped" for line in tail.splitlines()), tail
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    print("Runtime startup, health, unsupported API, and SIGTERM checks passed.")


if __name__ == "__main__":
    main()
