#!/usr/bin/env python3
"""Owned, disposable listeners for the Moo host-forwarding probe."""

import argparse
import hashlib
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def process_start(pid):
    # The final ')' ends comm, which can itself contain spaces or parentheses.
    return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["start-grpc", "start-tcp", "stop", "serve-tcp"])
    parser.add_argument("--run", required=True)
    args = parser.parse_args()
    if not re.fullmatch("[0-9a-f]{32}", args.run) or sys.platform != "linux":
        parser.error("Select a synthetic run ID inside the task guest")
    directory = Path("/tmp/gcp-local-moo-protocol-" + args.run)
    state = directory / "owner.json"
    if args.action == "serve-tcp":
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("0.0.0.0", 8080))
            listener.listen()
            while True:
                connection, _ = listener.accept()
                with connection:
                    connection.settimeout(5)
                    try:
                        data = b""
                        while len(data) < 1024:
                            part = connection.recv(1024 - len(data))
                            if not part:
                                break
                            data += part
                        if data:
                            connection.sendall(b"ACK:" + data)
                    except OSError:
                        pass
    elif args.action == "stop":
        if not state.exists():
            if directory.exists():
                directory.rmdir()
            return
        owner = json.loads(state.read_text())
        pid = owner["pid"]
        try:
            if process_start(pid) != owner["start"]:
                raise RuntimeError(
                    "Owned probe process was replaced; refusing to signal another process"
                )
            os.killpg(pid, signal.SIGTERM)
            deadline = time.monotonic() + 10
            while Path(f"/proc/{pid}").exists():
                if Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] == "Z":
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError("Owned probe did not stop within its deadline")
                time.sleep(0.05)
        except ProcessLookupError:
            pass
        except FileNotFoundError:
            pass
        shutil.rmtree(directory)
        print(json.dumps({"owned_process_stopped": True}))
    else:
        with socket.socket() as test:
            test.bind(("0.0.0.0", 8080))  # Never replace an existing listener.
        directory.mkdir(mode=0o700)
        if args.action == "start-grpc":
            pin = json.loads((ROOT / "toolchains/pubsub.json").read_text())
            jar = ROOT / ".tools/pubsub/emulator.jar"
            if hashlib.sha256(jar.read_bytes()).hexdigest() != pin["jar_sha256"]:
                raise RuntimeError("Backend JAR does not match the committed pin")
            java = shutil.which("java")
            if not java:
                raise RuntimeError("Install Java 17 in the task guest")
            command = [
                java,
                "-Xms32m",
                "-Xmx256m",
                "-XX:MaxDirectMemorySize=128m",
                "-XX:ActiveProcessorCount=2",
                "-jar",
                str(jar),
                "--host=0.0.0.0",
                "--port=8080",
                "--projects=backend-probe,backend-peer",
            ]
        else:
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "serve-tcp",
                "--run",
                args.run,
            ]
        with (directory / "server.log").open("w") as log:
            process = subprocess.Popen(
                command,
                cwd=directory,
                env={"PATH": "/usr/bin:/bin", "TMPDIR": str(directory)},
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        state.write_text(json.dumps({"pid": process.pid, "start": process_start(process.pid)}))
        deadline = time.monotonic() + 20
        while True:
            if process.poll() is not None:
                raise RuntimeError(
                    "Disposable probe listener failed: "
                    + (directory / "server.log").read_text()[-2000:]
                )
            try:
                with socket.create_connection(("127.0.0.1", 8080), timeout=0.2):
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise RuntimeError("Disposable probe readiness timed out")
                time.sleep(0.05)
        print(json.dumps({"ready": True, "port": 8080, "kind": args.action}))


if __name__ == "__main__":
    main()
