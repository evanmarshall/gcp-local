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
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def process_start(pid: int) -> str:
    """Return the process start time field, which distinguishes a reused pid."""
    # The final ')' ends comm, which can itself contain spaces or parentheses.
    return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]


def serve_tcp() -> None:
    """Echo a bounded request back with an ACK prefix, then close."""
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # 0.0.0.0 is correct here: the probe listens inside the isolated task guest.
        listener.bind(("0.0.0.0", 8080))  # noqa: S104 -- in-guest probe port
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


def stop(directory: Path) -> int:
    """Signal the owned probe process and remove its state directory."""
    state = directory / "owner.json"
    if not state.exists():
        if directory.exists():
            directory.rmdir()
        return 0
    owner = json.loads(state.read_text())
    pid = owner["pid"]
    try:
        if process_start(pid) != owner["start"]:
            raise RuntimeError(
                "Owned probe process was replaced; refusing to signal another process"
            )
        os.killpg(pid, signal.SIGTERM)
        await_stop(pid)
    except (ProcessLookupError, FileNotFoundError):
        pass
    shutil.rmtree(directory)
    print(json.dumps({"owned_process_stopped": True}))
    return 0


def await_stop(pid: int) -> None:
    """Wait for the signaled process to exit or become a zombie."""
    deadline = time.monotonic() + 10
    while Path(f"/proc/{pid}").exists():
        if Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] == "Z":
            break
        if time.monotonic() > deadline:
            raise RuntimeError("Owned probe did not stop within its deadline")
        time.sleep(0.05)


def grpc_command() -> list[str]:
    """Build the emulator command from the committed JAR pin."""
    pin = json.loads((ROOT / "toolchains/pubsub.json").read_text())
    jar = ROOT / ".tools/pubsub/emulator.jar"
    if hashlib.sha256(jar.read_bytes()).hexdigest() != pin["jar_sha256"]:
        raise RuntimeError("Backend JAR does not match the committed pin")
    java = shutil.which("java")
    if not java:
        raise RuntimeError("Install Java 17 in the task guest")
    return [
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


def start(action: str, run_id: str, directory: Path) -> int:
    """Bind the probe port, launch the backend, and wait for readiness."""
    with socket.socket() as test:
        # 0.0.0.0 is correct here: the probe binds inside the isolated task guest.
        test.bind(("0.0.0.0", 8080))  # noqa: S104 -- in-guest probe port
    directory.mkdir(mode=0o700)
    command = (
        grpc_command()
        if action == "start-grpc"
        else [
            sys.executable,
            str(Path(__file__).resolve()),
            "serve-tcp",
            "--run",
            run_id,
        ]
    )
    with (directory / "server.log").open("w") as log:
        process = subprocess.Popen(
            command,
            cwd=directory,
            env={"PATH": "/usr/bin:/bin", "TMPDIR": str(directory)},
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    state = directory / "owner.json"
    state.write_text(json.dumps({"pid": process.pid, "start": process_start(process.pid)}))
    await_ready()
    print(json.dumps({"ready": True, "port": 8080, "kind": action}))
    return 0


def await_ready() -> None:
    """Wait for the probe port to answer, surfacing its log on failure."""
    deadline = time.monotonic() + 20
    while True:
        try:
            with socket.create_connection(("127.0.0.1", 8080), timeout=0.2):
                break
        except OSError:
            if time.monotonic() > deadline:
                raise RuntimeError("Disposable probe readiness timed out") from None
            time.sleep(0.05)


def main() -> int:
    """Verify inside the guest that the server process is isolated."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["start-grpc", "start-tcp", "stop", "serve-tcp"])
    parser.add_argument("--run", required=True)
    args = parser.parse_args()
    if not re.fullmatch("[0-9a-f]{32}", args.run) or sys.platform != "linux":
        parser.error("Select a synthetic run ID inside the task guest")
    directory = Path(tempfile.gettempdir()) / ("gcp-local-moo-protocol-" + args.run)
    if args.action == "serve-tcp":
        serve_tcp()
        return 0
    if args.action == "stop":
        return stop(directory)
    return start(args.action, args.run, directory)


if __name__ == "__main__":
    main()
