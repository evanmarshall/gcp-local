#!/usr/bin/env python3
"""Exercise actual moo isolation and restore using disposable worktrees/machines."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import re
import subprocess
import tempfile
import time
from urllib.request import ProxyHandler, build_opener
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]


def run(args, cwd=ROOT, timeout=120):
    result = subprocess.run(args, cwd=cwd, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{args[0]} failed: {result.stdout}\n{result.stderr}")
    return result.stdout


def check_http(port, expected):
    opener = build_opener(ProxyHandler({}))
    deadline = time.monotonic() + 15
    while True:
        try:
            with opener.open(f"http://127.0.0.1:{port}/", timeout=2) as response:
                actual = response.read().decode().strip()
            if actual != expected:
                raise AssertionError(f"Port {port}: expected {expected!r}, got {actual!r}")
            return
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.25)


def main():
    run(["moo", "doctor"])
    token = uuid4().hex[:10]
    started = time.monotonic()
    machines = []
    worktrees = []
    cleanup_errors = []
    with tempfile.TemporaryDirectory(prefix="gcp-local-moo-smoke-") as directory:
        try:
            for label in ("left", "right"):
                tree = Path(directory) / label
                handle = f"gcp-smoke-{token}-{label}"
                run(["git", "worktree", "add", "--detach", str(tree), "HEAD"])
                worktrees.append(tree)
                (tree / "moo-smoke-marker.txt").write_text(label + "\n")
                (tree / "credentials").mkdir(exist_ok=True)
                (tree / "credentials/smoke-sentinel").write_text("synthetic ignored data\n")
                machines.append((handle, tree, label))
                run(["moo", "new", handle, "from", "base"], cwd=tree)

            def start_server(machine):
                handle, tree, label = machine
                command = (
                    "set -eu; "
                    "test ! -e /srv/app/credentials/smoke-sentinel; "
                    f'test "$(cat /srv/app/moo-smoke-marker.txt)" = {label}; '
                    "mkdir -p /var/tmp/gcp-local-smoke; "
                    "cp /srv/app/moo-smoke-marker.txt /var/tmp/gcp-local-smoke/index.html; "
                    "nohup python3 -m http.server 8080 --bind 0.0.0.0 "
                    "--directory /var/tmp/gcp-local-smoke "
                    ">/var/tmp/gcp-local-smoke/server.log 2>&1 </dev/null &"
                )
                run(["moo", "run", handle, "--", command], cwd=tree)

            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(start_server, machines))

            listing = run(["moo", "ls"])
            ports = []
            for handle, _, label in machines:
                rows = [line for line in listing.splitlines() if handle in line.split()]
                if len(rows) != 1:
                    raise AssertionError(f"Expected one machine row for {handle}")
                match = re.search(r"\b(\d+)->8080\b", rows[0])
                if not match:
                    raise AssertionError(f"Missing port forwarding for {handle}")
                port = int(match.group(1))
                ports.append(port)
                check_http(port, label)
            if ports[0] == ports[1]:
                raise AssertionError("Two machines share a host port")
            print("PASS: concurrent worktrees, ignored-file exclusion, independent state and HTTP ports", flush=True)

            handle, tree, _ = machines[0]
            run(["moo", "save", handle], cwd=tree)
            run(["moo", "run", handle, "--",
                 "printf changed > /var/tmp/gcp-local-smoke/index.html"], cwd=tree)
            check_http(ports[0], "changed")
            check_http(ports[1], "right")
            run(["moo", "new", handle], cwd=tree)
            restored = run(["moo", "run", handle, "--",
                            "cat /var/tmp/gcp-local-smoke/index.html"], cwd=tree)
            if restored.strip() != "left":
                raise AssertionError("Saved state was not restored")
            check_http(ports[1], "right")
            print("PASS: snapshot restore without changing the other machine", flush=True)
        finally:
            for handle, tree, _ in reversed(machines):
                try:
                    run(["moo", "drop", handle, "--force", "--snapshots"], cwd=tree)
                except Exception as exc:
                    cleanup_errors.append(f"{handle}: {exc}")
            for tree in reversed(worktrees):
                try:
                    run(["git", "worktree", "remove", "--force", str(tree)])
                except Exception as exc:
                    cleanup_errors.append(f"{tree}: {exc}")
            if cleanup_errors:
                raise RuntimeError("Smoke cleanup failed:\n" + "\n".join(cleanup_errors))
    print(f"PASS: temporary machines/worktrees removed ({time.monotonic() - started:.1f}s)")


if __name__ == "__main__":
    main()
