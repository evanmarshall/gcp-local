#!/usr/bin/env python3
"""Run the Go race tests, splitting slow serialized packages into parallel shards.

Every package matching the patterns runs in one `go test -race` invocation,
except the SHARDED packages: their tests serialize on process-global state
(BigQuery's GoogleSQL wasm module, so t.Parallel gains nothing), so each is
compiled once and its tests are dealt round-robin into several processes.
Every process gets the same -timeout as the rest of the run; any failure fails
the run. Extra go test flags (for example -count=1) come before the patterns.

Usage: race.py [-count=1] ./...
"""

import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TIMEOUT = "60s"
SHARDS = 3
SHARDED = ("internal/providers/databases/bigquery",)
MODULE = "github.com/evanmarshall/gcp-local/"


def go(*args: str, capture: bool = False) -> subprocess.CompletedProcess:
    """Run the pinned go command from the repository root."""
    return subprocess.run(
        [shutil.which("sh") or "/bin/sh", "scripts/go.sh", *args],
        cwd=ROOT,
        check=False,
        capture_output=capture,
        text=True,
    )


def shard(package: str, flags: list[str]) -> bool:
    """Run one package's tests in SHARDS processes; report each and return success."""
    binary = ROOT / "bin" / (package.rsplit("/", 1)[-1] + ".test")
    binary.parent.mkdir(exist_ok=True)
    if go("test", "-race", *flags, "-c", "-o", str(binary), "./" + package).returncode != 0:
        return False
    directory = ROOT / package
    try:
        listed = subprocess.run(
            [binary, "-test.list", "."], cwd=directory, capture_output=True, text=True, check=True
        )
        tests = sorted(line for line in listed.stdout.split() if line.startswith("Test"))
        groups = [tests[index::SHARDS] for index in range(SHARDS)]
        started, runs = time.monotonic(), []
        for index, group in enumerate(groups):
            if group:
                pattern = "^(" + "|".join(group) + ")$"
                command = [binary, "-test.timeout", TIMEOUT, "-test.run", pattern]
                runs.append(
                    (
                        index,
                        len(group),
                        subprocess.Popen(
                            command,
                            cwd=directory,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            text=True,
                        ),
                    )
                )
        passed = True
        for index, count, process in runs:
            output, _ = process.communicate()
            if process.returncode != 0:
                passed = False
                sys.stdout.write(output)
            outcome = "ok  " if process.returncode == 0 else "FAIL"
            name = f"{MODULE}{package} shard {index + 1}/{SHARDS} ({count} tests)"
            print(f"{outcome}\t{name}\t{time.monotonic() - started:.3f}s", flush=True)
        return passed
    finally:
        binary.unlink(missing_ok=True)


def main(argv: list[str]) -> int:
    """Run the unsharded packages, then each sharded one."""
    flags = [arg for arg in argv if arg.startswith("-")]
    patterns = [arg for arg in argv if not arg.startswith("-")] or ["./..."]
    listed = go("list", *patterns, capture=True)
    if listed.returncode != 0:
        sys.stderr.write(listed.stderr)
        return 1
    packages = listed.stdout.split()
    sharded = [MODULE + name for name in SHARDED]
    rest = [package for package in packages if package not in sharded]
    passed = go("test", "-race", *flags, "-timeout", TIMEOUT, *rest).returncode == 0
    for package in sharded:
        if package in packages:
            passed = shard(package.removeprefix(MODULE), flags) and passed
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
