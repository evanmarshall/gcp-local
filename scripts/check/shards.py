#!/usr/bin/env python3
"""Run one compiled Go test binary's tests in parallel shards, one process each.

A package whose tests serialize on process-global state (BigQuery's GoogleSQL
module) runs faster as several processes. Tests are dealt round-robin by name;
each shard gets the same -test.timeout as the rest of the race run, and the
run fails if any shard fails.

Usage: shards.py --binary bin/x.test --dir internal/pkg --shards 3 --timeout 60s
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path


def main() -> int:
    """Run the shards and report each one's outcome and duration."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--binary", required=True)
    parser.add_argument("--dir", required=True, help="Package directory the tests run in")
    parser.add_argument("--shards", type=int, default=3)
    parser.add_argument("--timeout", default="60s")
    args = parser.parse_args()
    binary, directory = Path(args.binary).resolve(), Path(args.dir).resolve()
    listed = subprocess.run(
        [binary, "-test.list", "."], cwd=directory, capture_output=True, text=True, check=True
    )
    tests = sorted(line for line in listed.stdout.split() if line.startswith("Test"))
    shards = [tests[index :: args.shards] for index in range(args.shards)]
    started, runs = time.monotonic(), []
    for index, shard in enumerate(shards):
        if shard:
            command = [
                binary,
                "-test.timeout",
                args.timeout,
                "-test.run",
                "^(" + "|".join(shard) + ")$",
            ]
            runs.append(
                (
                    index,
                    subprocess.Popen(
                        command,
                        cwd=directory,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                    ),
                )
            )
    failed = False
    for index, process in runs:
        output, _ = process.communicate()
        elapsed = time.monotonic() - started
        if process.returncode != 0:
            failed = True
            sys.stdout.write(output)
        outcome = "ok  " if process.returncode == 0 else "FAIL"
        shard = f"shard {index + 1}/{args.shards} ({len(shards[index])} tests)"
        print(f"{outcome}\t{args.dir} {shard}\t{elapsed:.3f}s")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
