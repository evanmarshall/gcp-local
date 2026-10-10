#!/usr/bin/env python3
"""Reject merge commits and unsafe main updates without modifying Git state."""

import subprocess
import shutil
import sys

GIT = shutil.which("git") or "git"


def git(*args):
    result = subprocess.run([GIT, *args], capture_output=True, text=True)
    if result.returncode:
        raise ValueError(result.stderr.strip() or f"git {' '.join(args)} failed")
    return result.stdout.strip()


def check_linear(tip):
    git("rev-parse", "--verify", f"{tip}^{{commit}}")
    if git("rev-list", "--max-count=1", "--min-parents=2", tip):
        raise ValueError("Merge commits are not allowed; integrate with git merge --ff-only.")


def is_zero(oid):
    return bool(oid) and set(oid) == {"0"}


def check_push(lines):
    for line in lines:
        fields = line.split()
        if len(fields) != 4:
            raise ValueError("Invalid pre-push input; expected four fields.")
        _, local_oid, remote_ref, remote_oid = fields
        if not remote_ref.startswith("refs/heads/"):
            continue
        if is_zero(local_oid):
            if remote_ref == "refs/heads/main":
                raise ValueError("Deleting main is not allowed.")
            continue
        check_linear(local_oid)
        if remote_ref == "refs/heads/main" and not is_zero(remote_oid):
            try:
                git("rev-parse", "--verify", f"{remote_oid}^{{commit}}")
            except ValueError as exc:
                raise ValueError("Remote main is not available locally; fetch and retry.") from exc
            result = subprocess.run(
                [GIT, "merge-base", "--is-ancestor", remote_oid, local_oid],
                capture_output=True,
                text=True,
            )
            if result.returncode:
                raise ValueError("main must advance by fast-forward; fetch and update your branch.")


def main():
    try:
        if sys.argv[1:] == ["--pre-push"]:
            check_push(sys.stdin)
        elif len(sys.argv) == 2:
            check_linear(sys.argv[1])
        else:
            raise ValueError("Usage: check_history.py REVISION | --pre-push")
    except (ValueError, OSError) as exc:
        print(f"History check failed: {exc}", file=sys.stderr)
        return 1
    print("History check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
