#!/usr/bin/env python3
"""Reject merge commits and unsafe main updates without modifying Git state."""

import shutil
import subprocess
import sys

GIT = shutil.which("git") or "git"


def git(*args: str) -> str:
    """Run git against this repository."""
    result = subprocess.run([GIT, *args], capture_output=True, text=True, check=False)
    if result.returncode:
        raise ValueError(result.stderr.strip() or f"git {' '.join(args)} failed")
    return result.stdout.strip()


def check_linear(tip: str) -> None:
    """Require a linear, merge-free first-parent history."""
    git("rev-parse", "--verify", f"{tip}^{{commit}}")
    if git("rev-list", "--max-count=1", "--min-parents=2", tip):
        raise ValueError("Merge commits are not allowed; integrate with git merge --ff-only.")


def is_zero(oid: str) -> bool:
    """Report whether an oid is the all-zero object id."""
    return bool(oid) and set(oid) == {"0"}


def check_push(lines: list[str]) -> None:
    """Validate the commits that a push would publish."""
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
                check=False,
            )
            if result.returncode:
                raise ValueError("main must advance by fast-forward; fetch and update your branch.")


def check_cli(arguments: list[str]) -> None:
    """Dispatch the requested history validation mode."""
    if arguments == ["--pre-push"]:
        check_push(sys.stdin)
    elif len(arguments) == 1:
        check_linear(arguments[0])
    elif len(arguments) == 2:
        # The pre-push hook passes the range endpoints; CI passes one revision.
        check_linear(arguments[1])
    else:
        raise ValueError("Usage: check_history.py REVISION | --pre-push")


def main() -> int:
    """Validate the pending push history of the current branch."""
    try:
        check_cli(sys.argv[1:])
    except (ValueError, OSError) as exc:
        print(f"History check failed: {exc}", file=sys.stderr)
        return 1
    print("History check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
