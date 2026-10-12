"""Run a command-line client and check its result; shared by gcloud.py and rclone.py."""

import os
import subprocess

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
HTTP = os.environ["GCP_LOCAL_HTTP"]
TOOLS = os.environ["GCP_LOCAL_TOOLS"]
DENIAL_MARKERS = ("403", "PERMISSION_DENIED", "AccessDenied", "Forbidden")


def run(command, env, stdin=None):
    """Run one step; raise with its output when it fails. Return stdout."""
    result = subprocess.run(
        command, env=env, input=stdin, capture_output=True, text=True, timeout=60, check=False
    )
    if result.returncode != 0:
        step = " ".join(argument for argument in command if not argument.startswith("/"))
        raise AssertionError(f"{step[:80]}: {result.stderr.strip()[-400:]}")
    return result.stdout


def expect_denied(step, command, env):
    """Require the ungranted identity's command to fail with a permission denial."""
    result = subprocess.run(
        command, env=env, capture_output=True, text=True, timeout=60, check=False
    )
    output = result.stdout + result.stderr
    if result.returncode == 0:
        raise AssertionError(f"{step}: the ungranted identity was allowed")
    if not any(marker in output for marker in DENIAL_MARKERS):
        raise AssertionError(f"{step}: want permission denied, got: {output.strip()[-400:]}")


def main(workflows, argv):
    """Run the workflow named in argv; print the failing step as the last line."""
    if len(argv) != 2 or argv[1] not in workflows:
        raise SystemExit(f"usage: {argv[0]} SERVICE; services: {', '.join(sorted(workflows))}")
    try:
        workflows[argv[1]]()
    except (AssertionError, subprocess.TimeoutExpired) as error:
        raise SystemExit(str(error).replace("\n", " ")) from None
