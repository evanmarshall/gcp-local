#!/usr/bin/env python3
"""Run the allowlisted compiled upstream acceptance tests inside local isolation."""

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> int:
    """Run the allowlisted compiled upstream acceptance tests in isolation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test", help="One exact allowlisted ID; defaults to the entire selection")
    parser.add_argument("--output", default=".artifacts/terraform")
    args = parser.parse_args()
    if sys.platform != "linux" or os.environ.get("GCP_LOCAL_ISOLATED") != "1":
        raise RuntimeError(
            "Terraform acceptance runs require Linux isolation (GCP_LOCAL_ISOLATED=1)"
        )
    if {name for _, name in socket.if_nameindex()} != {"lo"}:
        raise RuntimeError("Terraform acceptance runs require the loopback-only network namespace")
    root = Path(__file__).resolve().parents[1]
    tools = root / ".tools/terraform"
    artifacts = root / args.output
    artifacts.mkdir(parents=True, exist_ok=True)
    selection = json.loads((root / "tests/terraform/selection.json").read_text())
    if args.test and args.test not in selection["test_ids"]:
        parser.error("Test ID is outside the pinned selection")
    tests = [args.test] if args.test else selection["test_ids"]
    with tempfile.TemporaryDirectory(prefix="gcp-local-tf-run-") as temporary:
        config = Path(temporary) / "terraform.rc"
        config.write_text(
            "disable_checkpoint = true\nprovider_installation {\n filesystem_mirror {\n path = "
            + json.dumps(str(tools / "mirror"))
            + '\n include = ["registry.terraform.io/hashicorp/google"]\n }\n}\n'
        )
        env = {key: os.environ[key] for key in ("PATH", "HOME", "USER") if key in os.environ}
        env.update(
            {
                "GCP_LOCAL_ISOLATED": "1",
                "TF_ACC": "1",
                "TF_IN_AUTOMATION": "1",
                "TF_ACC_TERRAFORM_PATH": str(tools / "bin/terraform"),
                "TF_CLI_CONFIG_FILE": str(config),
                "CHECKPOINT_DISABLE": "1",
                "TF_ACC_LOG": "OFF",
                "GCP_LOCAL_TF_ARTIFACTS": str(artifacts),
                "TF_ACC_TEMP_DIR": temporary,
                "TF_ACC_PROVIDER_HOST": "registry.terraform.io",
                "TF_ACC_PROVIDER_NAMESPACE": "hashicorp",
            }
        )
        command = [
            str(root / ".tools/go/bin/go"),
            "tool",
            "test2json",
            "-t",
            "-p",
            "gcp-local/terraform-tests",
            str(root / "bin/terraform-tests"),
            "-test.v",
            "-test.parallel=1",
            "-test.timeout=180s",
            "-test.run=^(" + "|".join(tests) + ")$",
        ]
        process = subprocess.Popen(
            command,
            cwd=root / "tests/terraform/runner",
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        timed_out = False
        try:
            stdout, stderr = process.communicate(timeout=200)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGKILL)
            stdout, stderr = process.communicate()
        events = {test: {"Test": test, "Action": "not-run"} for test in tests}
        for line in stdout.splitlines():
            event = json.loads(line)
            if event.get("Action") == "output":
                # No real credentials are available, but omit the fixed fixture
                # token from diagnostic output too. Never enable HTTP/VCR logs.
                print(
                    event.get("Output", "").replace("gcp-local-test:fixture", "<fixture-token>"),
                    end="",
                )
            elif event.get("Test") in tests and event.get("Action") in {"pass", "fail", "skip"}:
                events[event["Test"]] = {
                    key: event[key] for key in ("Test", "Action", "Elapsed") if key in event
                }
        success = process.returncode == 0 and all(
            event["Action"] == "pass" for event in events.values()
        )
        exit_code = 124 if timed_out else (0 if success else (process.returncode or 1))
        report = {
            "kind": "terraform-acceptance",
            "mode": "local",
            "terraform": "1.16.5",
            "provider": "google/8.5.0",
            "upstream_revision": selection["revision"],
            "backend": "fake-gcs-server/1.56.1",
            "cloud_terraform_execution": False,
            "exit_code": exit_code,
            "timed_out": timed_out,
            "tests": list(events.values()),
        }
        (artifacts / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        if stderr:
            print(stderr.replace("gcp-local-test:fixture", "<fixture-token>"), file=sys.stderr)
        return exit_code


if __name__ == "__main__":
    sys.exit(main())
