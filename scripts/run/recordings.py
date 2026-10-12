#!/usr/bin/env python3
"""Replay every committed recording against the local providers and require each verified.

Recordings live at tests/conformance/recordings/<service path>/<id>; the scenario
that replays one lives under tests/conformance/<service path>/. This plans one run
per runner from that tree, executes each inside scripts/run/isolated.py, and then
fails unless the verified scenario IDs in the runs' coverage reports equal the
recording IDs. A skipped, unverified, or missing recording fails.

Usage: recordings.py [--plan] [--verify-only]
"""

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONFORMANCE = Path("tests/conformance")
RECORDINGS = CONFORMANCE / "recordings"
ARTIFACTS = Path(".artifacts/recordings")
SERVER = ["bin/storage-conformance"]
READY = "http://127.0.0.1:4566/_local/health"
# Pub/Sub scenarios need the provider started by its Go integration harness with
# production local ADC, so they replay through these Go tests. Each test runs
# its scenario with pytest and writes coverage under ARTIFACTS/pubsub-*.
PUBSUB_PACKAGE = "internal/providers/messaging/pubsub"
PUBSUB_BINARY = "bin/pubsub-integration"
PUBSUB_TESTS = {
    "pubsub-pull-authorization": "TestBoundedRecorderThroughProductionLocalADC",
    "pubsub-delivery-features": "TestDeliveryScenarioThroughProductionLocalADC",
    "pubsub-schemas": "TestSchemasScenarioThroughProductionLocalADC",
}
# Only reported versions differ between the two pytest runs.
STORAGE_BACKEND = "fake-gcs-server/1.56.1"


@dataclass
class Run:
    """One replay invocation and the recordings it must verify."""

    name: str
    ids: set[str] = field(default_factory=set)
    paths: set[str] = field(default_factory=set)


def recordings(root: Path = ROOT) -> dict[str, str]:
    """Map each recording ID to its service path (for example databases/spanner)."""
    found = {}
    for provenance in sorted((root / RECORDINGS).rglob("provenance.json")):
        directory = provenance.parent
        found[directory.name] = directory.parent.relative_to(root / RECORDINGS).as_posix()
    return found


def scenario_module(root: Path, scenario: str, service: str) -> Path | None:
    """Return the test module under tests/conformance/<service> that declares the scenario."""
    declared = re.compile(rf"""\bid\s*=\s*["']{re.escape(scenario)}["']""")
    directory = root / CONFORMANCE / service
    modules = sorted(directory.rglob("test_*.py")) if directory.is_dir() else []
    return next((module for module in modules if declared.search(module.read_text())), None)


def needs_harness(module: Path) -> bool:
    """Scenarios whose fixtures take --gcp-pubsub-identities need the Go Pub/Sub harness."""
    conftest = module.parent / "conftest.py"
    return conftest.is_file() and "gcp_pubsub_identities" in conftest.read_text()


def plan(root: Path = ROOT) -> tuple[list[Run], list[str]]:
    """Assign every recording to one replay run; return the runs and unreplayable recordings.

    Pub/Sub harness scenarios run as their Go tests; Storage scenarios run against
    the Storage backend; every other scenario module runs in the REST run.
    """
    storage, rest, pubsub = Run("storage"), Run("rest"), Run("pubsub")
    errors = []
    for scenario, service in recordings(root).items():
        module = scenario_module(root, scenario, service)
        if module is None:
            errors.append(
                f"{RECORDINGS}/{service}/{scenario}: no scenario with this id under "
                f"{CONFORMANCE}/{service}/"
            )
        elif scenario in PUBSUB_TESTS:
            pubsub.ids.add(scenario)
            pubsub.paths.add(PUBSUB_TESTS[scenario])
        elif needs_harness(module):
            errors.append(f"{scenario}: no Pub/Sub harness test replays this recording")
        else:
            run = storage if service.split("/")[0] == "storage" else rest
            run.ids.add(scenario)
            run.paths.add(module.relative_to(root).as_posix())
    return [run for run in (storage, rest, pubsub) if run.ids], errors


def isolated(*command: str, server: bool = True, cwd: Path = ROOT) -> None:
    """Run one command inside the loopback-only namespace, optionally with the server."""
    wrapper = [sys.executable, str(ROOT / "scripts/run/isolated.py"), "--timeout", "900"]
    if server:
        wrapper += ["--server-json", json.dumps(SERVER), "--ready-url", READY]
    subprocess.run([*wrapper, "--", *command], cwd=cwd, check=True)


def execute(run: Run) -> None:
    """Replay one run's recordings; its reports land under ARTIFACTS."""
    python = os.environ.get("GCP_LOCAL_TEST_PYTHON", str(ROOT / ".venv-tools/bin/python"))
    if run.name == "pubsub":
        pattern = "^(" + "|".join(sorted(run.paths)) + ")$"
        isolated(
            str(ROOT / PUBSUB_BINARY), "-test.count=1", "-test.v", "-test.run", pattern,
            server=False, cwd=ROOT / PUBSUB_PACKAGE,
        )  # fmt: skip
        return
    isolated(
        python, "-m", "pytest", *sorted(run.paths), "-q",
        "--gcp-mode", "local", "--gcp-project", "gcp-local-test",
        "--gcp-endpoint", "http://127.0.0.1:4566",
        "--gcp-artifacts", str(ARTIFACTS / run.name),
        "--gcp-provider-version", "gcp-local/" + os.environ.get("GITHUB_SHA", "working-tree"),
        "--gcp-backend-version", STORAGE_BACKEND if run.name == "storage" else "not-applicable",
    )  # fmt: skip


def verify(root: Path = ROOT) -> list[str]:
    """Return one error per recording that no coverage report shows verified."""
    expected = set(recordings(root))
    verified, seen = set(), {}
    for report in sorted((root / ARTIFACTS).glob("*/coverage.json")):
        for row in json.loads(report.read_text())["results"]:
            scenario = (row.get("scenario") or {}).get("id")
            if scenario:
                seen.setdefault(scenario, []).append(f"{row['status']} ({report.parent.name})")
                if row["verified"]:
                    verified.add(scenario)
    errors = [
        f"{scenario}: not verified; outcomes: {', '.join(seen.get(scenario, ['not run']))}"
        for scenario in sorted(expected - verified)
    ]
    errors += [
        f"{scenario}: verified but has no recording" for scenario in sorted(verified - expected)
    ]
    return errors


def main() -> int:
    """Plan, replay, and verify every recording."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", action="store_true", help="print the replay plan and exit")
    parser.add_argument("--verify-only", action="store_true", help="only check existing reports")
    args = parser.parse_args()
    runs, errors = plan()
    if args.plan:
        for run in runs:
            print(f"{run.name}: {len(run.ids)} recordings: {' '.join(sorted(run.paths))}")
        print("\n".join(errors), file=sys.stderr)
        return 1 if errors else 0
    if not args.verify_only:
        for report in (ROOT / ARTIFACTS).glob("*/coverage.json"):
            report.unlink()
        for run in runs:
            try:
                execute(run)
            except subprocess.CalledProcessError:
                errors.append(f"{run.name} replay failed")
    errors += verify()
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Verified all {len(recordings())} recordings.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
