#!/usr/bin/env python3
"""Revalidate the cheap recorded scenarios against GCP and summarize any drift.

Runs every entry of .github/drift/plan.json as its own pytest invocation in
revalidate mode with that entry's scheduled scope, keeps going after a failure,
and writes a Markdown summary. Exits nonzero when any entry failed.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DRIFT = ROOT / ".github/drift"


def load_plan() -> list[dict]:
    """Read the plan and check that every test module and scope file exists."""
    entries = json.loads((DRIFT / "plan.json").read_text())["entries"]
    for entry in entries:
        paths = [ROOT / test for test in entry["tests"]]
        if "scope" in entry:
            paths.append(DRIFT / entry["scope"])
        missing = [str(path.relative_to(ROOT)) for path in paths if not path.is_file()]
        if missing:
            raise SystemExit(f"drift plan entry {entry['name']} names missing files: {missing}")
    return entries


def command(entry: dict, profile: str, artifacts: Path) -> list[str]:
    """Build one entry's pytest command."""
    arguments = [sys.executable, "-m", "pytest", *entry["tests"], "-q", "--tb=short",
                 "--gcp-mode", "revalidate", f"--gcp-profile={profile}", "--gcp-allow-live",
                 f"--gcp-artifacts={artifacts / entry['name']}"]
    if "scope" in entry:
        arguments.append(f"{entry['option']}={DRIFT / entry['scope']}")
    return arguments


def main(argv: list[str] | None = None) -> int:
    """Run the plan and write the summary."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", help="Explicit live CI profile")
    parser.add_argument("--artifacts", default=".artifacts/drift")
    parser.add_argument("--summary", default=".artifacts/drift/summary.md")
    parser.add_argument("--check", action="store_true", help="Validate the plan offline only")
    args = parser.parse_args(argv)
    entries = load_plan()
    if args.check:
        print(f"Drift plan is valid: {len(entries)} entries.")
        return 0
    if not args.profile:
        parser.error("--profile is required for a live run")
    artifacts = ROOT / args.artifacts
    rows, failed = [], []
    for entry in entries:
        result = subprocess.run(command(entry, args.profile, artifacts), cwd=ROOT, check=False)
        passed = result.returncode == 0
        rows.append(f"| {entry['name']} | {'pass' if passed else '**fail**'} |")
        if not passed:
            failed.append(entry["name"])
    summary = Path(args.summary)
    summary.parent.mkdir(parents=True, exist_ok=True)
    summary.write_text(
        "| Scenario set | Result |\n| --- | --- |\n" + "\n".join(rows) + "\n\n"
        + (f"Drift or failure in: {', '.join(failed)}.\n" if failed else "No drift.\n")
    )
    print(summary.read_text())
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
