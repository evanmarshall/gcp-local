#!/usr/bin/env python3
"""Retry only pending owned manifests in the explicitly selected sandbox."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gcp_validation.sandbox import Sandbox
from gcp_validation.storage import main as cleanup


def main(argv: list[str] | None = None) -> int:
    """Retry pending owned manifests against the selected sandbox."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--allow-live", action="store_true", required=True)
    args = parser.parse_args(argv)
    profile = Sandbox.load(args.profile)
    state = Path.home() / ".local/state/gcp-local" / profile.project
    failures = 0
    for path in sorted(state.glob("*.json")):
        try:
            if json.loads(path.read_text()).get("cleanup") != "passed":
                cleanup(
                    ["cleanup", "--profile", args.profile, "--manifest", str(path), "--allow-live"]
                )
        except Exception:
            failures += 1
            print(
                "An owned manifest still needs cleanup; retain the run's "
                "resource-manifest artifact.",
                file=sys.stderr,
            )
    return bool(failures)


if __name__ == "__main__":
    sys.exit(main())
