#!/usr/bin/env python3
"""Write the explicitly selected CI profile privately, outside the checkout."""

import argparse
import os
import shlex
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from gcp_validation.sandbox import Sandbox, inspect_credentials


def main(argv: list[str] | None = None) -> None:
    """Write the selected CI profile privately, outside the checkout."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    if (
        os.environ.get("GITHUB_EVENT_NAME") != "workflow_dispatch"
        or os.environ.get("GITHUB_REF") != "refs/heads/main"
    ):
        parser.error("Only manual main-branch workflows may prepare live credentials")
    fields = {
        name: os.environ[name]
        for name in (
            "GCP_PROJECT_ID",
            "GCP_PROJECT_NUMBER",
            "GCP_REGION",
            "GCP_TEST_LIVE_SCOPE",
            "GCP_TEST_WIF_PROVIDER",
            "GCP_TEST_SERVICE_ACCOUNT",
            "GCP_TEST_CREDENTIALS_FILE",
            "GCP_TEST_FIXTURE_SERVICE_ACCOUNT",
            "GCP_TEST_FIXTURE_CREDENTIALS_FILE",
            "GCP_TEST_DENIED_SERVICE_ACCOUNT",
            "GCP_TEST_DENIED_CREDENTIALS_FILE",
        )
    }
    for name, value in fields.items():
        if not value or "\n" in value or "\r" in value:
            parser.error("Missing or invalid CI field: " + name)
    destination = Path(args.output).resolve()
    workspace = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
    if destination.is_relative_to(workspace):
        parser.error("Write the live profile outside the repository checkout")
    for name, value in fields.items():
        if name.endswith("_CREDENTIALS_FILE"):
            path = Path(value)
            if path.is_symlink() or not path.is_file():
                parser.error("Expected an explicit generated credential file")
            path.chmod(0o600)
    fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as output:
        output.write(
            "".join(name + "=" + shlex.quote(value) + "\n" for name, value in fields.items())
        )
    try:
        profile = Sandbox.load(destination)
        for identity in profile.identities.values():
            inspect_credentials(identity, profile.project)
    except Exception:
        destination.unlink()
        raise
    print("Selected private CI profile prepared; credentials were not printed.")


if __name__ == "__main__":
    main()
