#!/usr/bin/env python3
"""Create a private, synthetic ADC bundle for the local identity server."""

import argparse
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gcp_local.bootstrap import loopback_endpoint, write_identity


def main() -> int:
    """Create a private synthetic ADC bundle for the local identity server."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--endpoint", default="http://127.0.0.1:4566")
    parser.add_argument("--project", required=True)
    parser.add_argument("--principal", required=True, help="user:email or serviceAccount:email")
    args = parser.parse_args()
    if not loopback_endpoint(args.endpoint):
        parser.error("Select an explicit numeric loopback HTTP endpoint and port")
    if not args.project or any(
        c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
        for c in args.project
    ):
        parser.error("Supply an explicit synthetic project ID")
    if (
        not args.principal.startswith(("user:", "serviceAccount:"))
        or "@" not in args.principal
        or any(c.isspace() for c in args.principal)
    ):
        parser.error("Supply an explicit user or service-account subject")
    directory = args.directory.expanduser().absolute()
    if directory.exists() or directory.is_symlink():
        parser.error("The destination already exists; choose a new private directory")
    directory.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory = directory.parent.resolve() / directory.name
    try:
        write_identity(
            directory, project=args.project, principal=args.principal, endpoint=args.endpoint
        )
    except Exception:
        shutil.rmtree(directory, ignore_errors=True)
        raise
    print(
        json.dumps(
            {
                "server_config": str(directory / "server.json"),
                "adc": str(directory / "adc.json"),
                "terraform_adc": str(directory / "terraform-adc.json"),
                "project": args.project,
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
