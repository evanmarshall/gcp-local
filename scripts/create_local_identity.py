#!/usr/bin/env python3
"""Create a private, synthetic ADC bundle for the local identity server."""

import argparse
import ipaddress
import json
import os
import secrets
import shutil
from pathlib import Path
from urllib.parse import urlsplit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--endpoint", default="http://127.0.0.1:4566")
    parser.add_argument("--project", required=True)
    parser.add_argument("--principal", required=True, help="user:email or serviceAccount:email")
    args = parser.parse_args()
    url = urlsplit(args.endpoint)
    try:
        local = ipaddress.ip_address(url.hostname or "").is_loopback and url.port
    except ValueError:
        local = False
    if not (
        local
        and url.scheme == "http"
        and not url.username
        and not url.password
        and url.path in {"", "/"}
        and not url.query
        and not url.fragment
    ):
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
    directory = directory.resolve()
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    subject = "gls." + secrets.token_urlsafe(32)
    values = {
        "server.json": json.dumps(
            {
                "token_lifetime_seconds": 3600,
                "principals": [
                    {"subject": args.principal, "project": args.project, "subject_token": subject}
                ],
            },
            indent=2,
        )
        + "\n",
        "subject.txt": subject,
        "adc.json": json.dumps(
            {
                "type": "external_account",
                "audience": "//gcp-local/identity",
                "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "token_url": args.endpoint.rstrip("/") + "/v1/token",
                "credential_source": {"file": str(directory / "subject.txt")},
            },
            indent=2,
        )
        + "\n",
    }
    try:
        for name, content in values.items():
            with os.fdopen(
                os.open(directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w"
            ) as output:
                output.write(content)
    except Exception:
        shutil.rmtree(directory)
        raise
    print(
        json.dumps(
            {
                "server_config": str(directory / "server.json"),
                "adc": str(directory / "adc.json"),
                "project": args.project,
            }
        )
    )


if __name__ == "__main__":
    main()
