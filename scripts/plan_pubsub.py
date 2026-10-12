#!/usr/bin/env python3
"""Print a Pub/Sub setup/recording plan offline; never run gcloud or approve it."""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gcp_validation.pubsub_scope import LIMITS, private_json
from gcp_validation.sandbox import Sandbox


def plan(profile: Sandbox, setup_account: str) -> dict[str, Any]:
    """Return the offline Pub/Sub setup and recording command plan."""
    if not re.fullmatch(r"[A-Za-z0-9._+%-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", setup_account):
        raise ValueError("Select an explicit setup account")
    flags = ["--project=" + profile.project, "--account=" + setup_account, "--quiet"]
    member = "serviceAccount:" + profile.identities["fixture"].email
    return {
        "status": "proposed-not-authorized",
        "scope": {
            "schema": 1,
            "approved": False,
            "services": ["pubsub"],
            "project": profile.project,
            "project_number": profile.project_number,
            "location": "global",
            "limits": LIMITS,
            "cleanup_required": True,
            "scheduled_runs": False,
        },
        "preconditions": [
            "Explicit owner approval for this Pub/Sub plan, separate from Storage approval",
            "Dedicated project number matches; no existing topics/subscriptions",
            "Fixture/allowed/denied profiles are private, keyless, and explicitly selected",
            "Allowed/denied callers have Service Usage Consumer "
            "but no inherited Pub/Sub permissions",
            "Record the fixture account current grants before adding anything",
        ],
        "setup": [
            ["gcloud", "services", "enable", "pubsub.googleapis.com", *flags],
            [
                "gcloud",
                "projects",
                "add-iam-policy-binding",
                profile.project,
                "--member=" + member,
                "--role=roles/pubsub.admin",
                "--condition=None",
                *flags,
            ],
        ],
        "resource_grants": {
            "topic": {"role": "roles/pubsub.publisher", "identity": "allowed"},
            "subscription": {"role": "roles/pubsub.subscriber", "identity": "allowed"},
        },
        "execution": [
            "One record run followed by one revalidation run, serially",
            "No more than one topic/subscription concurrently "
            "and ten 1 KiB publish attempts per run",
            "At most 600 seconds of scenario work, then bounded cleanup",
            "Review and promote sanitized recordings only after successful cleanup",
            "Restore only newly added fixture role bindings "
            "after the recording/revalidation session",
        ],
        "restore_new_fixture_grant": [
            "gcloud",
            "projects",
            "remove-iam-policy-binding",
            profile.project,
            "--member=" + member,
            "--role=roles/pubsub.admin",
            "--condition=None",
            *flags,
        ],
        "limits_note": "Resource/time bounds reduce exposure; they are not a billing cap. "
        "API enablement persists.",
    }


def main(argv: list[str] | None = None) -> None:
    """Print the plan; never run gcloud or approve it."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--setup-account", required=True)
    parser.add_argument(
        "--scope-output",
        type=Path,
        help="Create an unapproved private scope file; refuses overwrites",
    )
    args = parser.parse_args(argv)
    result = plan(Sandbox.load(args.profile), args.setup_account)
    if args.scope_output:
        path = args.scope_output.expanduser().absolute()
        if path.exists() or path.is_symlink():
            parser.error(
                "Scope output already exists; this planner never overwrites approval state"
            )
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        private_json(path, result["scope"])
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
