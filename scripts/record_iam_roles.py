#!/usr/bin/env python3
"""Record a candidate of allowlisted predefined service roles (read-only)."""

import argparse
import hashlib
import json
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

GCLOUD = shutil.which("gcloud") or "gcloud"

ROLES = {
    "storage": (
        "storage.admin",
        "storage.objectUser",
        "storage.objectViewer",
        "storage.objectCreator",
    ),
    "pubsub": ("pubsub.admin", "pubsub.publisher", "pubsub.subscriber", "pubsub.viewer"),
    "identity": ("iam.serviceAccountUser", "iam.serviceAccountTokenCreator"),
    # Candidate sets for the providers implemented without recorded role
    # snapshots; each entry must be re-recorded read-only before its
    # checksummed manifest snapshot is promoted.
    "run": ("run.admin", "run.developer", "run.invoker", "run.viewer"),
    "compute": ("compute.admin", "compute.networkAdmin", "compute.viewer"),
    "secretmanager": (
        "secretmanager.admin", "secretmanager.secretAccessor", "secretmanager.viewer",
    ),
    "cloudtasks": ("cloudtasks.admin", "cloudtasks.enqueuer"),
    "networkmanagement": ("networkmanagement.admin", "networkmanagement.viewer"),
    "kms": (
        "cloudkms.admin",
        "cloudkms.cryptoKeyEncrypter",
        "cloudkms.cryptoKeyEncrypterDecrypter",
        "cloudkms.viewer",
    ),
    "firestore": ("datastore.indexAdmin", "datastore.user", "datastore.viewer"),
    "bigquery": (
        "bigquery.admin",
        "bigquery.dataEditor",
        "bigquery.dataOwner",
        "bigquery.dataViewer",
        "bigquery.jobUser",
        "bigquery.resourceViewer",
    ),
    "bigtable": ("bigtable.admin", "bigtable.user", "bigtable.viewer"),
    "spanner": ("spanner.admin", "spanner.databaseAdmin", "spanner.viewer"),
    "artifactregistry": (
        "artifactregistry.admin", "artifactregistry.repoAdmin", "artifactregistry.reader",
    ),
    "serviceaccounts": (
        "iam.serviceAccountAdmin", "iam.serviceAccountKeyAdmin", "iam.serviceAccountViewer",
    ),
    "resourcemanager": ("resourcemanager.projectIamAdmin",),
    "redis": ("redis.admin", "redis.editor", "redis.viewer"),
    "cloudsql": ("cloudsql.admin", "cloudsql.editor", "cloudsql.viewer", "cloudsql.client"),
}


def main() -> int:
    """Record a candidate of allowlisted predefined service roles (read-only)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", required=True)
    parser.add_argument("--quota-project", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--service", choices=sorted(ROLES), action="append", help="repeatable; default storage"
    )
    parser.add_argument("--allow-live", action="store_true", required=True)
    args = parser.parse_args()
    services = args.service or ["storage"]
    args.output.mkdir(parents=True, exist_ok=False)
    files = []
    for role in (role for service in services for role in ROLES[service]):
        # Predefined roles are global: gcloud rejects --project for this command.
        raw = subprocess.check_output(
            [
                GCLOUD,
                "iam",
                "roles",
                "describe",
                "roles/" + role,
                "--account=" + args.account,
                "--billing-project=" + args.quota_project,
                "--format=json",
            ]
        )
        value = json.loads(raw)
        if value.get("name") != "roles/" + role or not value.get("includedPermissions"):
            raise SystemExit("Unexpected role response; candidate is incomplete")
        name = role + ".json"
        (args.output / name).write_bytes(raw)
        files.append(
            {"file": name, "name": value["name"], "sha256": hashlib.sha256(raw).hexdigest()}
        )
    manifest = {
        "source": "https://iam.googleapis.com/v1/roles/{role}",
        "recorded_at": datetime.now(UTC).isoformat(),
        "scope": "Read-only predefined "
        + ", ".join(services)
        + " role definitions; no resource mutation or grant",
        "roles": files,
    }
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print("Candidate recorded. Inspect role changes before replacing committed snapshots.")


if __name__ == "__main__":
    main()
