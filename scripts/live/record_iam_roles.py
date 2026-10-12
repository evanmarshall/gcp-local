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
        "cloudkms.cryptoKeyDecrypter",
        "cloudkms.signer",
        "cloudkms.signerVerifier",
        "cloudkms.publicKeyViewer",
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
    "customroles": ("iam.roleAdmin", "iam.roleViewer"),
    "resourcemanager": ("resourcemanager.projectIamAdmin",),
    "redis": ("redis.admin", "redis.editor", "redis.viewer"),
    "cloudsql": ("cloudsql.admin", "cloudsql.editor", "cloudsql.viewer", "cloudsql.client"),
    "aiplatform": ("aiplatform.user", "aiplatform.viewer"),
    "logging": ("logging.admin", "logging.logWriter", "logging.viewer"),
    "cloudscheduler": (
        "cloudscheduler.admin", "cloudscheduler.jobRunner", "cloudscheduler.viewer",
        "cloudscheduler.serviceAgent",
    ),
}


# Snapshot directory for each role prefix, following the provider layout.
ROLE_DIRS = {
    "aiplatform": "vertexai",
    "artifactregistry": "artifactregistry",
    "bigquery": "databases/bigquery",
    "bigtable": "databases/bigtable",
    "cloudkms": "security/cloudkms",
    "cloudsql": "databases/cloudsql",
    "cloudscheduler": "messaging/cloudscheduler",
    "cloudtasks": "messaging/cloudtasks",
    "compute": "compute",
    "datastore": "databases/firestore",
    "iam": "security/iam",
    "logging": "logging",
    "networkmanagement": "compute",
    "pubsub": "messaging/pubsub",
    "redis": "databases/memorystore",
    "resourcemanager": "security/resourcemanager",
    "run": "run",
    "secretmanager": "security/secretmanager",
    "spanner": "databases/spanner",
    "storage": "storage",
}


# Custom-role permission validity: every permission testable on the sandbox
# project, grouped by customRolesSupportLevel (absent means SUPPORTED).
PERMISSIONS_FILE = "testable-permissions.json"


def testable_permissions(args: argparse.Namespace) -> dict:
    """Record the project's testable permissions and return their manifest entry."""
    raw = subprocess.check_output(
        [
            GCLOUD,
            "iam",
            "list-testable-permissions",
            "//cloudresourcemanager.googleapis.com/projects/" + args.quota_project,
            "--account=" + args.account,
            "--billing-project=" + args.quota_project,
            "--format=json",
        ]
    )
    levels: dict[str, list[str]] = {"SUPPORTED": [], "TESTING": [], "NOT_SUPPORTED": []}
    for permission in json.loads(raw):
        levels[permission.get("customRolesSupportLevel", "SUPPORTED")].append(permission["name"])
    if not levels["SUPPORTED"]:
        raise SystemExit("Unexpected testable-permissions response; candidate is incomplete")
    value = {
        "source": "https://iam.googleapis.com/v1/permissions:queryTestablePermissions",
        "resource": "//cloudresourcemanager.googleapis.com/projects/{project}",
        "customRolesSupportLevel": {level: sorted(names) for level, names in levels.items()},
    }
    encoded = (json.dumps(value, indent=0) + "\n").encode()
    (args.output / PERMISSIONS_FILE).write_bytes(encoded)
    return {"file": PERMISSIONS_FILE, "sha256": hashlib.sha256(encoded).hexdigest()}


def main() -> None:
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
        name = ROLE_DIRS[role.split(".", 1)[0]] + "/" + role + ".json"
        (args.output / name).parent.mkdir(parents=True, exist_ok=True)
        (args.output / name).write_bytes(raw)
        files.append(
            {"file": name, "name": value["name"], "sha256": hashlib.sha256(raw).hexdigest()}
        )
    manifest = {
        "source": "https://iam.googleapis.com/v1/roles/{role}",
        "permissions": testable_permissions(args),
        "recorded_at": datetime.now(UTC).isoformat(),
        "scope": "Read-only predefined "
        + ", ".join(services)
        + " role definitions and the project's testable permissions; no resource mutation or grant",
        "roles": files,
    }
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print("Candidate recorded. Inspect role changes before replacing committed snapshots.")


if __name__ == "__main__":
    main()
