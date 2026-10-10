#!/usr/bin/env python3
"""Print an explicit, keyless sandbox setup plan; never execute it or discover defaults."""

import argparse
import json
import re
import shlex

APIS = (
    "serviceusage.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "sts.googleapis.com",
    "storage.googleapis.com",
)
IDENTITIES = {
    "fixture": "gcp-local-fixtures",
    "allowed": "gcp-local-test",
    "denied": "gcp-local-denied",
}


def plan(project, project_number, setup_account, repository, repository_id, owner_id):
    for value, pattern in (
        (project, r"[a-z][a-z0-9-]{4,28}[a-z0-9]"),
        (project_number, r"[0-9]{6,20}"),
        (repository_id, r"[0-9]+"),
        (owner_id, r"[0-9]+"),
        (setup_account, r"[A-Za-z0-9._+%-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
        (repository, r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+"),
    ):
        if not re.fullmatch(pattern, value):
            raise ValueError("Invalid explicit sandbox setup input")
    flags = ["--project=" + project, "--account=" + setup_account, "--quiet"]
    pool = "projects/" + project_number + "/locations/global/workloadIdentityPools/local-github"
    provider = pool + "/providers/manual-storage"
    workflow = repository + "/.github/workflows/cloud-conformance.yml@refs/heads/main"
    claims = {
        "repository_id": repository_id,
        "repository_owner_id": owner_id,
        "ref": "refs/heads/main",
        "environment": "gcp-conformance",
        "event_name": "workflow_dispatch",
        "workflow_ref": workflow,
    }
    condition = " && ".join(
        "assertion." + key + " == '" + value + "'" for key, value in claims.items()
    )
    mapping = ",".join(
        ["google.subject=assertion.sub"]
        + ["attribute." + key + "=assertion." + key for key in claims]
    )
    principal = (
        "principalSet://iam.googleapis.com/" + pool + "/attribute.repository_id/" + repository_id
    )
    steps = []

    def add(label, command, check=None, expected=None):
        steps.append(
            {
                "description": label,
                "command": ["gcloud", *command, *flags],
                "check": ["gcloud", *check, *flags, "--format=json"] if check else None,
                "expected": expected,
            }
        )

    add("Enable only the sandbox and Storage authentication APIs", ["services", "enable", *APIS])
    for alias, name in IDENTITIES.items():
        email = name + "@" + project + ".iam.gserviceaccount.com"
        add(
            "Ensure " + alias + " identity",
            ["iam", "service-accounts", "create", name, "--display-name=gcp-local " + alias],
            ["iam", "service-accounts", "describe", email],
        )
        add(
            "Allow setup-user impersonation of " + alias,
            [
                "iam",
                "service-accounts",
                "add-iam-policy-binding",
                email,
                "--member=user:" + setup_account,
                "--role=roles/iam.serviceAccountTokenCreator",
                "--condition=None",
            ],
        )
        roles = ["roles/serviceusage.serviceUsageConsumer"] + (
            ["roles/storage.admin"] if alias == "fixture" else []
        )
        for role in roles:
            add(
                "Grant " + role + " to " + alias,
                [
                    "projects",
                    "add-iam-policy-binding",
                    project,
                    "--member=serviceAccount:" + email,
                    "--role=" + role,
                    "--condition=None",
                ],
            )
    add(
        "Ensure dedicated GitHub pool",
        ["iam", "workload-identity-pools", "create", "local-github", "--location=global"],
        ["iam", "workload-identity-pools", "describe", "local-github", "--location=global"],
        {"name": pool, "state": "ACTIVE"},
    )
    provider_flags = ["--location=global", "--workload-identity-pool=local-github"]
    add(
        "Ensure manual-main-only provider",
        [
            "iam",
            "workload-identity-pools",
            "providers",
            "create-oidc",
            "manual-storage",
            *provider_flags,
            "--issuer-uri=https://token.actions.githubusercontent.com",
            "--attribute-mapping=" + mapping,
            "--attribute-condition=" + condition,
        ],
        [
            "iam",
            "workload-identity-pools",
            "providers",
            "describe",
            "manual-storage",
            *provider_flags,
        ],
        {
            "name": provider,
            "state": "ACTIVE",
            "attributeCondition": condition,
            "attributeMapping": dict(entry.split("=", 1) for entry in mapping.split(",")),
            "oidc": {"issuerUri": "https://token.actions.githubusercontent.com"},
        },
    )
    add(
        "Allow selected workflow quota charges during impersonation",
        [
            "projects",
            "add-iam-policy-binding",
            project,
            "--role=roles/serviceusage.serviceUsageConsumer",
            "--member=" + principal,
            "--condition=None",
        ],
    )
    for alias, name in IDENTITIES.items():
        add(
            "Permit bounded workflow to impersonate " + alias,
            [
                "iam",
                "service-accounts",
                "add-iam-policy-binding",
                name + "@" + project + ".iam.gserviceaccount.com",
                "--role=roles/iam.workloadIdentityUser",
                "--member=" + principal,
                "--condition=None",
            ],
        )
    return {
        "project": project,
        "project_number": project_number,
        "repository": repository,
        "provider": provider,
        "preconditions": [
            "Existing, explicitly authorized dedicated sandbox with approved billing",
            "No inherited Storage grants on allowed/denied callers",
            "GitHub gcp-conformance environment limited to main",
        ],
        "steps": steps,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in (
        "project",
        "project-number",
        "setup-account",
        "repository",
        "repository-id",
        "owner-id",
    ):
        parser.add_argument("--" + flag, required=True)
    parser.add_argument("--format", choices=["json", "shell"], default="json")
    args = vars(parser.parse_args(argv))
    output_format = args.pop("format")
    try:
        result = plan(**args)
    except ValueError as exc:
        parser.error(str(exc))
    if output_format == "json":
        print(json.dumps(result, indent=2))
    else:
        print(
            "# Inspect checks before creating resources. "
            "Existing provider configuration must match expected JSON."
        )
        for step in result["steps"]:
            print("\n# " + step["description"])
            if step["check"]:
                print("# Check existing: " + shlex.join(step["check"]))
            print(shlex.join(step["command"]))


if __name__ == "__main__":
    main()
