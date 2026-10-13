"""Bounded project IAM recording: policy versions, conditions, audit configs, and update masks.

The run adds one conditional binding for the allowed caller and one Data Access
audit config, then removes exactly those. Every write starts from the current
policy, so concurrent grants made by other runs are kept.
"""

import argparse
import copy
from pathlib import Path
from typing import Protocol

from gcp_validation.config import Target
from gcp_validation.evidence import check_observation, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = "projectiampolicy"
LOCK = "projectiam"
LIMITS = {
    "max_added_bindings": 1,
    "max_audit_configs": 1,
    "max_policy_writes": 12,
    "max_run_seconds": 600,
}
LIVE_ENDPOINT = "https://cloudresourcemanager.googleapis.com"
ROLE = "roles/iam.serviceAccountViewer"
CONDITION = {
    "title": "gcp-local-conformance",
    "description": "Expires in 2100",
    "expression": 'request.time < timestamp("2100-01-01T00:00:00Z")',
}
AUDIT_SERVICE = "cloudscheduler.googleapis.com"
OK = 200


class Snapshot(Protocol):
    """The snapshot fixture's matching interface."""

    def match(self, name: str, value: object) -> None:
        """Record or compare one observation."""


def load_scope(filename: str | None, sandbox: Sandbox | None, mode: str) -> LiveScope:
    """Load the owner-approved scope for this scenario's fixed limits."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


def ours(binding: dict, member: str) -> bool:
    """Whether a binding is the scenario's conditional grant, in either version's form."""
    return binding.get("role", "").startswith(ROLE + "_withcond_") or (
        binding.get("role") == ROLE and "condition" in binding and member in binding["members"]
    )


def audit_ours(config: dict) -> bool:
    """Whether an audit config is the scenario's."""
    return config.get("service") == AUDIT_SERVICE


def without_ours(policy: dict, member: str) -> list[dict]:
    """Return the policy's bindings minus the scenario's conditional grant."""
    return [copy.deepcopy(b) for b in policy.get("bindings", []) if not ours(b, member)]


class VersionFixture(OwnedRun):
    """Owns one conditional binding and one audit config; cleanup removes exactly those."""

    def __init__(self, client: RestClient, state_dir: Path, *, manifest: str | None = None) -> None:
        """Bind the run to the project policy lock shared with project-iam-policy-binding."""
        super().__init__(
            client,
            state_dir,
            LOCK,
            LIMITS["max_run_seconds"],
            manifest=manifest,
            check=lambda v: type(v.get("writes", 0)) is int,
        )
        self.manifest.setdefault("writes", 0)
        self.member = "serviceAccount:" + client.emails["allowed"]
        self.api = f"/v1/projects/{self.project}"

    def read(self, options: dict | None = None) -> Response:
        """Read the project policy with the given GetPolicyOptions."""
        body = {} if options is None else {"options": options}
        return self.client.request(
            "fixture", "POST", f"{self.api}:getIamPolicy", body=body, timeout=self.remaining()
        )

    def current(self) -> dict:
        """Read the current policy in version 3 form."""
        response = self.read({"requestedPolicyVersion": 3})
        if response.status != OK:
            raise ValueError(f"Project policy read returned HTTP {response.status}")
        return response.json()

    def write(self, body: dict) -> Response:
        """Send one setIamPolicy request body, counting it against the write limit."""
        if self.manifest["writes"] >= LIMITS["max_policy_writes"]:
            raise ValueError("Project policy write limit reached")
        self.manifest["writes"] += 1
        self.save()
        return self.client.request(
            "fixture", "POST", f"{self.api}:setIamPolicy", body=body, timeout=self.remaining()
        )

    def setup(self) -> dict:
        """Refuse a policy that already holds conditions or audit configs, then mark the run."""
        self.acquire()
        self.client.verify_project()
        policy = self.current()
        if any("condition" in b for b in policy.get("bindings", [])) or policy.get("auditConfigs"):
            raise ValueError(
                "Conditional bindings or audit configs are outside this bounded scenario"
            )
        self.manifest["attempted"] = True
        self.save()
        return policy

    def cleanup(self) -> None:
        """Remove the scenario's binding and audit config; keep version 1 and other grants."""
        if not self.manifest["attempted"]:
            self.release()
            return
        self.acquire()
        try:
            policy = self.current()
            if any(ours(b, self.member) for b in policy.get("bindings", [])) or any(
                audit_ours(c) for c in policy.get("auditConfigs", [])
            ):
                body = {
                    "policy": {
                        "version": 1,
                        "etag": policy["etag"],
                        "bindings": without_ours(policy, self.member),
                        "auditConfigs": [
                            c for c in policy.get("auditConfigs", []) if not audit_ours(c)
                        ],
                    },
                    "updateMask": "bindings,etag,auditConfigs",
                }
                restored = self.client.request(
                    "fixture", "POST", f"{self.api}:setIamPolicy", body=body
                )
                if restored.status != OK:
                    raise ValueError(
                        f"Restoring the project policy returned HTTP {restored.status}"
                    )
            policy = self.current()
            if (
                policy.get("version", 1) != 1
                or any("condition" in b for b in policy.get("bindings", []))
                or policy.get("auditConfigs")
            ):
                raise ValueError(
                    "Project policy still holds conditions or audit configs; inspect it"
                )
            self.manifest.update(cleanup="passed", finished_at=now())
            self.save()
        finally:
            self.release()


def view(observe: Observations, response: Response, member: str) -> dict:
    """Select a policy's version, fields, the scenario's binding, and audit configs."""
    if response.status != OK:
        return observe.error(response)
    value = response.json()
    mine = []
    for binding in value.get("bindings", []):
        if not ours(binding, member):
            continue
        role, marker, suffix = binding["role"].partition("_withcond_")
        mine.append(
            {
                "role": role,
                "members": [observe.text(m) for m in binding["members"]],
                "withcond_suffix": {
                    "length": len(suffix),
                    "hex": all(c in "0123456789abcdef" for c in suffix),
                }
                if marker
                else None,
                "condition": binding.get("condition"),
            }
        )
    selected = {
        "http_status": response.status,
        "fields": sorted(value),
        "version": value.get("version"),
        "scenario_bindings": mine,
        "audit_configs": [c for c in value.get("auditConfigs", []) if audit_ours(c)],
    }
    check_observation(selected)
    return selected


def run_scenario(fixture: VersionFixture, snapshot: Snapshot, original: dict) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    member = fixture.member
    observe = Observations({client.emails[actor]: "<" + actor + ">" for actor in client.emails})

    def write(name: str, policy: dict, mask: str | None = None, *, etag: str | None = None) -> dict:
        """Send one set request built on the current policy's etag and record its view."""
        body = {"policy": {"etag": etag or fixture.current()["etag"], **policy}}
        if mask is not None:
            body["updateMask"] = mask
        response = fixture.write(body)
        snapshot.match(name, view(observe, response, member))
        return response.json()

    for label, version in (("two", 2), ("seven", 7), ("negative", -1)):
        response = fixture.read({"requestedPolicyVersion": version})
        snapshot.match(f"read-version-{label}", view(observe, response, member))
    conditional = {"role": ROLE, "members": [member], "condition": CONDITION}
    added = write(
        "set-conditional",
        {"version": 3, "bindings": [*without_ours(original, member), conditional]},
        etag=original["etag"],
    )
    for name, options in (
        ("read-v3", {"requestedPolicyVersion": 3}),
        ("read-v1", {"requestedPolicyVersion": 1}),
        ("read-v0", {"requestedPolicyVersion": 0}),
        ("read-unversioned", None),
    ):
        snapshot.match(name, view(observe, fixture.read(options), member))
    audit = [{"service": AUDIT_SERVICE, "auditLogConfigs": [{"logType": "DATA_READ"}]}]
    write(
        "set-v1-dropping-condition",
        {"version": 1, "bindings": without_ours(fixture.current(), member)},
    )
    write("set-audit-mask-with-conditions", {"auditConfigs": audit}, "auditConfigs")
    write(
        "set-v3-dropping-condition",
        {"version": 3, "bindings": without_ours(fixture.current(), member)},
    )
    write("set-audit-mask", {"auditConfigs": audit}, "auditConfigs")
    snapshot.match("read-audit", view(observe, fixture.read({"requestedPolicyVersion": 3}), member))
    write("set-default-mask-keeps-audit", {"version": 1, "bindings": fixture.current()["bindings"]})
    write(
        "set-default-mask-ignores-audit",
        {"version": 1, "bindings": fixture.current()["bindings"], "auditConfigs": []},
    )
    other = [{"service": AUDIT_SERVICE, "auditLogConfigs": [{"logType": "DATA_WRITE"}]}]
    write(
        "set-bindings-mask-ignores-audit",
        {"version": 1, "bindings": fixture.current()["bindings"], "auditConfigs": other},
        "bindings",
    )
    write("set-audit-mask-stale-etag", {"auditConfigs": []}, "auditConfigs", etag=added["etag"])
    write("set-version-mask", {"version": 1}, "version")
    write("set-audit-mask-clears", {"auditConfigs": []}, "auditConfigs")
    snapshot.match("read-final", view(observe, fixture.read({"requestedPolicyVersion": 3}), member))


def main(argv: list[str] | None = None) -> None:
    """Remove only the scenario's binding and audit config after an interrupted run."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("command", choices=["cleanup"])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--allow-live", action="store_true", required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox, "cleanup")
    target = Target.select(
        "record",
        project=sandbox.project,
        credentials_file=sandbox.identities["allowed"].credentials_file,
        live_scope=sandbox.live_scope,
        allow_live=args.allow_live,
    )
    client = RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    VersionFixture(
        client,
        Path.home() / ".local/state/gcp-local" / sandbox.project / LOCK,
        manifest=args.manifest,
    ).cleanup()
    print("Owned project-policy cleanup passed.")  # noqa: T201 -- the CLI reports on stdout


if __name__ == "__main__":
    main()
