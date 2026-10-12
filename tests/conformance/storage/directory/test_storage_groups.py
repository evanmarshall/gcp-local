"""Bucket reads granted through real Google groups, direct and nested.

The sandbox organization keeps two standing groups the owner maintains
(docs/contributing/gcp-sandbox.md): ALLOWED_GROUP lists the allowed caller,
and NESTED_GROUP lists only ALLOWED_GROUP. The denied caller is in neither.
The scenario never changes membership. It first removes the fixture's direct
grant to the allowed caller. Each grant is revoked and the revocation
observed before the next one, so a passing read proves that grant, not a
leftover. Locally, the conformance server declares the same groups in its
directory.
"""

import platform
import sys
import time

import pytest

from gcp_validation.services.storage.authorization import StorageAuthorization
from gcp_validation.services.storage.normalization import StorageObservations

ALLOWED_GROUP = "group:gcp-local-allowed@stitchwink.com"
NESTED_GROUP = "group:gcp-local-nested@stitchwink.com"
VIEWER = "roles/storage.objectViewer"


class GroupAuthorization(StorageAuthorization):
    """Replaces the scenario's group bindings; every other binding stays."""

    def put(self, *bindings):
        policy = self.policy()
        kept = []
        for original in policy.get("bindings", []):
            members = [m for m in original["members"] if not m.startswith("group:")]
            if members:
                kept.append(dict(original, members=members))
        conditional = any("condition" in binding for binding in bindings)
        policy.update(bindings=[*kept, *bindings], version=3 if conditional else 1)
        if self.storage.sandbox:
            time.sleep(2)  # GCP allows about one bucket metadata update per second.
        path = self.storage.path + "/iam"
        written = self.storage.client.request("fixture", "PUT", path, body=policy).require(200)
        views = [[b["role"], b["members"], sorted(b.get("condition", {}))]
                 for b in written["bindings"] if any(m.startswith("group:") for m in b["members"])]
        return sorted(views, key=str)

    def wait(self, name, expected, step):
        started = time.monotonic()
        response = self.wait_read(name, "allowed", expected)
        elapsed = time.monotonic() - started
        print(f"{step}: allowed read reached {expected} after {elapsed:.0f}s", file=sys.stderr)
        return response


@pytest.mark.gcp_scenario(
    id="storage-iam-groups", service="storage", transport="rest-json-v1",
    versions={"sdk": "stdlib-http/" + platform.python_version() + ";google-auth/2.56.2",
              "provider": "not-applicable", "backend": "not-applicable"},
    contract_files=(
        "gcp_validation/services/storage/client.py",
        "gcp_validation/services/storage/normalization.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
        "gcp_validation/services/storage/authorization.py",
        "tests/conformance/storage/conftest.py",
    ),
)
def test_storage_iam_groups(storage, gcp_snapshot):
    iam = GroupAuthorization(storage)
    observed = StorageObservations(storage)
    match = gcp_snapshot.match
    payload = b"gcp-local group evidence\n"
    storage.upload("public-a.txt", payload, actor="fixture").require(200)
    storage.upload("private-b.txt", payload, actor="fixture").require(200)

    def readable(step, name="public-a.txt"):
        return observed.object(iam.wait(name, 200, step).json())

    def refused(step, name="public-a.txt"):
        return observed.error(iam.wait(name, 403, step))

    def denied(name="public-a.txt", actor="denied"):
        return observed.error(storage.read(name, actor=actor))

    # The fixture grants the allowed caller objectUser directly; drop it first.
    match("direct-member-removed", iam.set_role("allowed"))
    match("no-grant", refused("remove fixture grant"))

    match("direct-grant", iam.put({"role": VIEWER, "members": [ALLOWED_GROUP]}))
    match("direct-allowed", readable("direct"))
    match("direct-denied-caller", denied())

    match("direct-revoked", iam.put())
    match("direct-revoked-read", refused("revoke direct"))

    match("nested-grant", iam.put({"role": VIEWER, "members": [NESTED_GROUP]}))
    match("nested-allowed", readable("nested"))
    match("nested-denied-caller", denied())

    match("nested-revoked", iam.put())
    match("nested-revoked-read", refused("revoke nested"))

    prefix = f"projects/_/buckets/{storage.bucket}/objects/public-"
    condition = {"title": "gcp-local-group-prefix",
                 "expression": f'resource.name.startsWith("{prefix}")'}
    match("conditional-grant",
          iam.put({"role": VIEWER, "members": [NESTED_GROUP], "condition": condition}))
    match("conditional-allowed", readable("conditional"))
    match("conditional-outside-prefix", denied("private-b.txt", actor="allowed"))
    match("conditional-denied-caller", denied())
