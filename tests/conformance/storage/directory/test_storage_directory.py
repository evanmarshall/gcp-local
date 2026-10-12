"""Bucket bindings that name groups and domains GCP's directory can't resolve.

The sandbox organization has no Google groups, so this records only the
refusals: a group or domain that doesn't exist, a group reference that names a
service account, and empty references. Locally these are names missing from
the declared directory. Every refusal leaves the policy unchanged.
"""

import platform
import time

import pytest

from gcp_validation.services.storage.authorization import StorageAuthorization
from gcp_validation.services.storage.normalization import StorageObservations

MISSING_GROUP = "gcp-local-missing-group@example.com"
MISSING_DOMAIN = "gcp-local-missing.invalid"


@pytest.mark.gcp_scenario(
    id="storage-iam-directory", service="storage", transport="rest-json-v1",
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
def test_storage_iam_directory(storage, gcp_snapshot):
    iam = StorageAuthorization(storage)
    observed = StorageObservations(storage)
    storage.upload("object.txt", b"gcp-local directory evidence\n", actor="fixture").require(200)
    before = iam.policy()
    account = iam.member("allowed").removeprefix("serviceAccount:")
    members = {
        "missing-group": "group:" + MISSING_GROUP,
        "missing-group-principal-set": "principalSet://goog/group/" + MISSING_GROUP,
        "group-names-service-account": "group:" + account,
        "empty-group": "group:",
        "missing-domain": "domain:" + MISSING_DOMAIN,
        "empty-domain": "domain:",
    }
    for name, member in members.items():
        if storage.sandbox:
            time.sleep(2)  # GCP allows about one bucket metadata update per second.
        binding = {"role": "roles/storage.objectViewer", "members": [member]}
        policy = dict(before, bindings=[*before.get("bindings", []), binding])
        response = storage.client.request("fixture", "PUT", storage.path + "/iam", body=policy)
        gcp_snapshot.match(name, observed.error(response))

    after = iam.policy()
    assert after["etag"] == before["etag"]
    gcp_snapshot.match("policy-unchanged", {"etag_unchanged": True})
    gcp_snapshot.match("denied-caller", observed.error(storage.read("object.txt", actor="denied")))
