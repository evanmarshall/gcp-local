"""Conditional bucket bindings: version gating, CEL prefix scoping, and time expiry."""

import platform

import pytest

from gcp_validation.services.storage.conditions import (
    EXPIRED,
    ConditionalAuthorization,
    binding_view,
)
from gcp_validation.services.storage.normalization import StorageObservations


@pytest.mark.gcp_scenario(
    id="storage-iam-conditions", service="storage", transport="rest-json-v1",
    versions={"sdk": "stdlib-http/" + platform.python_version() + ";google-auth/2.56.2",
              "provider": "not-applicable", "backend": "not-applicable"},
    contract_files=(
        "gcp_validation/services/storage/client.py",
        "gcp_validation/services/storage/normalization.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
        "gcp_validation/services/storage/authorization.py",
        "gcp_validation/services/storage/conditions.py",
        "tests/conformance/storage/conftest.py",
    ),
)
def test_storage_iam_conditions(storage, gcp_snapshot):
    iam = ConditionalAuthorization(storage)
    observed = StorageObservations(storage)
    payload = b"gcp-local conditions evidence\n"
    storage.upload("public-a.txt", payload, actor="fixture").require(200)
    storage.upload("private-b.txt", payload, actor="fixture").require(200)

    rejected = iam.put(iam.prefix_expression(), version=1)
    gcp_snapshot.match("condition-needs-version-3", observed.error(rejected))
    invalid = iam.put('resource.name.startsWith(')
    # The message embeds GCP's internal CEL parser output; compare the error shape only.
    gcp_snapshot.match("invalid-expression", {key: value for key, value in observed.error(invalid).items()
                                              if key != "message"})

    written = iam.put(iam.prefix_expression())
    gcp_snapshot.match("conditional-grant", {"status": written.status, **binding_view(observed, written.require(200))})
    gcp_snapshot.match("read-v1-with-conditions", {"status": iam.policy_v3().status,
                                                   "v1_status": storage.client.request(
                                                       "fixture", "GET", storage.path + "/iam").status})
    gcp_snapshot.match("read-v3", binding_view(observed, iam.policy_v3().require(200)))

    allowed = iam.wait_status("public-a.txt", 200)
    gcp_snapshot.match("prefix-allowed", observed.object(allowed.require(200)))
    outside = iam.wait_status("private-b.txt", 403)
    gcp_snapshot.match("outside-prefix-denied", observed.error(outside))
    gcp_snapshot.match("denied-caller", observed.error(storage.read("public-a.txt", actor="denied")))

    expired = iam.put(EXPIRED)
    gcp_snapshot.match("expired-grant", {"status": expired.status, **binding_view(observed, expired.require(200))})
    lapsed = iam.wait_status("public-a.txt", 403)
    gcp_snapshot.match("expired-condition-denied", observed.error(lapsed))
