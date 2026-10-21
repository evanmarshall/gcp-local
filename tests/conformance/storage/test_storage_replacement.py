"""Create-only versus replacement permissions on one owned private bucket."""

import platform
import time

import pytest

from gcp_validation.evidence import Inconclusive
from gcp_validation.services.storage.authorization import StorageAuthorization
from gcp_validation.services.storage.normalization import StorageObservations


@pytest.mark.gcp_scenario(
    id="storage-object-replacement", service="storage", transport="rest-json-v1",
    versions={"sdk": "stdlib-http/" + platform.python_version() + ";google-auth/2.56.2",
              "provider": "not-applicable", "backend": "not-applicable"},
    contract_files=(
        "gcp_validation/services/storage/client.py",
        "gcp_validation/services/storage/normalization.py",
        "gcp_validation/services/storage/authorization.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
        "tests/conformance/storage/conftest.py",
    ),
)
def test_storage_object_replacement(storage, gcp_snapshot):
    def payload(name):
        # GCP can briefly refuse the allowed caller while the bucket policy
        # change above propagates; the grant is unchanged, so retry a 403.
        deadline = time.monotonic() + (120 if storage.sandbox else 0)
        read = storage.read(name, media=True)
        while read.status == 403 and time.monotonic() < deadline:
            time.sleep(5)
            read = storage.read(name, media=True)
        assert read.status == 200, f"Storage returned HTTP {read.status} for the media read"
        return read.body

    iam = StorageAuthorization(storage)
    observed = StorageObservations(storage)
    member = iam.member("denied")
    policy = iam.policy()
    assert not any(member in binding["members"] for binding in policy.get("bindings", []))
    policy.setdefault("bindings", []).append({"role": "roles/storage.objectCreator", "members": [member]})
    storage.client.request("fixture", "PUT", storage.path + "/iam", body=policy).require(200)
    # Repeating a denied create is harmless; stop at the first successful write.
    deadline = time.monotonic() + (420 if storage.sandbox else 0)
    while True:
        created = storage.upload("creator.txt", b"original payload", actor="denied", generation=0)
        if created.status == 200:
            break
        created.require(403)
        if time.monotonic() >= deadline:
            raise Inconclusive("Creator grant did not converge within the bounded deadline")
        time.sleep(5)
    original = created.require(200)
    gcp_snapshot.match("creator-created", observed.object(original))
    rejected = storage.upload("creator.txt", b"must not replace", actor="denied", generation=original["generation"])
    rejected.require(403)
    gcp_snapshot.match("creator-replacement-denied", observed.error(rejected))
    after = storage.read("creator.txt").require(200)
    assert after["generation"] == original["generation"] and after["etag"] == original["etag"]
    assert payload("creator.txt") == b"original payload"
    gcp_snapshot.match("unchanged-after-denial", observed.object(after))
    replacement = storage.upload("creator.txt", b"replacement payload", generation=original["generation"]).require(200)
    assert replacement["generation"] != original["generation"] and replacement["etag"] != original["etag"]
    assert payload("creator.txt") == b"replacement payload"
    gcp_snapshot.match("object-user-replaced", observed.object(replacement))
    same = storage.upload("creator.txt", b"replacement payload", generation=replacement["generation"]).require(200)
    assert same["generation"] != replacement["generation"] and same["etag"] != replacement["etag"]
    gcp_snapshot.match("same-payload-replaced", observed.object(same))
    stale = storage.upload("creator.txt", b"stale writer", generation=original["generation"])
    stale.require(412)
    gcp_snapshot.match("stale-generation-denied", observed.error(stale))
    assert payload("creator.txt") == b"replacement payload"
