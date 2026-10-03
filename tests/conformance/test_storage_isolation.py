"""A foreign project caller gains access only through a target bucket grant."""

import platform

import pytest

from gcp_validation.authorization import StorageAuthorization
from gcp_validation.normalization import StorageObservations


@pytest.mark.gcp_scenario(
    id="storage-cross-project", service="storage", transport="rest-json-v1",
    versions={"sdk": "stdlib-http/" + platform.python_version() + ";google-auth/2.56.2",
              "provider": "not-applicable", "backend": "not-applicable"},
    contract_files=("gcp_validation/storage.py", "gcp_validation/normalization.py",
                    "gcp_validation/sandbox.py", "gcp_validation/authorization.py",
                    "tests/conformance/conftest.py"),
)
def test_storage_cross_project(peer_storage, gcp_snapshot):
    storage = peer_storage
    iam = StorageAuthorization(storage)
    observed = StorageObservations(storage)
    payload = b"gcp-local cross-project evidence\n"
    original = storage.upload("object.txt", payload).require(200)
    gcp_snapshot.match("initial-object", observed.object(original))
    gcp_snapshot.match("project-relationship", {"resource_project": "<project>",
        "caller_project": "<peer-project>", "caller": "peer", "same_project": False})

    for label, response in (("foreign-read-denied", storage.read("object.txt", actor="peer")),
                            ("foreign-write-denied", storage.upload("foreign.txt", payload, actor="peer"))):
        response.require(403)
        gcp_snapshot.match(label, observed.error(response))
    listing = storage.list_objects(actor="fixture").require(200)
    assert not listing.get("nextPageToken")
    assert [entry["name"] for entry in listing.get("items", [])] == ["object.txt"]
    gcp_snapshot.match("unchanged-after-foreign-write", [observed.object(entry) for entry in listing["items"]])

    gcp_snapshot.match("foreign-bucket-grant", iam.set_role("peer", "roles/storage.objectViewer"))
    granted = iam.wait_read("object.txt", "peer", 200).require(200)
    assert granted["generation"] == original["generation"] and granted["etag"] == original["etag"]
    gcp_snapshot.match("foreign-read-granted", observed.object(granted))
    downloaded = storage.read("object.txt", actor="peer", media=True)
    assert downloaded.status == 200 and downloaded.body == payload
    gcp_snapshot.match("foreign-download", observed.content(downloaded))
    gcp_snapshot.match("foreign-bucket-revocation", iam.set_role("peer"))
    revoked = iam.wait_read("object.txt", "peer", 403)
    gcp_snapshot.match("foreign-read-revoked", observed.error(revoked))
    assert storage.read("object.txt", media=True).body == payload
