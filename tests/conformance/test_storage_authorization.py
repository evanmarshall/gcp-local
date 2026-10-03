"""Private UBLA bucket: caller grants, inherited admin access, and revocation."""

import platform

import pytest

from gcp_validation.authorization import StorageAuthorization
from gcp_validation.normalization import StorageObservations


@pytest.mark.gcp_scenario(
    id="storage-authorization", service="storage", transport="rest-json-v1",
    versions={"sdk": "stdlib-http/" + platform.python_version() + ";google-auth/2.56.2",
              "provider": "not-applicable", "backend": "not-applicable"},
    contract_files=("gcp_validation/storage.py", "gcp_validation/normalization.py",
                    "gcp_validation/sandbox.py", "gcp_validation/authorization.py",
                    "tests/conformance/conftest.py"),
)
def test_storage_authorization(storage, gcp_snapshot):
    iam = StorageAuthorization(storage)
    observed = StorageObservations(storage)
    payload = b"gcp-local authorization evidence\n"
    original = storage.upload("object.txt", payload).require(200)
    gcp_snapshot.match("initial-object", observed.object(original))

    def unchanged(label):
        listing = storage.list_objects(actor="fixture").require(200)
        assert not listing.get("nextPageToken")
        assert [entry["name"] for entry in listing.get("items", [])] == ["object.txt"]
        current = storage.read("object.txt", actor="fixture").require(200)
        assert current["generation"] == original["generation"] and current["etag"] == original["etag"]
        assert storage.read("object.txt", actor="fixture", media=True).body == payload
        gcp_snapshot.match(label, observed.object(current))

    allowed = storage.read("object.txt", media=True)
    assert allowed.status == 200 and allowed.body == payload
    gcp_snapshot.match("bucket-granted-read", observed.content(allowed))
    denied = storage.read("object.txt", actor="denied")
    denied.require(403)
    gcp_snapshot.match("missing-read-permission", observed.error(denied))
    denied = storage.upload("denied.txt", payload, actor="denied")
    denied.require(403)
    gcp_snapshot.match("missing-create-permission", observed.error(denied))
    unchanged("unchanged-after-denied-write")

    for operation, response in (("read", storage.read("object.txt", actor="anonymous")),
                                ("write", storage.upload("anonymous.txt", payload, actor="anonymous"))):
        assert response.status in {401, 403}
        gcp_snapshot.match("anonymous-" + operation, observed.error(response))
    unchanged("unchanged-after-anonymous-write")

    # There must be no explicit fixture grant on the bucket. Its Storage admin
    # grant is attached to the project, as required by the selected sandbox setup.
    fixture_email = (storage.sandbox.identities["fixture"].email if storage.sandbox
                     else "fixture@" + storage.project + ".iam.gserviceaccount.com")
    assert not any("serviceAccount:" + fixture_email in binding["members"]
                   for binding in iam.policy().get("bindings", []))
    gcp_snapshot.match("allowed-grant-removed", iam.set_role("allowed"))
    denied = iam.wait_read("object.txt", "allowed", 403)
    gcp_snapshot.match("bucket-revocation", observed.error(denied))
    inherited = storage.read("object.txt", actor="fixture", media=True)
    assert inherited.status == 200 and inherited.body == payload
    gcp_snapshot.match("project-inherited-read", {
        "actor": "fixture", "attachment": "project", "role": "roles/storage.admin",
        "explicit_bucket_binding": False, "result": observed.content(inherited)})
    gcp_snapshot.match("allowed-grant-restored", iam.set_role("allowed", "roles/storage.objectUser"))
    iam.wait_read("object.txt", "allowed", 200)

    gcp_snapshot.match("reader-granted", iam.set_role("denied", "roles/storage.objectViewer"))
    granted = iam.wait_read("object.txt", "denied", 200)
    gcp_snapshot.match("reader-after-grant", observed.object(granted.require(200)))
    denied_write = storage.upload("reader-write.txt", payload, actor="denied")
    denied_write.require(403)
    gcp_snapshot.match("reader-still-cannot-write", observed.error(denied_write))
    unchanged("unchanged-after-reader-write")
    gcp_snapshot.match("reader-revoked", iam.set_role("denied"))
    revoked = iam.wait_read("object.txt", "denied", 403)
    gcp_snapshot.match("reader-after-revocation", observed.error(revoked))
    unchanged("unchanged-after-transitions")
