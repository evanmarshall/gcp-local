"""One bounded contract, executable against GCP or an explicit local endpoint."""

import platform

import pytest

from gcp_validation.normalization import StorageObservations


@pytest.mark.gcp_scenario(
    id="storage-object-lifecycle", service="storage", transport="rest-json-v1",
    versions={"sdk": "stdlib-http/" + platform.python_version() + ";google-auth/2.56.2",
              "provider": "not-applicable", "backend": "not-applicable"},
    contract_files=("gcp_validation/storage.py", "gcp_validation/normalization.py",
                    "gcp_validation/sandbox.py", "tests/conformance/conftest.py"),
)
def test_storage_object_lifecycle(storage, gcp_snapshot):
    observations = StorageObservations(storage)
    payload = b"gcp-local bounded Storage conformance\n"
    name = "object.txt"
    gcp_snapshot.match("bucket-created", observations.bucket(storage.creation_response))

    uploaded_response = storage.upload(name, payload)
    uploaded = uploaded_response.require(200)
    assert uploaded["bucket"] == storage.bucket and uploaded["name"] == name
    assert int(uploaded["size"]) == len(payload)
    gcp_snapshot.match("uploaded", {"status": uploaded_response.status, "object": observations.object(uploaded)})

    downloaded = storage.read(name, media=True)
    assert downloaded.status == 200 and downloaded.body == payload
    gcp_snapshot.match("downloaded", observations.content(downloaded))

    listed = storage.list_objects().require(200)
    assert not listed.get("nextPageToken")
    assert [entry["name"] for entry in listed["items"]] == [name]
    gcp_snapshot.match("listed", [observations.object(entry) for entry in listed["items"]])

    conditional = storage.upload(name, b"must not replace the object", generation=0)
    assert conditional.status == 412
    gcp_snapshot.match("conditional-write-rejected", observations.error(conditional))
    after = storage.read(name).require(200)
    assert after["etag"] == uploaded["etag"] and after["generation"] == uploaded["generation"]
    assert storage.read(name, media=True).body == payload
    gcp_snapshot.match("unchanged-after-precondition", observations.object(after))

    denied = storage.upload("denied.txt", payload, actor="denied")
    assert denied.status == 403
    error = observations.error(denied)
    assert "storage.objects.create" in error["message"]
    gcp_snapshot.match("denied-write", error)
    after_denial = storage.list_objects().require(200)
    assert [entry["name"] for entry in after_denial["items"]] == [name]
    assert not after_denial.get("nextPageToken")
    gcp_snapshot.match("unchanged-after-denial", [observations.object(entry) for entry in after_denial["items"]])

    deleted = storage.delete(name, uploaded["generation"])
    deleted.require(204)
    gcp_snapshot.match("deleted", {"status": deleted.status})
    missing = storage.read(name)
    assert missing.status == 404
    gcp_snapshot.match("missing-after-delete", observations.error(missing))
