"""Run unchanged assertions from the pinned Google Python Storage system suite."""

import base64
import hashlib

import pytest

from gcp_validation.services.storage.normalization import StorageObservations
from gcp_validation.services.storage.sdk_slice import OwnedBucket, sdk_client, upstream_slice


@pytest.mark.gcp_scenario(
    id="storage-python-sdk", service="storage", transport="rest-json-v1",
    versions={"sdk": "google-cloud-storage/3.10.1;google-auth/2.56.2",
              "provider": "not-applicable", "backend": "not-applicable"},
    contract_files=(
        "gcp_validation/services/storage/client.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
        "gcp_validation/services/storage/normalization.py",
        "gcp_validation/services/storage/sdk_slice.py",
        "tests/conformance/storage/conftest.py",
        "third_party/python-storage/storage_slice.py",
        "third_party/python-storage/manifest.json",
        "requirements/sdk.txt",
    ),
)
def test_storage_python_sdk(sdk_storage, gcp_snapshot, tmp_path):
    storage = sdk_storage
    observed = StorageObservations(storage)
    manifest, upstream = upstream_slice()
    client, session = sdk_client(storage)
    payload = b"gcp-local SDK conformance fixture\n"
    assert len(payload) <= (storage.sandbox.max_object_bytes if storage.sandbox else 1048576)
    path = tmp_path / "simple.txt"
    path.write_bytes(payload)
    bucket = client.bucket(storage.bucket)
    owned = OwnedBucket(storage, bucket)
    completed = []
    try:
        for entry in manifest["tests"]:
            name = entry["id"].split("::")[1]
            arguments = {"shared_bucket": owned, "blobs_to_delete": [], "service_account": client._credentials}
            if "file_data" in upstream[name].__code__.co_varnames[:upstream[name].__code__.co_argcount]:
                arguments["file_data"] = {"simple": {"path": str(path), "hash": base64.b64encode(hashlib.md5(payload).digest())}}
            upstream[name](**arguments)
            completed.append(entry["id"])
        listed = list(client.list_blobs(bucket, page_size=1, max_results=3, timeout=30))
        assert sorted(blob.name for blob in listed) == ["mybuffer", "smallfile"]
        gcp_snapshot.match("upstream-tests", completed)
        gcp_snapshot.match("listed", [observed.object(blob._properties) for blob in sorted(listed, key=lambda blob: blob.name)])
        for blob in listed:
            blob.delete(if_generation_match=blob.generation, timeout=30)
        assert list(client.list_blobs(bucket, max_results=1, timeout=30)) == []
        gcp_snapshot.match("deleted", {"remaining_objects": 0, "objects_deleted": len(listed)})
    finally:
        client.close()
        session.close()
