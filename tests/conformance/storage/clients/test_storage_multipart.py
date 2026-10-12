"""The bounded multipart request shape used by the Google Terraform provider."""

import json

import pytest

from gcp_validation.services.storage.normalization import StorageObservations
from gcp_validation.services.storage.sdk_slice import sdk_client


@pytest.mark.gcp_scenario(
    id="storage-multipart-query-name", service="storage", transport="rest-json-v1",
    versions={"sdk": "google-cloud-storage/3.10.1;google-auth/2.56.2",
              "provider": "not-applicable", "backend": "not-applicable"},
    contract_files=(
        "gcp_validation/services/storage/client.py",
        "gcp_validation/services/storage/normalization.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
        "gcp_validation/services/storage/sdk_slice.py",
        "tests/conformance/storage/conftest.py",
    ),
)
def test_storage_multipart_query_name(sdk_storage, gcp_snapshot):
    storage = sdk_storage
    observed = StorageObservations(storage)
    _, session = sdk_client(storage)
    name = "terraform-multipart.txt"
    assert len(storage.manifest["objects"]) < storage.max_objects
    storage.manifest["objects"].append(name)
    storage.save()
    boundary = "gcp-local-bounded-multipart"
    try:
        generation = "0"
        for key, payload, fields in (("query-name-upload", b"data data data", {"bucket": storage.bucket}),
                                     ("standard-replacement", b"datum", {"bucket": storage.bucket, "storageClass": "STANDARD"})):
            metadata = json.dumps(fields).encode()
            body = (b"--" + boundary.encode() + b"\r\nContent-Type: application/json\r\n\r\n"
                    + metadata + b"\r\n--" + boundary.encode()
                    + b"\r\nContent-Type: text/plain\r\n\r\n" + payload
                    + b"\r\n--" + boundary.encode() + b"--\r\n")
            response = session.request("POST", storage.client.endpoint + "/upload/storage/v1/b/" + storage.bucket + "/o",
                params={"uploadType": "multipart", "name": name, "ifGenerationMatch": generation}, data=body,
                headers={"Content-Type": "multipart/related; boundary=" + boundary})
            assert response.status_code == 200
            value = response.json()
            assert value["name"] == name and value["bucket"] == storage.bucket
            assert value["generation"] != generation and value["storageClass"] == "STANDARD"
            generation = value["generation"]
            assert storage.read(name, media=True).body == payload
            gcp_snapshot.match(key, observed.object(value))
            gcp_snapshot.match(key + "-read", observed.object(storage.read(name).require(200)))
    finally:
        session.close()
