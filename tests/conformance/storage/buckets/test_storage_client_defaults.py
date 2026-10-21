"""Two client defaults checked against Cloud Storage before reporting them upstream.

thanos-io/objstore's acceptance test uploads with ifGenerationMatch=-1 and
expects a precondition failure. gcsfs's mkdir creates a bucket with
predefinedAcl=projectPrivate and predefinedDefaultObjectAcl=bucketOwnerFullControl
and no iamConfiguration; the sandbox organization enforces
storage.uniformBucketLevelAccess. The scenario reuses the owned bucket name,
so cleanup and interrupted-run recovery cover the recreated bucket.
"""

import platform
import time

import pytest

from gcp_validation.evidence import Inconclusive
from gcp_validation.services.storage.normalization import StorageObservations


@pytest.mark.gcp_scenario(
    id="storage-client-defaults", service="storage", transport="rest-json-v1",
    versions={"sdk": "stdlib-http/" + platform.python_version() + ";google-auth/2.56.2",
              "provider": "not-applicable", "backend": "not-applicable"},
    contract_files=(
        "gcp_validation/services/storage/client.py",
        "gcp_validation/services/storage/normalization.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
        "tests/conformance/storage/conftest.py",
    ),
)
def test_storage_client_defaults(storage, gcp_snapshot):
    observed = StorageObservations(storage)
    negative = storage.upload("negative.txt", b"payload", generation=-1)
    if negative.status == 200:
        storage.delete("negative.txt", negative.json()["generation"]).require(204)
    outcome = {"status": 200} if negative.status == 200 else observed.error(negative)
    gcp_snapshot.match("negative-generation-match", outcome)

    storage.client.request("fixture", "DELETE", storage.path).require(204)
    storage.client.request("fixture", "GET", storage.path).require(404)
    labels = {"managed-by": "gcp-local", "gcp-local-run": storage.manifest["run_id"]}
    deadline = time.monotonic() + (60 if storage.sandbox else 0)
    while True:
        created = storage.client.request(
            "fixture", "POST", "/storage/v1/b",
            query={"project": storage.project, "predefinedAcl": "projectPrivate",
                   "predefinedDefaultObjectAcl": "bucketOwnerFullControl"},
            body={"name": storage.bucket, "location": storage.region.upper(), "labels": labels})
        if created.status != 409 or time.monotonic() >= deadline:
            break
        time.sleep(5)  # A deleted bucket's name can take a moment to free.
    if created.status == 409:
        raise Inconclusive("The deleted bucket's name was not reusable within the deadline")
    if created.status == 200:
        bucket = created.json()
        storage.assert_owned(bucket)
        iam = bucket.get("iamConfiguration", {})
        gcp_snapshot.match("predefined-acl-create", {
            "status": 200,
            "uniform_access": iam.get("uniformBucketLevelAccess", {}).get("enabled"),
            "acl_returned": "acl" in bucket,
            "default_object_acl_returned": "defaultObjectAcl" in bucket,
        })
    else:
        gcp_snapshot.match("predefined-acl-create", observed.error(created))
