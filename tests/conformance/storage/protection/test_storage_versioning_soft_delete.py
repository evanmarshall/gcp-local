"""Soft delete in a versioned bucket: a generation is soft-deleted only when it leaves the bucket.

An overwrite or a plain delete makes the live generation noncurrent, which soft
delete does not keep; deleting a noncurrent generation soft-deletes it.
Restoring it writes a new live generation and makes the old live one
noncurrent. GCP keeps this scenario's bucket soft-deleted for seven days after
cleanup, so it is not in the weekly drift plan.
"""

import platform

import pytest

from gcp_validation.services.storage.normalization import StorageObservations

CONTRACT = (
    "gcp_validation/services/storage/client.py",
    "gcp_validation/services/storage/configured.py",
    "gcp_validation/services/storage/normalization.py",
    "gcp_validation/sandbox.py",
    "gcp_validation/federation.py",
    "tests/conformance/storage/protection/conftest.py",
)
VERSIONS = {
    "sdk": "stdlib-http/" + platform.python_version() + ";google-auth/2.56.2",
    "provider": "not-applicable",
    "backend": "not-applicable",
}


@pytest.mark.gcp_scenario(
    id="storage-versioning-soft-delete",
    service="storage",
    transport="rest-json-v1",
    versions=VERSIONS,
    contract_files=CONTRACT,
)
def test_storage_versioning_soft_delete(soft_delete_bucket, gcp_snapshot):
    bucket = soft_delete_bucket
    observed = StorageObservations(bucket)
    doc = bucket.object_path("doc")

    def version(item):
        return {
            "generation": observed.reference("generation", item["generation"]),
            "noncurrent": "timeDeleted" in item,
            "soft_deleted": "softDeleteTime" in item,
        }

    def listing(**query):
        response = bucket.client.request(
            "allowed", "GET", bucket.path + "/o", query={"maxResults": 20, **query}
        )
        return [version(item) for item in response.require(200).get("items", [])]

    bucket.patch_bucket({"versioning": {"enabled": True}}).require(200)
    first = bucket.put("doc", b"one", query={"ifGenerationMatch": 0}).require(200)
    second = bucket.put("doc", b"two", query={"ifGenerationMatch": first["generation"]}).require(
        200
    )
    gcp_snapshot.match("soft-deleted-after-overwrite", listing(softDeleted="true"))
    bucket.client.request(
        "allowed", "DELETE", doc, query={"ifGenerationMatch": second["generation"]}
    ).require(204)
    gcp_snapshot.match("soft-deleted-after-live-delete", listing(softDeleted="true"))
    gcp_snapshot.match("versions-after-live-delete", listing(versions="true"))
    bucket.client.request(
        "allowed", "DELETE", doc, query={"generation": first["generation"]}
    ).require(204)
    gcp_snapshot.match("soft-deleted-after-generation-delete", listing(softDeleted="true"))
    gcp_snapshot.match("versions-after-generation-delete", listing(versions="true"))
    restored = bucket.client.request(
        "allowed", "POST", doc + "/restore", query={"generation": first["generation"]}
    )
    gcp_snapshot.match("restored", version(restored.require(200)))
    gcp_snapshot.match("versions-after-restore", listing(versions="true"))
    gcp_snapshot.match("soft-deleted-after-restore", listing(softDeleted="true"))
    bucket.put("doc", b"three", query={"ifGenerationMatch": restored.json()["generation"]}).require(
        200
    )
    gcp_snapshot.match("soft-deleted-after-restored-overwrite", listing(softDeleted="true"))
