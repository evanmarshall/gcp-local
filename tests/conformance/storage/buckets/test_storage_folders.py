"""Folders: served only in hierarchical-namespace buckets.

- storage-hierarchical-namespace: a bucket created with hierarchicalNamespace
  enabled (it requires uniform bucket-level access); folder create, read,
  list, and delete; the refusals for a missing parent, a duplicate, a name
  without a trailing slash, and a non-empty folder; the folders an object
  upload creates; a bucket delete while folders remain, which GCP allows.
- storage-folders-flat: every folder method on a bucket without hierarchical
  namespace, which GCP refuses with 409.
"""

import platform
from urllib.parse import quote

import pytest

from gcp_validation.services.storage.configured import owned_bucket
from gcp_validation.services.storage.normalization import StorageObservations

VERSIONS = {
    "sdk": "stdlib-http/" + platform.python_version() + ";google-auth/2.56.2",
    "provider": "not-applicable",
    "backend": "not-applicable",
}
CONTRACT = (
    "gcp_validation/services/storage/client.py",
    "gcp_validation/services/storage/configured.py",
    "gcp_validation/services/storage/normalization.py",
    "gcp_validation/sandbox.py",
    "gcp_validation/federation.py",
)
NAMESPACE = {"hierarchicalNamespace": {"enabled": True}}


@pytest.fixture
def hns_bucket(gcp_snapshot, gcp_target, pytestconfig):
    yield from owned_bucket(gcp_snapshot, gcp_target, pytestconfig, body=NAMESPACE)


def folder_shape(observed, response):
    if response.status >= 400:
        return observed.error(response)
    if response.status == 204:
        return {"status": 204}
    value = response.json()
    return {
        "status": response.status,
        "fields": sorted(value),
        "kind": value.get("kind"),
        "name": value.get("name"),
        "bucket": observed.text(value.get("bucket", "")),
        "metageneration": value.get("metageneration"),
    }


def folders(fixture, actor="fixture", **query):
    return fixture.client.request(actor, "GET", fixture.path + "/folders", query=query or None)


def names(observed, response):
    if response.status >= 400:
        return observed.error(response)
    value = response.json()
    return {
        "status": response.status,
        "names": [item["name"] for item in value.get("items", [])],
        "has_next": "nextPageToken" in value,
        "kind": value.get("kind"),
    }


@pytest.mark.gcp_scenario(
    id="storage-hierarchical-namespace",
    service="storage",
    transport="rest-json-v1",
    versions=VERSIONS,
    contract_files=CONTRACT,
)
def test_storage_hierarchical_namespace(hns_bucket, gcp_snapshot):
    bucket = hns_bucket
    observed = StorageObservations(bucket)

    def create(name, actor="fixture", **query):
        return bucket.client.request(
            actor, "POST", bucket.path + "/folders", body={"name": name}, query=query or None
        )

    def folder(method, name, actor="fixture"):
        return bucket.client.request(
            actor, method, bucket.path + "/folders/" + quote(name, safe="")
        )

    created = bucket.creation_response.json()
    gcp_snapshot.match(
        "bucket-created",
        {
            "hierarchicalNamespace": created.get("hierarchicalNamespace"),
            "uniform": created["iamConfiguration"]["uniformBucketLevelAccess"]["enabled"],
        },
    )
    layout = bucket.client.request("fixture", "GET", bucket.path + "/storageLayout").require(200)
    gcp_snapshot.match(
        "storage-layout",
        {"fields": sorted(layout), "hierarchicalNamespace": layout.get("hierarchicalNamespace")},
    )
    without_uniform = bucket.client.request(
        "fixture",
        "POST",
        "/storage/v1/b",
        query={"project": bucket.project},
        body={
            "name": bucket.bucket,
            "location": bucket.region.upper(),
            **NAMESPACE,
            "iamConfiguration": {"uniformBucketLevelAccess": {"enabled": False}},
        },
    )
    gcp_snapshot.match("namespace-without-uniform", observed.error(without_uniform))

    gcp_snapshot.match("create", folder_shape(observed, create("f1/")))
    gcp_snapshot.match("create-nested", folder_shape(observed, create("f1/f2/")))
    gcp_snapshot.match("create-duplicate", folder_shape(observed, create("f1/")))
    gcp_snapshot.match("create-missing-parent", folder_shape(observed, create("x/y/")))
    gcp_snapshot.match("create-recursive", folder_shape(observed, create("x/y/", recursive="true")))
    gcp_snapshot.match("create-without-slash", folder_shape(observed, create("plain")))
    gcp_snapshot.match("create-denied", folder_shape(observed, create("d/", actor="denied")))
    gcp_snapshot.match("get", folder_shape(observed, folder("GET", "f1/")))
    gcp_snapshot.match("get-missing", folder_shape(observed, folder("GET", "absent/")))
    gcp_snapshot.match("list", names(observed, folders(bucket)))
    gcp_snapshot.match("list-prefix", names(observed, folders(bucket, prefix="f1/")))
    gcp_snapshot.match("list-page", names(observed, folders(bucket, pageSize=1)))

    bucket.put("g/obj", b"nested").require(200)
    gcp_snapshot.match("list-after-upload", names(observed, folders(bucket)))
    gcp_snapshot.match("delete-with-subfolder", folder_shape(observed, folder("DELETE", "f1/")))
    gcp_snapshot.match("delete-with-object", folder_shape(observed, folder("DELETE", "g/")))
    listing = bucket.client.request(
        "allowed",
        "GET",
        bucket.path + "/o",
        query={"delimiter": "/", "includeFoldersAsPrefixes": "true"},
    ).require(200)
    gcp_snapshot.match(
        "objects-with-folder-prefixes",
        {
            "prefixes": listing.get("prefixes", []),
            "items": [item["name"] for item in listing.get("items", [])],
        },
    )
    gcp_snapshot.match("delete-nested", folder_shape(observed, folder("DELETE", "f1/f2/")))
    gcp_snapshot.match("delete", folder_shape(observed, folder("DELETE", "f1/")))
    gcp_snapshot.match("delete-missing", folder_shape(observed, folder("DELETE", "f1/")))
    generation = bucket.read("g/obj").require(200)["generation"]
    bucket.client.request(
        "allowed", "DELETE", bucket.object_path("g/obj"), query={"ifGenerationMatch": generation}
    ).require(204)
    gcp_snapshot.match("list-after-object-delete", names(observed, folders(bucket)))
    gcp_snapshot.match(
        "bucket-delete-with-folders",
        folder_shape(observed, bucket.client.request("fixture", "DELETE", bucket.path)),
    )


@pytest.mark.gcp_scenario(
    id="storage-folders-flat",
    service="storage",
    transport="rest-json-v1",
    versions=VERSIONS,
    contract_files=(*CONTRACT, "tests/conformance/storage/conftest.py"),
)
def test_storage_folders_flat(storage, gcp_snapshot):
    observed = StorageObservations(storage)
    path = storage.path + "/folders"
    gcp_snapshot.match(
        "create",
        folder_shape(
            observed, storage.client.request("fixture", "POST", path, body={"name": "f1/"})
        ),
    )
    gcp_snapshot.match(
        "get", folder_shape(observed, storage.client.request("fixture", "GET", path + "/f1%2F"))
    )
    gcp_snapshot.match("list", names(observed, folders(storage)))
    gcp_snapshot.match(
        "delete",
        folder_shape(observed, storage.client.request("fixture", "DELETE", path + "/f1%2F")),
    )
    gcp_snapshot.match("denied-list", names(observed, folders(storage, actor="denied")))
