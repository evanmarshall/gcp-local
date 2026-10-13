"""Object versioning: noncurrent generations, versioned listings, and generation reads and deletes.

With versioning on, an overwrite or a plain delete keeps the live generation as
noncurrent (it gains timeDeleted); a delete naming a generation removes it. A
noncurrent generation keeps the bucket from deleting. Suspending versioning
stops keeping new noncurrent generations and keeps the existing ones.
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
MAX_PAGES = 8


@pytest.mark.gcp_scenario(
    id="storage-object-versioning",
    service="storage",
    transport="rest-json-v1",
    versions=VERSIONS,
    contract_files=CONTRACT,
)
def test_storage_object_versioning(bucket, gcp_snapshot):
    observed = StorageObservations(bucket)
    doc = bucket.object_path("doc")

    def version(item):
        return {
            "name": item["name"],
            "generation": observed.reference("generation", item["generation"]),
            "metageneration": item["metageneration"],
            "noncurrent": "timeDeleted" in item,
        }

    def outcome(response, shape=version):
        if response.status >= 400:
            return observed.error(response)
        if response.status == 204 or not response.body:
            return {"status": response.status}
        return {"status": response.status, **shape(response.json())}

    def call(method, path, *, actor="allowed", **query):
        return bucket.client.request(actor, method, path, query=query or None)

    def listing(**query):
        found, token = [], None
        for _ in range(MAX_PAGES):
            response = call(
                "GET", bucket.path + "/o", **query, **({"pageToken": token} if token else {})
            )
            page = response.require(200)
            found.append([version(item) for item in page.get("items", [])])
            token = page.get("nextPageToken")
            if not token:
                return found
        raise AssertionError("Listing did not end within the bounded page count")

    def versioning(*, enabled):
        response = bucket.patch_bucket({"versioning": {"enabled": enabled}})
        return {"status": response.status, "versioning": response.require(200).get("versioning")}

    gcp_snapshot.match("versioning-enabled", versioning(enabled=True))
    first = bucket.put("doc", b"one", query={"ifGenerationMatch": 0}).require(200)
    second = bucket.put("doc", b"two", query={"ifGenerationMatch": first["generation"]}).require(
        200
    )
    gcp_snapshot.match("overwritten", {"first": version(first), "second": version(second)})
    gcp_snapshot.match(
        "create-only-refused", outcome(bucket.put("doc", b"x", query={"ifGenerationMatch": 0}))
    )
    gcp_snapshot.match("live-listing", listing())
    gcp_snapshot.match("versions-listing", listing(versions="true"))
    gcp_snapshot.match("versions-pages", listing(versions="true", maxResults=1))
    gcp_snapshot.match("noncurrent-read", outcome(call("GET", doc, generation=first["generation"])))
    media = call("GET", doc, generation=first["generation"], alt="media")
    gcp_snapshot.match("noncurrent-media", observed.content(media))
    gcp_snapshot.match("live-read", outcome(call("GET", doc)))
    gcp_snapshot.match(
        "noncurrent-read-precondition",
        outcome(
            call("GET", doc, generation=first["generation"], ifGenerationMatch=second["generation"])
        ),
    )
    gcp_snapshot.match(
        "noncurrent-read-own-precondition",
        outcome(
            call("GET", doc, generation=first["generation"], ifGenerationMatch=first["generation"])
        ),
    )

    gcp_snapshot.match(
        "live-delete", outcome(call("DELETE", doc, ifGenerationMatch=second["generation"]))
    )
    gcp_snapshot.match("read-after-delete", outcome(call("GET", doc)))
    gcp_snapshot.match("versions-after-delete", listing(versions="true"))
    gcp_snapshot.match(
        "noncurrent-read-after-delete", outcome(call("GET", doc, generation=second["generation"]))
    )
    third = bucket.put("doc", b"three", query={"ifGenerationMatch": 0}).require(200)
    gcp_snapshot.match("recreated", version(third))

    gcp_snapshot.match(
        "noncurrent-delete-precondition",
        outcome(
            call(
                "DELETE", doc, generation=first["generation"], ifGenerationMatch=third["generation"]
            )
        ),
    )
    gcp_snapshot.match(
        "noncurrent-delete", outcome(call("DELETE", doc, generation=first["generation"]))
    )
    gcp_snapshot.match(
        "deleted-generation-read", outcome(call("GET", doc, generation=first["generation"]))
    )
    gcp_snapshot.match(
        "deleted-generation-delete", outcome(call("DELETE", doc, generation=first["generation"]))
    )
    gcp_snapshot.match("versions-after-generation-delete", listing(versions="true"))

    gcp_snapshot.match("live-delete-again", outcome(call("DELETE", doc)))
    gcp_snapshot.match(
        "bucket-delete-with-noncurrent", outcome(call("DELETE", bucket.path, actor="fixture"))
    )

    gcp_snapshot.match("versioning-suspended", versioning(enabled=False))
    fourth = bucket.put("doc", b"four", query={"ifGenerationMatch": 0}).require(200)
    bucket.put("doc", b"five", query={"ifGenerationMatch": fourth["generation"]}).require(200)
    gcp_snapshot.match("versions-after-suspend", listing(versions="true"))
