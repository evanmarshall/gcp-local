"""The calls cloud.google.com/go/storage v1.65.0 sends with projection=full.

Request shapes come from the Go client against a uniform bucket: it sends
alt=json and prettyPrint=false on every call, projection=full on bucket and
object reads, lists, and patches, and the default list options shown below.
The recording settles whether GCP returns acl, defaultObjectAcl, or owner
under projection=full on a uniform bucket, and how GCP answers an option the
emulator does not serve. Not recorded.
"""

import pytest

from gcp_validation.normalization import StorageObservations
from gcp_validation.storage_clients import ClientTransport, acl_shape

GO = {"alt": "json", "prettyPrint": "false"}
FULL = {**GO, "projection": "full"}


@pytest.mark.gcp_scenario(
    id="storage-go-client-projection",
    service="storage",
    transport="rest-json-v1",
    versions={
        "sdk": "cloud.google.com/go/storage/1.65.0 request shapes",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/storage.py",
        "gcp_validation/normalization.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
        "gcp_validation/storage_clients.py",
        "tests/conformance/conftest.py",
    ),
)
def test_storage_go_client_projection(storage, gcp_snapshot):
    observed = StorageObservations(storage)
    transport = ClientTransport(storage)
    base = storage.client.endpoint + "/storage/v1/b"
    bucket = base + "/" + storage.bucket

    got = transport.send("GET", bucket, actor="fixture", query=FULL)
    gcp_snapshot.match("bucket-get", {"status": got.status, **acl_shape(got.json())})
    listed = transport.send(
        "GET",
        base,
        actor="fixture",
        query={
            **FULL,
            "project": storage.project,
            "pageToken": "",
            "prefix": "",
            "returnPartialSuccess": "false",
        },
    )
    items = listed.json().get("items", [])
    gcp_snapshot.match(
        "bucket-list",
        {
            "status": listed.status,
            "count": len(items),
            "has_unreachable": "unreachable" in listed.json(),
            **acl_shape(items[0] if items else {}),
        },
    )
    patched = transport.send(
        "PATCH",
        bucket,
        actor="fixture",
        query=FULL,
        body={"labels": {"go-client": "projection-full"}},
    )
    gcp_snapshot.match(
        "bucket-patch",
        {
            "status": patched.status,
            "label": patched.json().get("labels", {}).get("go-client"),
            **acl_shape(patched.json()),
        },
    )

    transport.own("go-object.txt")
    storage.upload("go-object.txt", b"go client object\n", actor="fixture").require(200)
    obj = bucket + "/o/go-object.txt"
    read = transport.send("GET", obj, actor="fixture", query=FULL)
    gcp_snapshot.match(
        "object-get",
        {"status": read.status, **observed.object(read.json()), **acl_shape(read.json())},
    )
    objects = transport.send(
        "GET",
        bucket + "/o",
        actor="fixture",
        query={
            **FULL,
            "delimiter": "",
            "endOffset": "",
            "includeFoldersAsPrefixes": "false",
            "includeTrailingDelimiter": "false",
            "matchGlob": "",
            "pageToken": "",
            "prefix": "",
            "startOffset": "",
            "versions": "false",
        },
    )
    entries = objects.json().get("items", [])
    gcp_snapshot.match(
        "object-list",
        {
            "status": objects.status,
            "names": [entry["name"] for entry in entries],
            **acl_shape(entries[0] if entries else {}),
        },
    )
    object_patch = transport.send(
        "PATCH", obj, actor="fixture", query=FULL, body={"contentType": "text/plain"}
    )
    gcp_snapshot.match(
        "object-patch",
        {"status": object_patch.status, "content_type": object_patch.json().get("contentType")},
    )

    unknown = transport.send(
        "GET", bucket, actor="fixture", query={**FULL, "unsupportedOption": "true"}
    )
    gcp_snapshot.match("unknown-option", {"status": unknown.status})
    invalid = transport.send("GET", bucket, actor="fixture", query={**GO, "projection": "custom"})
    gcp_snapshot.match("invalid-projection", {"status": invalid.status})
