"""The object requests gcloud 588.0.0 sends for storage cp, cat, ls, and rm.

Request shapes come from gcloud --log-http against gcp-local: every call
carries alt=json; cp and cat first read the bucket's storageLayout (gcloud
checks locationType for zonal buckets and hierarchicalNamespace for folders);
cp uploads with apitools' multipart body, whose content type quotes the
boundary with Python's %r (boundary='===...=='); ls sends
includeFoldersAsPrefixes=True with a "/" delimiter and a fields mask.
"""

import uuid
from email import generator
from email.mime.multipart import MIMEMultipart
from email.mime.nonmultipart import MIMENonMultipart
from io import BytesIO

import pytest

from gcp_validation.normalization import StorageObservations
from gcp_validation.storage_clients import ClientTransport

JSON = {"alt": "json"}
LIST_FIELDS = "prefixes,items/name,items/size,items/generation,nextPageToken"


def apitools_multipart(metadata, media_type, data):
    """Build the body and content type apitools sends for a multipart upload."""
    root = MIMEMultipart("related", boundary="=" * 15 + uuid.uuid4().hex + "==")
    root._write_headers = lambda self: None
    part = MIMENonMultipart("application", "json")
    part.set_payload(metadata)
    root.attach(part)
    media = MIMENonMultipart(*media_type.split("/"))
    media["Content-Transfer-Encoding"] = "binary"
    media.set_payload(data.decode())
    root.attach(media)
    output = BytesIO()
    generator.BytesGenerator(output, mangle_from_=False).flatten(root, unixfrom=False)
    # apitools renders the boundary with %r, which single-quotes it.
    return output.getvalue(), f"multipart/related; boundary={root.get_boundary()!r}"


def layout_shape(observed, reply):
    if reply.status != 200:
        return observed.error(reply)
    value = reply.json()
    return {"status": reply.status, "kind": value.get("kind"),
            "bucket": observed.text(value["bucket"]), "location": value.get("location"),
            "locationType": value.get("locationType"),
            "hierarchicalNamespace": value.get("hierarchicalNamespace")}


@pytest.mark.gcp_scenario(
    id="storage-gcloud-object-requests",
    service="storage",
    transport="rest-json-v1",
    versions={"sdk": "gcloud/588.0.0 request shape", "provider": "not-applicable",
              "backend": "not-applicable"},
    contract_files=(
        "gcp_validation/storage.py",
        "gcp_validation/normalization.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
        "gcp_validation/storage_clients.py",
        "tests/conformance/conftest.py",
    ),
)
def test_storage_gcloud_object_requests(storage, gcp_snapshot):
    observed = StorageObservations(storage)
    transport = ClientTransport(storage)
    bucket = storage.client.endpoint + "/storage/v1/b/" + storage.bucket
    layout = storage.client.request("allowed", "GET", storage.path + "/storageLayout", query=JSON)
    gcp_snapshot.match("storage-layout", layout_shape(observed, layout))
    denied = storage.client.request("denied", "GET", storage.path + "/storageLayout", query=JSON)
    gcp_snapshot.match("storage-layout-denied", layout_shape(observed, denied))

    payload = b"hello from gcloud\n"
    body, content_type = apitools_multipart('{"name": "gcloud.txt"}', "text/plain", payload)
    assert content_type.startswith("multipart/related; boundary='")
    transport.own("gcloud.txt")
    upload = storage.client.endpoint + "/upload/storage/v1/b/" + storage.bucket + "/o"
    stored = transport.send("POST", upload, actor="allowed", headers={"Content-Type": content_type},
                            body=body, query={**JSON, "uploadType": "multipart"})
    value = stored.json()
    gcp_snapshot.match("multipart-upload", {"status": stored.status, **observed.object(value)})

    listed = transport.send("GET", bucket + "/o", actor="allowed", query={
        **JSON, "fields": LIST_FIELDS, "delimiter": "/", "includeFoldersAsPrefixes": "True",
        "maxResults": "1000", "projection": "noAcl"})
    items = listed.json().get("items", [])
    gcp_snapshot.match("list-folders-as-prefixes", {
        "status": listed.status, "names": [item["name"] for item in items],
        "prefixes": listed.json().get("prefixes", [])})

    stat = transport.send("GET", bucket + "/o/gcloud.txt", actor="allowed", query={
        **JSON, "fields": "bucket,name,size,generation", "projection": "noAcl"})
    gcp_snapshot.match("stat", {"status": stat.status, "name": stat.json().get("name"),
                                "size": stat.json().get("size")})
    download = f"{storage.client.endpoint}/download/storage/v1/b/{storage.bucket}/o/gcloud.txt"
    media = transport.send("GET", download, actor="allowed",
                           query={"alt": "media", "generation": value["generation"]})
    assert media.body == payload
    gcp_snapshot.match("download", {"status": media.status, "bytes": len(media.body)})
    removed = transport.send("DELETE", bucket + "/o/gcloud.txt", actor="allowed", query=JSON)
    gcp_snapshot.match("delete", {"status": removed.status})
