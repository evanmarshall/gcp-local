"""The object requests rclone 1.75.2 sends for copyto, cat, and moveto.

Request shapes come from rclone against gcp-local's conformance trace: every
JSON API call carries alt=json and prettyPrint=false; copyto uploads with
uploadType=multipart and the name in the query; cat reads the object and then
GETs its mediaLink as returned; moveto is objects.rewrite with a JSON null body
(the generated Go API client's nil request object) followed by a delete of the
source. The upload's metadata part is reduced to name, content type, and the
mtime rclone stores.
"""

import uuid
from urllib.parse import parse_qsl, urlsplit

import pytest

from gcp_validation.services.storage.client_shapes import ClientTransport
from gcp_validation.services.storage.normalization import StorageObservations

GO = {"alt": "json", "prettyPrint": "false"}
PAYLOAD = b"hello from rclone\n"


def go_multipart(metadata, media_type, data):
    """Build the multipart/related body Go's googleapi package sends."""
    boundary = uuid.uuid4().hex
    parts = [
        f"--{boundary}\r\nContent-Type: application/json\r\n\r\n".encode() + metadata + b"\r\n",
        f"--{boundary}\r\nContent-Type: {media_type}\r\n\r\n".encode() + data + b"\r\n",
        f"--{boundary}--\r\n".encode(),
    ]
    return b"".join(parts), f"multipart/related; boundary={boundary}"


def link_shape(observed, link):
    """Show a link's path and ordered query with aliased generations.

    The origin is left out: GCP's selfLink names www.googleapis.com, which the
    emulator replaces with its own origin. rclone requests the mediaLink as
    given, and the download below proves it reaches the endpoint.
    """
    parsed = urlsplit(link)
    query = []
    for name, value in parse_qsl(parsed.query, keep_blank_values=True):
        query.append([name, observed.reference(name, value) if name == "generation" else value])
    return {"path": observed.text(parsed.path), "query": query}


def rewrite_shape(observed, reply):
    if reply.status != 200:
        return observed.error(reply)
    value = reply.json()
    resource = value["resource"]
    return {"status": reply.status, "kind": value.get("kind"), "done": value.get("done"),
            "totalBytesRewritten": value.get("totalBytesRewritten"),
            "objectSize": value.get("objectSize"), "has_rewrite_token": "rewriteToken" in value,
            "resource": observed.object(resource),
            "mediaLink": link_shape(observed, resource["mediaLink"])}


@pytest.mark.gcp_scenario(
    id="storage-rclone-object-requests",
    service="storage",
    transport="rest-json-v1",
    versions={"sdk": "rclone/1.75.2 request shape", "provider": "not-applicable",
              "backend": "not-applicable"},
    contract_files=(
        "gcp_validation/services/storage/client.py",
        "gcp_validation/services/storage/normalization.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
        "gcp_validation/services/storage/client_shapes.py",
        "tests/conformance/storage/conftest.py",
    ),
)
def test_storage_rclone_object_requests(storage, gcp_snapshot):
    observed = StorageObservations(storage)
    transport = ClientTransport(storage)
    endpoint = storage.client.endpoint
    objects = endpoint + "/storage/v1/b/" + storage.bucket + "/o/"

    metadata = (b'{"contentType":"text/plain; charset=utf-8",'
                b'"metadata":{"mtime":"2026-10-12T00:00:00.000000000Z"},"name":"rclone.txt"}')
    body, content_type = go_multipart(metadata, "text/plain; charset=utf-8", PAYLOAD)
    transport.own("rclone.txt")
    upload = endpoint + "/upload/storage/v1/b/" + storage.bucket + "/o"
    query = {**GO, "name": "rclone.txt", "uploadType": "multipart"}
    stored = transport.send("POST", upload, actor="allowed",
                            headers={"Content-Type": content_type}, body=body, query=query)
    assert stored.status == 200, stored.body
    gcp_snapshot.match("multipart-upload",
                       {"status": stored.status, **observed.object(stored.json())})

    got = transport.send("GET", objects + "rclone.txt", actor="allowed", query=GO)
    value = got.json()
    gcp_snapshot.match("object-get", {
        "status": got.status, **observed.object(value),
        "selfLink": link_shape(observed, value["selfLink"]),
        "mediaLink": link_shape(observed, value["mediaLink"])})
    media = transport.send("GET", value["mediaLink"], actor="allowed")
    assert media.body == PAYLOAD
    gcp_snapshot.match("media-link-download", {"status": media.status, "bytes": len(media.body)})

    transport.own("moved.txt")
    rewrite = transport.send(
        "POST", objects + "rclone.txt/rewriteTo/b/" + storage.bucket + "/o/moved.txt",
        actor="allowed", headers={"Content-Type": "application/json"}, body=b"null\n", query=GO)
    gcp_snapshot.match("rewrite-null-body", rewrite_shape(observed, rewrite))
    removed = transport.send("DELETE", objects + "rclone.txt", actor="allowed", query=GO)
    gcp_snapshot.match("delete-source", {"status": removed.status})
    moved = transport.send("GET", objects + "moved.txt", actor="allowed", query={"alt": "media"})
    assert moved.body == PAYLOAD
    gcp_snapshot.match("read-destination", {"status": moved.status, "bytes": len(moved.body)})
