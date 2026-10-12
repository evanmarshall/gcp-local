"""Resumable upload session and V4 signed URL flows, as the client libraries send them.

Not recorded. A live recording needs the allowed identity to hold
roles/iam.serviceAccountTokenCreator on itself, because it signs with signBlob.
"""

import hashlib

import pytest

from gcp_validation.normalization import StorageObservations
from gcp_validation.storage_clients import BlobSigner, ClientTransport, signed_url, signer_email

CHUNK = 256 * 1024


@pytest.mark.gcp_scenario(
    id="storage-resumable-signed-url",
    service="storage",
    transport="rest-json-v1+xml",
    versions={
        "sdk": "google-cloud-storage/3.10.1;google-auth/2.56.2",
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
        "requirements-sdk.txt",
    ),
)
def test_storage_resumable_signed_url(storage, gcp_snapshot):
    observed = StorageObservations(storage)
    transport = ClientTransport(storage)
    payload = hashlib.sha256(b"gcp-local resumable").digest() * (2 * CHUNK // 32)
    assert len(payload) <= (storage.sandbox.max_object_bytes if storage.sandbox else 1048576)
    upload = storage.client.endpoint + "/upload/storage/v1/b/" + storage.bucket + "/o"

    transport.own("resumable.bin")
    started = transport.send(
        "POST",
        upload,
        actor="allowed",
        query={"uploadType": "resumable"},
        body={"name": "resumable.bin"},
        headers={
            "X-Upload-Content-Type": "application/octet-stream",
            "X-Upload-Content-Length": str(len(payload)),
        },
    )
    session = started.headers.get("location", "")
    gcp_snapshot.match(
        "initiate", {"status": started.status, "has_session_uri": "upload_id=" in session}
    )
    first = transport.send(
        "PUT",
        session,
        headers={"Content-Range": f"bytes 0-{CHUNK - 1}/{len(payload)}"},
        body=payload[:CHUNK],
    )
    gcp_snapshot.match("first-chunk", {"status": first.status, "range": first.headers.get("range")})
    probe = transport.send("PUT", session, headers={"Content-Range": f"bytes */{len(payload)}"})
    gcp_snapshot.match(
        "status-query", {"status": probe.status, "range": probe.headers.get("range")}
    )
    final = transport.send(
        "PUT",
        session,
        body=payload[CHUNK:],
        headers={"Content-Range": f"bytes {CHUNK}-{len(payload) - 1}/{len(payload)}"},
    )
    gcp_snapshot.match("final-chunk", {"status": final.status, **observed.object(final.json())})
    assert storage.read("resumable.bin", media=True).body == payload

    cancelled = transport.send(
        "POST", upload, actor="allowed", query={"uploadType": "resumable", "name": "cancelled.bin"}
    )
    cancelled_uri = cancelled.headers.get("location", "")
    gcp_snapshot.match("cancel", {"status": transport.send("DELETE", cancelled_uri).status})
    gcp_snapshot.match(
        "after-cancel", {"status": transport.send("PUT", cancelled_uri, body=b"late").status}
    )

    signer = BlobSigner(transport, "allowed", signer_email(storage, "allowed"))
    transport.own("signed.txt")
    put = signed_url(transport, signer, "PUT", "signed.txt", content_type="text/plain")
    stored = transport.send("PUT", put, headers={"Content-Type": "text/plain"}, body=b"signed body")
    gcp_snapshot.match(
        "signed-put",
        {"status": stored.status, "has_generation": "x-goog-generation" in stored.headers},
    )
    get = signed_url(transport, signer, "GET", "signed.txt")
    read = transport.send("GET", get)
    gcp_snapshot.match(
        "signed-get",
        {
            "status": read.status,
            "body_matches": read.body == b"signed body",
            "content_type": read.headers.get("content-type"),
        },
    )
    tampered = transport.send("GET", get.replace("X-Goog-Signature=", "X-Goog-Signature=00"))
    gcp_snapshot.match("tampered", {"status": tampered.status, "code": tampered.xml_code()})
    expired = transport.send(
        "GET",
        signed_url(
            transport, signer, "GET", "signed.txt", timestamp="20200101T000000Z", expiration=60
        ),
    )
    gcp_snapshot.match("expired", {"status": expired.status, "code": expired.xml_code()})
    wrong_method = transport.send("PUT", get, body=b"x")
    gcp_snapshot.match(
        "wrong-method", {"status": wrong_method.status, "code": wrong_method.xml_code()}
    )
