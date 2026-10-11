"""Cloud Run workflow worker: parse a Pub/Sub push delivery and store an object.

Authentication uses the workload metadata endpoint for the service's
configured service account; the write goes to the local Storage endpoint
selected by the GCP_LOCAL_STORAGE_ENDPOINT environment variable.
"""

import base64
import json
import os
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

STORAGE = os.environ.get("GCP_LOCAL_STORAGE_ENDPOINT", "http://storage.invalid/storage/v1")


def metadata_token() -> str:
    """Fetch an access token from the workload metadata endpoint."""
    request = urllib.request.Request(
        "http://" + os.environ["GCE_METADATA_HOST"]
        + "/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read())["access_token"]


def store(bucket: str, name: str, payload: bytes) -> None:
    """Upload one object with the workload's service account credentials."""
    request = urllib.request.Request(
        STORAGE + "/b/" + bucket + "/o?uploadType=media&name=" + name,
        data=payload, method="POST",
    )
    request.add_header("Authorization", "Bearer " + metadata_token())
    request.add_header("Content-Type", "application/octet-stream")
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            assert response.status == 200
    except urllib.error.HTTPError as error:
        raise RuntimeError(
            "store failed: " + str(error.code) + " " + error.read().decode()[:200]) from None


class Handler(BaseHTTPRequestHandler):
    """Serve one Pub/Sub push delivery."""

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        try:
            envelope = json.loads(body)
            payload = base64.urlsafe_b64decode(envelope["message"]["data"])
            # Real Pub/Sub push envelopes carry camelCase JSON fields. The
            # object name is the publisher's messageId attribute, not the
            # server-assigned message id.
            attributes = envelope["message"]["attributes"]
            store(attributes["bucket"], attributes["messageId"] + ".txt", payload)
            code, reply = 200, b"stored"
        except Exception as error:  # noqa: BLE001 -- the push agent reads the status only
            code, reply = 500, str(error).encode()
        self.send_response(code)
        self.send_header("Content-Length", str(len(reply)))
        self.end_headers()
        self.wfile.write(reply)


HTTPServer(("0.0.0.0", int(os.environ.get("PORT", "8080"))), Handler).serve_forever()
