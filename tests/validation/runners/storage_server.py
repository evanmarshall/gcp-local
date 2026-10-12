"""Synthetic protocol stub for the recorder tests; never cloud evidence."""

import base64
import hashlib
import json
from contextlib import contextmanager
from email.parser import BytesParser
from email.policy import default
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import parse_qs, unquote, urlsplit


class Handler(BaseHTTPRequestHandler):
    """Synthetic Storage API handler over the shared test state."""

    state: dict

    def log_message(self, *args):
        """Silence the request log."""

    def respond(self, status, body=None, content_type="application/json"):
        """Write a JSON or byte response, optionally forcing a redirect."""
        if isinstance(body, dict):
            body = json.dumps(body).encode()
        body = body or b""
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if self.state["redirect"]:
            self.send_header("Location", "https://storage.googleapis.com/storage/v1/b")
        self.end_headers()
        self.wfile.write(body)

    def error(self, status, message, reason):
        """Write a structured GCS-style error body."""
        self.respond(
            status,
            {"error": {"code": status, "message": message, "errors": [{"reason": reason}]}},
        )

    def context(self):
        """Describe the request: path, query, method, actor, and body."""
        parsed = urlsplit(self.path)
        path = unquote(parsed.path).removeprefix("/download")
        query = parse_qs(parsed.query)
        method = self.command
        authorization = self.headers.get("Authorization")
        actor = (
            authorization.removeprefix("Bearer gcp-local-test:") if authorization else "anonymous"
        )
        self.state["requests"].append((actor, method, path))
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        return path, query, method, actor, body

    def handle_request(self):
        """Route the request to its endpoint family."""
        path, query, method, actor, body = self.context()
        if self.state["redirect"]:
            return self.respond(307)
        if path == "/storage/v1/b":
            return self.buckets(method, body)
        if path.endswith("/iam"):
            return self.iam(method, body)
        return self.bucket_scoped(path, query, method, actor, body)

    def bucket_scoped(self, path, query, method, actor, body):
        """Route requests that already require an existing bucket."""
        bucket = self.state["bucket"]
        if bucket is None:
            return self.error(404, "Bucket not found", "notFound")
        if path == "/storage/v1/b/" + bucket["name"]:
            return self.bucket(method)
        if self.state.get("authorization") and actor != "fixture":
            rejected = self.authorize(path, method, actor)
            if rejected:
                return rejected
        if path.startswith("/upload/"):
            return self.upload(query, actor, body, bucket)
        if path.endswith("/o"):
            return self.list_objects(query)
        return self.object(path, query, method)

    def buckets(self, method, body):
        """List or create the single synthetic bucket."""
        if method == "GET":
            return self.respond(
                200, {"items": [self.state["bucket"]] if self.state["bucket"] else []}
            )
        self.state["bucket"] = json.loads(body)
        self.state["bucket"]["projectNumber"] = "000000000001"
        return self.respond(200, self.state["bucket"])

    def iam(self, method, body):
        """Return the policy, storing an update unless a fault is armed."""
        if method == "PUT" and not self.state.get("ignore_policy"):
            self.state["policy"] = json.loads(body)
        return self.respond(200, self.state["policy"])

    def bucket(self, method):
        """Delete or describe the bucket, honoring the cleanup fault."""
        if method == "DELETE":
            if self.state["fail_cleanup"]:
                return self.error(500, "Synthetic cleanup failure", "backendError")
            if self.state["objects"]:
                return self.error(409, "Bucket not empty", "conflict")
            self.state["bucket"] = None
            return self.respond(204)
        return self.respond(200, self.state["bucket"])

    def roles_of(self, actor):
        """Roles whose members include the actor."""
        return [
            binding["role"]
            for binding in self.state["policy"].get("bindings", [])
            if any(
                member.startswith("serviceAccount:" + actor + "@") for member in binding["members"]
            )
        ]

    def authorize(self, path, method, actor):
        """Reject the actor unless a policy role or fault permits it."""
        bucket = self.state["bucket"]
        roles = self.roles_of(actor)
        permission = (
            "storage.objects.create" if path.startswith("/upload/") else "storage.objects.get"
        )
        permitted = "roles/storage.objectUser" in roles or (
            method == "GET" and "roles/storage.objectViewer" in roles
        )
        if self.state.get("leaked_access") == actor:
            permitted = True
        if not permitted:
            if self.state.get("authorization_side_effect") and path.startswith("/upload/"):
                self.state["objects"]["object.txt"][0]["etag"] = "corrupted"
            self.error(
                403,
                "<" + actor + "> lacks " + permission + " on " + bucket["name"],
                "forbidden",
            )
            return True
        return False

    def upload(self, query, actor, body, bucket):
        """Handle multipart and simple uploads with their authorization cases."""
        if query.get("uploadType") == ["multipart"]:
            name, content_type, body = self.multipart(body)
        else:
            info, name = {}, query["name"][0]
            content_type = info.get("contentType")
        if actor == "denied" and not self.state["denied_side_effect"]:
            return self.denied_upload(bucket["name"], name)
        if name in self.state["objects"] and query.get("ifGenerationMatch") == ["0"]:
            return self.precondition()
        return self.store(name, body, content_type, bucket, actor)

    def multipart(self, body):
        """Parse the multipart envelope into metadata and content."""
        envelope = BytesParser(policy=default).parsebytes(
            (
                "Content-Type: " + self.headers["Content-Type"] + "\r\nMIME-Version: 1.0\r\n\r\n"
            ).encode()
            + body
        )
        parts = list(envelope.iter_parts())
        info = json.loads(parts[0].get_payload(decode=True))
        return info["name"], info.get("contentType"), parts[1].get_payload(decode=True)

    def denied_upload(self, bucket_name, name):
        """Return the recorded or plain denied shape."""
        if self.state.get("recorded_error_shape"):
            message = (
                "<denied> does not have storage.objects.create access "
                "to the Google Cloud Storage object. "
                "Permission 'storage.objects.create' denied on resource "
                "'//storage.googleapis.com/projects/_/buckets/"
                + bucket_name
                + "/objects/"
                + name
                + "' (or it may not exist)."
            )
            return self.respond(
                403,
                {"error": {"code": 403, "message": message, "errors": [{"reason": "forbidden"}]}},
                "text/html",
            )
        return self.error(
            403, "<denied> lacks storage.objects.create on " + bucket_name, "forbidden"
        )

    def precondition(self):
        """Reject a create over an existing object."""
        reason = "corruptedReason" if self.state["corrupt_reason"] else "conditionNotMet"
        return self.error(
            412, "At least one of the pre-conditions you specified did not hold.", reason
        )

    def store(self, name, body, content_type, bucket, actor):
        """Record the object and report its metadata."""
        self.state["generation"] += 1
        generation = self.state["generation"]
        metadata = {
            "name": name,
            "bucket": bucket["name"],
            "size": str(len(body)),
            "generation": str(generation),
            "metageneration": "1",
            "etag": "etag-" + str(generation),
            "contentType": content_type or "application/octet-stream",
            # md5Hash is part of the emulated API contract.
            "md5Hash": base64.b64encode(hashlib.md5(body).digest()).decode(),  # noqa: S324
            "authorization": "Bearer synthetic-secret-must-not-persist",
        }
        if self.state.get("sdk"):
            import google_crc32c  # noqa: PLC0415 -- optional test dependency

            metadata["crc32c"] = base64.b64encode(google_crc32c.Checksum(body).digest()).decode()
        self.state["objects"][name] = (metadata, body)
        if actor == "denied":
            return self.error(
                403, "<denied> lacks storage.objects.create on " + bucket["name"], "forbidden"
            )
        return self.respond(200, metadata)

    def list_objects(self, query):
        """Page the stored object metadata."""
        items = [value[0] for _, value in sorted(self.state["objects"].items())]
        offset = int(query.get("pageToken", ["0"])[0])
        size = int(query.get("maxResults", ["100"])[0])
        result = {"items": items[offset : offset + size]}
        if offset + size < len(items):
            result["nextPageToken"] = str(offset + size)
        return self.respond(200, result)

    def object(self, path, query, method):
        """Serve one object's metadata, content, or deletion."""
        name = path.split("/o/", 1)[1]
        if name not in self.state["objects"]:
            return self.error(
                404, "No such object: " + self.state["bucket"]["name"] + "/" + name, "notFound"
            )
        metadata, content = self.state["objects"][name]
        if self.state.get("sdk") and not self.state.get("ignore_preconditions"):
            blocked = self.precondition_blocked(metadata, query)
            if blocked:
                return blocked
        if method == "DELETE":
            if query.get("ifGenerationMatch") != [metadata["generation"]]:
                return self.error(412, "Precondition failed", "conditionNotMet")
            del self.state["objects"][name]
            return self.respond(204)
        if query.get("alt") == ["media"]:
            return self.respond(
                200,
                b"corrupted" if self.state["corrupt_content"] else content,
                "application/octet-stream",
            )
        return self.respond(200, metadata)

    def precondition_blocked(self, metadata, query):
        """Check the SDK precondition headers and query parameters."""
        if (
            self.headers.get("If-Match") not in {None, metadata["etag"]}
            or query.get("ifGenerationMatch", [metadata["generation"]]) != [metadata["generation"]]
            or query.get("ifMetagenerationMatch", [metadata["metageneration"]])
            != [metadata["metageneration"]]
        ):
            return self.error(412, "Precondition failed", "conditionNotMet")
        if self.headers.get("If-None-Match") == metadata["etag"]:
            return self.respond(304)
        return None

    do_GET = do_POST = do_PUT = do_DELETE = handle_request  # noqa: N815 -- stdlib contract


@contextmanager
def storage_server():
    """Serve a synthetic Storage API for the recorder tests."""
    state = {
        "bucket": None,
        "objects": {},
        "policy": {"bindings": []},
        "requests": [],
        "fail_cleanup": False,
        "denied_side_effect": False,
        "corrupt_reason": False,
        "corrupt_content": False,
        "redirect": False,
        "generation": 1000,
    }
    Handler.state = state
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield "http://127.0.0.1:" + str(server.server_port), state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
