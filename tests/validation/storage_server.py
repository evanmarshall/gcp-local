"""Synthetic protocol stub for the recorder tests; never cloud evidence."""

from contextlib import contextmanager
import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Thread
from urllib.parse import parse_qs, unquote, urlsplit


@contextmanager
def storage_server():
    state = {"bucket": None, "objects": {}, "policy": {"bindings": []}, "requests": [],
             "fail_cleanup": False, "denied_side_effect": False, "corrupt_reason": False,
             "corrupt_content": False, "redirect": False, "generation": 1000}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def respond(self, status, body=None, content_type="application/json"):
            if isinstance(body, dict):
                body = json.dumps(body).encode()
            body = body or b""
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            if state["redirect"]:
                self.send_header("Location", "https://storage.googleapis.com/storage/v1/b")
            self.end_headers()
            self.wfile.write(body)

        def error(self, status, message, reason):
            self.respond(status, {"error": {"code": status, "message": message,
                                           "errors": [{"reason": reason}]}})

        def handle_request(self):
            parsed = urlsplit(self.path)
            path = unquote(parsed.path)
            query = parse_qs(parsed.query)
            method = self.command
            authorization = self.headers.get("Authorization")
            actor = authorization.removeprefix("Bearer gcp-local-test:") if authorization else "anonymous"
            state["requests"].append((actor, method, path))
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if state["redirect"]:
                return self.respond(307)
            if path == "/storage/v1/b":
                if method == "GET":
                    return self.respond(200, {"items": [state["bucket"]] if state["bucket"] else []})
                state["bucket"] = json.loads(body)
                state["bucket"]["projectNumber"] = "000000000001"
                return self.respond(200, state["bucket"])
            bucket = state["bucket"]
            if bucket is None:
                return self.error(404, "Bucket not found", "notFound")
            if path.endswith("/iam"):
                if method == "PUT":
                    state["policy"] = json.loads(body)
                return self.respond(200, state["policy"])
            if path == "/storage/v1/b/" + bucket["name"]:
                if method == "DELETE":
                    if state["fail_cleanup"]:
                        return self.error(500, "Synthetic cleanup failure", "backendError")
                    if state["objects"]:
                        return self.error(409, "Bucket not empty", "conflict")
                    state["bucket"] = None
                    return self.respond(204)
                return self.respond(200, bucket)
            if path.startswith("/upload/"):
                name = query["name"][0]
                if actor == "denied" and not state["denied_side_effect"]:
                    return self.error(403, "<denied> lacks storage.objects.create on " + bucket["name"], "forbidden")
                if name in state["objects"] and query.get("ifGenerationMatch") == ["0"]:
                    reason = "corruptedReason" if state["corrupt_reason"] else "conditionNotMet"
                    return self.error(412, "At least one of the pre-conditions you specified did not hold.", reason)
                state["generation"] += 1
                metadata = {"name": name, "bucket": bucket["name"], "size": str(len(body)),
                            "generation": str(state["generation"]), "metageneration": "1",
                            "etag": "etag-" + str(state["generation"]), "contentType": "application/octet-stream",
                            "md5Hash": base64.b64encode(hashlib.md5(body).digest()).decode(),
                            "authorization": "Bearer synthetic-secret-must-not-persist"}
                state["objects"][name] = (metadata, body)
                if actor == "denied":
                    return self.error(403, "<denied> lacks storage.objects.create on " + bucket["name"], "forbidden")
                return self.respond(200, metadata)
            if path.endswith("/o"):
                return self.respond(200, {"items": [value[0] for _, value in sorted(state["objects"].items())]})
            name = path.split("/o/", 1)[1]
            if name not in state["objects"]:
                return self.error(404, "No such object: " + bucket["name"] + "/" + name, "notFound")
            metadata, content = state["objects"][name]
            if method == "DELETE":
                if query.get("ifGenerationMatch") != [metadata["generation"]]:
                    return self.error(412, "Precondition failed", "conditionNotMet")
                del state["objects"][name]
                return self.respond(204)
            if query.get("alt") == ["media"]:
                return self.respond(200, b"corrupted" if state["corrupt_content"] else content,
                                    "application/octet-stream")
            return self.respond(200, metadata)

        do_GET = do_POST = do_PUT = do_DELETE = handle_request

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield "http://127.0.0.1:" + str(server.server_port), state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
