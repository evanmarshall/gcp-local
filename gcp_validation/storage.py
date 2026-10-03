"""Bounded Storage JSON API transport and owned fixture lifecycle."""

import argparse
from dataclasses import dataclass
import fcntl
import json
import os
from pathlib import Path
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from uuid import uuid4

from .config import Target
from .evidence import Inconclusive, now
from .sandbox import Sandbox, load_credentials, refresh_credentials


@dataclass
class Response:
    status: int
    body: bytes
    content_type: str

    def json(self):
        return json.loads(self.body) if self.body else {}

    def require(self, status):
        if self.status != status:
            # Raw response bodies can include identifiers or injected secrets.
            raise AssertionError(f"Storage returned HTTP {self.status}; expected {status}")
        return self.json()


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class StorageClient:
    def __init__(self, target, *, sandbox=None):
        self.target, self.sandbox = target, sandbox
        if target.mode == "local":
            if sandbox is not None:
                raise ValueError("Local transport cannot receive real credentials")
            self.endpoint = target.endpoint
        elif target.mode in {"record", "revalidate"}:
            if (sandbox is None or sandbox.project != target.project
                    or sandbox.identities["allowed"].credentials_file != target.credentials_file
                    or sandbox.live_scope != target.live_scope):
                raise ValueError("Live target and explicit sandbox profile must agree")
            self.endpoint = "https://storage.googleapis.com"
        else:
            raise ValueError("Storage requires an explicit local or live target")
        # Local calls must not use an ambient proxy or follow redirects to GCP.
        self.opener = build_opener(ProxyHandler({}), NoRedirects())
        self.credentials = {}

    def request(self, actor, method, path, *, query=None, body=None, media=False):
        if actor not in {"fixture", "allowed", "denied", "anonymous"}:
            raise ValueError("Unknown scenario identity")
        if not path.startswith(("/storage/v1/", "/upload/storage/v1/")):
            raise ValueError("Select an explicit Storage JSON API path")
        headers = {"Content-Type": "application/octet-stream" if media else "application/json"}
        if actor != "anonymous":
            if self.target.mode == "local":
                headers["Authorization"] = "Bearer gcp-local-test:" + actor
            else:
                if actor not in self.credentials:
                    self.credentials[actor] = load_credentials(
                        self.sandbox.identities[actor], self.sandbox.project)
                credential = self.credentials[actor]
                if not credential.valid:
                    refresh_credentials(credential)
                headers["Authorization"] = "Bearer " + credential.token
                headers["x-goog-user-project"] = self.sandbox.project
        if isinstance(body, dict):
            body = json.dumps(body).encode()
        url = self.endpoint + path + ("?" + urlencode(query) if query else "")
        request = Request(url, data=body, headers=headers, method=method)
        try:
            response = self.opener.open(request, timeout=30)
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError):
            raise Inconclusive("Storage transport failed; no cloud fallback is available") from None
        with response:
            payload = response.read(2 * 1048576 + 1)
            if len(payload) > 2 * 1048576:
                raise ValueError("Storage response exceeded the bounded scenario size")
            return Response(response.code, payload, response.headers.get_content_type())


def save_manifest(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    fd = os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w") as output:
        json.dump(value, output, indent=2)
        output.write("\n")
    temporary.replace(path)


class StorageFixture:
    """One bucket; fixture administration is separate from scenario callers."""

    def __init__(self, client, state_dir, *, region="us-central1", manifest=None):
        self.client = client
        self.project = client.target.project
        self.sandbox = client.sandbox
        self.project_number = self.sandbox.project_number if self.sandbox else "000000000001"
        self.region = self.sandbox.region if self.sandbox else region
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.lock = None
        self.creation_attempted = False
        self.creation_response = None
        if manifest is None:
            run_id = uuid4().hex
            self.manifest = {"version": 1, "project": self.project, "project_number": self.project_number,
                             "run_id": run_id, "bucket": "gcp-local-" + self.project_number + "-" + run_id,
                             "objects": [], "cleanup": "pending", "started_at": now()}
            self.manifest_path = self.state_dir / (run_id + ".json")
        else:
            self.manifest_path = Path(manifest).resolve()
            self.manifest = json.loads(self.manifest_path.read_text())
            data = self.manifest
            if (data.get("version") != 1 or data.get("project") != self.project
                    or data.get("project_number") != self.project_number
                    or data.get("creation_attempted") is not True
                    or not re.fullmatch(r"[0-9a-f]{32}", data.get("run_id", ""))
                    or data.get("bucket") != "gcp-local-" + self.project_number + "-" + data["run_id"]
                    or not isinstance(data.get("objects"), list)
                    or len(data["objects"]) > self.max_objects
                    or any(not isinstance(name, str) or not re.fullmatch(r"[a-z0-9.-]{1,80}", name)
                           for name in data["objects"])):
                raise ValueError("Cleanup manifest is outside the selected sandbox ownership scope")
            self.creation_attempted = data.get("creation_attempted", False)
        self.bucket = self.manifest["bucket"]
        self.path = "/storage/v1/b/" + self.bucket

    @property
    def max_objects(self):
        return self.sandbox.max_objects if self.sandbox else 10

    def save(self):
        save_manifest(self.manifest_path, self.manifest)

    def acquire(self):
        self.lock = (self.state_dir / "storage.lock").open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            self.lock = None
            raise Inconclusive("Another Storage scenario holds this sandbox's resource lock") from None

    def wait_allowed(self, call):
        deadline = time.monotonic() + (120 if self.sandbox else 0)
        while True:
            response = call()
            if response.status != 403:
                return response
            if time.monotonic() >= deadline:
                raise Inconclusive("Fixture or grant propagation did not complete within the deadline")
            time.sleep(5)

    def setup(self):
        self.acquire()
        listing = self.wait_allowed(lambda: self.client.request("fixture", "GET", "/storage/v1/b",
            query={"project": self.project, "maxResults": 2})).require(200)
        if listing.get("items") or listing.get("nextPageToken"):
            raise Inconclusive("Sandbox already has a bucket; clean owned resources before continuing")
        self.creation_attempted = True
        self.manifest["creation_attempted"] = True
        self.save()  # Persist ownership intent before the first resource mutation.
        response = self.client.request("fixture", "POST", "/storage/v1/b", query={"project": self.project}, body={
            "name": self.bucket, "location": self.region.upper(), "storageClass": "STANDARD",
            "labels": {"managed-by": "gcp-local", "gcp-local-run": self.manifest["run_id"]},
            "iamConfiguration": {"uniformBucketLevelAccess": {"enabled": True}, "publicAccessPrevention": "enforced"},
            "softDeletePolicy": {"retentionDurationSeconds": "0"}, "versioning": {"enabled": False},
            "defaultEventBasedHold": False,
        })
        bucket = response.require(200)
        self.assert_owned(bucket)
        if (bucket.get("location") != self.region.upper()
                or not bucket.get("iamConfiguration", {}).get("uniformBucketLevelAccess", {}).get("enabled")
                or bucket.get("iamConfiguration", {}).get("publicAccessPrevention") != "enforced"
                or bucket.get("softDeletePolicy", {}).get("retentionDurationSeconds", "0") != "0"
                or bucket.get("versioning", {}).get("enabled") or bucket.get("retentionPolicy")
                or bucket.get("defaultEventBasedHold")):
            raise ValueError("Bucket settings do not match the private, temporary test scope")
        self.creation_response = response
        policy = self.client.request("fixture", "GET", self.path + "/iam").require(200)
        member = ("serviceAccount:" + self.sandbox.identities["allowed"].email
                  if self.sandbox else "serviceAccount:allowed@" + self.project + ".iam.gserviceaccount.com")
        policy.setdefault("bindings", []).append({"role": "roles/storage.objectUser", "members": [member]})
        self.client.request("fixture", "PUT", self.path + "/iam", body=policy).require(200)
        self.wait_allowed(lambda: self.list_objects()).require(200)

    def assert_owned(self, bucket):
        if (str(bucket.get("projectNumber")) != self.project_number or bucket.get("name") != self.bucket
                or bucket.get("labels", {}).get("gcp-local-run") != self.manifest["run_id"]):
            raise ValueError("Refusing to modify a bucket whose ownership does not match the manifest")

    def upload(self, name, payload, *, actor="allowed", generation=0):
        limit = self.sandbox.max_object_bytes if self.sandbox else 1048576
        if not re.fullmatch(r"[a-z0-9.-]{1,80}", name) or len(payload) > limit:
            raise ValueError("Object name or payload exceeds the bounded scenario scope")
        if name not in self.manifest["objects"]:
            if len(self.manifest["objects"]) >= self.max_objects:
                raise ValueError("Object count exceeds the bounded scenario scope")
            self.manifest["objects"].append(name)
            self.save()
        return self.client.request(actor, "POST", "/upload/storage/v1/b/" + self.bucket + "/o",
            query={"uploadType": "media", "name": name, "ifGenerationMatch": generation}, body=payload, media=True)

    def read(self, name, *, actor="allowed", media=False):
        return self.client.request(actor, "GET", self.path + "/o/" + quote(name, safe=""),
                                   query={"alt": "media"} if media else None)

    def list_objects(self, *, actor="allowed"):
        return self.client.request(actor, "GET", self.path + "/o", query={"maxResults": 11})

    def delete(self, name, generation, *, actor="allowed"):
        return self.client.request(actor, "DELETE", self.path + "/o/" + quote(name, safe=""),
                                   query={"generation": generation, "ifGenerationMatch": generation})

    def cleanup(self):
        try:
            if not self.creation_attempted:
                return
            response = self.client.request("fixture", "GET", self.path)
            if response.status != 404:
                self.assert_owned(response.require(200))
                for name in self.manifest["objects"]:
                    metadata = self.read(name, actor="fixture")
                    if metadata.status != 404:
                        generation = metadata.require(200)["generation"]
                        self.delete(name, generation, actor="fixture").require(204)
                self.client.request("fixture", "DELETE", self.path).require(204)
                self.client.request("fixture", "GET", self.path).require(404)
            self.manifest.update(cleanup="passed", finished_at=now())
            self.save()
        finally:
            if self.lock:
                self.lock.close()
                self.lock = None


def main(argv=None):
    parser = argparse.ArgumentParser(description="Clean only resources in an interrupted Storage manifest")
    parser.add_argument("command", choices=["cleanup"])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--allow-live", action="store_true", required=True)
    args = parser.parse_args(argv)
    profile = Sandbox.load(args.profile)
    target = Target.select("record", project=profile.project,
        credentials_file=profile.identities["allowed"].credentials_file,
        live_scope=profile.live_scope, allow_live=args.allow_live)
    state_dir = Path.home() / ".local/state/gcp-local" / profile.project
    fixture = StorageFixture(StorageClient(target, sandbox=profile), state_dir, manifest=args.manifest)
    fixture.acquire()
    fixture.cleanup()
    print("Owned Storage resource cleanup passed.")


if __name__ == "__main__":
    main()
