"""Explicit SDK transport and owned fixtures for the pinned upstream slice."""

import hashlib
import json
import os
from pathlib import Path
import runpy
from urllib.parse import urlsplit

from .evidence import Unverified
from .sandbox import load_credentials, refresh_credentials


ROOT = Path(__file__).resolve().parents[1]


def upstream_slice():
    root = ROOT / "third_party/python-storage"
    manifest = json.loads((root / "manifest.json").read_text())
    source = root / "storage_slice.py"
    if hashlib.sha256(source.read_bytes()).hexdigest() != manifest["slice_sha256"]:
        raise ValueError("Pinned upstream test slice has changed")
    return manifest, runpy.run_path(str(source))


def require_local_isolation():
    import socket
    import sys
    if (sys.platform != "linux" or os.environ.get("GCP_LOCAL_ISOLATED") != "1"
            or {name for _, name in socket.if_nameindex()} != {"lo"}):
        raise Unverified("Run local SDK conformance and the provider together with scripts/run_isolated.py")


def sdk_client(storage):
    from google.auth.transport.requests import AuthorizedSession
    from google.cloud.storage import Client
    from google.oauth2.credentials import Credentials
    if storage.client.target.mode == "local":
        require_local_isolation()
        credential = Credentials(token="gcp-local-test:allowed")
    else:
        credential = load_credentials(storage.sandbox.identities["allowed"], storage.project)
        refresh_credentials(credential)
    endpoint = storage.client.endpoint
    origin = urlsplit(endpoint)

    class ScopedSession(AuthorizedSession):
        def request(self, method, url, *args, **kwargs):
            target = urlsplit(url)
            if ((target.scheme, target.netloc) != (origin.scheme, origin.netloc)
                    or target.username or target.password or target.fragment
                    or not target.path.startswith(("/storage/v1/", "/upload/storage/v1/", "/download/storage/v1/"))):
                raise ValueError("SDK request left the explicitly selected Storage endpoint")
            # These audited tests use a small multipart upload; reject unexpected streaming bodies.
            payload = kwargs.get("data")
            if payload is not None and (not isinstance(payload, (bytes, str)) or len(payload) > 1048576 + 8192):
                raise ValueError("SDK upload exceeded the bounded multipart scope")
            kwargs["allow_redirects"] = False
            kwargs["timeout"] = 30
            response = super().request(method, url, *args, **kwargs)
            if 300 <= response.status_code < 400 and response.status_code != 304:
                response.close()
                raise ValueError("SDK redirects are not allowed")
            return response

    session = ScopedSession(credential)
    session.trust_env = False
    client = Client(project=storage.project, credentials=credential, _http=session,
                    client_options={"api_endpoint": endpoint}, use_auth_w_custom_endpoint=True)
    return client, session


class OwnedBucket:
    """Retain native SDK Blob behavior while bounding selected upstream names."""

    NAMES = {"MyBuffer": "mybuffer", "SmallFile": "smallfile"}

    def __init__(self, storage, bucket):
        self.storage, self.bucket = storage, bucket

    def blob(self, name, *args, **kwargs):
        if name not in self.NAMES:
            raise ValueError("Upstream test requested an object outside its allowlist")
        owned = self.NAMES[name]
        if owned not in self.storage.manifest["objects"]:
            if len(self.storage.manifest["objects"]) >= self.storage.max_objects:
                raise ValueError("Upstream object count exceeds the selected scope")
            self.storage.manifest["objects"].append(owned)
            self.storage.save()
        return self.bucket.blob(owned, *args, **kwargs)
