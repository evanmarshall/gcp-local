"""Standard Google ADC/Storage SDK against a controlled identity transport fixture."""

import datetime
import socket
import sys
from unittest import mock
from urllib.parse import urlsplit

import google.auth
import requests
from google.auth import _helpers
from google.auth.transport.requests import AuthorizedSession, Request
from google.cloud import storage

mode, api, metadata, subject = sys.argv[1:]
assert sys.platform == "linux" and {name for _, name in socket.if_nameindex()} == {"lo"}
origins = {(urlsplit(url).scheme, urlsplit(url).netloc) for url in (api, metadata)}


class LocalSession(requests.Session):
    def __init__(self):
        super().__init__()
        self.trust_env = False

    def request(self, method, url, **kwargs):
        parsed = urlsplit(url)
        assert (parsed.scheme, parsed.netloc) in origins, "Unexpected nonlocal SDK request"
        kwargs["allow_redirects"] = False
        kwargs.setdefault("timeout", 5)
        return super().request(method, url, **kwargs)


base = LocalSession()
transport = Request(session=base)
credentials, project = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"], request=transport
)
assert project == "synthetic"
expected_module = (
    "google.auth.identity_pool" if mode == "external" else "google.auth.compute_engine"
)
assert type(credentials).__module__.startswith(expected_module)
client = AuthorizedSession(credentials, auth_request=transport)
client.trust_env = False
sdk = storage.Client(
    project=project, credentials=credentials, _http=client, client_options={"api_endpoint": api}
)
try:
    assert [bucket.name for bucket in sdk.list_buckets()] == ["identity-fixture"]
    first = credentials.token
    assert first.startswith("gl.")
    identity = client.get(api + "/_local/identity", timeout=5).json()
    assert identity["state"] == "authenticated" and identity["subject"] == subject
    assert base.post(api + "/advance-test-clock", timeout=5).status_code == 204
    expired = base.get(api + "/_local/identity", headers={"Authorization": "Bearer " + first})
    assert expired.status_code == 401 and expired.json()["state"] == "expired"
    real_now = _helpers.utcnow
    with mock.patch.object(
        _helpers, "utcnow", side_effect=lambda: real_now() + datetime.timedelta(seconds=601)
    ):
        assert [bucket.name for bucket in sdk.list_buckets()] == ["identity-fixture"]
        assert credentials.token != first
        identity = client.get(api + "/_local/identity", timeout=5).json()
        assert identity["state"] == "authenticated" and identity["subject"] == subject
    unknown = base.get(api + "/_local/identity", headers={"Authorization": "Bearer not-issued"})
    assert unknown.status_code == 401 and unknown.json()["state"] == "unknown"
    anonymous = base.get(api + "/_local/identity").json()
    assert anonymous["state"] == "anonymous" and not anonymous["subject"]
    assert base.get(api + "/storage/v1/b?project=synthetic").status_code == 401
finally:
    sdk.close()
    client.close()
    base.close()
print(mode + " ADC, Storage transport, expiry, and refresh passed.")
