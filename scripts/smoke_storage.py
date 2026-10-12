#!/usr/bin/env python3
"""Use standard local ADC and the Python SDK against the production binary."""

import base64
import datetime as dt
import json
import os
import queue
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
from collections.abc import Callable
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

import google.auth
import requests
from google.api_core.exceptions import Forbidden, PreconditionFailed
from google.auth.credentials import Signing
from google.auth.transport.requests import AuthorizedSession, Request
from google.cloud import storage
from google.cloud.storage import _signing
from google.oauth2 import service_account

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gcp_local.bootstrap import private_json


def write_identity_configs(directory: Path) -> list[dict[str, str]]:
    """Write the synthetic identity and IAM configs and return the principals."""
    principals = [
        {
            "subject": f"serviceAccount:{actor}@synthetic.iam.gserviceaccount.com",
            "project": "synthetic",
            "subject_token": secrets.token_urlsafe(40),
        }
        for actor in ("fixture", "allowed", "denied")
    ]
    private_json(directory / "identity.json", {"principals": principals})
    project = "//cloudresourcemanager.googleapis.com/projects/synthetic"
    private_json(
        directory / "iam.json",
        {
            "resources": [
                {
                    "name": project,
                    "service": "resourcemanager",
                    "kind": "project",
                    "number": "123456",
                    "scope": {"project": "synthetic", "location": "global"},
                }
            ],
            "policies": {
                project: {
                    "bindings": [
                        {"role": role, "members": [principals[0]["subject"]]}
                        for role in (
                            "roles/storage.admin",
                            "roles/iam.serviceAccountAdmin",
                            "roles/iam.serviceAccountKeyAdmin",
                        )
                    ]
                }
            },
        },
    )
    return principals


def build_clients(
    directory: Path,
    principals: list[dict[str, str]],
    endpoint: str,
    transport_session: "requests.Session",
) -> tuple[list[storage.Client], list[AuthorizedSession]]:
    """Build one authenticated Storage client and session per principal."""
    clients, sessions = [], []
    transport = Request(session=transport_session)
    for principal in principals:
        actor = principal["subject"].split(":")[1].split("@")[0]
        subject = directory / (actor + ".txt")
        subject.write_text(principal["subject_token"])
        subject.chmod(0o600)
        adc = directory / (actor + ".json")
        private_json(
            adc,
            {
                "type": "external_account",
                "audience": "//gcp-local/identity",
                "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "token_url": endpoint + "/v1/token",
                "credential_source": {"file": str(subject)},
            },
        )
        with patch.dict(
            os.environ,
            {"GOOGLE_APPLICATION_CREDENTIALS": str(adc), "GOOGLE_CLOUD_PROJECT": "synthetic"},
        ):
            credentials, selected = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"], request=transport
            )
        assert selected == "synthetic"
        session = AuthorizedSession(credentials, auth_request=transport)
        session.trust_env = False
        sessions.append(session)
        clients.append(
            storage.Client(
                project=selected,
                credentials=credentials,
                _http=session,
                client_options={"api_endpoint": endpoint},
            )
        )
    return clients, sessions


def create_bucket(fixture: storage.Client, name: str, member: str) -> storage.Bucket:
    """Create a private uniform bucket and grant one member Object User on it."""
    bucket = fixture.bucket(name)
    bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    bucket.iam_configuration.public_access_prevention = "enforced"
    bucket.soft_delete_policy.retention_duration_seconds = 0
    bucket = fixture.create_bucket(bucket, location="us-central1", retry=None)
    grant(bucket, member, "roles/storage.objectUser")
    return bucket


def grant(bucket: storage.Bucket, member: str, role: str | None) -> None:
    """Replace the bucket's bindings for member with role, or remove them."""
    policy = bucket.get_iam_policy(requested_policy_version=1)
    policy.bindings = [binding for binding in policy.bindings if member not in binding["members"]]
    if role:
        policy.bindings.append({"role": role, "members": {member}})
    bucket.set_iam_policy(policy)


def object_lifecycle(clients: list, principals: list[dict[str, str]]) -> None:
    """Exercise grant, write, replacement, precondition, and revocation."""
    fixture, allowed, denied = clients
    bucket = create_bucket(fixture, "sdk-authenticated-bucket", principals[1]["subject"])
    blob = allowed.bucket(bucket.name).blob("nested/name %?.txt")
    blob.upload_from_string(b"original", if_generation_match=0, retry=None)
    original_generation = blob.generation
    assert blob.download_as_bytes() == b"original"
    expect_denied(lambda: denied.bucket(bucket.name).blob(blob.name).upload_from_string(b"denied"))
    blob.upload_from_string(b"replacement", if_generation_match=original_generation, retry=None)
    assert blob.generation != original_generation
    expect_precondition(
        lambda: blob.upload_from_string(b"stale", if_generation_match=original_generation)
    )
    policy = bucket.get_iam_policy(requested_policy_version=1)
    policy.bindings.clear()
    bucket.set_iam_policy(policy)
    expect_denied(blob.delete)
    admin_blob = bucket.blob(blob.name)
    assert admin_blob.download_as_bytes() == b"replacement"
    admin_blob.delete(retry=None)
    bucket.delete(retry=None)
    assert list(fixture.list_buckets()) == []


def resumable_uploads(clients: list, principals: list[dict[str, str]], directory: Path) -> None:
    """Upload over 8 MiB, which the SDK sends as a resumable upload, then read it back."""
    fixture, allowed, _ = clients
    bucket = create_bucket(fixture, "sdk-resumable-bucket", principals[1]["subject"])
    payload = os.urandom(9 * 1024 * 1024 + 7)
    path = directory / "large.bin"
    path.write_bytes(payload)
    # Three 4 MiB chunks, then the SDK's default single 100 MiB chunk.
    chunked = allowed.bucket(bucket.name).blob("chunked.bin", chunk_size=4 * 1024 * 1024)
    chunked.upload_from_filename(str(path), if_generation_match=0, retry=None)
    single = allowed.bucket(bucket.name).blob("single.bin")
    single.upload_from_filename(str(path), if_generation_match=0, retry=None)
    for blob in (chunked, single):
        assert blob.size == len(payload), blob.size
        downloaded = directory / (blob.name + ".out")
        allowed.bucket(bucket.name).blob(blob.name).download_to_filename(str(downloaded))
        assert downloaded.read_bytes() == payload
        bucket.blob(blob.name).delete(retry=None)
    bucket.delete(retry=None)


class LocalSigner(Signing):
    """Sign through the emulator's IAM Credentials signBlob as the caller.

    The SDK's own signBlob path calls iamcredentials.googleapis.com directly,
    so a client using it must supply a signer like this one.
    """

    def __init__(self, session: AuthorizedSession, endpoint: str, email: str) -> None:
        """Bind the authorized caller session and the signing account."""
        self.session, self.endpoint, self.email = session, endpoint, email

    @property
    def signer_email(self) -> str:
        """Return the signing account."""
        return self.email

    @property
    def signer(self) -> "LocalSigner":
        """Return this signer."""
        return self

    def sign_bytes(self, message: bytes) -> bytes:
        """Sign message with the account's Google-managed key."""
        response = self.session.post(
            f"{self.endpoint}/v1/projects/-/serviceAccounts/{self.email}:signBlob",
            json={"payload": base64.b64encode(message).decode()},
        )
        assert response.status_code == 200, response.text
        return base64.b64decode(response.json()["signedBlob"])


def signed_urls(
    clients: list, sessions: list[AuthorizedSession], endpoint: str, transport: requests.Session
) -> None:
    """Sign V4 URLs with a created key and with signBlob; the signer's grants authorize them."""
    fixture = clients[0]
    admin = sessions[0]
    accounts = endpoint + "/v1/projects/synthetic/serviceAccounts"
    assert admin.post(accounts, json={"accountId": "signer"}).status_code == 200
    email = "signer@synthetic.iam.gserviceaccount.com"
    created = admin.post(f"{accounts}/{email}/keys", json={})
    assert created.status_code == 200, created.text
    info = json.loads(base64.b64decode(created.json()["privateKeyData"]))
    signer = service_account.Credentials.from_service_account_info(info)
    bucket = create_bucket(fixture, "sdk-signed-bucket", "serviceAccount:" + email)
    blob = bucket.blob("dir/signed file.txt")
    expiration = dt.timedelta(minutes=5)
    put = blob.generate_signed_url(
        version="v4",
        expiration=expiration,
        method="PUT",
        content_type="text/plain",
        credentials=signer,
    )
    stored = transport.put(put, data=b"signed body", headers={"Content-Type": "text/plain"})
    assert stored.status_code == 200, stored.text
    get = blob.generate_signed_url(version="v4", expiration=expiration, credentials=signer)
    assert transport.get(get).content == b"signed body"
    tampered = get.replace("X-Goog-Signature=", "X-Goog-Signature=00")
    assert transport.get(tampered).status_code == 403
    expired = _signing.generate_signed_url_v4(
        signer,
        resource=f"/{bucket.name}/dir/signed%20file.txt",
        expiration=60,
        api_access_endpoint=endpoint,
        _request_timestamp="20200101T000000Z",
    )
    response = transport.get(expired)
    assert response.status_code == 400, response.text
    assert b"ExpiredToken" in response.content
    # signBlob: the fixture may sign as the account once it holds Token Creator on it.
    sign_blob = f"{endpoint}/v1/projects/-/serviceAccounts/{email}:signBlob"
    assert admin.post(sign_blob, json={"payload": "YQ=="}).status_code == 403
    policy = {
        "bindings": [
            {
                "role": "roles/iam.serviceAccountTokenCreator",
                "members": ["serviceAccount:fixture@synthetic.iam.gserviceaccount.com"],
            }
        ]
    }
    assert (
        admin.post(f"{accounts}/{email}:setIamPolicy", json={"policy": policy}).status_code == 200
    )
    local = LocalSigner(admin, endpoint, email)
    via_sign_blob = blob.generate_signed_url(version="v4", expiration=expiration, credentials=local)
    assert transport.get(via_sign_blob).content == b"signed body"
    # The signer's own Storage grant authorizes the URL.
    grant(bucket, "serviceAccount:" + email, None)
    assert transport.get(get).status_code == 403
    bucket.blob(blob.name).delete(retry=None)
    bucket.delete(retry=None)
    assert admin.delete(f"{accounts}/{email}").status_code == 200


def expect_denied(action: Callable[[], object]) -> None:
    """Require a Forbidden from the action."""
    try:
        action()
    except Forbidden:
        return
    raise AssertionError("Denied action succeeded")


def expect_precondition(action: Callable[[], object]) -> None:
    """Require a PreconditionFailed from the action."""
    try:
        action()
    except PreconditionFailed:
        return
    raise AssertionError("Stale precondition succeeded")


def main() -> None:
    """Exercise the storage runner end to end against the local emulator."""
    assert sys.platform == "linux"
    assert os.environ.get("GCP_LOCAL_ISOLATED") == "1"
    assert {name for _, name in socket.if_nameindex()} == {"lo"}
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="gcp-local-storage-") as temporary:
        directory = Path(temporary)
        principals = write_identity_configs(directory)
        process = subprocess.Popen(
            [
                str(root / "bin/gcp-local"),
                "-services",
                "storage,iam",
                "-identity-config",
                str(directory / "identity.json"),
                "-iam-config",
                str(directory / "iam.json"),
                "-listen",
                "127.0.0.1:0",
                "-grpc-listen",
                "127.0.0.1:0",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        lines, events = [], queue.Queue()

        def logs() -> None:
            for line in process.stdout:
                lines.append(line)
                events.put(line)

        reader = threading.Thread(target=logs, daemon=True)
        reader.start()
        clients, sessions = [], []
        try:
            started = json.loads(events.get(timeout=10))
            assert started["msg"] == "gcp-local listening"
            endpoint = "http://" + started["address"]
            origin = urlsplit(endpoint).netloc

            class LocalSession(requests.Session):
                def __init__(self) -> None:
                    super().__init__()
                    self.trust_env = False

                def request(self, method: str, url: str, **kwargs: object) -> "requests.Response":
                    parsed = urlsplit(url)
                    assert parsed.scheme == "http"
                    assert parsed.netloc == origin
                    kwargs["allow_redirects"] = False
                    kwargs.setdefault("timeout", 5)
                    return super().request(method, url, **kwargs)

            transport_session = LocalSession()
            sessions.append(transport_session)
            clients, principal_sessions = build_clients(
                directory, principals, endpoint, transport_session
            )
            sessions.extend(principal_sessions)
            object_lifecycle(clients, principals)
            resumable_uploads(clients, principals, directory)
            signed_urls(clients, principal_sessions, endpoint, transport_session)
            # A raw bearer token is not an identity: it must be rejected.
            assert (
                transport_session.get(
                    endpoint + "/storage/v1/b?project=synthetic",
                    headers={"Authorization": "Bearer gcp-local-test:fixture"},
                ).status_code
                == 401
            )
            assert all(client._credentials.token.startswith("gl.") for client in clients)  # noqa: SLF001 -- probing the issued token form
            process.terminate()
            assert process.wait(timeout=12) == 0
            reader.join(timeout=2)
            text = "".join(lines)
            assert all(principal["subject_token"] not in text for principal in principals)
            assert all(client._credentials.token not in text for client in clients)  # noqa: SLF001 -- verifying redaction
        finally:
            for client in clients:
                client.close()
            for session in sessions:
                session.close()
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            reader.join(timeout=2)
            process.stdout.close()
    print(
        "Production binary: local ADC, SDK bucket/object CRUD, replacement, revocation, "
        "resumable uploads, V4 signed URLs, and cleanup passed."
    )


if __name__ == "__main__":
    main()
