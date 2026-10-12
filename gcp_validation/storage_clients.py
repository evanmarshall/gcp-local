"""Storage request shapes that real clients send.

Covers resumable sessions, V4 signed URLs, and the Go client's projection=full
calls. The transport exposes response headers (resumable sessions report
progress in Range and Location) and refuses any URL outside the selected
Storage or IAM Credentials endpoint, so a session URI or signed URL cannot lead
elsewhere.
"""

import base64
import json
import re
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import ProxyHandler, Request, build_opener

from google.auth.credentials import Signing

from .evidence import Inconclusive, Unverified
from .sandbox import load_credentials, refresh_credentials
from .storage import NoRedirects, StorageFixture

MAX_BODY = 2 * 1048576
IAM_CREDENTIALS = "https://iamcredentials.googleapis.com"
HTTP_OK = 200


@dataclass
class Reply:
    """One bounded HTTP response with lowercase header names."""

    status: int
    headers: dict[str, str]
    body: bytes

    def json(self) -> dict:
        """Decode the body as a JSON object, or return an empty one."""
        return json.loads(self.body) if self.body else {}

    def xml_code(self) -> str | None:
        """Return the XML API error code, if the body is an XML error."""
        match = re.search(rb"<Code>([A-Za-z]+)</Code>", self.body)
        return match.group(1).decode() if match else None


class ClientTransport:
    """Bounded raw HTTP for one Storage fixture's client-shaped requests."""

    def __init__(self, storage: StorageFixture) -> None:
        """Bind the fixture's target, endpoint, and credential cache."""
        self.storage = storage
        self.client = storage.client
        self.endpoint = self.client.endpoint
        local = self.client.target.mode == "local"
        self.iam_endpoint = self.endpoint if local else IAM_CREDENTIALS
        self.opener = build_opener(ProxyHandler({}), NoRedirects())

    def token(self, actor: str) -> str:
        """Return the bearer token for one scenario identity."""
        if self.client.target.mode == "local":
            return "gcp-local-test:" + actor
        if actor not in self.client.credentials:
            self.client.credentials[actor] = load_credentials(
                self.storage.sandbox.identities[actor], self.storage.sandbox.project
            )
        credential = self.client.credentials[actor]
        if not credential.valid:
            refresh_credentials(credential)
        return credential.token

    def send(
        self,
        method: str,
        url: str,
        *,
        actor: str | None = None,
        headers: dict[str, str] | None = None,
        body: bytes | dict | None = None,
        query: dict[str, str] | None = None,
    ) -> Reply:
        """Send one request to an allowed origin; actor None sends no credential."""
        target = urlsplit(url)
        allowed = {urlsplit(self.endpoint)[:2], urlsplit(self.iam_endpoint)[:2]}
        if target[:2] not in allowed or target.username or target.password or target.fragment:
            raise ValueError("Client request left the selected endpoints")
        headers = dict(headers or {})
        if actor is not None:
            headers["Authorization"] = "Bearer " + self.token(actor)
            if self.storage.sandbox:
                headers["x-goog-user-project"] = self.storage.sandbox.project
        if isinstance(body, dict):
            body = json.dumps(body).encode()
            headers.setdefault("Content-Type", "application/json")
        if query:
            url += ("&" if "?" in url else "?") + urlencode(query)
        request = Request(url, data=body, headers=headers, method=method)  # noqa: S310 -- origin checked above
        try:
            response = self.opener.open(request, timeout=30)
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError):
            raise Inconclusive("Storage transport failed; no cloud fallback is available") from None
        with response:
            payload = response.read(MAX_BODY + 1)
            if len(payload) > MAX_BODY:
                raise ValueError("Response exceeded the bounded scenario size")
            names = {key.lower(): value for key, value in response.headers.items()}
            return Reply(response.code, names, payload)

    def own(self, name: str) -> None:
        """Record one object name for cleanup before it is written."""
        if name not in self.storage.manifest["objects"]:
            if len(self.storage.manifest["objects"]) >= self.storage.max_objects:
                raise ValueError("Object count exceeds the bounded scenario scope")
            self.storage.manifest["objects"].append(name)
            self.storage.save()


class BlobSigner(Signing):
    """The google.auth Signing interface over IAM Credentials signBlob.

    The account signs for itself, so it needs Token Creator on itself.
    """

    def __init__(self, transport: ClientTransport, actor: str, email: str) -> None:
        """Bind the caller identity and the signing account."""
        self.transport, self.actor, self.email = transport, actor, email

    @property
    def signer_email(self) -> str:
        """Return the signing account."""
        return self.email

    @property
    def signer(self) -> "BlobSigner":
        """Return this signer."""
        return self

    def sign_bytes(self, message: bytes) -> bytes:
        """Sign message with the account's Google-managed key."""
        reply = self.transport.send(
            "POST",
            f"{self.transport.iam_endpoint}/v1/projects/-/serviceAccounts/{self.email}:signBlob",
            actor=self.actor,
            body={"payload": base64.b64encode(message).decode()},
        )
        if reply.status != HTTP_OK:
            raise Unverified(
                f"signBlob returned HTTP {reply.status}; grant the signer Token Creator on itself"
            )
        return base64.b64decode(reply.json()["signedBlob"])


def signer_email(storage: StorageFixture, actor: str) -> str:
    """Return the email of one scenario identity."""
    if storage.sandbox:
        return storage.sandbox.identities[actor].email
    return f"{actor}@{storage.project}.iam.gserviceaccount.com"


def signed_url(
    transport: ClientTransport,
    signer: BlobSigner,
    method: str,
    name: str,
    *,
    content_type: str | None = None,
    timestamp: str | None = None,
    expiration: int = 300,
) -> str:
    """Build a V4 signed URL with the pinned SDK's signing code."""
    from google.cloud.storage import _signing  # noqa: PLC0415 -- the SDK is optional offline

    return _signing.generate_signed_url_v4(
        signer,
        resource=f"/{transport.storage.bucket}/{name}",
        expiration=expiration,
        api_access_endpoint=transport.endpoint,
        method=method,
        content_type=content_type,
        _request_timestamp=timestamp,
    )


def acl_shape(document: dict) -> dict[str, bool]:
    """Report which ACL-related fields a projection=full document carries."""
    return {
        "has_acl": "acl" in document,
        "has_default_object_acl": "defaultObjectAcl" in document,
        "has_owner": "owner" in document,
    }
