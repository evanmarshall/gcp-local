"""Drive google-cloud-secret-manager at its default gRPC transport with local ADC."""

import importlib.metadata
import json
import os
import socket
import sys
import time

import google.auth
import google_crc32c
import grpc
import requests
from google.api_core import exceptions
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
from google.cloud import secretmanager
from google.cloud.secretmanager_v1.services.secret_manager_service.transports import (
    SecretManagerServiceGrpcTransport,
)

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


def client_for(endpoint: str, filename: str | None = None):
    """A client over a loopback channel carrying local ADC call credentials."""
    session = requests.Session()
    session.trust_env = False
    request = Request(session=session)
    if filename:
        credentials, _ = google.auth.load_credentials_from_file(filename, scopes=SCOPES)
    else:
        credentials, project = google.auth.default(scopes=SCOPES, request=request)
        assert project == "project-a"
    channel = secure_authorized_channel(
        credentials, request, endpoint, ssl_credentials=grpc.local_channel_credentials()
    )
    transport = SecretManagerServiceGrpcTransport(channel=channel)
    return secretmanager.SecretManagerServiceClient(transport=transport), channel


def main() -> None:
    assert os.environ.get("GCP_LOCAL_ISOLATED") == "1"
    assert {name for _, name in socket.if_nameindex()} == {"lo"}
    endpoint = sys.argv[1]
    client, channel = client_for(endpoint)
    parent = "projects/project-a"
    secret = client.create_secret(
        request={
            "parent": parent,
            "secret_id": "python-sdk",
            "secret": {"replication": {"automatic": {}}, "labels": {"sdk": "python"}},
        }
    )
    # Names come back in the project-number form, as GCP returns them.
    assert secret.name == "projects/123456/secrets/python-sdk", secret.name
    assert secret.labels["sdk"] == "python"
    payload = b"python-payload"
    crc = google_crc32c.Checksum(payload)
    version = client.add_secret_version(
        request={"parent": secret.name, "payload": {"data": payload}}
    )
    assert version.state == secretmanager.SecretVersion.State.ENABLED
    accessed = client.access_secret_version(request={"name": secret.name + "/versions/latest"})
    assert accessed.payload.data == payload
    assert accessed.payload.data_crc32c == int.from_bytes(crc.digest(), "big")
    assert [s.name for s in client.list_secrets(request={"parent": parent})] == [secret.name]
    assert [v.name for v in client.list_secret_versions(request={"parent": secret.name})] == [
        version.name
    ]
    client.disable_secret_version(request={"name": version.name})
    try:
        client.access_secret_version(request={"name": version.name})
        raise AssertionError("disabled version was readable")
    except exceptions.FailedPrecondition:
        pass
    client.enable_secret_version(request={"name": version.name})

    denied, denied_channel = client_for(endpoint, os.environ["GCP_LOCAL_DENIED_ADC"])
    try:
        denied.access_secret_version(request={"name": version.name})
        raise AssertionError("denied caller read the payload")
    except exceptions.PermissionDenied:
        pass
    finally:
        denied_channel.close()

    # Unimplemented coverage fails at once under the default retry settings.
    started = time.monotonic()
    for call in (
        lambda: client.get_secret(
            request={"name": "projects/project-a/locations/us-central1/secrets/python-sdk"}
        ),
        lambda: client.test_iam_permissions(
            request={"resource": secret.name, "permissions": ["secretmanager.secrets.get"]}
        ),
    ):
        try:
            call()
            raise AssertionError("unimplemented call succeeded")
        except exceptions.MethodNotImplemented:
            pass
    assert time.monotonic() - started < 5

    client.delete_secret(request={"name": secret.name})
    try:
        client.get_secret(request={"name": secret.name})
        raise AssertionError("deleted secret was readable")
    except exceptions.NotFound:
        pass
    channel.close()
    print(
        json.dumps(
            {
                "kind": "local-sdk",
                "transport": "grpc",
                "versions": {
                    name: importlib.metadata.version(name)
                    for name in ("google-cloud-secret-manager", "grpcio", "google-auth")
                },
            }
        )
    )


if __name__ == "__main__":
    main()
