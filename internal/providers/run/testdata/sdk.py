"""Drive google-cloud-run over its default gRPC transport with local ADC."""

import os
import sys

import google.auth
import grpc
import requests
from google.api_core import exceptions
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
from google.cloud import run_v2
from google.cloud.run_v2.services.revisions.transports import RevisionsGrpcTransport
from google.cloud.run_v2.services.services.transports import ServicesGrpcTransport
from google.iam.v1 import iam_policy_pb2
from google.longrunning import operations_pb2

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
PARENT = "projects/demo-project/locations/us-central1"
NAME = PARENT + "/services/sdk-svc"


def channel_for(endpoint: str, filename: str | None = None) -> grpc.Channel:
    """A local gRPC channel that attaches tokens from local ADC, with no ambient proxy."""
    session = requests.Session()
    session.trust_env = False
    request = Request(session=session)
    if filename:
        credentials, _ = google.auth.load_credentials_from_file(filename, scopes=SCOPES)
    else:
        credentials, _ = google.auth.default(scopes=SCOPES, request=request)
    return secure_authorized_channel(
        credentials, request, endpoint, ssl_credentials=grpc.local_channel_credentials()
    )


def deploy(services: run_v2.ServicesClient) -> run_v2.Service:
    """Create the service and wait on its operation through the operations client."""
    service = run_v2.Service(
        template=run_v2.RevisionTemplate(
            containers=[
                run_v2.Container(
                    image="example.local/svc:1", env=[run_v2.EnvVar(name="MODE", value="sdk")]
                )
            ]
        )
    )
    operation = services.create_service(parent=PARENT, service=service, service_id="sdk-svc")
    created = operation.result(timeout=60)
    assert created.name == NAME, created
    assert created.latest_ready_revision == "sdk-svc-00001", created
    assert created.uri, created
    assert operation.metadata.name == NAME, operation.metadata
    return created


def check(endpoint: str) -> None:
    channel = channel_for(endpoint)
    services = run_v2.ServicesClient(transport=ServicesGrpcTransport(channel=channel))
    revisions = run_v2.RevisionsClient(transport=RevisionsGrpcTransport(channel=channel))
    deploy(services)

    got = services.get_service(name=NAME)
    assert got.template.containers[0].env[0].value == "sdk", got
    assert [listed.name for listed in services.list_services(parent=PARENT)] == [NAME]
    revision = revisions.get_revision(name=NAME + "/revisions/sdk-svc-00001")
    assert revision.service == NAME and revision.containers[0].image == "example.local/svc:1", (
        revision
    )
    assert len(list(revisions.list_revisions(parent=NAME))) == 1

    operations = services.list_operations(operations_pb2.ListOperationsRequest(name=PARENT))
    assert any(operation.done for operation in operations.operations), operations

    try:
        services.test_iam_permissions(
            iam_policy_pb2.TestIamPermissionsRequest(
                resource=NAME, permissions=["run.services.get"]
            )
        )
    except exceptions.MethodNotImplemented as error:
        assert "No provider implements" in str(error), error
    else:
        raise AssertionError("TestIamPermissions answered instead of naming the gap")

    deleted = services.delete_service(name=NAME).result(timeout=60)
    assert deleted.name == NAME, deleted
    try:
        services.get_service(name=NAME)
    except exceptions.NotFound:
        pass
    else:
        raise AssertionError("The deleted service is still readable")

    denied = run_v2.ServicesClient(
        transport=ServicesGrpcTransport(
            channel=channel_for(endpoint, os.environ["GCP_LOCAL_DENIED_ADC"])
        )
    )
    try:
        denied.create_service(
            parent=PARENT, service=run_v2.Service(template=got.template), service_id="denied-svc"
        )
    except exceptions.PermissionDenied as error:
        assert "run.services.create" in str(error), error
    else:
        raise AssertionError("An ungranted caller created a service")


if __name__ == "__main__":
    check(sys.argv[1])
    print("google-cloud-run gRPC: deploy, wait, get, list, revisions, delete, and denial passed")
