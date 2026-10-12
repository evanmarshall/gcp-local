"""Drive google-cloud-logging over REST and gRPC against the local provider with local ADC."""

import os
import sys

import google.auth
import grpc
import requests
from google.api_core import exceptions
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
from google.cloud import logging as cloud_logging
from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2Client
from google.cloud.logging_v2.services.logging_service_v2.transports import (
    LoggingServiceV2GrpcTransport,
)
from google.cloud.logging_v2.types import ListLogEntriesRequest, LogEntry, WriteLogEntriesRequest

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
PROJECT = "project-a"


def credentials_for(filename: str | None = None):
    """Load local ADC already scoped to cloud-platform, with no ambient proxy."""
    session = requests.Session()
    session.trust_env = False
    request = Request(session=session)
    if filename:
        credentials, _ = google.auth.load_credentials_from_file(filename, scopes=SCOPES)
    else:
        credentials, _ = google.auth.default(scopes=SCOPES, request=request)
    return credentials, request


def check_rest(endpoint: str) -> None:
    """The handwritten client over HTTP: write, filter, struct payloads, delete."""
    credentials, _ = credentials_for()
    client = cloud_logging.Client(
        project=PROJECT,
        credentials=credentials,
        _use_grpc=False,
        client_options={"api_endpoint": endpoint},
    )
    logger = client.logger("sdk-rest")
    logger.log_text("rest hello", severity="INFO")
    logger.log_text("rest failure", severity="ERROR")
    logger.log_struct({"event": "signup", "user": "alice"}, severity="NOTICE")
    owned = 'logName="projects/project-a/logs/sdk-rest"'
    errors = list(client.list_entries(filter_=owned + " severity>=ERROR"))
    assert [entry.payload for entry in errors] == ["rest failure"], errors
    structured = list(client.list_entries(filter_="jsonPayload.user=alice"))
    assert structured[0].payload == {"event": "signup", "user": "alice"}, structured
    newest = list(client.list_entries(filter_=owned, order_by=cloud_logging.DESCENDING))
    assert newest[0].payload == {"event": "signup", "user": "alice"}, newest
    logger.delete()
    assert not list(client.list_entries(filter_=owned))

    denied_credentials, _ = credentials_for(os.environ["GCP_LOCAL_DENIED_ADC"])
    denied = cloud_logging.Client(
        project=PROJECT,
        credentials=denied_credentials,
        _use_grpc=False,
        client_options={"api_endpoint": endpoint},
    )
    try:
        denied.logger("sdk-rest").log_text("must not land")
    except exceptions.Forbidden as error:
        assert "logging.logEntries.create" in str(error), error
    else:
        raise AssertionError("An ungranted caller wrote a log entry")


def check_grpc(grpc_endpoint: str) -> None:
    """The generated client over a local gRPC channel that keeps ADC."""
    credentials, request = credentials_for()
    channel = secure_authorized_channel(
        credentials, request, grpc_endpoint, ssl_credentials=grpc.local_channel_credentials()
    )
    client = LoggingServiceV2Client(transport=LoggingServiceV2GrpcTransport(channel=channel))
    log_name = f"projects/{PROJECT}/logs/sdk-grpc"
    client.write_log_entries(
        request=WriteLogEntriesRequest(
            log_name=log_name,
            resource={"type": "global"},
            entries=[LogEntry(text_payload=f"grpc {index}") for index in range(5)],
        )
    )
    pager = client.list_log_entries(
        request=ListLogEntriesRequest(
            resource_names=[f"projects/{PROJECT}"], filter=f'logName="{log_name}"', page_size=2
        )
    )
    assert [entry.text_payload for entry in pager] == [f"grpc {index}" for index in range(5)]
    assert log_name in list(client.list_logs(parent=f"projects/{PROJECT}"))
    client.delete_log(log_name=log_name)
    try:
        client.delete_log(log_name=log_name)
    except exceptions.NotFound:
        pass
    else:
        raise AssertionError("Deleting a missing log succeeded")


def main() -> None:
    """Run the REST and gRPC checks."""
    check_rest(sys.argv[1])
    check_grpc(sys.argv[2])
    print("google-cloud-logging SDK checks passed")


if __name__ == "__main__":
    main()
