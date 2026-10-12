"""Drive google-cloud-tasks (v2) at its default gRPC transport with local ADC."""

import http.server
import importlib.metadata
import json
import os
import queue
import socket
import sys
import threading
import time

import google.auth
import grpc
import requests
from google.api_core import exceptions
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
from google.cloud import tasks_v2
from google.cloud.tasks_v2.services.cloud_tasks.transports import CloudTasksGrpcTransport

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
    return tasks_v2.CloudTasksClient(transport=CloudTasksGrpcTransport(channel=channel)), channel


def target():
    """A loopback HTTP server that records each delivered task."""
    delivered = queue.Queue()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            delivered.put((self.headers.get("X-Job"), self.rfile.read(length)))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, delivered


def main() -> None:
    assert os.environ.get("GCP_LOCAL_ISOLATED") == "1"
    assert {name for _, name in socket.if_nameindex()} == {"lo"}
    endpoint = sys.argv[1]
    client, channel = client_for(endpoint)
    server, delivered = target()
    parent = "projects/project-a/locations/us-central1"
    made = client.create_queue(
        request={
            "parent": parent,
            "queue": {"name": parent + "/queues/python-sdk", "retry_config": {"max_attempts": 3}},
        }
    )
    assert made.state == tasks_v2.Queue.State.RUNNING
    assert made.retry_config.max_attempts == 3
    url = f"http://127.0.0.1:{server.server_port}/work"
    task = client.create_task(
        request={
            "parent": made.name,
            "task": {
                "http_request": {
                    "url": url,
                    "http_method": tasks_v2.HttpMethod.POST,
                    "headers": {"X-Job": "python"},
                    "body": b"python-task",
                }
            },
        }
    )
    assert task.name.startswith(made.name + "/tasks/")
    assert delivered.get(timeout=10) == ("python", b"python-task")
    assert [q.name for q in client.list_queues(request={"parent": parent})] == [made.name]
    paused = client.pause_queue(request={"name": made.name})
    assert paused.state == tasks_v2.Queue.State.PAUSED

    denied, denied_channel = client_for(endpoint, os.environ["GCP_LOCAL_DENIED_ADC"])
    try:
        denied.get_queue(request={"name": made.name})
        raise AssertionError("denied caller read the queue")
    except exceptions.PermissionDenied:
        pass
    finally:
        denied_channel.close()

    # Unimplemented coverage fails at once under the default retry settings.
    started = time.monotonic()
    try:
        client.test_iam_permissions(
            request={"resource": made.name, "permissions": ["cloudtasks.queues.get"]}
        )
        raise AssertionError("unimplemented call succeeded")
    except exceptions.MethodNotImplemented:
        pass
    assert time.monotonic() - started < 5

    client.delete_queue(request={"name": made.name})
    try:
        client.get_queue(request={"name": made.name})
        raise AssertionError("deleted queue was readable")
    except exceptions.NotFound:
        pass
    channel.close()
    server.shutdown()
    print(
        json.dumps(
            {
                "kind": "local-sdk",
                "transport": "grpc",
                "versions": {
                    name: importlib.metadata.version(name)
                    for name in ("google-cloud-tasks", "grpcio", "google-auth")
                },
            }
        )
    )


if __name__ == "__main__":
    main()
