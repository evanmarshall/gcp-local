"""CI-only acceptance for the packaged workflow on real Docker.

A Pub/Sub push delivers to the Cloud Run worker, which stores an object under
its own service account; revoking the worker's Storage grant fails the write
and restoring it recovers. The control plane calls use an allUsers fixture
grant on the synthetic project; the boundaries under test (invocation and the
worker's Storage write) use real per-identity grants.
"""

import json
import os
import secrets
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import google.auth
from google.auth.transport.grpc import secure_authorized_channel
from google.cloud import pubsub_v1
from google.pubsub_v1.services.publisher.transports.grpc import PublisherGrpcTransport
from google.pubsub_v1.services.subscriber.transports.grpc import SubscriberGrpcTransport
from smoke_cloudrun import SERVE_SOCKET, build_image, start_core, write_configs  # helpers

PUBSUB_GRPC_PORT = SERVE_SOCKET + 1


REPO = Path(__file__).resolve().parents[1]
SERVE_SOCKET = 9298
BASE = "http://127.0.0.1:" + str(SERVE_SOCKET)
PROJECT = "demo-run"
SCOPE = "/v2/projects/" + PROJECT + "/locations/us-central1"
WORKER_IMAGE = "gcp-local/workflow:latest"
WORKER_EMAIL = "worker@" + PROJECT + ".iam.gserviceaccount.com"
ENDPOINT = BASE + "/_run/worker-" + PROJECT + ".us-central1.run.app"


class LocalChannelClients:
    """Pub/Sub clients pinned to the local gRPC gateway only."""

    def __init__(self, credentials: google.auth.credentials.Credentials, port: int) -> None:
        """Pin both clients to the local gRPC gateway channel."""
        channel = secure_authorized_channel(
            credentials, None, "127.0.0.1:" + str(port))
        self.publisher = pubsub_v1.PublisherClient(
            transport=PublisherGrpcTransport(channel=channel))
        self.subscriber = pubsub_v1.SubscriberClient(
            transport=SubscriberGrpcTransport(channel=channel))


def sdk_credentials(adc: Path) -> google.auth.credentials.Credentials:
    """Load the fixture ADC bundle pinned to the local endpoint."""
    credentials, _ = google.auth.load_credentials_from_file(
        str(adc), scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return credentials


def rest(method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
    """Issue one control-plane REST request against the local core."""
    data = json.dumps(body).encode() if body is not None else None
    head = urllib.request.Request(BASE + path, data=data, method=method)  # noqa: S310 -- fixed loopback base
    if data:
        head.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(head, timeout=30) as response:  # noqa: S310 -- fixed loopback base
            payload = response.read()
            return response.status, json.loads(payload) if payload else {}
    except urllib.error.HTTPError as error:
        payload = error.read()
        return error.code, json.loads(payload) if payload else {}
    data = json.dumps(body).encode() if body is not None else None
    head = urllib.request.Request(BASE + path, data=data, method=method)  # noqa: S310 -- fixed loopback base
    if data:
        head.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(head, timeout=30) as response:  # noqa: S310 -- fixed loopback base
            payload = response.read()
            return response.status, json.loads(payload) if payload else {}
    except urllib.error.HTTPError as error:
        payload = error.read()
        return error.code, json.loads(payload) if payload else {}


def write_adc(_project: str, subject_token: str) -> Path:
    """Write the developer fixture ADC bundle for the local token exchange."""
    directory = Path(tempfile.mkdtemp())
    directory.chmod(stat.S_IRWXU)
    (directory / "subject.txt").write_text(subject_token)
    adc = {
        "type": "external_account",
        "audience": "//gcp-local/identity",
        "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "token_url": "http://127.0.0.1:" + str(SERVE_SOCKET) + "/v1/token",
        "credential_source": {"file": str(directory / "subject.txt")},
    }
    path = directory / "adc.json"
    path.write_text(json.dumps(adc))
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return path


def install_java() -> str:
    """Install the pinned Pub/Sub emulator jar and return an absolute Java path."""
    install = subprocess.run(
        [sys.executable, "scripts/install_pubsub.py"], check=True, capture_output=True, text=True)
    assert install.returncode == 0, install.stdout + install.stderr
    java = os.environ.get("GCP_LOCAL_JAVA") or shutil.which("java") or ""
    assert java.startswith("/"), "An absolute Java path is required"
    jar = REPO / ".tools/pubsub/emulator.jar"
    assert jar.exists(), "The pinned Pub/Sub emulator jar is missing"
    return java


def write_identity(project: str) -> tuple[Path, str]:
    """Write the owner-only identity config with the workflow principals."""
    directory = Path(tempfile.mkdtemp())
    directory.chmod(stat.S_IRWXU)
    principals = [
        {"subject": "serviceAccount:" + email, "project": project,
         "subject_token": "gls." + secrets.token_urlsafe(32)}
        for email in ("developer@" + project, WORKER_EMAIL)
    ]
    path = directory / "server.json"
    path.write_text(json.dumps({"token_lifetime_seconds": 3600, "principals": principals}))
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return path, principals[0]["subject_token"]


def await_operation(name: str, deadline: float = 90.0) -> dict[str, Any]:
    """Poll one long-running operation until it reports done."""
    last: dict[str, Any] = {}
    end = time.time() + deadline
    while time.time() < end:
        _, body = rest("GET", "/v2/" + name)
        if body.get("done"):
            return body
        last = body
        time.sleep(0.5)
    raise AssertionError("Operation did not finish: " + name + " last=" + json.dumps(last))


def deploy_worker(clients: LocalChannelClients) -> str:
    """Create the bucket, topic, push subscription, and the worker service."""
    bucket = "workflow-" + PROJECT
    status, body = rest("POST", "/storage/v1/b?project=" + PROJECT, {
        "name": bucket, "location": "US-CENTRAL1", "storageClass": "STANDARD",
        "iamConfiguration": {"uniformBucketLevelAccess": {"enabled": True},
                             "publicAccessPrevention": "enforced"},
        "softDeletePolicy": {"retentionDurationSeconds": "0"},
    })
    assert status == 200, "bucket create failed: " + json.dumps(body)[:200]
    status, _ = rest("GET", "/storage/v1/b/" + bucket + "/iam")
    assert status == 200
    # Grant the worker storage.objects.create on the bucket.
    policy = {"bindings": [{"role": "roles/storage.objectUser",
                            "members": ["serviceAccount:" + WORKER_EMAIL]}],
              "resourceId": "/b/" + bucket, "kind": "storage#policy"}
    status, body = rest("PUT", "/storage/v1/b/" + bucket + "/iam", policy)
    assert status == 200, "bucket grant failed: " + json.dumps(body)[:200]

    clients.publisher.create_topic(request={"name": pubsub_v1.TopicName(PROJECT, "workflow")})
    status, service = rest("POST", SCOPE + "/services", {
        "name": SCOPE + "/services/worker",
        "template": {"containers": [{"image": WORKER_IMAGE, "containerPort": 8080}],
                     "serviceAccount": "serviceAccount:" + WORKER_EMAIL},
    })
    assert status == 200, service
    result = await_operation(service["name"])
    assert "error" not in result, result

    # The push delivery mints its ID token for the configured OIDC account;
    # the invocation contract requires that account to hold run.invoker.
    status, policy = rest("GET", SCOPE + "/services/worker:getIamPolicy")
    assert status == 200, policy
    policy.setdefault("bindings", []).append(
        {"role": "roles/run.invoker", "members": ["serviceAccount:" + WORKER_EMAIL]})
    status, _ = rest("POST", SCOPE + "/services/worker:setIamPolicy", {"policy": policy})
    assert status == 200
    clients.subscriber.create_subscription(request={
        "name": pubsub_v1.SubscriptionName(PROJECT, "workflow-push"),
        "topic": pubsub_v1.TopicName(PROJECT, "workflow"),
        "pushConfig": pubsub_v1.PushConfig(
            push_endpoint=ENDPOINT,
            oidc_token=pubsub_v1.PushConfig.OidcToken(
                service_account_email=WORKER_EMAIL, audience=ENDPOINT),
        ),
        "ack_deadline_seconds": 10,
    })
    return bucket


def publish(clients: LocalChannelClients, bucket: str, payload: str, message_id: str) -> None:
    """Publish one message through the SDK client."""
    future = clients.publisher.publish(
        pubsub_v1.TopicName(PROJECT, "workflow"), payload.encode(),
        bucket=bucket, messageId=message_id)
    assert future.result(timeout=15), "publish failed"


def stored_object(bucket: str, name: str) -> tuple[int, str]:
    """Read the stored object bytes as the fixture principal."""
    head = urllib.request.Request(  # noqa: S310 -- fixed loopback base
        BASE + "/storage/v1/b/" + bucket + "/o/" + name + "?alt=media")
    try:
        with urllib.request.urlopen(head, timeout=30) as response:  # noqa: S310 -- fixed loopback base
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def expect_object(bucket: str, message_id: str, payload: str) -> None:
    """Wait for the worker to store the message payload."""
    end = time.time() + 60
    while time.time() < end:
        status, body = stored_object(bucket, message_id + ".txt")
        if status == 200:
            assert body == payload, body
            return
        time.sleep(1)
    raise AssertionError("The worker did not store the object: " + message_id)


def denial_cycle(clients: LocalChannelClients, bucket: str) -> None:
    """Revoke the worker's Storage grant, expect the write to fail, then restore."""
    status, _ = rest("PUT", "/storage/v1/b/" + bucket + "/iam",
                     {"bindings": [], "resourceId": "/b/" + bucket, "kind": "storage#policy"})
    assert status == 200, "revoke failed"
    publish(clients, bucket, "denied-payload", "denied-msg")
    end = time.time() + 30
    while time.time() < end:
        status, _ = stored_object(bucket, "denied-msg.txt")
        if status == 404:
            break
        time.sleep(1)
    else:
        raise AssertionError("The denied worker still stored the object")
    policy = {"bindings": [{"role": "roles/storage.objectUser",
                            "members": ["serviceAccount:" + WORKER_EMAIL]}],
              "resourceId": "/b/" + bucket, "kind": "storage#policy"}
    status, _ = rest("PUT", "/storage/v1/b/" + bucket + "/iam", policy)
    assert status == 200, "restore failed"
    publish(clients, bucket, "restored-payload", "restored-msg")
    expect_object(bucket, "restored-msg", "restored-payload")


def main() -> int:
    """Run the end-to-end workflow acceptance and clean everything up."""
    build = subprocess.run(
        [sys.executable, "scripts/install_go.py"], check=True, capture_output=True, text=True)
    assert build.returncode == 0, build.stdout + build.stderr
    binary = REPO / "bin" / "gcp-local"
    subprocess.run(
        ["/bin/sh", str(REPO / "scripts/go.sh"), "build", "-o", str(binary), "./cmd/gcp-local"],
        check=True, capture_output=True)
    build_image(WORKER_IMAGE)
    java = install_java()
    jar = str(REPO / ".tools/pubsub/emulator.jar")
    docker = os.environ.get("DOCKER", "docker")
    before = subprocess.check_output([docker, "ps", "-aq"], text=True)
    iam_path = write_configs(PROJECT, ["roles/pubsub.admin", "roles/storage.admin"])
    identity_path, developer_token = write_identity(PROJECT)
    adc = write_adc(PROJECT, developer_token)
    core = start_core(binary, iam_path, identity_path, port=SERVE_SOCKET,
                      services="storage,pubsub,run", pubsub_java=java, pubsub_jar=jar)
    clients = LocalChannelClients(sdk_credentials(adc), PUBSUB_GRPC_PORT)
    try:
        bucket = deploy_worker(clients)
        publish(clients, bucket, "workflow-payload", "wf-msg-1")
        expect_object(bucket, "wf-msg-1", "workflow-payload")
        denial_cycle(clients, bucket)
        status, _ = rest("DELETE", SCOPE + "/services/worker")
        assert status == 200
        docker = os.environ.get("DOCKER", "docker")
        after = subprocess.check_output([docker, "ps", "-aq"], text=True)
        assert after == before, "The workflow left a container behind"
    finally:
        core.terminate()
        core.wait(timeout=10)
    return 0


if __name__ == "__main__":
    sys.exit(main())
