"""CI-only acceptance for the packaged workflow on real Docker.

A Pub/Sub push delivers to the Cloud Run worker, which stores an object under
its own service account; revoking the worker's Storage grant fails the write
and restoring it recovers. The control plane calls use an allUsers fixture
grant on the synthetic project; the boundaries under test (invocation and the
worker's Storage write) use real per-identity grants.
"""

import base64
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

from smoke_cloudrun import build_image, start_core, write_configs  # harness helpers

REPO = Path(__file__).resolve().parents[1]
SERVE_SOCKET = 9298
BASE = "http://127.0.0.1:" + str(SERVE_SOCKET)
PROJECT = "demo-run"
SCOPE = "/v2/projects/" + PROJECT + "/locations/us-central1"
WORKER_IMAGE = "gcp-local/workflow:latest"
WORKER_EMAIL = "worker@" + PROJECT + ".iam.gserviceaccount.com"
ENDPOINT = BASE + "/_run/worker-" + PROJECT + ".us-central1.run.app"


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


def write_identity(project: str) -> Path:
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
    return path


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


def deploy_worker() -> None:
    """Create the bucket, topic, push subscription, and the worker service."""
    bucket = "workflow-" + PROJECT
    status, _ = rest("POST", "/storage/v1/b?project=" + PROJECT, {
        "name": bucket, "location": "US-CENTRAL1",
        "iamConfiguration": {"uniformBucketLevelAccess": {"enabled": True}},
    })
    assert status == 200, "bucket create failed"
    status, _ = rest("GET", "/storage/v1/b/" + bucket + "/iam")
    assert status == 200
    # Grant the worker storage.objects.create on the bucket.
    policy = {"bindings": [{"role": "roles/storage.objectUser",
                            "members": ["serviceAccount:" + WORKER_EMAIL]}],
              "resourceId": bucket}
    status, _ = rest("PUT", "/storage/v1/b/" + bucket + "/iam", policy)
    assert status == 200, "bucket grant failed"

    status, _ = rest("POST", "/v1/projects/" + PROJECT + "/topics",
                     {"name": "projects/" + PROJECT + "/topics/workflow"})
    assert status == 200, "topic create failed"
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
    status, _ = rest("POST", "/v1/projects/" + PROJECT + "/subscriptions", {
        "name": "projects/" + PROJECT + "/subscriptions/workflow-push",
        "topic": "projects/" + PROJECT + "/topics/workflow",
        "pushConfig": {"pushEndpoint": ENDPOINT,
                       "oidcToken": {"serviceAccountEmail": WORKER_EMAIL,
                                     "audience": ENDPOINT}},
        "ackDeadlineSeconds": 10,
    })
    assert status == 200, "subscription create failed"
    return bucket


def publish(bucket: str, payload: str, message_id: str) -> int:
    """Publish one message and return the number of delivery attempts observed."""
    topic = "projects/" + PROJECT + "/topics/workflow"
    status, _ = rest("POST", topic + ":publish", {
        "messages": [{"data": base64.urlsafe_b64encode(payload.encode()).decode(),
                      "attributes": {"bucket": bucket}, "messageId": message_id}],
    })
    assert status == 200, "publish failed"
    return status


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


def denial_cycle(bucket: str) -> None:
    """Revoke the worker's Storage grant, expect the write to fail, then restore."""
    status, _ = rest("PUT", "/storage/v1/b/" + bucket + "/iam",
                     {"bindings": [], "resourceId": bucket})
    assert status == 200, "revoke failed"
    publish(bucket, "denied-payload", "denied-msg")
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
              "resourceId": bucket}
    status, _ = rest("PUT", "/storage/v1/b/" + bucket + "/iam", policy)
    assert status == 200, "restore failed"
    publish(bucket, "restored-payload", "restored-msg")
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
    iam_path = write_configs(PROJECT, ["roles/pubsub.admin"])
    identity_path = write_identity(PROJECT)
    core = start_core(binary, iam_path, identity_path, port=SERVE_SOCKET,
                      services="storage,pubsub,run", pubsub_java=java, pubsub_jar=jar)
    try:
        bucket = deploy_worker()
        publish(bucket, "workflow-payload", "wf-msg-1")
        expect_object(bucket, "wf-msg-1", "workflow-payload")
        denial_cycle(bucket)
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
