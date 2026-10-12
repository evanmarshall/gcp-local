"""CI-only acceptance for the local Cloud Run control plane on real Docker.

Deploys a real container image, invokes it under the invoker contract, updates
it, and deletes it, requiring container cleanup. This script runs on ordinary
Linux Docker in CI (issue #13 execution boundary); it is not part of the
offline validation suite.
"""

import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Sequence
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[2]
DOCKER = os.environ.get("DOCKER", "docker")
SERVE_SOCKET = 9199
BASE = "http://127.0.0.1:" + str(SERVE_SOCKET)
SCOPE = "/v2/projects/demo-run/locations/us-central1"
IMAGE = "gcp-local/acceptance:latest"
HOST = "acceptance-demo-run.us-central1.run.app"
ACCEPT_IMAGE = (
    "python:3.12.15-slim-bookworm@"
    "sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3"
)


def request(method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
    """Issue one REST request against the local core and parse the JSON body."""
    data = json.dumps(body).encode() if body is not None else None
    # The base URL is a fixed loopback address built above.
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


def await_operation(name: str, deadline: float = 90.0) -> dict[str, Any]:
    """Poll one long-running operation until it reports done."""
    last: dict[str, Any] = {}
    end = time.time() + deadline
    while time.time() < end:
        # The operation name is already the full relative resource path.
        _, body = request("GET", "/v2/" + name)
        if body.get("done"):
            return body
        last = body
        time.sleep(0.5)
    raise AssertionError("Operation did not finish: " + name + " last=" + json.dumps(last))


def build_image(tag: str, *, dockerless: bool) -> tuple[str, str]:
    """Build the acceptance image, or stage its server for the process runtime."""
    context = Path(tempfile.mkdtemp())
    (context / "server.py").write_text(
        "import os\nfrom http.server import BaseHTTPRequestHandler, HTTPServer\n\n"
        "class Handler(BaseHTTPRequestHandler):\n"
        "    def do_GET(self):\n"
        "        body = ('served by ' + os.environ.get('K_SERVICE', '?')\n"
        "                + ' at revision ' + os.environ.get('K_REVISION', '?')).encode()\n"
        "        self.send_response(200)\n"
        "        self.send_header('Content-Length', str(len(body)))\n"
        "        self.end_headers()\n"
        "        self.wfile.write(body)\n\n"
        "HTTPServer(('0.0.0.0', int(os.environ.get('PORT', '8080'))), Handler).serve_forever()\n"
    )
    (context / "Dockerfile").write_text(
        "FROM " + ACCEPT_IMAGE + "\n"
        "COPY server.py /server.py\n"
        "ENV PORT=8080\n"
        "EXPOSE 8080\n"
        'ENTRYPOINT ["python3", "/server.py"]\n'
    )
    command = "python3 " + str(context / "server.py")
    if dockerless:
        # The process runtime runs the same server file directly.
        return tag, command
    subprocess.run([DOCKER, "build", "-t", tag, str(context)], check=True, capture_output=True)
    return tag, ""


def write_configs() -> Path:
    """Write an owner-only IAM configuration and return its path."""
    directory = Path(tempfile.mkdtemp())
    directory.chmod(stat.S_IRWXU)
    resource = "//cloudresourcemanager.googleapis.com/projects/demo-run"
    iam = {
        "resources": [{"name": resource, "service": "resourcemanager", "kind": "project",
                       # A real-shaped numeric project number: the push
                       # contract requires one without leading zeros.
                       "number": "123456789012",
                       "scope": {"project": "demo-run", "location": "global"}}],
        # The smoke drives the control plane without a local identity mount,
        # so its fixture principal is allUsers; the invoker contract under test
        # is enforced per service, not here.
        # Deploying as the default compute account needs actAs on it, which
        # resolves against this project policy when the account isn't declared.
        "policies": {resource: {"bindings": [
            {"role": "roles/run.admin", "members": ["allUsers"]},
            {"role": "roles/iam.serviceAccountUser", "members": ["allUsers"]},
        ]}},
    }
    path = directory / "iam.json"
    path.write_text(json.dumps(iam))
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return path


def answers(port: int) -> bool:
    """Report whether anything serves the loopback health URL, even with an error."""
    try:
        with urllib.request.urlopen("http://127.0.0.1:" + str(port) + "/_local/health", timeout=1):
            pass
    except urllib.error.HTTPError:
        return True
    except (urllib.error.URLError, TimeoutError, OSError):
        return False
    return True


def start_core(
    binary: Path, iam_path: Path, identity_path: Path | None = None,
    port: int = SERVE_SOCKET, services: str = "run", pubsub_java: str = "",
    pubsub_jar: str = "", extra_flags: Sequence[str] = (),
) -> subprocess.Popen:
    """Start the core with the selected providers and revision runtime."""
    # A stale server from an earlier crashed run would answer health checks
    # with a different identity config and poison the run; fail loudly here.
    if answers(port):
        raise RuntimeError("Port " + str(port) + " is already served by an earlier run")
    logs = Path(tempfile.gettempdir()) / ("gcp-local-core-" + str(port) + ".log")
    log_handle = logs.open("w")
    process = subprocess.Popen(
        [
            str(binary),
            "--listen",
            "127.0.0.1:" + str(port),
            "--grpc-listen",
            "127.0.0.1:" + str(port + 1),
            "--iam-config",
            str(iam_path),
            *(("--identity-config", str(identity_path)) if identity_path else ()),
            *(("--pubsub-java", pubsub_java) if pubsub_java else ()),
            *(("--pubsub-jar", pubsub_jar) if pubsub_jar else ()),
            "--services",
            services,
            *(("--run-docker-socket", "/var/run/docker.sock")
              if "--run-process" not in extra_flags else ()),
            *extra_flags,
        ],
        # File-backed logs: a chatty child (the Java emulator) must never
        # fill an unread pipe and block the server or its children.
        stdout=subprocess.DEVNULL,
        stderr=log_handle,
        text=True,
    )
    # The handle stays open for the child; the parent does not need it.
    log_handle.close()
    end = time.time() + 20
    while time.time() < end:
        try:
            # The health URL is a fixed loopback address built above.
            with urllib.request.urlopen(
                "http://127.0.0.1:" + str(port) + "/_local/health", timeout=2
            ) as response:
                if json.loads(response.read()).get("ready"):
                    return process
        except (urllib.error.URLError, TimeoutError, OSError):
            time.sleep(0.3)
    process.terminate()
    raise AssertionError("The core did not become ready; see " + str(logs))


def invoke(host: str, headers: dict[str, str] | None = None, timeout: int = 15) -> tuple[int, str]:
    """Issue one local invocation against a deployed service."""
    head = urllib.request.Request(BASE + "/_run/" + host + "/", headers=headers or {})  # noqa: S310 -- fixed loopback base
    try:
        with urllib.request.urlopen(head, timeout=timeout) as response:  # noqa: S310 -- fixed loopback base
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def create_and_grant() -> None:
    """Deploy the acceptance service and grant allUsers the invoker role."""
    status, operation = request(
        "POST",
        SCOPE + "/services",
        {
            "name": SCOPE + "/services/acceptance",
            "template": {"containers": [{"image": IMAGE, "ports": [{"containerPort": 8080}]}]},
            "ingress": "INGRESS_TRAFFIC_ALL",
        },
    )
    assert status == 200, operation
    result = await_operation(operation["name"])
    assert "error" not in result, result

    code, _ = invoke(HOST)
    assert code == 401, "anonymous invocation before the grant returned " + str(code)

    _, policy = request("GET", SCOPE + "/services/acceptance:getIamPolicy")
    policy.setdefault("bindings", []).append({"role": "roles/run.invoker", "members": ["allUsers"]})
    status, _ = request("POST", SCOPE + "/services/acceptance:setIamPolicy", {"policy": policy})
    assert status == 200
    code, body = invoke(HOST)
    assert code == 200, (code, body)
    assert "served by acceptance" in body, body


def update_and_delete(before: str, *, dockerless: bool) -> None:
    """Update the service to a new revision, then delete it and require cleanup."""
    status, operation = request(
        "PATCH",
        SCOPE + "/services/acceptance",
        {"template": {"containers": [{"image": IMAGE, "ports": [{"containerPort": 8080}]}]}},
    )
    assert status == 200, operation
    if "name" in operation and "template" not in operation:
        await_operation(operation["name"])
    end = time.time() + 30
    while True:
        code, body = invoke(HOST)
        if code == 200 and "served by acceptance" in body:
            break
        assert time.time() < end, (code, body)
        time.sleep(0.5)

    status, _ = request("DELETE", SCOPE + "/services/acceptance")
    assert status == 200
    if not dockerless:
        after = subprocess.check_output([DOCKER, "ps", "-aq"], text=True)
        assert after == before, "Cloud Run delete left a container behind"


def containers_before(*, dockerless: bool) -> str:
    """Snapshot the container list for the leak check; empty when dockerless."""
    if dockerless:
        return ""
    return subprocess.check_output([DOCKER, "ps", "-aq"], text=True)


def main() -> int:
    """Build the core and the acceptance image, then run the sequence."""
    subprocess.run(
        [sys.executable, "scripts/install/go.py"], check=True, capture_output=True, text=True
    )
    binary = REPO / "bin" / "gcp-local"
    subprocess.run(
        ["/bin/sh", str(REPO / "scripts/go.sh"), "build", "-o", str(binary), "./cmd/gcp-local"],
        check=True,
        capture_output=True,
    )
    dockerless = shutil.which(DOCKER) is None
    image, process_command = build_image(IMAGE, dockerless=dockerless)
    extra = ("--run-process", image + "=" + process_command) if dockerless else ()
    before = containers_before(dockerless=dockerless)
    iam_path = write_configs()
    core = start_core(binary, iam_path, extra_flags=extra)
    try:
        create_and_grant()
        update_and_delete(before, dockerless=dockerless)
    finally:
        core.terminate()
        core.wait(timeout=10)
    return 0


if __name__ == "__main__":
    sys.exit(main())
