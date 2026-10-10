"""CI-only acceptance for the local Cloud Run control plane on real Docker.

Deploys a real container image, invokes it under the invoker contract, updates
it, and deletes it, requiring container cleanup. This script runs on ordinary
Linux Docker in CI (issue #13 execution boundary); it is not part of the
offline validation suite.
"""

import json
import os
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
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


def await_operation(name: str, deadline: float = 60.0) -> dict[str, Any]:
    """Poll one long-running operation until it reports done."""
    end = time.time() + deadline
    while time.time() < end:
        _, body = request("GET", SCOPE + "/operations/" + name)
        if body.get("done"):
            return body
        time.sleep(0.5)
    raise AssertionError("Operation did not finish: " + name)


def build_image(tag: str) -> str:
    """Build the acceptance image: a tiny HTTP server echoing K_SERVICE."""
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
        'ENTRYPOINT ["python3", "/server.py"]\n'
    )
    build = subprocess.run(
        [DOCKER, "build", "-t", tag, str(context)], check=True, capture_output=True
    )
    assert build.returncode == 0
    return tag


def write_configs() -> Path:
    """Write an owner-only IAM configuration and return its directory."""
    directory = Path(tempfile.mkdtemp())
    directory.chmod(stat.S_IRWXU)
    project = "demo-run"
    resource = "//cloudresourcemanager.googleapis.com/projects/" + project
    iam = {
        "resources": [{"name": resource, "service": "resourcemanager", "kind": "project",
                       "number": "000000000001",
                       "scope": {"project": project, "location": "global"}}],
        # The smoke drives the control plane without a local identity mount,
        # so its fixture principal is allUsers; the invoker contract under test
        # is enforced per service, not here.
        "policies": {resource: {"bindings": [
            {"role": "roles/run.admin", "members": ["allUsers"]},
        ]}},
    }
    path = directory / "iam.json"
    path.write_text(json.dumps(iam))
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return path


def start_core(binary: Path, iam_path: Path) -> subprocess.Popen:
    """Start the core with the run provider and Docker runtime."""
    process = subprocess.Popen(
        [
            str(binary),
            "--listen",
            "127.0.0.1:" + str(SERVE_SOCKET),
            "--grpc-listen",
            "127.0.0.1:" + str(SERVE_SOCKET + 1),
            "--iam-config",
            str(iam_path),
            "--services",
            "run",
            "--run-docker-socket",
            "/var/run/docker.sock",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    end = time.time() + 20
    while time.time() < end:
        try:
            # The health URL is a fixed loopback address built above.
            with urllib.request.urlopen(BASE + "/_local/health", timeout=2) as response:  # noqa: S310 -- fixed loopback base
                if json.loads(response.read()).get("ready"):
                    return process
        except (urllib.error.URLError, TimeoutError, OSError):
            time.sleep(0.3)
    process.terminate()
    stderr = process.stderr.read() if process.stderr else ""
    raise AssertionError("The core did not become ready: " + stderr)


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
            "template": {"containers": [{"image": IMAGE, "containerPort": 8080}]},
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


def update_and_delete(before: str) -> None:
    """Update the service to a new revision, then delete it and require cleanup."""
    status, operation = request(
        "PATCH",
        SCOPE + "/services/acceptance",
        {"template": {"containers": [{"image": IMAGE, "containerPort": 8080}]}},
    )
    assert status == 200, operation
    if "name" in operation and "template" not in operation:
        await_operation(operation["name"])
    code, body = invoke(HOST)
    assert code == 200, (code, body)
    assert "served by acceptance" in body, body

    status, _ = request("DELETE", SCOPE + "/services/acceptance")
    assert status == 200
    after = subprocess.check_output([DOCKER, "ps", "-aq"], text=True)
    assert after == before, "Cloud Run delete left a container behind"


def main() -> int:
    """Build the core and the acceptance image, then run the sequence."""
    build = subprocess.run(
        [sys.executable, "scripts/install_go.py"], check=True, capture_output=True, text=True
    )
    assert build.returncode == 0, build.stdout + build.stderr
    binary = REPO / "bin" / "gcp-local"
    subprocess.run(
        ["/bin/sh", str(REPO / "scripts/go.sh"), "build", "-o", str(binary), "./cmd/gcp-local"],
        check=True,
        capture_output=True,
    )
    build_image(IMAGE)
    before = subprocess.check_output([DOCKER, "ps", "-aq"], text=True)
    iam_path = write_configs()
    core = start_core(binary, iam_path)
    try:
        create_and_grant()
        update_and_delete(before)
    finally:
        core.terminate()
        core.wait(timeout=10)
    return 0


if __name__ == "__main__":
    sys.exit(main())
