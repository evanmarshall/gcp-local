#!/usr/bin/env python3
"""Exercise packaged startup, standard local ADC, isolated reset, and teardown."""

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, build_opener
from urllib.request import Request as HTTPRequest

import google.auth
import requests
from google.api_core.exceptions import NotFound
from google.auth.transport.requests import AuthorizedSession, Request
from google.cloud import storage

DOCKER = shutil.which("docker") or "docker"

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from gcp_local.bootstrap import SERVICES  # noqa: E402

# One read per packaged provider (Cloud Run, Connectivity Tests, and project
# IAM included). The developer's explicit grants must allow each; an enrolled
# identity without grants must be refused, so IAM is still enforced.
SERVICE_PROBES = [
    ("storage", "GET", "/storage/v1/b?project={project}"),
    ("pubsub", "GET", "/v1/projects/{project}/topics"),
    ("run", "GET", "/v2/projects/{project}/locations/us-central1/services"),
    ("firestore", "GET", "/v1/projects/{project}/databases/(default)/documents/probe"),
    ("spanner", "GET", "/v1/projects/{project}/instances"),
    ("bigtable", "GET", "/v2/projects/{project}/instances"),
    ("bigquery", "GET", "/bigquery/v2/projects/{project}/datasets"),
    ("cloudkms", "GET", "/v1/projects/{project}/locations/us-central1/keyRings"),
    ("secretmanager", "GET", "/v1/projects/{project}/secrets"),
    ("cloudtasks", "GET", "/v2beta3/projects/{project}/locations/us-central1/queues"),
    ("artifactregistry", "GET", "/v1/projects/{project}/locations/us-central1/repositories"),
    ("iam", "GET", "/v1/projects/{project}/serviceAccounts"),
    ("resourcemanager", "POST", "/v1/projects/{project}:getIamPolicy"),
    ("redis", "GET", "/v1/projects/{project}/locations/us-central1/instances"),
    ("compute", "GET", "/compute/v1/projects/{project}/global/networks"),
    ("compute", "GET", "/v1/projects/{project}/locations/global/connectivityTests"),
]
UNGRANTED = "ungranted"


def cli(
    directory: Path, action: str, *options: str, check: bool = True
) -> subprocess.CompletedProcess[str]:
    """Invoke the environment CLI with the selected directory."""
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/local_env.py"),
            "--directory",
            str(directory),
            *options,
            action,
        ],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    if check and result.returncode:
        raise AssertionError(result.stdout + result.stderr)
    return result


def unused_port() -> int:
    """Reserve an ephemeral loopback port and release it."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def client_for(directory: Path) -> tuple[storage.Client, requests.Session, requests.Session]:
    """Build a Storage client and sessions pinned to this environment only."""
    env = json.loads(cli(directory, "env").stdout)
    endpoint = env["GCP_LOCAL_ENDPOINT"]

    def scoped(url: str) -> None:
        parsed = urlsplit(url)
        expected = urlsplit(endpoint)
        if (parsed.scheme, parsed.netloc) != (expected.scheme, expected.netloc):
            raise ValueError("The SDK attempted to leave the selected local environment")

    class LocalSession(requests.Session):
        def __init__(self) -> None:
            super().__init__()
            self.trust_env = False

        def request(self, method: str, url: str, **kwargs: object) -> requests.Response:
            scoped(url)
            kwargs.update(allow_redirects=False, timeout=5)
            return super().request(method, url, **kwargs)

    transport_session = LocalSession()
    transport = Request(session=transport_session)
    credentials, _ = google.auth.load_credentials_from_file(
        env["GOOGLE_APPLICATION_CREDENTIALS"],
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
        request=transport,
    )

    class API(AuthorizedSession):
        def request(self, method: str, url: str, **kwargs: object) -> requests.Response:
            scoped(url)
            kwargs.update(allow_redirects=False, timeout=5)
            return super().request(method, url, **kwargs)

    session = API(credentials, auth_request=transport)
    session.trust_env = False
    client = storage.Client(
        project=env["GOOGLE_CLOUD_PROJECT"],
        credentials=credentials,
        _http=session,
        client_options={"api_endpoint": endpoint},
        use_auth_w_custom_endpoint=True,
    )
    return client, session, transport_session


def create(client: storage.Client) -> storage.Bucket:
    """Create the smoke-test bucket with public access prevention."""
    bucket = client.bucket("packaged-smoke-bucket")
    bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    bucket.iam_configuration.public_access_prevention = "enforced"
    bucket.soft_delete_policy.retention_duration_seconds = 0
    return client.create_bucket(bucket, location="us-central1", retry=None, timeout=5)


def verify_isolation(configuration: dict[str, object], probe_image: str) -> None:
    """Require a loopback-only network, read-only root filesystem, and unprivileged user."""
    network = subprocess.check_output(
        [DOCKER, "network", "inspect", configuration["compose_project"] + "_backend"], text=True
    )
    assert json.loads(network)[0]["Internal"] is True
    probe = """import errno,socket
for family,address in ((socket.AF_INET,('192.0.2.1',443)),(socket.AF_INET6,('2001:db8::1',443))):
 with socket.socket(family,socket.SOCK_STREAM) as connection:
  connection.settimeout(2)
  assert connection.connect_ex(address) in {errno.ENETUNREACH,errno.EHOSTUNREACH}
"""
    subprocess.run(
        [
            DOCKER,
            "run",
            "--rm",
            "--pull=never",
            "--cap-drop=ALL",
            "--network",
            configuration["compose_project"] + "_backend",
            probe_image,
            "python3",
            "-c",
            probe,
        ],
        check=True,
        timeout=10,
    )
    container = configuration["compose_project"] + "-gcp-local-1"
    state = json.loads(subprocess.check_output([DOCKER, "inspect", container], text=True))[0]
    assert state["HostConfig"]["ReadonlyRootfs"]
    assert state["Config"]["User"] == f"{os.getuid()}:{os.getgid()}"
    assert state["HostConfig"]["Tmpfs"].get("/tmp")  # noqa: S108 -- in-container mount point
    assert len(state["NetworkSettings"]["Networks"]) == 1
    entrypoint = json.loads(
        subprocess.check_output(
            [DOCKER, "inspect", configuration["compose_project"] + "-entrypoint-1"], text=True
        )
    )[0]
    assert entrypoint["NetworkSettings"]["Ports"]["4566/tcp"][0]["HostIp"] == "127.0.0.1"
    assert entrypoint["NetworkSettings"]["Ports"]["4567/tcp"][0]["HostIp"] == "127.0.0.1"


def verify_examples(directory: Path) -> None:
    """Run both packaged example programs against one live environment."""
    example = subprocess.run(
        [sys.executable, str(ROOT / "examples/storage.py"), "--directory", str(directory)],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert example.returncode == 0, example.stderr
    assert "Hello from local GCP." in example.stdout
    second_example = subprocess.run(
        [sys.executable, str(ROOT / "examples/pubsub.py"), "--directory", str(directory)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert second_example.returncode == 0, second_example.stderr
    assert "Hello from local Pub/Sub." in second_example.stdout


def verify_doctor(directory: Path, temporary: Path) -> None:
    """Exercise the portable doctor's supported and unsupported service decisions."""
    app = temporary / "application"
    app.mkdir()
    (app / "requirements.txt").write_text("google-cloud-storage==3.10.1\n")
    endpoint = json.loads(cli(directory, "env").stdout)["GCP_LOCAL_ENDPOINT"]
    doctor = [
        sys.executable,
        str(ROOT / "skills/gcp-local-setup/scripts/inspect_repo.py"),
        "--repo",
        str(app),
        "--doctor",
        "--endpoint",
        endpoint,
    ]
    assert subprocess.run(doctor, capture_output=True, timeout=10, check=False).returncode == 0
    (app / "requirements.txt").write_text("google-cloud-storage==3.10.1\ngoogle-cloud-pubsub\n")
    served = subprocess.run(doctor, capture_output=True, text=True, timeout=10, check=False)
    assert served.returncode == 0, served.stdout + served.stderr
    assert json.loads(served.stdout)["profiles"]["local"]["missing_services"] == []
    # Detected names that differ from provider names (cloud-run -> run) still resolve.
    (app / "requirements.txt").write_text(
        "google-cloud-bigquery\ngoogle-cloud-run\ngoogle-cloud-secret-manager\n"
    )
    result = subprocess.run(doctor, capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["profiles"]["local"]["missing_services"] == []


def local_call(endpoint: str, method: str, path: str, **headers: str) -> tuple[int, bytes]:
    """Issue one loopback request without proxies or redirects."""
    body = b"{}" if method == "POST" else None
    # The endpoint is the environment's own numeric loopback base from env.
    request = HTTPRequest(endpoint + path, data=body, method=method, headers=headers)  # noqa: S310
    try:
        with build_opener(ProxyHandler({})).open(request, timeout=90) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def exchange(endpoint: str, subject_token: str) -> str:
    """Exchange one enrolled subject token at the local STS for an access token."""
    form = urllib.parse.urlencode(
        {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "audience": "//gcp-local/identity",
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "subject_token": subject_token,
            "scope": "https://www.googleapis.com/auth/cloud-platform",
        }
    ).encode()
    request = HTTPRequest(  # noqa: S310 -- the environment's loopback STS endpoint
        endpoint + "/v1/token",
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with build_opener(ProxyHandler({})).open(request, timeout=10) as response:
        return json.loads(response.read())["access_token"]


def verify_services(directory: Path) -> None:
    """Require every packaged service to allow the developer and refuse an ungranted identity."""
    env = json.loads(cli(directory, "env").stdout)
    endpoint, project = env["GCP_LOCAL_ENDPOINT"], env["GOOGLE_CLOUD_PROJECT"]
    principals = json.loads((directory / "identity/server.json").read_text())["principals"]
    tokens = {
        principal["subject"].split(":", 1)[1].split("@", 1)[0]: exchange(
            endpoint, principal["subject_token"]
        )
        for principal in principals
    }
    assert set(tokens) == {"developer", UNGRANTED}
    covered = set()
    for service, method, template in SERVICE_PROBES:
        path = template.format(project=project)
        allowed, body = local_call(
            endpoint, method, path, Authorization="Bearer " + tokens["developer"]
        )
        assert allowed == 200, f"{service} refused the developer: {allowed} {body[:200]!r}"
        denied, body = local_call(
            endpoint, method, path, Authorization="Bearer " + tokens[UNGRANTED]
        )
        assert denied == 403, f"{service} did not refuse an ungranted caller: {denied}"
        anonymous, _ = local_call(endpoint, method, path)
        assert anonymous in {401, 403}, f"{service} accepted an anonymous caller: {anonymous}"
        covered.add(service)
    assert covered == set(SERVICES), sorted(set(SERVICES) - covered)


def verify_standalone(image: str, temporary: Path) -> None:
    """Run the image with docker run alone and require the SDK examples to pass."""
    state = temporary / "standalone"
    state.mkdir(mode=0o700)
    port = unused_port()
    name = "gcp-local-standalone-" + str(port)
    subprocess.run(
        [
            DOCKER, "run", "--detach", "--rm", "--pull=never", "--name", name,
            "--user", f"{os.getuid()}:{os.getgid()}",
            "--read-only", "--tmpfs", "/tmp:size=256m,mode=1777",  # noqa: S108 -- in-container mount point
            "--cap-drop=ALL", "--security-opt", "no-new-privileges:true",
            "--publish", f"127.0.0.1:{port}:{port}",
            "--publish", f"127.0.0.1:{port + 1}:4567",
            "--env", f"GCP_LOCAL_PORT={port}",
            "--env", f"GCP_LOCAL_HOST_DIRECTORY={state}",
            "--volume", f"{state}:/state",
            image,
        ],
        check=True, capture_output=True, text=True, timeout=60,
    )  # fmt: skip
    try:
        deadline = time.monotonic() + 60
        while True:
            health = subprocess.run(
                [DOCKER, "inspect", "--format", "{{.State.Health.Status}}", name],
                capture_output=True, text=True, check=True, timeout=10,
            ).stdout.strip()  # fmt: skip
            if health == "healthy":
                break
            assert time.monotonic() < deadline, "The standalone container never became healthy"
            time.sleep(1)
        status = json.loads(cli(state, "status").stdout)
        assert status["ready"]
        assert status["providers"] == SERVICES
        assert cli(state, "reset", check=False).returncode != 0, "reset managed a docker run"
        verify_examples(state)
    finally:
        subprocess.run([DOCKER, "rm", "--force", name], capture_output=True, check=False)


def teardown(directories: Sequence[Path]) -> None:
    """Stop every smoke environment and require no owned containers or networks remain."""
    failures = []
    for directory in directories:
        if cli(directory, "down", check=False).returncode:
            failures.append(str(directory))
        if (directory / "environment.json").exists():
            project = json.loads((directory / "environment.json").read_text())["compose_project"]
            remaining = subprocess.check_output(
                [
                    DOCKER,
                    "ps",
                    "-aq",
                    "--filter",
                    "label=com.docker.compose.project=" + project,
                ],
                text=True,
            )
            assert remaining.strip() == "", "Packaged teardown left an owned container"
            networks = subprocess.check_output(
                [
                    DOCKER,
                    "network",
                    "ls",
                    "-q",
                    "--filter",
                    "label=com.docker.compose.project=" + project,
                ],
                text=True,
            )
            assert networks.strip() == "", "Packaged teardown left an owned network"
    assert not failures, "Teardown failed for: " + ", ".join(failures)


def main() -> int:
    """Exercise the full packaged environment workflow."""
    probe_image = next(
        word
        for line in (ROOT / "Dockerfile").read_text().splitlines()
        if line.startswith("FROM ")
        for word in line.split()[1:]
        if not word.startswith("--")
    )
    subprocess.run(
        [DOCKER, "pull", probe_image], check=True, stdout=subprocess.DEVNULL, timeout=120
    )
    with tempfile.TemporaryDirectory(prefix="gcp-local-package-") as temporary:
        directories = [Path(temporary) / name for name in ("first", "second", "failed")]
        resources: list[Any] = []
        try:
            for directory in directories[:2]:
                cli(directory, "up", "--port", str(unused_port()), "--workload", UNGRANTED)
                status = json.loads(cli(directory, "status").stdout)
                assert status["ready"]
                assert status["providers"] == SERVICES
                assert status["backends"] == []
                # This is a private, synthetic file only. Source credentials are never copied.
                assert (directory / "identity/adc.json").stat().st_mode & 0o077 == 0
                client, session, transport = client_for(directory)
                resources.extend((client, session, transport))
                bucket = create(client)
                blob = bucket.blob("sample.txt")
                blob.upload_from_string(
                    b"packaged local storage", if_generation_match=0, retry=None, timeout=5
                )
                assert blob.download_as_bytes(retry=None, timeout=5) == b"packaged local storage"
                configuration = json.loads((directory / "environment.json").read_text())
                verify_isolation(configuration, probe_image)
            first, second = directories[:2]
            cli(first, "reset")
            fresh, session, transport = client_for(first)
            resources.extend((fresh, session, transport))
            assert list(fresh.list_buckets(retry=None, timeout=5)) == []
            try:
                fresh.get_bucket("packaged-smoke-bucket", retry=None, timeout=5)
                raise AssertionError("Reset retained the first environment bucket")
            except NotFound:
                pass
            other, session, transport = client_for(second)
            resources.extend((other, session, transport))
            assert (
                other.bucket("packaged-smoke-bucket")
                .blob("sample.txt")
                .download_as_bytes(retry=None, timeout=5)
                == b"packaged local storage"
            )
            create(fresh).delete(retry=None, timeout=5)
            verify_examples(first)
            verify_services(first)
            # Bind a port to require a visible startup failure
            # without affecting the working environment.
            with socket.socket() as occupied:
                occupied.bind(("127.0.0.1", 0))
                occupied.listen()
                failed = cli(
                    directories[2], "up", "--port", str(occupied.getsockname()[1]), check=False
                )
                assert failed.returncode != 0
            verify_doctor(first, Path(temporary))
            image = "gcp-local:" + json.loads((first / "environment.json").read_text())["version"]
            verify_standalone(image, Path(temporary).resolve())
        finally:
            for resource in resources:
                resource.close()
            teardown(directories)
    print(
        "Packaged workflow passed: local ADC/Storage and Pub/Sub, every packaged service "
        "with IAM enforced, isolated reset, failed startup, portable doctor, standalone "
        "docker run, and teardown."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
