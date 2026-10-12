#!/usr/bin/env python3
"""Exercise packaged startup, standard local ADC, isolated reset, and teardown."""

import base64
import hashlib
import hmac
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.error
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, build_opener
from urllib.request import Request as HTTPRequest

import google.auth
import grpc
import requests
from google.api_core.exceptions import NotFound
from google.auth.transport.requests import AuthorizedSession, Request
from google.cloud import storage

DOCKER = shutil.which("docker") or "docker"

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from smoke_support import exchange, free_ports  # noqa: E402

from gcp_local.bootstrap import SERVICES  # noqa: E402

# Each environment publishes HTTP, gRPC, 16 data-plane ports, and gRPC TLS from its base port.
PORT_BLOCK = 19

# One read per packaged provider (Cloud Run, Connectivity Tests, and project
# IAM included). The developer's explicit grants must allow each; an enrolled
# identity without grants must be refused, so IAM is still enforced.
SERVICE_PROBES = [
    ("storage", "GET", "/storage/v1/b?project={project}"),
    ("pubsub", "GET", "/v1/projects/{project}/topics"),
    ("run", "GET", "/v2/projects/{project}/locations/us-central1/services"),
    ("firestore", "GET", "/v1/projects/{project}/databases/(default)/documents/probe"),
    ("firestore", "POST", "/v1/projects/{project}/databases/(default)/documents:listCollectionIds"),
    ("spanner", "GET", "/v1/projects/{project}/instances"),
    ("bigtable", "GET", "/v2/projects/{project}/instances"),
    ("bigquery", "GET", "/bigquery/v2/projects/{project}/datasets"),
    ("cloudkms", "GET", "/v1/projects/{project}/locations/us-central1/keyRings"),
    ("secretmanager", "GET", "/v1/projects/{project}/secrets"),
    ("cloudtasks", "GET", "/v2beta3/projects/{project}/locations/us-central1/queues"),
    ("cloudscheduler", "GET", "/v1/projects/{project}/locations/us-central1/jobs"),
    ("artifactregistry", "GET", "/v1/projects/{project}/locations/us-central1/repositories"),
    ("iam", "GET", "/v1/projects/{project}/serviceAccounts"),
    ("resourcemanager", "POST", "/v1/projects/{project}:getIamPolicy"),
    ("redis", "GET", "/v1/projects/{project}/locations/us-central1/instances"),
    ("cloudsql", "GET", "/sql/v1beta4/projects/{project}/instances"),
    ("logging", "GET", "/v2/projects/{project}/logs"),
    (
        "vertexai",
        "POST",
        "/v1/projects/{project}/locations/us-central1/publishers/google/models/gemini-2.5-flash:countTokens",
    ),
    ("compute", "GET", "/compute/v1/projects/{project}/global/networks"),
    ("compute", "GET", "/v1/projects/{project}/locations/global/connectivityTests"),
]
UNGRANTED = "ungranted"
# Request bodies for probes whose POST needs more than an empty object.
PROBE_BODIES = {"vertexai": b'{"contents":[{"role":"user","parts":[{"text":"probe"}]}]}'}


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


def verify_isolation(configuration: dict[str, Any], probe_image: str) -> None:
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
    tls = f"{configuration['grpc_tls_port']}/tcp"
    assert entrypoint["NetworkSettings"]["Ports"][tls][0]["HostIp"] == "127.0.0.1"


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


def local_call(
    endpoint: str,
    method: str,
    path: str,
    body: bytes | None = None,
    *,
    headers: dict[str, str] | None = None,
    timeout: int = 90,
) -> tuple[int, bytes]:
    """Issue one loopback request without proxies; return the status and raw reply."""
    if body is None and method == "POST":
        body = b"{}"
    # The endpoint is the environment's own numeric loopback base from env.
    request = HTTPRequest(endpoint + path, data=body, method=method, headers=headers or {})  # noqa: S310
    try:
        with build_opener(ProxyHandler({})).open(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def bearer(token: str) -> dict[str, str]:
    """Return the authorization header for one access token."""
    return {"Authorization": "Bearer " + token}


def json_headers(token: str) -> dict[str, str]:
    """Return the headers of an authenticated JSON request."""
    return {**bearer(token), "Content-Type": "application/json"}


def environment_tokens(directory: Path) -> tuple[dict[str, str], dict[str, str]]:
    """Return the environment settings and an access token per enrolled account name."""
    env = json.loads(cli(directory, "env").stdout)
    principals = json.loads((directory / "identity/server.json").read_text())["principals"]
    tokens = {
        principal["subject"].split(":", 1)[1].split("@", 1)[0]: exchange(
            env["GCP_LOCAL_ENDPOINT"], principal["subject_token"]
        )
        for principal in principals
    }
    return env, tokens


def verify_services(directory: Path) -> None:
    """Require every packaged service to allow the developer and refuse an ungranted identity."""
    env, tokens = environment_tokens(directory)
    endpoint, project = env["GCP_LOCAL_ENDPOINT"], env["GOOGLE_CLOUD_PROJECT"]
    assert set(tokens) == {"developer", UNGRANTED}
    covered = set()
    for service, method, template in SERVICE_PROBES:
        path = template.format(project=project)
        request = PROBE_BODIES.get(service)
        allowed, body = local_call(
            endpoint, method, path, request, headers=bearer(tokens["developer"])
        )
        assert allowed == 200, f"{service} refused the developer: {allowed} {body[:200]!r}"
        denied, body = local_call(
            endpoint, method, path, request, headers=bearer(tokens[UNGRANTED])
        )
        assert denied == 403, f"{service} did not refuse an ungranted caller: {denied}"
        anonymous, _ = local_call(endpoint, method, path, request)
        assert anonymous in {401, 403}, f"{service} accepted an anonymous caller: {anonymous}"
        covered.add(service)
    assert covered == set(SERVICES), sorted(set(SERVICES) - covered)


class WireError(Exception):
    """A protocol-level refusal from Redis or PostgreSQL."""


def resp_call(sock: socket.socket, *args: str) -> str:
    """Send one RESP command and return a simple, bulk, or integer reply."""
    sock.sendall(
        b"*%d\r\n" % len(args)
        + b"".join(b"$%d\r\n%s\r\n" % (len(arg.encode()), arg.encode()) for arg in args)
    )
    reader = sock.makefile("rb")
    line = reader.readline().rstrip(b"\r\n")
    if not line:
        raise ConnectionError("Redis closed the connection")
    if line[:1] == b"-":
        raise WireError(line[1:].decode())
    if line[:1] == b"$":
        size = int(line[1:])
        return "" if size < 0 else reader.read(size + 2)[:-2].decode()
    return line[1:].decode()


def pg_message(kind: bytes, body: bytes) -> bytes:
    """Frame one typed PostgreSQL frontend message."""
    return kind + struct.pack("!I", len(body) + 4) + body


def pg_read(sock: socket.socket) -> tuple[bytes, bytes]:
    """Read one typed backend message."""
    header = b""
    while len(header) < 5:
        chunk = sock.recv(5 - len(header))
        if not chunk:
            raise ConnectionError("PostgreSQL closed the connection")
        header += chunk
    size = struct.unpack("!I", header[1:])[0] - 4
    body = b""
    while len(body) < size:
        chunk = sock.recv(size - len(body))
        if not chunk:
            raise ConnectionError("PostgreSQL closed the connection")
        body += chunk
    if header[:1] == b"E":
        fields = {item[:1]: item[1:].decode() for item in body.split(b"\0") if item}
        raise WireError(fields.get(b"C", "") + " " + fields.get(b"M", ""))
    return header[:1], body


def scram_reply(password: str, first_bare: str, server_first: str) -> tuple[str, bytes]:
    """Compute the SCRAM-SHA-256 client-final message and the expected server signature."""
    attributes = dict(item.split("=", 1) for item in server_first.split(","))
    salted = hashlib.pbkdf2_hmac(
        "sha256", password.encode(), base64.b64decode(attributes["s"]), int(attributes["i"])
    )
    client_key = hmac.digest(salted, b"Client Key", "sha256")
    without_proof = "c=biws,r=" + attributes["r"]
    message = f"{first_bare},{server_first},{without_proof}".encode()
    signature = hmac.digest(hashlib.sha256(client_key).digest(), message, "sha256")
    proof = bytes(a ^ b for a, b in zip(client_key, signature, strict=True))
    server_key = hmac.digest(salted, b"Server Key", "sha256")
    final = without_proof + ",p=" + base64.b64encode(proof).decode()
    return final, hmac.digest(server_key, message, "sha256")


def pg_connect(port: int, user: str, password: str) -> socket.socket:
    """Open a PostgreSQL session with SCRAM-SHA-256, declining TLS as the server does."""
    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    params = f"user\0{user}\0database\0postgres\0\0".encode()
    sock.sendall(struct.pack("!II", len(params) + 8, 196608) + params)
    first_bare = "n=,r=" + base64.b64encode(os.urandom(18)).decode()
    expected = b""
    while True:
        kind, body = pg_read(sock)
        code = struct.unpack("!I", body[:4])[0] if kind == b"R" else None
        if code == 10:  # AuthenticationSASL
            initial = ("n,," + first_bare).encode()
            sock.sendall(
                pg_message(b"p", b"SCRAM-SHA-256\0" + struct.pack("!I", len(initial)) + initial)
            )
        elif code == 11:  # AuthenticationSASLContinue
            final, expected = scram_reply(password, first_bare, body[4:].decode())
            sock.sendall(pg_message(b"p", final.encode()))
        elif code == 12:  # AuthenticationSASLFinal
            assert body[4:].decode() == "v=" + base64.b64encode(expected).decode()
        elif kind == b"Z":
            return sock


def pg_query(sock: socket.socket, sql: str) -> list[list[str]]:
    """Run one simple query and return its text rows."""
    sock.sendall(pg_message(b"Q", sql.encode() + b"\0"))
    rows = []
    while True:
        kind, body = pg_read(sock)
        if kind == b"D":
            row, offset = [], 2
            for _ in range(struct.unpack("!H", body[:2])[0]):
                size = struct.unpack("!i", body[offset : offset + 4])[0]
                row.append(body[offset + 4 : offset + 4 + size].decode())
                offset += 4 + size
            rows.append(row)
        elif kind == b"Z":
            return rows


def expect_closed(port: int, probe: bytes) -> None:
    """Require a published port to give no protocol answer.

    HAProxy may accept the TCP connection, but it closes it when nothing in
    the core listens on that port.
    """
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
            sock.sendall(probe)
            assert sock.recv(1) == b"", f"port {port} still answers after deletion"
    except (ConnectionError, TimeoutError):
        pass


def developer_token(directory: Path) -> tuple[str, str, str]:
    """Return the endpoint, project, and a developer access token for one environment."""
    env, tokens = environment_tokens(directory)
    return env["GCP_LOCAL_ENDPOINT"], env["GOOGLE_CLOUD_PROJECT"], tokens["developer"]


def api(endpoint: str, token: str, method: str, path: str, body: object = None) -> dict:
    """Call the local API as the developer and require success."""
    data = None if body is None else json.dumps(body).encode()
    status, reply = local_call(
        endpoint, method, path, data, headers=json_headers(token), timeout=180
    )
    if not 200 <= status < 300:
        raise AssertionError(f"{method} {path}: {status} {reply[:500]!r}")
    return json.loads(reply)


def verify_redis(directory: Path, *, auth: bool = True) -> None:
    """Create a Memorystore instance, use it from the host, and require delete to close it."""
    endpoint, project, token = developer_token(directory)
    base = f"/v1/projects/{project}/locations/us-central1/instances"
    first, last = json.loads((directory / "environment.json").read_text())["data_ports"]
    created = api(
        endpoint, token, "POST", base + "?instanceId=smoke-redis",
        {"tier": "BASIC", "memorySizeGb": 1, "authEnabled": auth},
    )["response"]  # fmt: skip
    assert created["host"] == "127.0.0.1", created
    assert first <= created["port"] <= last, created
    port = created["port"]
    with socket.create_connection(("127.0.0.1", port), timeout=10) as session:
        if auth:
            secret = api(endpoint, token, "GET", base + "/smoke-redis/authString")["authString"]
            for args, refusal in (
                (("PING",), "NOAUTH"),
                (("AUTH", "not-the-auth-string"), "WRONGPASS"),
            ):
                try:
                    resp_call(session, *args)
                    raise AssertionError(f"Redis accepted {args[0]} without AUTH")
                except WireError as error:
                    assert str(error).startswith(refusal), error
            assert resp_call(session, "AUTH", secret) == "OK"
        assert resp_call(session, "SET", "greeting", "from the host") == "OK"
        assert resp_call(session, "GET", "greeting") == "from the host"
        api(endpoint, token, "DELETE", base + "/smoke-redis")
        try:
            resp_call(session, "PING")
            raise AssertionError("A Redis session survived instance deletion")
        except (ConnectionError, OSError):
            pass
    expect_closed(port, b"*1\r\n$4\r\nPING\r\n")


def verify_cloudsql(directory: Path) -> None:
    """Create a Cloud SQL instance and user, connect with a password, and delete it."""
    endpoint, project, token = developer_token(directory)
    base = f"/sql/v1beta4/projects/{project}/instances"
    api(endpoint, token, "POST", base, {
        "name": "smoke-pg", "region": "us-central1", "databaseVersion": "POSTGRES_16",
        "rootPassword": "smoke-root", "settings": {"tier": "db-f1-micro"},
    })  # fmt: skip
    api(endpoint, token, "POST", base + "/smoke-pg/users", {"name": "app", "password": "app-pw"})
    instance = api(endpoint, token, "GET", base + "/smoke-pg")
    primary = next(item for item in instance["ipAddresses"] if item["type"] == "PRIMARY")
    assert primary["ipAddress"] == "127.0.0.1", primary
    first, last = json.loads((directory / "environment.json").read_text())["data_ports"]
    port = primary["port"]
    assert first <= port <= last, primary
    try:
        pg_connect(port, "app", "wrong").close()
        raise AssertionError("PostgreSQL accepted a wrong password")
    except WireError as error:
        assert str(error).startswith("28P01"), error
    session = pg_connect(port, "app", "app-pw")
    try:
        assert pg_query(session, "SELECT current_user, 40 + 2") == [["app", "42"]]
        api(endpoint, token, "DELETE", base + "/smoke-pg")
        try:
            pg_query(session, "SELECT 1")
            raise AssertionError("A PostgreSQL session survived instance deletion")
        except (ConnectionError, OSError, WireError):
            pass
    finally:
        session.close()
    expect_closed(port, struct.pack("!II", 8, 80877103))


def json_call(endpoint: str, method: str, path: str, token: str, body: object) -> tuple[int, Any]:
    """Issue one authenticated loopback JSON request and decode the reply."""
    data = json.dumps(body).encode()
    status, reply = local_call(endpoint, method, path, data, headers=json_headers(token))
    return status, json.loads(reply or b"{}")


def varint(value: int) -> bytes:
    """Encode one non-negative protobuf varint."""
    out = bytearray()
    while True:
        byte, value = value & 0x7F, value >> 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)


def field(number: int, value: bytes | int) -> bytes:
    """Encode one length-delimited or varint protobuf field."""
    if isinstance(value, int):
        return varint(number << 3) + varint(value)
    return varint(number << 3 | 2) + varint(len(value)) + value


def spanner_rows(env: dict[str, str], developer: str, ungranted: str) -> str:
    """Create a Spanner database, write and query one row over REST; return the session."""
    endpoint, project = env["GCP_LOCAL_ENDPOINT"], env["GOOGLE_CLOUD_PROJECT"]
    instances = f"/v1/projects/{project}/instances"
    config = f"projects/{project}/instanceConfigs/regional-us-central1"
    instance = {"config": config, "displayName": "smoke", "processingUnits": 100}
    created = {"instanceId": "smoke", "instance": instance}
    assert json_call(endpoint, "POST", instances, developer, created)[0] == 200
    table = "CREATE TABLE Items (Id STRING(36) NOT NULL) PRIMARY KEY (Id)"
    database = {"createStatement": "CREATE DATABASE `smoke_db`", "extraStatements": [table]}
    databases = instances + "/smoke/databases"
    assert json_call(endpoint, "POST", databases, developer, database)[0] == 200
    sessions = instances + "/smoke/databases/smoke_db/sessions"
    status, session = json_call(endpoint, "POST", sessions, developer, {})
    assert status == 200, session
    path = sessions + "/" + session["name"].rsplit("/", 1)[1]
    insert = {"insert": {"table": "Items", "columns": ["Id"], "values": [["a"]]}}
    commit = {"singleUseTransaction": {"readWrite": {}}, "mutations": [insert]}
    assert json_call(endpoint, "POST", path + ":commit", developer, commit)[0] == 200
    query = {"sql": "SELECT Id FROM Items"}
    status, result = json_call(endpoint, "POST", path + ":executeSql", developer, query)
    assert status == 200, result
    assert result.get("rows") == [["a"]], result
    denied, _ = json_call(endpoint, "POST", sessions, ungranted, {})
    assert denied == 403, f"Spanner sessions accepted an ungranted caller: {denied}"
    return session["name"]


def bigtable_table(env: dict[str, str], developer: str) -> str:
    """Create a Bigtable instance and table over REST; return the table name."""
    endpoint, project = env["GCP_LOCAL_ENDPOINT"], env["GOOGLE_CLOUD_PROJECT"]
    instances = f"/v2/projects/{project}/instances"
    cluster = {
        "location": f"projects/{project}/locations/us-central1-b",
        "serveNodes": 1,
        "defaultStorageType": "SSD",
    }
    created = {
        "instanceId": "smoke-bt",
        "instance": {"displayName": "smoke", "type": "PRODUCTION"},
        "clusters": {"smoke-btc": cluster},
    }
    assert json_call(endpoint, "POST", instances, developer, created)[0] == 200
    table = {"tableId": "t", "table": {"columnFamilies": {"cf": {}}}}
    assert json_call(endpoint, "POST", instances + "/smoke-bt/tables", developer, table)[0] == 200
    return f"projects/{project}/instances/smoke-bt/tables/t"


def grpc_status(
    channel: grpc.Channel, method: str, request: bytes, token: str, *, stream: bool
) -> grpc.StatusCode:
    """Call one raw gRPC method as a token; a stream must yield at least one reply."""
    metadata = (("authorization", "Bearer " + token),)
    try:
        if stream:
            replies = list(channel.unary_stream(method)(request, metadata=metadata, timeout=30))
            if not replies:
                return grpc.StatusCode.UNKNOWN
        else:
            channel.unary_unary(method)(request, metadata=metadata, timeout=30)
    except grpc.RpcError as error:
        return error.code()
    return grpc.StatusCode.OK


def verify_dataplane(directory: Path) -> None:
    """Call each gRPC data plane and transcoded service through the gateway, IAM enforced."""
    env, tokens = environment_tokens(directory)
    developer, ungranted = tokens["developer"], tokens[UNGRANTED]
    session = spanner_rows(env, developer, ungranted)
    table = bigtable_table(env, developer).encode()
    project = env["GOOGLE_CLOUD_PROJECT"].encode()
    set_cell = field(1, b"cf") + field(2, b"c") + field(3, 1000) + field(4, b"v")
    checks = [
        (
            "/google.bigtable.v2.Bigtable/MutateRow",
            field(1, table) + field(2, b"row") + field(3, field(1, set_cell)),
            False,
        ),
        ("/google.bigtable.v2.Bigtable/ReadRows", field(1, table), True),
        (
            "/google.spanner.v1.Spanner/ExecuteStreamingSql",
            field(1, session.encode()) + field(3, b"SELECT Id FROM Items"),
            True,
        ),
        (
            "/google.cloud.secretmanager.v1.SecretManagerService/ListSecrets",
            field(1, b"projects/" + project),
            False,
        ),
        (
            "/google.cloud.tasks.v2.CloudTasks/ListQueues",
            field(1, b"projects/" + project + b"/locations/us-central1"),
            False,
        ),
        (
            "/google.cloud.kms.v1.KeyManagementService/ListKeyRings",
            field(1, b"projects/" + project + b"/locations/us-central1"),
            False,
        ),
    ]
    with grpc.insecure_channel(env["GCP_LOCAL_GRPC_ENDPOINT"]) as channel:
        for method, request, stream in checks:
            allowed = grpc_status(channel, method, request, developer, stream=stream)
            assert allowed == grpc.StatusCode.OK, (method, allowed)
            denied = grpc_status(channel, method, request, ungranted, stream=stream)
            assert denied == grpc.StatusCode.PERMISSION_DENIED, (method, denied)
    verify_grpc_tls(env, developer, ungranted)


def verify_grpc_tls(env: dict[str, str], developer: str, ungranted: str | None) -> None:
    """Call gRPC over TLS through HAProxy's TCP pass-through, trusting this environment's CA."""
    method = "/google.cloud.secretmanager.v1.SecretManagerService/ListSecrets"
    request = field(1, b"projects/" + env["GOOGLE_CLOUD_PROJECT"].encode())
    trusted = grpc.ssl_channel_credentials(Path(env["GCP_LOCAL_GRPC_TLS_CA"]).read_bytes())
    with grpc.secure_channel(env["GCP_LOCAL_GRPC_TLS_ENDPOINT"], trusted) as channel:
        assert grpc_status(channel, method, request, developer, stream=False) == grpc.StatusCode.OK
        if ungranted is not None:
            denied = grpc_status(channel, method, request, ungranted, stream=False)
            assert denied == grpc.StatusCode.PERMISSION_DENIED, denied
    # Without the CA (gRPC's public roots only), the handshake fails before any handler runs.
    with grpc.secure_channel(
        env["GCP_LOCAL_GRPC_TLS_ENDPOINT"], grpc.ssl_channel_credentials()
    ) as channel:
        untrusted = grpc_status(channel, method, request, developer, stream=False)
        assert untrusted == grpc.StatusCode.UNAVAILABLE, untrusted


def verify_standalone(image: str, temporary: Path) -> None:
    """Run the image with docker run alone and require the SDK examples to pass."""
    state = temporary / "standalone"
    state.mkdir(mode=0o700)
    port = free_ports(PORT_BLOCK)
    name = "gcp-local-standalone-" + str(port)
    subprocess.run(
        [
            DOCKER, "run", "--detach", "--rm", "--pull=never", "--name", name,
            "--user", f"{os.getuid()}:{os.getgid()}",
            "--read-only", "--tmpfs", "/tmp:size=256m,mode=1777",  # noqa: S108 -- in-container mount point
            "--cap-drop=ALL", "--security-opt", "no-new-privileges:true",
            "--publish", f"127.0.0.1:{port}:{port}",
            "--publish", f"127.0.0.1:{port + 1}:4567",
            "--publish", f"127.0.0.1:{port + 18}:{port + 18}",
            # The default data-plane range follows the gRPC port.
            "--publish", f"127.0.0.1:{port + 2}-{port + 17}:{port + 2}-{port + 17}",
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
        verify_redis(state, auth=False)
        env, tokens = environment_tokens(state)
        verify_grpc_tls(env, tokens["developer"], None)
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
            owned = "label=com.docker.compose.project=" + project
            for kind, listing in (("container", "ps -aq"), ("network", "network ls -q")):
                remaining = subprocess.check_output(
                    [DOCKER, *listing.split(), "--filter", owned], text=True
                )
                assert remaining.strip() == "", "Packaged teardown left an owned " + kind
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
                cli(directory, "up", "--port", str(free_ports(PORT_BLOCK)), "--workload", UNGRANTED)
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
            verify_redis(first)
            verify_cloudsql(first)
            verify_redis(second, auth=False)
            verify_dataplane(first)
            # Each environment has its own CA.
            authorities = {(path / "tls/ca.pem").read_bytes() for path in (first, second)}
            assert len(authorities) == 2, "Two environments share a gRPC TLS CA"
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
        "Packaged workflow passed: local ADC/Storage and Pub/Sub, Spanner and Bigtable "
        "rows, every packaged service with IAM enforced, gRPC over TLS with a per-environment CA, "
        "Redis and Cloud SQL from the host, "
        "isolated reset, failed startup, "
        "portable doctor, standalone docker run, and teardown."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
