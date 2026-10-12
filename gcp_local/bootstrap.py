"""Create and validate private environment state: identity, IAM, and the manifest.

This module has no Docker dependency. The host CLI and the in-image launcher
both use it, so a Compose environment and a standalone ``docker run`` write the
same files.
"""

import hashlib
import ipaddress
import json
import os
import re
import secrets
import stat
from collections.abc import Sequence
from pathlib import Path
from urllib.parse import urlsplit

SCHEMA = 2
PROJECT_PATTERN = r"[a-z][a-z0-9-]{4,60}"
ACCOUNT_PATTERN = r"[a-z][a-z0-9-]{4,28}[a-z0-9]"
VERSION_PATTERN = r"source-[0-9a-f]{16}"
IMAGE_PATTERN = r"[a-z0-9][a-z0-9._/:@-]{0,254}"
MODES = ("compose", "standalone")

# Provider names as the health endpoint reports them, and the -services flag
# that selects them. The flag names Cloud KMS "kms"; health reports "cloudkms".
SERVICES = [
    "artifactregistry",
    "bigquery",
    "bigtable",
    "cloudkms",
    "cloudtasks",
    "compute",
    "firestore",
    "iam",
    "pubsub",
    "redis",
    "resourcemanager",
    "run",
    "secretmanager",
    "spanner",
    "storage",
]
SERVICE_FLAG = ",".join(sorted("kms" if name == "cloudkms" else name for name in SERVICES))

# Explicit project-level grants for the generated developer identity. Each is an
# ordinary IAM binding the engine evaluates per request; nothing bypasses IAM.
# The set mirrors the conformance fixture's grants, plus the KMS
# encrypter/decrypter (Cloud KMS Admin cannot use keys) and Service Account
# User (deploying a Cloud Run revision as another account needs actAs).
DEVELOPER_ROLES = [
    "roles/artifactregistry.admin",
    "roles/bigquery.dataOwner",
    "roles/bigquery.jobUser",
    "roles/bigtable.admin",
    "roles/cloudkms.admin",
    "roles/cloudkms.cryptoKeyEncrypterDecrypter",
    "roles/cloudtasks.admin",
    "roles/compute.admin",
    "roles/datastore.indexAdmin",
    "roles/datastore.user",
    "roles/iam.serviceAccountAdmin",
    "roles/iam.serviceAccountUser",
    "roles/networkmanagement.admin",
    "roles/pubsub.admin",
    "roles/redis.admin",
    "roles/resourcemanager.projectIamAdmin",
    "roles/run.admin",
    "roles/secretmanager.admin",
    "roles/spanner.admin",
    "roles/storage.admin",
]

LOCAL_UNIVERSE = "gcp-local.invalid"


def private_json(path: Path, value: dict[str, object]) -> None:
    """Write JSON to path atomically with owner-only permissions."""
    temporary = path.with_suffix(".tmp")
    with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as output:
        json.dump(value, output, indent=2)
        output.write("\n")
    temporary.replace(path)


def private_text(path: Path, content: str) -> None:
    """Create one new owner-only file; refuse to overwrite."""
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as output:
        output.write(content)


def loopback_endpoint(endpoint: str) -> bool:
    """Report whether endpoint is an explicit numeric loopback HTTP base with a port."""
    url = urlsplit(endpoint)
    try:
        local = ipaddress.ip_address(url.hostname or "").is_loopback and url.port
    except ValueError:
        return False
    return bool(
        local
        and url.scheme == "http"
        and not url.username
        and not url.password
        and url.path in {"", "/"}
        and not url.query
        and not url.fragment
    )


def project_number(project: str) -> str:
    """Derive a stable 12-digit project number without a leading zero.

    The Pub/Sub push contract names the service agent by project number and
    refuses leading zeros, so the number has a real shape.
    """
    digest = int(hashlib.sha256(project.encode()).hexdigest(), 16)
    return str(10**11 + digest % (9 * 10**11))


def write_identity(
    directory: Path,
    *,
    project: str,
    principal: str,
    endpoint: str,
    workloads: Sequence[str] = (),
    recorded: Path | None = None,
) -> None:
    """Create the private identity bundle in a new directory.

    ``recorded`` is the directory path application processes will see, which
    differs from ``directory`` when a container writes into a host bind mount.
    Workload service accounts are enrolled for Cloud Run metadata tokens only;
    no ADC file is written for them.
    """
    recorded = recorded or directory
    directory.mkdir(mode=0o700)
    subject = "gls." + secrets.token_urlsafe(32)
    principals = [{"subject": principal, "project": project, "subject_token": subject}]
    principals.extend(
        {
            "subject": "serviceAccount:" + email,
            "project": project,
            "subject_token": "gls." + secrets.token_urlsafe(32),
        }
        for email in workloads
    )
    adc = {
        "type": "external_account",
        "audience": "//gcp-local/identity",
        "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "token_url": endpoint.rstrip("/") + "/v1/token",
        "credential_source": {"file": str(recorded / "subject.txt")},
    }
    # The google Terraform provider refuses a universe_domain override unless
    # the credential names the same universe; the Python SDKs compare it with
    # their default instead, so they keep the unmarked file.
    terraform = {**adc, "universe_domain": LOCAL_UNIVERSE}
    files = {
        "server.json": {"token_lifetime_seconds": 3600, "principals": principals},
        "subject.txt": subject,
        "adc.json": adc,
        "terraform-adc.json": terraform,
    }
    for name, content in files.items():
        text = content if isinstance(content, str) else json.dumps(content, indent=2) + "\n"
        private_text(directory / name, text)


def write_iam(path: Path, project: str, number: str, principal: str) -> None:
    """Write the bootstrap project resource and the developer's explicit grants."""
    resource = "//cloudresourcemanager.googleapis.com/projects/" + project
    private_json(
        path,
        {
            "resources": [
                {
                    "name": resource,
                    "service": "resourcemanager",
                    "kind": "project",
                    "number": number,
                    "scope": {"project": project, "location": "global"},
                }
            ],
            "policies": {
                resource: {
                    "bindings": [{"role": role, "members": [principal]} for role in DEVELOPER_ROLES]
                }
            },
        },
    )


def compose_project(directory: Path) -> str:
    """Name the Compose project after the environment's absolute path."""
    return "gcp-local-" + hashlib.sha256(str(directory).encode()).hexdigest()[:16]


def initialize(
    directory: Path,
    port: int,
    project: str,
    *,
    workloads: Sequence[str] = (),
    mode: str = "compose",
    recorded: Path | None = None,
) -> dict[str, object]:
    """Create identity, IAM, and the manifest in an empty private directory."""
    recorded = recorded or directory
    if not re.fullmatch(PROJECT_PATTERN, project) or not 1024 <= port <= 65535:
        raise ValueError("Select a synthetic project ID and port between 1024 and 65535")
    if any(not re.fullmatch(ACCOUNT_PATTERN, name) for name in workloads):
        raise ValueError("Workload names are service-account IDs such as worker")
    if mode not in MODES:
        raise ValueError("Unknown environment mode")
    if any(path.name != ".lock" for path in directory.iterdir()):
        raise ValueError("An uninitialized environment directory must be empty")
    grpc_port = port + 1
    if grpc_port > 65535:
        raise ValueError("Select a base port that leaves room for the gRPC gateway")
    principal = "serviceAccount:developer@" + project + ".iam.gserviceaccount.com"
    number = project_number(project)
    write_identity(
        directory / "identity",
        project=project,
        principal=principal,
        endpoint=f"http://127.0.0.1:{port}",
        workloads=[name + "@" + project + ".iam.gserviceaccount.com" for name in workloads],
        recorded=recorded / "identity",
    )
    write_iam(directory / "iam.json", project, number, principal)
    value: dict[str, object] = {
        "owner": "gcp-local",
        "schema": SCHEMA,
        "mode": mode,
        "directory": str(recorded),
        "port": port,
        "grpc_port": grpc_port,
        "project": project,
        "project_number": number,
        "workloads": list(workloads),
        "compose_project": compose_project(recorded),
        "image": None,
        "version": None,
        "uid": os.getuid(),
        "gid": os.getgid(),
    }
    private_json(directory / "environment.json", value)
    return value


def check_private_files(paths: Sequence[Path]) -> None:
    """Require regular, owner-only files."""
    for path in paths:
        mode = path.lstat().st_mode
        if not stat.S_ISREG(mode) or stat.S_IMODE(mode) & 0o077:
            raise ValueError("Environment files must be regular owner-only files")


def load(
    directory: Path, *, check_credentials: bool = True, recorded: Path | None = None
) -> dict[str, object]:
    """Read and validate the environment state, optionally requiring credentials."""
    recorded = recorded or directory
    paths = [directory / "environment.json"]
    if check_credentials:
        paths.extend(
            directory / name
            for name in (
                "iam.json",
                "identity/server.json",
                "identity/adc.json",
                "identity/terraform-adc.json",
                "identity/subject.txt",
            )
        )
    check_private_files(paths)
    value = json.loads((directory / "environment.json").read_text())
    if value.get("owner") == "gcp-local" and value.get("schema") != SCHEMA:
        raise ValueError(
            "This environment was created by an older gcp-local; "
            "run down, then select a new directory"
        )
    if (
        value.get("owner") != "gcp-local"
        or value.get("mode") not in MODES
        or value.get("directory") != str(recorded)
        or value.get("compose_project") != compose_project(recorded)
        or value.get("uid") != os.getuid()
        or value.get("gid") != os.getgid()
    ):
        raise ValueError(
            "Environment ownership/path changed; select its original directory and user"
        )
    port, grpc_port = value.get("port"), value.get("grpc_port")
    image, version = value.get("image"), value.get("version")
    if (
        type(port) is not int
        or type(grpc_port) is not int
        or not 1024 <= port < grpc_port <= 65535
        or not isinstance(value.get("project"), str)
        or not re.fullmatch(PROJECT_PATTERN, value["project"])
        or value.get("project_number") != project_number(value["project"])
        or not isinstance(value.get("workloads"), list)
        or not all(
            isinstance(name, str) and re.fullmatch(ACCOUNT_PATTERN, name)
            for name in value["workloads"]
        )
        or not (image is None or (isinstance(image, str) and re.fullmatch(IMAGE_PATTERN, image)))
        or not (
            version is None or (isinstance(version, str) and re.fullmatch(VERSION_PATTERN, version))
        )
    ):
        raise ValueError("Invalid environment port, project, image, or source version")
    return value
