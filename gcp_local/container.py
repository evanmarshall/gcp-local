"""Image entry point: start the core with the packaged flags.

``serve`` (Compose) reads the two read-only configuration mounts under /config.
``standalone`` (the default ``docker run`` command) creates a private
environment in the /state bind mount on first start, recording host paths from
GCP_LOCAL_HOST_DIRECTORY so host SDKs can use the generated ADC file, then
serves it. ``health`` probes the local listener. Anything else passes through
to the core binary unchanged.
"""

import os
import re
import stat
import sys
from collections.abc import Sequence
from pathlib import Path

from gcp_local.bootstrap import (
    SERVICE_FLAG,
    TLS_DIRECTORY,
    check_data_ports,
    default_data_ports,
    default_grpc_tls_port,
    initialize,
    load,
    parse_data_ports,
)

CORE = "/gcp-local"
PYTHON = "/usr/local/bin/python3"
STATE = Path("/state")
CONFIG = Path("/config")
# Compose mounts the environment's writable tls directory here.
TLS = Path("/tls")
PASSWD_IMAGE = Path("/etc/passwd.image")
PASSWD = Path("/tmp/passwd")  # noqa: S108 -- the container's private tmpfs
# The default packaged Cloud Run runtime: revisions run as processes inside
# the core's container. Only the bundled workflow worker image is mapped;
# other images are refused unless Docker mode is enabled (run_arguments).
WORKFLOW_IMAGE = "gcp-local/workflow:latest"
WORKFLOW_WORKER = "/opt/gcp-local/workflow/worker.py"


def port() -> int:
    """Read the HTTP port the core listens on; it matches the published host port."""
    value = os.environ.get("GCP_LOCAL_PORT", "4566")
    if not re.fullmatch(r"[0-9]{4,5}", value) or not 1024 <= int(value) <= 65535:
        raise SystemExit("GCP_LOCAL_PORT must be a port between 1024 and 65535")
    return int(value)


def grpc_tls_port() -> int:
    """Read the gRPC TLS port; it matches the published host port."""
    value = os.environ.get("GCP_LOCAL_GRPC_TLS_PORT")
    if value is None:
        return default_grpc_tls_port(port())
    if not re.fullmatch(r"[0-9]{4,5}", value) or not 1024 <= int(value) <= 65535:
        raise SystemExit("GCP_LOCAL_GRPC_TLS_PORT must be a port between 1024 and 65535")
    if int(value) in {port(), 4567}:
        raise SystemExit("GCP_LOCAL_GRPC_TLS_PORT must differ from the HTTP and gRPC ports")
    return int(value)


def data_ports() -> tuple[int, int]:
    """Read the published data-plane range; it matches the host ports one to one."""
    value = os.environ.get("GCP_LOCAL_DATA_PORTS")
    http = port()
    try:
        ports = parse_data_ports(value) if value else default_data_ports(http)
        # Inside the container the gRPC gateway always listens on 4567.
        return check_data_ports(ports, http, 4567, grpc_tls_port())
    except ValueError as error:
        raise SystemExit("GCP_LOCAL_DATA_PORTS: " + str(error)) from None


def core_arguments(identity: Path, iam: Path, tls: Path) -> list[str]:
    """Build the core command line for the packaged runtime."""
    first, last = data_ports()
    arguments = [
        CORE,
        f"-listen=0.0.0.0:{port()}",
        "-grpc-listen=0.0.0.0:4567",
        # The same gRPC server over TLS, for clients that send credentials only
        # over TLS. The core creates the CA and server key in tls on first start.
        f"-grpc-tls-listen=0.0.0.0:{grpc_tls_port()}",
        f"-grpc-tls-dir={tls}",
        # Redis and public-IP Cloud SQL instances get ports from this range.
        # The core binds them on the container's interfaces; the entry point
        # (or docker run --publish) maps the same numbers on host loopback.
        f"-data-ports={first}-{last}",
        "-data-listen=0.0.0.0",
        "-services=" + SERVICE_FLAG,
        f"-identity-config={identity}",
        f"-iam-config={iam}",
        "-pubsub-java=/usr/bin/java",
        "-pubsub-jar=/opt/pubsub/emulator.jar",
        "-firestore-java=/usr/bin/java",
        "-firestore-jar=/opt/firestore/emulator.jar",
        "-spanner-emulator=/opt/spanner/emulator_main",
        "-spanner-libraries=/opt/spanner/lib",
        "-redis-server=/opt/redis/redis-server",
        "-cloudsql-postgres=/opt/postgres",
        "-cloudsql-mysql=/opt/mysql",
    ]
    return arguments + run_arguments()


def run_arguments() -> list[str]:
    """Select the Cloud Run runtime: the bundled worker process, or opt-in Docker.

    Docker mode (``gcp-local --cloud-run-docker up``, compose.run.yaml) runs
    every image as a container on the environment's network and labels it with
    the environment so reset and down remove only its own containers.
    """
    socket = os.environ.get("GCP_LOCAL_RUN_DOCKER_SOCKET")
    if not socket:
        return [f"-run-process={WORKFLOW_IMAGE}={PYTHON} -I {WORKFLOW_WORKER}"]
    environment = os.environ.get("GCP_LOCAL_RUN_ENVIRONMENT", "")
    network = os.environ.get("GCP_LOCAL_RUN_DOCKER_NETWORK", "")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", environment) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", network
    ):
        raise SystemExit(
            "GCP_LOCAL_RUN_DOCKER_SOCKET needs GCP_LOCAL_RUN_ENVIRONMENT (a label value) "
            "and GCP_LOCAL_RUN_DOCKER_NETWORK (the network this container shares with "
            "its revisions); see docs/cloudrun.md"
        )
    return [
        "-run-docker-socket=" + socket,
        "-run-docker-network=" + network,
        "-run-docker-environment=" + environment,
    ]


def standalone() -> list[str]:
    """Create or reuse the /state environment and return the core command line."""
    host = os.environ.get("GCP_LOCAL_HOST_DIRECTORY", "")
    if not host.startswith("/") or "\0" in host:
        raise SystemExit("Set GCP_LOCAL_HOST_DIRECTORY to the absolute host path mounted at /state")
    info = STATE.lstat() if STATE.exists() else None
    if (
        info is None
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise SystemExit(
            "Mount a private (mode 0700) directory owned by --user at /state; "
            "see docs/quickstart.md"
        )
    recorded = Path(host)
    if (STATE / "environment.json").exists():
        value = load(STATE, recorded=recorded)
        if (
            value["mode"] != "standalone"
            or value["port"] != port()
            or value["data_ports"] != list(data_ports())
            or value["grpc_tls_port"] != grpc_tls_port()
        ):
            raise SystemExit("This /state directory belongs to another port or environment")
    else:
        project = os.environ.get("GCP_LOCAL_PROJECT", "gcp-local-test")
        workloads = [name for name in os.environ.get("GCP_LOCAL_WORKLOADS", "").split(",") if name]
        try:
            initialize(
                STATE,
                port(),
                project,
                workloads=workloads,
                mode="standalone",
                recorded=recorded,
                data_ports=data_ports(),
            )
        except ValueError as error:
            raise SystemExit(str(error)) from None
    return core_arguments(STATE / "identity/server.json", STATE / "iam.json", STATE / TLS_DIRECTORY)


def write_passwd(image: Path = PASSWD_IMAGE, target: Path = PASSWD) -> None:
    """Give the running UID a passwd entry on the tmpfs.

    The image links /etc/passwd to ``target``. PostgreSQL's initdb refuses a
    UID without an entry, and the container runs as the host user's UID.
    """
    if not image.is_file():
        return
    entries = image.read_text()
    uid, gid = os.getuid(), os.getgid()
    if not any(line.split(":")[2:3] == [str(uid)] for line in entries.splitlines()):
        entries += f"gcp-local:x:{uid}:{gid}:gcp-local:/tmp:/usr/sbin/nologin\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW
    with os.fdopen(os.open(target, flags, 0o644), "w") as output:
        output.write(entries)


def main(argv: Sequence[str]) -> int:
    """Dispatch the launcher modes and exec the core."""
    mode = argv[0] if argv else "standalone"
    if mode in {"standalone", "serve"}:
        write_passwd()
    if mode == "standalone":
        arguments = standalone()
    elif mode == "serve":
        arguments = core_arguments(CONFIG / "identity.json", CONFIG / "iam.json", TLS)
    elif mode == "health":
        arguments = [CORE, "health", f"-endpoint=http://127.0.0.1:{port()}"]
    else:
        arguments = [CORE, *argv]
    os.execv(CORE, arguments)  # noqa: S606 -- fixed core binary, no shell
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
