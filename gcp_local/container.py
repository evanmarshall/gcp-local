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

from gcp_local.bootstrap import SERVICE_FLAG, initialize, load

CORE = "/gcp-local"
PYTHON = "/usr/local/bin/python3"
STATE = Path("/state")
CONFIG = Path("/config")
# The packaged Cloud Run runtime: revisions run as processes inside the core's
# container. Only the bundled workflow worker image is mapped; other images
# are refused (container-in-container is not supported).
WORKFLOW_IMAGE = "gcp-local/workflow:latest"
WORKFLOW_WORKER = "/opt/gcp-local/workflow/worker.py"


def port() -> int:
    """Read the HTTP port the core listens on; it matches the published host port."""
    value = os.environ.get("GCP_LOCAL_PORT", "4566")
    if not re.fullmatch(r"[0-9]{4,5}", value) or not 1024 <= int(value) <= 65535:
        raise SystemExit("GCP_LOCAL_PORT must be a port between 1024 and 65535")
    return int(value)


def core_arguments(identity: Path, iam: Path) -> list[str]:
    """Build the core command line for the packaged runtime."""
    arguments = [
        CORE,
        f"-listen=0.0.0.0:{port()}",
        "-grpc-listen=0.0.0.0:4567",
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
        f"-run-process={WORKFLOW_IMAGE}={PYTHON} -I {WORKFLOW_WORKER}",
    ]
    socket = os.environ.get("GCP_LOCAL_RUN_DOCKER_SOCKET")
    if socket:
        # Opt-in container execution (compose.run.yaml); the socket runtime
        # takes precedence over the process mapping.
        arguments.append("-run-docker-socket=" + socket)
    return arguments


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
        if value["mode"] != "standalone" or value["port"] != port():
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
            )
        except ValueError as error:
            raise SystemExit(str(error)) from None
    return core_arguments(STATE / "identity/server.json", STATE / "iam.json")


def main(argv: Sequence[str]) -> int:
    """Dispatch the launcher modes and exec the core."""
    mode = argv[0] if argv else "standalone"
    if mode == "standalone":
        arguments = standalone()
    elif mode == "serve":
        arguments = core_arguments(CONFIG / "identity.json", CONFIG / "iam.json")
    elif mode == "health":
        arguments = [CORE, "health", f"-endpoint=http://127.0.0.1:{port()}"]
    else:
        arguments = [CORE, *argv]
    os.execv(CORE, arguments)  # noqa: S606 -- fixed core binary, no shell
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
