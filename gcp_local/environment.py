"""Manage one private, disposable Compose environment without cloud credentials."""

import argparse
import email.message
import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from importlib import resources
from pathlib import Path
from typing import IO
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from gcp_local import IMAGE_REPOSITORY, __version__
from gcp_local.bootstrap import (
    IMAGE_PATTERN,
    SERVICES,
    TLS_DIRECTORY,
    compose_project,
    data_port_range,
    default_data_ports,
    default_grpc_tls_port,
    initialize,
    load,
    parse_data_ports,
    private_json,
)

DOCKER = shutil.which("docker") or "docker"

# A source checkout holds the package next to the Dockerfile; an installed
# wheel carries the Compose files as package data instead.
CHECKOUT = Path(__file__).resolve().parents[1]
SOURCE_FILES = (
    "Dockerfile",
    ".dockerignore",
    "compose.yaml",
    "compose.build.yaml",
    "compose.run.yaml",
    "packaging/haproxy.cfg",
    "go.mod",
    "go.sum",
    "scripts/install_go.py",
    "toolchains/go.json",
    "scripts/install_emulator_jar.py",
    "toolchains/pubsub.json",
    "toolchains/firestore.json",
    "scripts/install_spanner.py",
    "toolchains/spanner.json",
    "scripts/install_redis.py",
    "toolchains/redis.json",
    "scripts/install_postgres.py",
    "toolchains/postgres.json",
    "scripts/install_mysql.py",
    "toolchains/mysql.json",
    "examples/workflow/worker.py",
)
SOURCE_DIRECTORIES = ("api", "cmd", "internal", "gcp_local")
ENVIRONMENT_VARIABLE = "GCP_LOCAL_IMAGE"
# The label the core's Cloud Run Docker runtime puts on every revision
# container; its value is the environment's Compose project.
REVISION_LABEL = "gcp-local.environment"


def checkout() -> Path | None:
    """Return the source checkout when this package runs from one."""
    if all((CHECKOUT / name).is_file() for name in ("Dockerfile", "compose.build.yaml", "go.mod")):
        return CHECKOUT
    return None


def compose_directory() -> Path:
    """Locate compose.yaml and packaging/haproxy.cfg for this installation."""
    root = checkout()
    if root is not None:
        return root
    data = Path(str(resources.files("gcp_local") / "data"))
    if not (data / "compose.yaml").is_file():
        raise ValueError("This gcp-local installation is missing its Compose files")
    return data


def source_version(root: Path) -> str:
    """Digest the packaged runtime sources into a short version identifier."""
    files = [root / name for name in SOURCE_FILES]
    for directory in SOURCE_DIRECTORIES:
        files.extend(
            path
            for path in (root / directory).rglob("*")
            if path.is_file() and "__pycache__" not in path.parts
        )
    digest = hashlib.sha256()
    for path in sorted(files):
        digest.update(path.relative_to(root).as_posix().encode() + b"\0" + path.read_bytes())
    return "source-" + digest.hexdigest()[:16]


def select_image(requested: str | None) -> tuple[str | None, str | None]:
    """Choose a published image or a source build; return (image, source version).

    Precedence: --image, then GCP_LOCAL_IMAGE, then a build from this checkout,
    then the published image matching this package version.
    """
    image = requested or os.environ.get(ENVIRONMENT_VARIABLE) or None
    if image is None:
        root = checkout()
        if root is not None:
            return None, source_version(root)
        image = IMAGE_REPOSITORY + ":" + __version__
    if not re.fullmatch(IMAGE_PATTERN, image):
        raise ValueError("Select an image reference such as " + IMAGE_REPOSITORY + ":VERSION")
    return image, None


def image_of(value: dict[str, object]) -> str:
    """Return the image reference Compose runs for this environment."""
    if value.get("image"):
        return str(value["image"])
    return "gcp-local:" + str(value["version"])


def docker_env() -> dict[str, str]:
    """Return the host variables Docker itself needs, and nothing else."""
    return {
        key: item
        for key, item in os.environ.items()
        if key
        in {
            "PATH",
            "HOME",
            "DOCKER_HOST",
            "DOCKER_CONTEXT",
            "DOCKER_CONFIG",
            "DOCKER_TLS_VERIFY",
            "DOCKER_CERT_PATH",
            "SSH_AUTH_SOCK",
        }
    }


def compose(
    value: dict[str, object],
    *arguments: str,
    check: bool = True,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run docker compose against the packaged runtime with a minimal environment."""
    env = docker_env()
    env.update(
        {
            "GCP_LOCAL_DIRECTORY": str(value["directory"]),
            "GCP_LOCAL_PORT": str(value["port"]),
            "GCP_LOCAL_GRPC_PORT": str(value["grpc_port"]),
            "GCP_LOCAL_GRPC_TLS_PORT": str(value["grpc_tls_port"]),
            "GCP_LOCAL_DATA_PORTS": data_port_range(value),
            "GCP_LOCAL_UID": str(value["uid"]),
            "GCP_LOCAL_GID": str(value["gid"]),
            "GCP_LOCAL_VERSION": str(value.get("version") or "image"),
            "GCP_LOCAL_IMAGE": image_of(value),
        }
    )
    directory = compose_directory()
    files = ["--file", str(directory / "compose.yaml")]
    if value.get("version"):
        files += ["--file", str(directory / "compose.build.yaml")]
    if value.get("cloud_run_docker"):
        files += ["--file", str(directory / "compose.run.yaml")]
        env["GCP_LOCAL_RUN_ENVIRONMENT"] = str(value["compose_project"])
        env["GCP_LOCAL_DOCKER_GID"] = str(value["docker_socket_gid"])
    return subprocess.run(  # noqa: S603 -- fixed docker binary and validated arguments
        [
            DOCKER,
            "compose",
            "--env-file",
            os.devnull,
            "--project-name",
            str(value["compose_project"]),
            "--project-directory",
            str(directory),
            *files,
            *arguments,
        ],
        cwd=directory,
        env=env,
        check=check,
        capture_output=capture,
        text=True,
    )


def docker(*arguments: str) -> subprocess.CompletedProcess[str]:
    """Run one docker command with the minimal environment and capture its output."""
    return subprocess.run(  # noqa: S603 -- fixed docker binary and validated arguments
        [DOCKER, *arguments], env=docker_env(), check=True, capture_output=True, text=True
    )


def socket_gid(value: dict[str, object]) -> int:
    """Read the engine socket's group as the core container sees it.

    The core runs as the host user without capabilities, so it needs the
    socket's group. On Docker Desktop the socket lives in the engine VM, where
    its group differs from the host's, so the image itself reports it.
    """
    probe = docker(
        "run", "--rm", "--pull", "never", "--network", "none",
        "--user", f"{value['uid']}:{value['gid']}",
        "--volume", "/var/run/docker.sock:/run/engine.sock:ro",
        "--entrypoint", "/usr/local/bin/python3", image_of(value),
        "-I", "-c", "import os; print(os.stat('/run/engine.sock').st_gid)",
    ).stdout.strip()  # fmt: skip
    if not probe.isdigit():
        raise ValueError("Could not read the Docker socket group for Cloud Run Docker mode")
    return int(probe)


def revision_containers(value: dict[str, object]) -> list[str]:
    """List this environment's Cloud Run revision containers, running or not."""
    label = REVISION_LABEL + "=" + str(value["compose_project"])
    return docker("ps", "--all", "--quiet", "--filter", "label=" + label).stdout.split()


def remove_revisions(value: dict[str, object]) -> None:
    """Force-remove this environment's revision containers and no others."""
    containers = revision_containers(value)
    if containers:
        docker("rm", "--force", *containers)


def application_env(value: dict[str, object]) -> dict[str, str]:
    """Return the environment variables application processes inherit."""
    identity = Path(str(value["directory"])) / "identity"
    run = {}
    if value.get("cloud_run_docker"):
        # Revision containers reach the core by its Compose service name.
        run = {
            "GCP_LOCAL_RUN_ENDPOINT": f"http://gcp-local:{value['port']}",
            "GCP_LOCAL_RUN_GRPC_ENDPOINT": "gcp-local:4567",
        }
    return {
        "GOOGLE_APPLICATION_CREDENTIALS": str(identity / "adc.json"),
        "GOOGLE_CLOUD_PROJECT": str(value["project"]),
        "GCP_LOCAL_ENDPOINT": f"http://127.0.0.1:{value['port']}",
        "GCP_LOCAL_GRPC_ENDPOINT": f"127.0.0.1:{value['grpc_port']}",
        # The same gRPC gateway over TLS, signed by this environment's own CA.
        "GCP_LOCAL_GRPC_TLS_ENDPOINT": f"127.0.0.1:{value['grpc_tls_port']}",
        "GCP_LOCAL_GRPC_TLS_CA": str(Path(str(value["directory"])) / TLS_DIRECTORY / "ca.pem"),
        # Redis and public-IP Cloud SQL instances report ports from this range.
        "GCP_LOCAL_DATA_PORTS": data_port_range(value),
        "GCP_LOCAL_PROJECT_NUMBER": str(value["project_number"]),
        "GCP_LOCAL_TERRAFORM_CREDENTIALS": str(identity / "terraform-adc.json"),
        **run,
    }


def ready(value: dict[str, object]) -> dict[str, object]:
    """Poll the health endpoint without proxies and require the expected ready report."""

    class NoRedirects(HTTPRedirectHandler):
        def redirect_request(
            self,
            _request: Request,
            _fp: IO[bytes],
            _code: int,
            _message: str,
            _headers: email.message.Message,
            _newurl: str,
        ) -> None:
            return None

    with build_opener(ProxyHandler({}), NoRedirects()).open(
        application_env(value)["GCP_LOCAL_ENDPOINT"] + "/_local/health", timeout=3
    ) as response:
        health = json.loads(response.read(1 << 20))
        if (
            response.status != 200
            or health.get("service") != "gcp-local"
            or health.get("ready") is not True
            or (value.get("version") and health.get("version") != value["version"])
            or health.get("providers") != SERVICES
        ):
            raise ValueError("The published endpoint did not report the expected ready runtime")
    return health


@contextmanager
def environment_lock(directory: Path) -> Iterator[None]:
    """Serialize environment mutations across processes with an exclusive lock file."""
    lock = directory / ".lock"
    with os.fdopen(os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600), "w") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def legacy(directory: Path) -> dict[str, object] | None:
    """Return an older manifest's Compose identity so down can still remove it."""
    try:
        value = json.loads((directory / "environment.json").read_text())
    except (OSError, ValueError):
        return None
    if (
        value.get("owner") == "gcp-local"
        and value.get("schema") == 1
        and value.get("compose_project") == compose_project(directory)
    ):
        return value
    return None


def prepare(directory: Path, args: argparse.Namespace) -> dict[str, object]:
    """Resolve the environment state for the requested action."""
    if not directory.is_dir() or stat.S_IMODE(directory.stat().st_mode) & 0o077:
        raise ValueError("Select a private environment directory (mode 0700)")
    if not (directory / "environment.json").exists():
        if args.action != "up":
            raise ValueError("Environment is not initialized")
        value = initialize(
            directory,
            args.port if args.port is not None else 4566,
            args.project or "gcp-local-test",
            workloads=args.workload or [],
            data_ports=parse_data_ports(args.data_ports) if args.data_ports else None,
        )
    elif args.action == "down" and (old := legacy(directory)) is not None:
        return {
            **old,
            "mode": "compose",
            "workloads": [],
            "version": None,
            "image": "legacy",
            "data_ports": list(default_data_ports(int(old["port"]))),
            "grpc_tls_port": default_grpc_tls_port(int(old["port"])),
        }
    else:
        value = load(directory, check_credentials=args.action in {"up", "reset", "env"})
    if (
        (args.port is not None and args.port != value["port"])
        or (args.project is not None and args.project != value["project"])
        or (args.data_ports is not None and args.data_ports != data_port_range(value))
        or (args.workload and sorted(args.workload) != sorted(value["workloads"]))
    ):
        raise ValueError(
            "Port/data ports/project/workloads are fixed for this directory; select a new directory"
        )
    if value["mode"] == "standalone" and args.action not in {"status", "env"}:
        raise ValueError(
            "This environment is managed by docker run; stop or restart that container instead"
        )
    return value


def await_ready(value: dict[str, object]) -> None:
    """Wait for the runtime health endpoint after a start or reset."""
    deadline = time.monotonic() + 30
    while True:
        try:
            ready(value)
            break
        except (OSError, ValueError):
            if time.monotonic() >= deadline:
                raise ValueError(
                    "The host endpoint is unavailable; inspect logs before retrying"
                ) from None
            time.sleep(0.2)


def start(args: argparse.Namespace, value: dict[str, object], directory: Path) -> None:
    """Build or pull the runtime image, then start or recreate the environment."""
    if args.action == "up":
        value["image"], value["version"] = select_image(args.image)
        value["cloud_run_docker"], value["docker_socket_gid"] = False, None
        private_json(directory / "environment.json", value)
        if value["version"]:
            compose(value, "build")
            compose(value, "pull", "--policy", "missing", "entrypoint")
        else:
            compose(value, "pull", "--policy", "missing")
        if args.cloud_run_docker:
            value["cloud_run_docker"], value["docker_socket_gid"] = True, socket_gid(value)
            private_json(directory / "environment.json", value)
    elif args.image is not None or args.cloud_run_docker:
        raise ValueError(
            "Select an image and Cloud Run Docker mode with up; reset reuses the running selection"
        )
    # Compose mounts it for the core's TLS material; older environments lack it.
    (directory / TLS_DIRECTORY).mkdir(mode=0o700, exist_ok=True)
    flags = ["--force-recreate"] if args.action == "reset" else []
    compose(
        value,
        "up",
        "--detach",
        "--wait",
        "--wait-timeout",
        "60",
        "--no-build",
        "--pull",
        "never",
        *flags,
    )
    if not value.get("cloud_run_docker"):
        # A core without Docker mode cannot see containers an earlier
        # Docker-mode core left behind. A Docker-mode core sweeps at startup.
        remove_revisions(value)
    await_ready(value)
    print(
        json.dumps(
            {
                "ready": True,
                "environment": str(directory),
                "image": image_of(value),
                "services": SERVICES,
                "endpoint": application_env(value)["GCP_LOCAL_ENDPOINT"],
                "grpc_endpoint": application_env(value)["GCP_LOCAL_GRPC_ENDPOINT"],
                "grpc_tls_endpoint": application_env(value)["GCP_LOCAL_GRPC_TLS_ENDPOINT"],
                "data_ports": data_port_range(value),
            }
        )
    )


def run(args: argparse.Namespace, value: dict[str, object], directory: Path) -> int:
    """Execute one environment lifecycle action against the loaded state."""
    if args.action == "env":
        print(json.dumps(application_env(value), indent=2))
    elif args.action in {"up", "reset"}:
        start(args, value, directory)
    elif args.action == "down":
        # Revision containers hold endpoints on the backend network, so they
        # go before Compose removes it, and again after the core has stopped.
        remove_revisions(value)
        compose(value, "down", "--timeout", "15")
        remove_revisions(value)
        print(
            "Environment stopped; in-memory service state is discarded. "
            "Local credentials are retained."
        )
    elif args.action == "status":
        print(json.dumps(ready(value)))
    elif args.action == "logs":
        compose(value, "logs", "--no-color", "--tail", "100")
    return 0


def parser() -> argparse.ArgumentParser:
    """Describe the lifecycle command line shared by the CLI and the script wrapper."""
    result = argparse.ArgumentParser(prog="gcp-local", description=__doc__)
    result.add_argument("--version", action="version", version="gcp-local " + __version__)
    result.add_argument("--directory", type=Path, default=Path.cwd() / ".gcp-local/environment")
    result.add_argument("--port", type=int, help="Initial loopback HTTP port; default 4566")
    result.add_argument(
        "--data-ports",
        metavar="FIRST-LAST",
        help="Initial loopback range for Redis and Cloud SQL instances (at most 64 ports); "
        "default: the 16 ports after the gRPC port",
    )
    result.add_argument("--project", help="Initial synthetic project; default gcp-local-test")
    result.add_argument(
        "--workload",
        action="append",
        metavar="ACCOUNT_ID",
        help="Initial Cloud Run workload service account to enroll (repeatable), e.g. worker",
    )
    result.add_argument(
        "--image",
        help="Runtime image for up, e.g. "
        + IMAGE_REPOSITORY
        + ":VERSION; default: GCP_LOCAL_IMAGE, a checkout build, or the published "
        + __version__
        + " image",
    )
    result.add_argument(
        "--cloud-run-docker",
        action="store_true",
        help="With up: run Cloud Run revisions as Docker containers. Mounts the Docker "
        "socket, which gives the core root-equivalent access to the host's Docker",
    )
    result.add_argument("action", choices=("up", "down", "reset", "status", "env", "logs"))
    return result


def main(argv: Sequence[str] | None = None) -> int:
    """Drive the environment lifecycle subcommands."""
    options = parser()
    args = options.parse_args(argv)
    if args.directory.is_symlink():
        options.error("An environment directory cannot be a symlink")
    directory = args.directory.expanduser().resolve()
    if not directory.exists() and args.action == "down":
        print("Environment is already down.")
        return 0
    if not directory.exists() and args.action != "up":
        print("Run up first to initialize this environment", file=sys.stderr)
        return 1
    try:
        directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        with environment_lock(directory):
            return run(args, prepare(directory, args), directory)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        print(
            "Use logs to inspect startup errors and down to remove this environment.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
