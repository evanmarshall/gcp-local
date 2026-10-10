#!/usr/bin/env python3
"""Manage one private, disposable Compose environment without cloud credentials."""

import argparse
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
from pathlib import Path
from urllib.request import HTTPRedirectHandler, ProxyHandler, build_opener

GCLOUD = shutil.which("gcloud") or "gcloud"

ROOT = Path(__file__).resolve().parents[1]
SERVICES = ["pubsub", "storage"]


def private_json(path, value):
    temporary = path.with_suffix(".tmp")
    with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as output:
        json.dump(value, output, indent=2)
        output.write("\n")
    temporary.replace(path)


def source_version():
    files = [
        ROOT / name
        for name in (
            "Dockerfile",
            ".dockerignore",
            "compose.yaml",
            "go.mod",
            "go.sum",
            "scripts/install_go.py",
            "toolchains/go.json",
            "scripts/install_pubsub.py",
            "toolchains/pubsub.json",
        )
    ]
    for directory in ("api", "cmd", "internal"):
        files.extend(path for path in (ROOT / directory).rglob("*") if path.is_file())
    digest = hashlib.sha256()
    for path in sorted(files):
        digest.update(path.relative_to(ROOT).as_posix().encode() + b"\0" + path.read_bytes())
    return "source-" + digest.hexdigest()[:16]


def initialize(directory, port, project):
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,60}", project) or not 1024 <= port <= 65535:
        raise ValueError("Select a synthetic project ID and port between 1024 and 65535")
    if any(path.name != ".lock" for path in directory.iterdir()):
        raise ValueError("An uninitialized environment directory must be empty")
    grpc_port = port + 1
    if grpc_port > 65535:
        raise ValueError("Select a base port that leaves room for the gRPC gateway")
    principal = "serviceAccount:developer@" + project + ".iam.gserviceaccount.com"
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/create_local_identity.py"),
            "--directory",
            str(directory / "identity"),
            "--project",
            project,
            "--principal",
            principal,
            "--endpoint",
            f"http://127.0.0.1:{port}",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    resource = "//cloudresourcemanager.googleapis.com/projects/" + project
    private_json(
        directory / "iam.json",
        {
            "resources": [
                {
                    "name": resource,
                    "service": "resourcemanager",
                    "kind": "project",
                    "number": "000000000001",
                    "scope": {"project": project, "location": "global"},
                }
            ],
            "policies": {
                resource: {
                    "bindings": [
                        {"role": "roles/storage.admin", "members": [principal]},
                        {"role": "roles/pubsub.admin", "members": [principal]},
                    ]
                }
            },
        },
    )
    value = {
        "owner": "gcp-local",
        "schema": 1,
        "directory": str(directory),
        "port": port,
        "grpc_port": grpc_port,
        "project": project,
        "compose_project": "gcp-local-" + hashlib.sha256(str(directory).encode()).hexdigest()[:16],
        "version": source_version(),
        "uid": os.getuid(),
        "gid": os.getgid(),
    }
    private_json(directory / "environment.json", value)
    return value


def load(directory, check_credentials=True):
    paths = [directory / "environment.json"]
    if check_credentials:
        paths.extend(
            (
                directory / "iam.json",
                directory / "identity/server.json",
                directory / "identity/adc.json",
                directory / "identity/subject.txt",
            )
        )
    for path in paths:
        mode = path.lstat().st_mode
        if not stat.S_ISREG(mode) or stat.S_IMODE(mode) & 0o077:
            raise ValueError("Environment files must be regular owner-only files")
    value = json.loads((directory / "environment.json").read_text())
    expected = "gcp-local-" + hashlib.sha256(str(directory).encode()).hexdigest()[:16]
    if (
        value.get("owner") != "gcp-local"
        or value.get("schema") != 1
        or value.get("directory") != str(directory)
        or value.get("compose_project") != expected
        or value.get("uid") != os.getuid()
        or value.get("gid") != os.getgid()
    ):
        raise ValueError(
            "Environment ownership/path changed; select its original directory and user"
        )
    port = value.get("port")
    if type(port) is not int:
        raise ValueError("Invalid environment port, project, or source version")
    value["grpc_port"] = value.get("grpc_port", port + 1)
    if (
        not 1024 <= port <= 65535
        or type(value.get("grpc_port")) is not int
        or not port < value["grpc_port"] <= 65535
        or not isinstance(value.get("project"), str)
        or not re.fullmatch(r"[a-z][a-z0-9-]{4,60}", value["project"])
        or not isinstance(value.get("version"), str)
        or not re.fullmatch(r"source-[0-9a-f]{16}", value["version"])
    ):
        raise ValueError("Invalid environment port, project, or source version")
    return value


def compose(value, *arguments, check=True, capture=False):
    env = {
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
    env.update(
        {
            "GCP_LOCAL_DIRECTORY": value["directory"],
            "GCP_LOCAL_PORT": str(value["port"]),
            "GCP_LOCAL_GRPC_PORT": str(value["grpc_port"]),
            "GCP_LOCAL_UID": str(value["uid"]),
            "GCP_LOCAL_GID": str(value["gid"]),
            "GCP_LOCAL_VERSION": value["version"],
            "GCP_LOCAL_IMAGE": "gcp-local:" + value["version"],
        }
    )
    return subprocess.run(
        [
            DOCKER,
            "compose",
            "--env-file",
            os.devnull,
            "--project-name",
            value["compose_project"],
            "--file",
            str(ROOT / "compose.yaml"),
            *arguments,
        ],
        cwd=ROOT,
        env=env,
        check=check,
        capture_output=capture,
        text=True,
    )


def application_env(value):
    return {
        "GOOGLE_APPLICATION_CREDENTIALS": str(Path(value["directory"]) / "identity/adc.json"),
        "GOOGLE_CLOUD_PROJECT": value["project"],
        "GCP_LOCAL_ENDPOINT": f"http://127.0.0.1:{value['port']}",
        "GCP_LOCAL_GRPC_ENDPOINT": f"127.0.0.1:{value['grpc_port']}",
    }


def ready(value):
    class NoRedirects(HTTPRedirectHandler):
        def redirect_request(self, request, fp, code, message, headers, newurl):
            return None

    with build_opener(ProxyHandler({}), NoRedirects()).open(
        application_env(value)["GCP_LOCAL_ENDPOINT"] + "/_local/health", timeout=3
    ) as response:
        health = json.loads(response.read(1 << 20))
        if (
            response.status != 200
            or health.get("service") != "gcp-local"
            or health.get("ready") is not True
            or health.get("version") != value["version"]
            or health.get("providers") != SERVICES
        ):
            raise ValueError("The published endpoint did not report the expected ready runtime")
    return health


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path.cwd() / ".gcp-local/environment")
    parser.add_argument("--port", type=int, help="Initial loopback HTTP port; default 4566")
    parser.add_argument("--project", help="Initial synthetic project; default gcp-local-test")
    parser.add_argument("action", choices=("up", "down", "reset", "status", "env", "logs"))
    args = parser.parse_args(argv)
    if args.directory.is_symlink():
        parser.error("An environment directory cannot be a symlink")
    directory = args.directory.expanduser().resolve()
    try:
        if not directory.exists():
            if args.action == "down":
                print("Environment is already down.")
                return 0
            if args.action != "up":
                raise ValueError("Run up first to initialize this environment")
            directory.mkdir(parents=True, mode=0o700)
        if not directory.is_dir() or stat.S_IMODE(directory.stat().st_mode) & 0o077:
            raise ValueError("Select a private environment directory (mode 0700)")
        lock = directory / ".lock"
        with os.fdopen(os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600), "w") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if not (directory / "environment.json").exists():
                if args.action != "up":
                    raise ValueError("Environment is not initialized")
                value = initialize(
                    directory,
                    args.port if args.port is not None else 4566,
                    args.project or "gcp-local-test",
                )
            else:
                value = load(directory, check_credentials=args.action in {"up", "reset", "env"})
            if (args.port is not None and args.port != value["port"]) or (
                args.project is not None and args.project != value["project"]
            ):
                raise ValueError(
                    "Port/project are fixed for this directory; select a new directory"
                )
            if args.action == "env":
                print(json.dumps(application_env(value), indent=2))
                return 0
            if args.action in {"up", "reset"}:
                if args.action == "up":
                    value["version"] = source_version()
                    private_json(directory / "environment.json", value)
                    compose(value, "build")
                    compose(value, "pull", "--policy", "missing", "entrypoint")
                flags = ["--force-recreate"] if args.action == "reset" else []
                compose(
                    value,
                    "up",
                    "--detach",
                    "--wait",
                    "--wait-timeout",
                    "40",
                    "--no-build",
                    "--pull",
                    "never",
                    *flags,
                )
                deadline = time.monotonic() + 10
                while True:
                    try:
                        ready(value)
                        break
                    except (OSError, ValueError):
                        if time.monotonic() >= deadline:
                            raise ValueError(
                                "The host endpoint is unavailable; inspect logs before retrying"
                            ) from None
                        time.sleep(0.1)
                print(
                    json.dumps(
                        {
                            "ready": True,
                            "environment": str(directory),
                            "services": SERVICES,
                            "endpoint": application_env(value)["GCP_LOCAL_ENDPOINT"],
                            "grpc_endpoint": application_env(value)["GCP_LOCAL_GRPC_ENDPOINT"],
                        }
                    )
                )
            elif args.action == "down":
                compose(value, "down", "--timeout", "15")
                print(
                    "Environment stopped; in-memory service state is discarded. "
                    "Local credentials are retained."
                )
            elif args.action == "status":
                print(json.dumps(ready(value)))
            elif args.action == "logs":
                compose(value, "logs", "--no-color", "--tail", "100")
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        print(
            "Use logs to inspect startup errors and down to remove this environment.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
