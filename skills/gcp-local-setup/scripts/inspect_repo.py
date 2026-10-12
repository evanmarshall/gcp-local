#!/usr/bin/env python3
"""Inspect an application's GCP dependencies without executing its code or contacting GCP."""

import argparse
import ipaddress
import json
import os
import re
import shutil
from pathlib import Path
from urllib.parse import urlsplit

IGNORE = {
    ".git",
    ".agents",
    ".codex",
    ".terraform",
    ".venv",
    ".venv-tools",
    "venv",
    "node_modules",
    "vendor",
    "dist",
    "build",
    "__pycache__",
    "credentials",
    ".gcp-local",
}
PATTERNS = {
    "storage": (
        "google-cloud-storage",
        "google.cloud.storage",
        "@google-cloud/storage",
        "cloud.google.com/go/storage",
        "Google.Cloud.Storage",
        "google_storage_",
    ),
    "pubsub": (
        "google-cloud-pubsub",
        "google.cloud.pubsub",
        "@google-cloud/pubsub",
        "cloud.google.com/go/pubsub",
        "Google.Cloud.PubSub",
        "google_pubsub_",
    ),
    "cloud-run": (
        "google-cloud-run",
        "google.cloud.run",
        "@google-cloud/run",
        "cloud.google.com/go/run",
        "google_cloud_run_",
    ),
    "secret-manager": (
        "google-cloud-secret-manager",
        "google.cloud.secretmanager",
        "@google-cloud/secret-manager",
        "google_secret_manager_",
    ),
    "bigquery": (
        "google-cloud-bigquery",
        "google.cloud.bigquery",
        "@google-cloud/bigquery",
        "cloud.google.com/go/bigquery",
        "google_bigquery_",
    ),
    "firestore": (
        "google-cloud-firestore",
        "google.cloud.firestore",
        "@google-cloud/firestore",
        "cloud.google.com/go/firestore",
        "google_firestore_",
    ),
}
# Detected service names that differ from the runtime's health provider names.
PROVIDERS = {"cloud-run": "run", "secret-manager": "secretmanager"}
# Evidence level per detected service; see the checkout's docs/quickstart.md.
LOCAL_SUPPORT = {
    "storage": "partial-recorded-slice",
    "pubsub": "partial-recorded-slice",
    "secret-manager": "single-recorded-scenario",
    "bigquery": "single-recorded-scenario",
    "firestore": "single-recorded-scenario",
    "cloud-run": "local-only-slice",
}
ENDPOINT_VARIABLES = (
    "STORAGE_EMULATOR_HOST",
    "PUBSUB_EMULATOR_HOST",
    "FIRESTORE_EMULATOR_HOST",
    "GOOGLE_CLOUD_PROJECT",
    "GOOGLE_APPLICATION_CREDENTIALS",
)
SUFFIXES = {
    ".py": "python",
    ".js": "javascript",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".go": "go",
    ".java": "java",
    ".cs": "csharp",
    ".tf": "terraform",
}


def selected(name: str, path: Path) -> bool:
    """Report whether the file is one the inspector reads."""
    return (
        path.suffix in SUFFIXES
        or name in {"package.json", "pyproject.toml", "go.mod", "pom.xml", "pytest.ini"}
        or (name.startswith("requirements") and path.suffix == ".txt")
    )


def mark_language(state: dict, path: Path, name: str) -> None:
    """Record the file's implementation language from its name and suffix."""
    if path.suffix in SUFFIXES:
        state["languages"].add(SUFFIXES[path.suffix])
    if name == "pyproject.toml" or name.startswith("requirements"):
        state["languages"].add("python")
    if name == "go.mod":
        state["languages"].add("go")
    if name == "pom.xml":
        state["languages"].add("java")


def add_package_scripts(text: str, cwd: str, commands: set) -> None:
    """Collect npm test scripts as command candidates."""
    try:
        package = json.loads(text)
        scripts = package.get("scripts", {})
        for script in scripts:
            if re.fullmatch(r"test(?::[a-zA-Z0-9_-]+)*", script):
                commands.add((cwd, "npm run " + script))
    except (ValueError, AttributeError, TypeError):
        pass


def scan_file(root: Path, path: Path, state: dict) -> None:
    """Read one file and record its language, service, and command hints."""
    text = path.read_text(errors="replace")
    name = path.name
    relative = path.relative_to(root).as_posix()
    mark_language(state, path, name)
    for service, patterns in PATTERNS.items():
        if any(pattern in text for pattern in patterns):
            state["services"].setdefault(service, set()).add(relative)
            state["sdk_references"].update(
                pattern
                for pattern in patterns
                if pattern in text and not pattern.startswith("google_")
            )
    state["endpoints"].update(name for name in ENDPOINT_VARIABLES if name in text)
    if path.suffix == ".tf":
        state["resources"].update(re.findall(r'\bresource\s+"(google_[a-z0-9_]+)"', text))
    cwd = path.parent.relative_to(root).as_posix()
    if name == "package.json":
        state["languages"].add("javascript")
        add_package_scripts(text, cwd, state["commands"])
    if name == "pytest.ini" or (name == "pyproject.toml" and "pytest" in text):
        state["languages"].add("python")
        state["commands"].add((cwd, "python -m pytest"))
    if name.startswith("test_") and path.suffix == ".py":
        state["languages"].add("python")
        state["commands"].add((".", "python -m pytest"))
    if name.endswith("_test.go"):
        state["commands"].add((".", "go test ./..."))


def walk(root: Path, state: dict) -> tuple[int, bool]:
    """Walk the repository until the inspection budget is spent."""
    count, truncated = 0, False
    for directory, names, filenames in os.walk(root, followlinks=False):
        names[:] = sorted(
            name
            for name in names
            if name not in IGNORE
            and not name.startswith(".")
            and not (Path(directory) / name).is_symlink()
        )
        for name in sorted(filenames):
            path = Path(directory) / name
            if path.is_symlink() or name.startswith("."):
                continue
            if not selected(name, path):
                continue
            if count >= 2000:
                return count, True
            if path.stat().st_size > 1048576:
                truncated = True
                continue
            count += 1
            scan_file(root, path, state)
        if count >= 2000:
            return count, True
    return count, truncated


def inspect(root: Path) -> dict[str, object]:
    """Inspect the repository without running anything or contacting any cloud."""
    state = {
        "languages": set(),
        "commands": set(),
        "endpoints": set(),
        "resources": set(),
        "services": {},
        "sdk_references": set(),
    }
    count, truncated = walk(root, state)
    return {
        "generator": "gcp-local-setup",
        "schema_version": 1,
        "inspection_only": True,
        "files_inspected": count,
        "truncated": truncated,
        "languages": sorted(state["languages"]),
        "sdk_references": sorted(state["sdk_references"]),
        "services": [
            {
                "name": name,
                "detected_from": sorted(paths),
                "local_support": LOCAL_SUPPORT.get(name, "not-implemented"),
            }
            for name, paths in sorted(state["services"].items())
        ],
        "terraform_resource_types": sorted(state["resources"]),
        "endpoint_variable_references": sorted(state["endpoints"]),
        "test_command_candidates": [
            {"cwd": cwd, "command": command, "execution": "not-run"}
            for cwd, command in sorted(state["commands"])
        ],
        "prerequisites": {
            name: bool(shutil.which(name)) for name in ("python3", "gcloud", "docker", "terraform")
        },
        "profiles": {
            "offline": {
                "cloud_credentials": False,
                "action": "Inspect and run existing offline tests",
            },
            "local": {
                "status": "unavailable",
                "reason": "No explicit running endpoint was checked",
            },
            "live": {
                "status": "requires-explicit-sandbox",
                "credentials": "external file paths only",
            },
        },
        "next_actions": [
            "Review detected services and test commands; "
            "detection is heuristic, not compatibility certification.",
            "Keep local endpoints and real-GCP credential profiles separate.",
            "Start the pinned packaged runtime; most services have one cloud-recorded scenario "
            "and Cloud Run and Pub/Sub push are locally modeled only.",
            "For approved live tests, configure the explicit sandbox profile and Google Cloud CLI.",
        ],
    }


def doctor(report: dict, endpoint: str) -> None:
    """Probe a local runtime endpoint and record the service profile."""
    parsed = urlsplit(endpoint)
    try:
        local = ipaddress.ip_address(parsed.hostname or "").is_loopback and parsed.port
    except ValueError:
        local = False
    if not (
        local
        and parsed.scheme == "http"
        and parsed.path in {"", "/"}
        and not parsed.username
        and not parsed.password
        and not parsed.query
        and not parsed.fragment
    ):
        raise ValueError("Select a numeric loopback HTTP endpoint and explicit port")

    profile = report["profiles"]["local"]
    profile["endpoint"] = endpoint.rstrip("/")
    try:
        value, providers = read_health(profile)
    except (OSError, ValueError, AttributeError):
        profile.update(
            status="unavailable",
            reason="The selected local runtime is unavailable or returned invalid health data",
        )
        return False
    missing = sorted(
        item["name"]
        for item in report["services"]
        if PROVIDERS.get(item["name"], item["name"]) not in providers
    )
    profile.update(
        status="ready" if not missing else "unsupported-services",
        providers=providers,
        missing_services=missing,
        version=value.get("version"),
        application_tests="not-run",
        credentials="not-tested",
        reason="Readiness and provider availability only; "
        "inspect operation/SDK coverage before testing",
    )
    return not missing


def read_health(profile: dict) -> tuple[dict, list]:
    """Fetch the runtime health endpoint and require a ready gcp-local reply."""
    from urllib.request import (  # noqa: PLC0415 -- beside the single use
        HTTPRedirectHandler,
        ProxyHandler,
        build_opener,
    )

    class NoRedirects(HTTPRedirectHandler):
        def redirect_request(
            self,
            request: object,
            fp: object,
            code: int,
            message: str,
            headers: object,
            newurl: str,
        ) -> None:
            """Refuse redirects: the health check must be the endpoint itself."""

    with build_opener(ProxyHandler({}), NoRedirects()).open(
        profile["endpoint"] + "/_local/health", timeout=3
    ) as response:
        raw = response.read(1048577)
        if len(raw) > 1048576:
            raise ValueError("Health response exceeded the supported size")
        value = json.loads(raw)
        providers = value.get("providers")
        if (
            response.status != 200
            or value.get("service") != "gcp-local"
            or value.get("ready") is not True
            or not isinstance(providers, list)
            or not all(isinstance(item, str) for item in providers)
        ):
            raise ValueError("Endpoint did not report a ready gcp-local runtime")
    return value, providers


def write_report(root: Path, relative: str, report: dict) -> None:
    """Write the report to a repository-relative JSON path, refusing takeovers."""
    relative = Path(relative)
    if relative.is_absolute() or ".." in relative.parts or relative.suffix != ".json":
        raise ValueError("Report output must be a repository-relative JSON path")
    path = root
    for part in relative.parts:
        path = path / part
        if path.is_symlink():
            raise ValueError("Report output cannot traverse symlinks")
    if path.exists():
        try:
            owned = json.loads(path.read_text()).get("generator") == "gcp-local-setup"
        except (ValueError, AttributeError):
            owned = False
        if not owned:
            raise ValueError("Refusing to overwrite an unrelated report file")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n")


def main(argv: list[str] | None = None) -> int:
    """Inspect the repository and optionally probe a local runtime."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--output", help="Optional repository-relative JSON report")
    parser.add_argument(
        "--doctor", action="store_true", help="Exit 2 when local emulation is unavailable"
    )
    parser.add_argument(
        "--endpoint", help="With --doctor, check one explicit numeric loopback HTTP endpoint"
    )
    args = parser.parse_args(argv)
    root = Path(args.repo).resolve()
    if not root.is_dir():
        parser.error("Select an existing application directory")
    report = inspect(root)
    if args.endpoint and not args.doctor:
        parser.error("--endpoint requires --doctor; ordinary inspection does not use the network")
    ready = False
    if args.endpoint:
        try:
            ready = doctor(report, args.endpoint)
        except ValueError as error:
            parser.error(str(error))
    if args.output:
        try:
            write_report(root, args.output, report)
        except (OSError, ValueError) as error:
            parser.exit(1, str(error) + "\n")
    print(json.dumps(report, indent=2))
    return 2 if args.doctor and not ready else 0


if __name__ == "__main__":
    raise SystemExit(main())
