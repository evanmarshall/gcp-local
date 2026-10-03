#!/usr/bin/env python3
"""Inspect an application's GCP dependencies without executing its code or contacting GCP."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil


IGNORE = {".git", ".agents", ".codex", ".terraform", ".venv", ".venv-tools", "venv",
          "node_modules", "vendor", "dist", "build", "__pycache__", "credentials", ".gcp-local"}
PATTERNS = {
    "storage": ("google-cloud-storage", "google.cloud.storage", "@google-cloud/storage", "cloud.google.com/go/storage", "Google.Cloud.Storage", "google_storage_"),
    "pubsub": ("google-cloud-pubsub", "google.cloud.pubsub", "@google-cloud/pubsub", "cloud.google.com/go/pubsub", "Google.Cloud.PubSub", "google_pubsub_"),
    "cloud-run": ("google-cloud-run", "google.cloud.run", "@google-cloud/run", "cloud.google.com/go/run", "google_cloud_run_"),
    "secret-manager": ("google-cloud-secret-manager", "google.cloud.secretmanager", "@google-cloud/secret-manager", "google_secret_manager_"),
    "bigquery": ("google-cloud-bigquery", "google.cloud.bigquery", "@google-cloud/bigquery", "cloud.google.com/go/bigquery", "google_bigquery_"),
    "firestore": ("google-cloud-firestore", "google.cloud.firestore", "@google-cloud/firestore", "cloud.google.com/go/firestore", "google_firestore_"),
}
ENDPOINT_VARIABLES = ("STORAGE_EMULATOR_HOST", "PUBSUB_EMULATOR_HOST", "FIRESTORE_EMULATOR_HOST",
                      "GOOGLE_CLOUD_PROJECT", "GOOGLE_APPLICATION_CREDENTIALS")
SUFFIXES = {".py": "python", ".js": "javascript", ".ts": "typescript", ".tsx": "typescript",
            ".go": "go", ".java": "java", ".cs": "csharp", ".tf": "terraform"}


def inspect(root):
    languages, commands, endpoints, resources, sdk_references = set(), set(), set(), set(), set()
    services = {}
    count, truncated = 0, False
    for directory, names, filenames in os.walk(root, followlinks=False):
        names[:] = sorted(name for name in names if name not in IGNORE and not name.startswith(".")
                          and not (Path(directory) / name).is_symlink())
        for name in sorted(filenames):
            path = Path(directory) / name
            if path.is_symlink() or name.startswith("."):
                continue
            selected = path.suffix in SUFFIXES or name in {"package.json", "pyproject.toml", "go.mod", "pom.xml", "pytest.ini"} or (name.startswith("requirements") and path.suffix == ".txt")
            if not selected:
                continue
            if count >= 2000:
                truncated = True
                break
            if path.stat().st_size > 1048576:
                truncated = True
                continue
            count += 1
            text = path.read_text(errors="replace")
            relative = path.relative_to(root).as_posix()
            if path.suffix in SUFFIXES:
                languages.add(SUFFIXES[path.suffix])
            if name == "pyproject.toml" or name.startswith("requirements"):
                languages.add("python")
            if name == "go.mod":
                languages.add("go")
            if name == "pom.xml":
                languages.add("java")
            for service, patterns in PATTERNS.items():
                if any(pattern in text for pattern in patterns):
                    services.setdefault(service, set()).add(relative)
                    sdk_references.update(pattern for pattern in patterns if pattern in text and not pattern.startswith("google_"))
            endpoints.update(name for name in ENDPOINT_VARIABLES if name in text)
            if path.suffix == ".tf":
                resources.update(re.findall(r'\bresource\s+"(google_[a-z0-9_]+)"', text))
            cwd = path.parent.relative_to(root).as_posix()
            if name == "package.json":
                languages.add("javascript")
                try:
                    package = json.loads(text)
                    scripts = package.get("scripts", {})
                    for script in scripts:
                        if re.fullmatch(r"test(?::[a-zA-Z0-9_-]+)*", script):
                            commands.add((cwd, "npm run " + script))
                except (ValueError, AttributeError, TypeError):
                    pass
            if name == "pytest.ini" or (name == "pyproject.toml" and "pytest" in text):
                languages.add("python")
                commands.add((cwd, "python -m pytest"))
            if name.startswith("test_") and path.suffix == ".py":
                languages.add("python")
                commands.add((".", "python -m pytest"))
            if name.endswith("_test.go"):
                commands.add((".", "go test ./..."))
        if count >= 2000:
            break
    return {"generator": "gcp-local-setup", "schema_version": 1, "inspection_only": True,
            "files_inspected": count, "truncated": truncated, "languages": sorted(languages),
            "sdk_references": sorted(sdk_references),
            "services": [{"name": name, "detected_from": sorted(paths), "local_support": "not-implemented"}
                         for name, paths in sorted(services.items())],
            "terraform_resource_types": sorted(resources), "endpoint_variable_references": sorted(endpoints),
            "test_command_candidates": [{"cwd": cwd, "command": command, "execution": "not-run"}
                                        for cwd, command in sorted(commands)],
            "prerequisites": {name: bool(shutil.which(name)) for name in ("python3", "gcloud", "docker", "terraform")},
            "profiles": {"offline": {"cloud_credentials": False, "action": "Inspect and run existing offline tests"},
                         "local": {"status": "unavailable", "reason": "gcp-local emulator runtime is not implemented"},
                         "live": {"status": "requires-explicit-sandbox", "credentials": "external file paths only"}},
            "next_actions": ["Review detected services and test commands; detection is heuristic, not compatibility certification.",
                             "Keep local endpoints and real-GCP credential profiles separate.",
                             "Local emulator execution is unavailable; do not redirect local tests to GCP.",
                             "For approved live tests, configure the explicit sandbox profile and Google Cloud CLI."]}


def write_report(root, relative, report):
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


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--output", help="Optional repository-relative JSON report")
    parser.add_argument("--doctor", action="store_true", help="Exit 2 when local emulation is unavailable")
    args = parser.parse_args(argv)
    root = Path(args.repo).resolve()
    if not root.is_dir():
        parser.error("Select an existing application directory")
    report = inspect(root)
    if args.output:
        try:
            write_report(root, args.output, report)
        except (OSError, ValueError) as error:
            parser.exit(1, str(error) + "\n")
    print(json.dumps(report, indent=2))
    return 2 if args.doctor else 0


if __name__ == "__main__":
    raise SystemExit(main())
