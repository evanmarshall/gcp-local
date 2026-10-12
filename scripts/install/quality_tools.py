#!/usr/bin/env python3
"""Install the repository-pinned Go analyzers used by scripts/check/quality.py.

Staticcheck and deadcode are built from the pinned module versions with the
pinned Go toolchain. golangci-lint is a released binary, verified against the
checksum recorded in toolchains/quality.json. Python analyzers come from
requirements/tools.txt.
"""

import hashlib
import json
import os
import platform
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DESTINATION = ROOT / ".tools/quality/bin"
GO = ROOT / ".tools/go/bin/go"

PLATFORMS = {"linux": "linux", "darwin": "darwin"}
ARCHITECTURES = {"x86_64": "amd64", "amd64": "amd64", "arm64": "arm64", "aarch64": "arm64"}


def platform_key() -> str:
    """Return the platform identifier used in the pinned manifest."""
    system = PLATFORMS.get(platform.system().lower())
    machine = ARCHITECTURES.get(platform.machine().lower())
    if not system or not machine:
        raise SystemExit("Select a supported linux or macOS host on amd64 or arm64")
    return f"{system}-{machine}"


def install_golangci_lint(config: dict[str, object], key: str) -> None:
    """Download and verify the pinned golangci-lint binary."""
    target = DESTINATION / "golangci-lint"
    os_name, arch = key.split("-", 1)
    archive = config["archive"].format(os=os_name, arch=arch)
    url = f"{config['release_url']}/{archive}"
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / archive
        with urllib.request.urlopen(url, timeout=120) as response:  # noqa: S310 - pinned release URL
            path.write_bytes(response.read())
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != config["sha256"][key]:
            raise SystemExit(f"Checksum mismatch for {archive}")
        with tarfile.open(path) as bundle:
            member = next(name for name in bundle.getnames() if name.endswith("/golangci-lint"))
            bundle.extract(member, directory, filter="data")
            target.write_bytes((Path(directory) / member).read_bytes())
    target.chmod(0o755)
    print(f"golangci-lint {config['version']}")


def install_go_module(name: str, config: dict[str, object]) -> None:
    """Install a pinned Go module tool into the quality directory."""
    environment = dict(os.environ, GOBIN=str(DESTINATION))
    subprocess.run(
        [str(GO), "install", f"{config['module']}@{config['version']}"],
        cwd=ROOT,
        env=environment,
        check=True,
    )
    print(f"{name} {config['version']}")


def main() -> None:
    """Install every pinned quality tool with verified checksums."""
    config = json.loads((ROOT / "toolchains/quality.json").read_text())
    DESTINATION.mkdir(parents=True, exist_ok=True)
    if not GO.is_file():
        raise SystemExit("Install the pinned Go toolchain first: python3 scripts/install/go.py")
    install_golangci_lint(config["golangci_lint"], platform_key())
    install_go_module("staticcheck", config["staticcheck"])
    install_go_module("deadcode", config["deadcode"])
    print(
        f"Python analyzers are pinned in requirements/tools.txt "
        f"(ruff=={config['ruff']['version']}, vulture=={config['vulture']['version']})"
    )


if __name__ == "__main__":
    try:
        main()
    except (
        OSError,
        subprocess.CalledProcessError,
        KeyError,
        StopIteration,
        urllib.error.URLError,
    ) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error
