#!/usr/bin/env python3
"""Build the checksum-pinned official Redis source release into .tools/redis.

The source tarball is verified by size and SHA-256 against toolchains/redis.json before
extraction, extracted without links or paths outside the scratch tree, and built with
the bundled dependencies only. The installed redis-server must report the pinned version.
"""

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

ARCHITECTURES = {
    "x86_64": "linux/amd64",
    "amd64": "linux/amd64",
    "aarch64": "linux/arm64",
    "arm64": "linux/arm64",
}
CHUNK = 1 << 20
MAX_MEMBER = 16 << 20
BUILD_ENVIRONMENT = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LC_ALL": "C"}


def _download(pin: dict, target: Path) -> None:
    """Download the source release with no ambient proxy and verify size and checksum."""
    if urlparse(pin["url"]).scheme != "https":
        raise SystemExit("Refusing a non-https Redis source download")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    digest, total = hashlib.sha256(), 0
    # The release host rejects the default urllib agent; name this installer instead.
    request = urllib.request.Request(pin["url"], headers={"User-Agent": "gcp-local-install-redis"})  # noqa: S310
    with opener.open(request, timeout=120) as response, target.open("wb") as handle:
        while chunk := response.read(CHUNK):
            total += len(chunk)
            if total > pin["size"]:
                raise SystemExit("Redis source release exceeds its pinned size")
            digest.update(chunk)
            handle.write(chunk)
    if total != pin["size"] or digest.hexdigest() != pin["sha256"]:
        raise SystemExit("Redis source release failed size/checksum verification")


def _extract(archive_path: Path, scratch: Path, version: str) -> Path:
    """Extract only regular files and directories under the expected top-level folder."""
    root = "redis-" + version
    with tarfile.open(archive_path, "r:gz") as archive:
        for member in archive:
            parts = Path(member.name).parts
            if (
                not parts
                or parts[0] != root
                or ".." in parts
                or Path(member.name).is_absolute()
                or not (member.isfile() or member.isdir())
                or member.size > MAX_MEMBER
            ):
                raise SystemExit("Unexpected Redis source archive member: " + member.name)
            destination = scratch.joinpath(*parts)
            if member.isdir():
                destination.mkdir(parents=True, exist_ok=True)
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source, destination.open("wb") as handle:
                shutil.copyfileobj(source, handle, CHUNK)
            destination.chmod(0o755 if member.mode & 0o111 else 0o644)
    return scratch / root


def _reported_version(server: Path) -> str:
    """Return the version token redis-server reports, or an empty string."""
    if not server.is_file():
        return ""
    result = subprocess.run(  # absolute path to the installed pinned build
        [str(server), "--version"],
        capture_output=True,
        text=True,
        timeout=30,
        env=BUILD_ENVIRONMENT,
        check=False,
    )
    for field in result.stdout.split():
        if field.startswith("v="):
            return field.removeprefix("v=")
    return ""


def _target_platform() -> str:
    if sys.platform != "linux":
        raise SystemExit("The pinned Redis build is verified on Linux only; use Linux CI or moo")
    machine = platform.machine()
    target = ARCHITECTURES.get(machine.lower())
    if target is None:
        raise SystemExit("Unsupported architecture for the pinned Redis build: " + machine)
    return target


def main(argv: list[str] | None = None) -> int:
    """Download, verify, build, and install the pinned Redis release."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, help="install directory (default: .tools/redis)")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    pin = json.loads((root / "toolchains/redis.json").read_text())
    target = _target_platform()
    if target not in pin["platforms"]:
        raise SystemExit("The Redis pin does not list " + target)
    directory = (args.output or root / ".tools/redis").resolve()
    stamp = directory / "build.json"
    expected = {"version": pin["version"], "sha256": pin["sha256"], "platform": target}
    if (
        stamp.is_file()
        and json.loads(stamp.read_text()) == expected
        and _reported_version(directory / "bin/redis-server") == pin["version"]
    ):
        print("Pinned Redis " + pin["version"] + " is already installed.")
        return 0
    with tempfile.TemporaryDirectory(prefix="gcp-local-redis-") as scratch_name:
        scratch = Path(scratch_name)
        archive = scratch / "source.tar.gz"
        _download(pin, archive)
        source = _extract(archive, scratch / "tree", pin["version"])
        jobs = str(max(1, min(os.cpu_count() or 1, 8)))
        subprocess.run(  # fixed make invocation over the verified source tree
            ["make", "-j", jobs, *pin["make_arguments"]],  # noqa: S607 -- make comes from the fixed PATH
            cwd=source,
            env=BUILD_ENVIRONMENT,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        binaries = directory / "bin"
        binaries.mkdir(parents=True, exist_ok=True)
        for name in pin["outputs"]:
            candidate = binaries / (name + ".partial")
            shutil.copyfile(source / "src" / name, candidate)
            candidate.chmod(0o755)
            candidate.replace(binaries / name)
    if _reported_version(directory / "bin/redis-server") != pin["version"]:
        raise SystemExit("Installed redis-server does not report the pinned version")
    stamp.write_text(json.dumps(expected, indent=2) + "\n")
    print("Installed Redis " + pin["version"] + " (" + target + ") in " + str(directory))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
