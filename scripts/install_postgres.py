#!/usr/bin/env python3
"""Install the checksum-pinned PostgreSQL server binaries for the Cloud SQL backend.

Each Maven Central artifact, the archive inside it, and the extracted server binaries
are verified against toolchains/postgres.json before the tree is moved into place.
"""

import argparse
import hashlib
import io
import json
import platform
import posixpath
import shutil
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from urllib.parse import urlparse

ARCHITECTURES = {
    "x86_64": "linux/amd64",
    "amd64": "linux/amd64",
    "aarch64": "linux/arm64",
    "arm64": "linux/arm64",
}
CHUNK = 1 << 20
MAX_TREE = 512 << 20
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def _download(pin: dict, target: Path) -> None:
    """Download one artifact over https with no ambient proxy and verify size and digest."""
    if urlparse(pin["url"]).scheme != "https":
        raise SystemExit("Refusing a non-https PostgreSQL download")
    # Maven Central's CDN has returned transient 404s for pinned artifacts;
    # retry the fetch a few times before failing the install.
    for attempt in range(4):
        try:
            digest, total = _fetch(pin, target)
            break
        except urllib.error.URLError:
            if attempt == 3:
                raise
            time.sleep(5 * (attempt + 1))
    if total != pin["size"] or digest != pin["sha256"]:
        raise SystemExit("PostgreSQL artifact failed size/digest verification")


def _fetch(pin: dict, target: Path) -> tuple[str, int]:
    """Stream one artifact to target, refusing more than its pinned size."""
    digest, total = hashlib.sha256(), 0
    with _OPENER.open(pin["url"], timeout=120) as response, target.open("wb") as handle:
        while chunk := response.read(CHUNK):
            total += len(chunk)
            if total > pin["size"]:
                raise SystemExit("PostgreSQL artifact exceeds its pinned size")
            digest.update(chunk)
            handle.write(chunk)
    return digest.hexdigest(), total


def _archive(jar: Path, pin: dict) -> bytes:
    """Return the pinned archive member of the artifact after verifying it."""
    with zipfile.ZipFile(jar) as artifact:
        info = artifact.getinfo(pin["archive"])
        if info.file_size != pin["archive_size"]:
            raise SystemExit("PostgreSQL archive size does not match the pin")
        raw = artifact.read(info)
    if hashlib.sha256(raw).hexdigest() != pin["archive_sha256"]:
        raise SystemExit("PostgreSQL archive checksum mismatch")
    return raw


def _safe_name(name: str) -> str:
    clean = posixpath.normpath(name.removeprefix("./"))
    if clean.startswith(("/", "..")) or clean == ".":
        raise SystemExit("Unsafe PostgreSQL archive member: " + name)
    return clean


def _extract(raw: bytes, directory: Path) -> None:
    """Extract only directories, regular files, and in-tree relative symlinks."""
    total = 0
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:xz") as archive:
        for member in archive:
            name = _safe_name(member.name)
            target = directory / name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.issym():
                link = posixpath.normpath(posixpath.join(posixpath.dirname(name), member.linkname))
                if member.linkname.startswith("/") or link.startswith(".."):
                    raise SystemExit("Unsafe PostgreSQL archive link: " + member.name)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.symlink_to(member.linkname)
            elif member.isfile():
                total += member.size
                if total > MAX_TREE:
                    raise SystemExit("PostgreSQL archive exceeds the extraction bound")
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as source, target.open("wb") as handle:
                    shutil.copyfileobj(source, handle, CHUNK)
                target.chmod(0o755 if member.mode & 0o111 else 0o644)
            else:
                raise SystemExit("Unexpected PostgreSQL archive member type: " + member.name)


def _installed(directory: Path, binaries: dict) -> bool:
    return all(
        (directory / name).is_file() and _sha256(directory / name) == digest
        for name, digest in binaries.items()
    )


def install(version: str, pin: dict, directory: Path) -> None:
    """Install one pinned major version into directory, replacing it atomically."""
    if _installed(directory, pin["binaries"]):
        print("Pinned PostgreSQL " + version + " is already installed.")
        return
    directory.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="gcp-local-postgres-", dir=directory.parent) as scratch:
        jar = Path(scratch) / "artifact.jar"
        _download(pin, jar)
        tree = Path(scratch) / "tree"
        tree.mkdir()
        _extract(_archive(jar, pin), tree)
        if not _installed(tree, pin["binaries"]):
            raise SystemExit("Extracted PostgreSQL binaries do not match the pin")
        if directory.exists():
            shutil.rmtree(directory)
        tree.replace(directory)
    print("Installed PostgreSQL " + version + " in " + str(directory))


def main(argv: list[str] | None = None) -> int:
    """Install every pinned major version for one Linux architecture."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--platform",
        choices=["linux/amd64", "linux/arm64"],
        help="target platform (default: this Linux host)",
    )
    parser.add_argument("--output", type=Path, help="install root (default: .tools/postgres)")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    pins = json.loads((root / "toolchains/postgres.json").read_text())
    target = args.platform
    if target is None:
        if sys.platform != "linux":
            raise SystemExit("The Cloud SQL PostgreSQL backend runs only on Linux")
        target = ARCHITECTURES.get(platform.machine().lower())
        if target is None:
            raise SystemExit("Unsupported architecture for PostgreSQL: " + platform.machine())
    output = (args.output or root / ".tools/postgres").resolve()
    for name, entry in pins["versions"].items():
        major = name.removeprefix("POSTGRES_")
        install(entry["version"], entry["platforms"][target], output / major)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
