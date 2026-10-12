#!/usr/bin/env python3
"""Install checksum-pinned Node.js, gcloud CLI components, and rclone for the client matrix."""

import hashlib
import io
import json
import platform
import posixpath
import shutil
import tarfile
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from urllib.request import urlopen

LIMIT = 200 * 1024 * 1024


def download(url: str, sha256: str) -> bytes:
    """Fetch a pinned archive and verify its checksum."""
    with urlopen(url, timeout=120) as response:  # noqa: S310 -- pinned https release URL
        data = response.read(LIMIT + 1)
    if len(data) > LIMIT or hashlib.sha256(data).hexdigest() != sha256:
        raise SystemExit("Archive failed size/checksum verification: " + url)
    return data


def safe_name(name: str, strip: int) -> PurePosixPath | None:
    """Return a relative member path with leading components removed, or None to skip it."""
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts:
        raise SystemExit("Unsafe archive member: " + name)
    parts = path.parts[strip:]
    return PurePosixPath(*parts) if parts else None


def extract_tar(data: bytes, destination: Path, strip: int) -> None:
    """Extract files, directories, and in-tree symlinks; reject anything else."""
    with tarfile.open(fileobj=io.BytesIO(data)) as archive:
        for member in archive.getmembers():
            relative = safe_name(member.name, strip)
            if relative is None:
                continue
            target = destination / relative
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.issym():
                resolved = posixpath.normpath(posixpath.join(relative.parent, member.linkname))
                if member.linkname.startswith("/") or resolved.split("/")[0] == "..":
                    raise SystemExit("Unsafe archive link: " + member.name)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.symlink_to(member.linkname)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                source = archive.extractfile(member)
                if source is None:
                    raise SystemExit("Unreadable archive member: " + member.name)
                target.write_bytes(source.read())
                target.chmod(0o755 if member.mode & 0o111 else 0o644)
            else:
                raise SystemExit("Unexpected archive member type: " + member.name)


def install(directory: Path, key: str, digest: str, unpack) -> None:  # noqa: ANN001 -- callable
    """Unpack into a fresh directory unless a receipt shows the same archive."""
    receipt = directory / ".verified.json"
    if receipt.is_file() and json.loads(receipt.read_text()).get(key) == digest:
        return
    staging = Path(tempfile.mkdtemp(dir=directory.parent, prefix="." + directory.name + "-"))
    unpack(staging)
    (staging / ".verified.json").write_text(json.dumps({key: digest}) + "\n")
    if directory.exists():
        shutil.rmtree(directory)
    staging.replace(directory)


def main() -> None:
    """Install every client tool for this platform under .tools/clients."""
    root = Path(__file__).resolve().parents[2]
    manifest = json.loads((root / "toolchains/clients.json").read_text())
    arch = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64"}.get(platform.machine())
    target = platform.system().lower() + "_" + str(arch)
    tools = root / ".tools/clients"
    tools.mkdir(parents=True, exist_ok=True)
    for name in ("node", "rclone"):
        archive = manifest[name]["archives"].get(target)
        if archive is None:
            raise SystemExit(f"Unsupported {name} platform: {target}")

        def unpack(staging: Path, archive: dict = archive, name: str = name) -> None:
            data = download(archive["url"], archive["sha256"])
            if name == "node":
                extract_tar(data, staging, strip=1)
                return
            with zipfile.ZipFile(io.BytesIO(data)) as package:
                binaries = [
                    info for info in package.infolist() if info.filename.endswith("/rclone")
                ]
                if len(binaries) != 1:
                    raise SystemExit("Unexpected rclone archive layout")
                (staging / "bin").mkdir()
                (staging / "bin/rclone").write_bytes(package.read(binaries[0]))
                (staging / "bin/rclone").chmod(0o755)

        install(tools / name, "sha256", archive["sha256"], unpack)
    components = manifest["gcloud"]["components"]

    def unpack_gcloud(staging: Path) -> None:
        sdk = staging / "google-cloud-sdk"
        sdk.mkdir()
        for component in components:
            extract_tar(download(component["url"], component["sha256"]), sdk, strip=0)

    digest = hashlib.sha256("".join(c["sha256"] for c in components).encode()).hexdigest()
    install(tools / "gcloud", "components", digest, unpack_gcloud)
    print(
        f"Installed Node.js {manifest['node']['version']}, rclone {manifest['rclone']['version']}, "
        f"and gcloud {manifest['gcloud']['version']} in .tools/clients."
    )


if __name__ == "__main__":
    main()
