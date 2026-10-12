#!/usr/bin/env python3
"""Install the checksum-pinned Go toolchain inside this checkout, never globally."""

import hashlib
import json
import platform
import shutil
import tarfile
import tempfile
import time
from pathlib import Path
from urllib.request import urlopen

ATTEMPTS = 4


def download(url: str, archive: Path) -> None:
    """Fetch the archive, retrying transient network failures with backoff."""
    for attempt in range(1, ATTEMPTS + 1):
        try:
            with urlopen(url, timeout=60) as source, archive.open("wb") as output:  # noqa: S310 -- fixed https URL
                shutil.copyfileobj(source, output)
        except OSError as error:  # timeouts, resets, and URLError are all OSError
            if attempt == ATTEMPTS:
                raise SystemExit(f"Go download failed after {ATTEMPTS} attempts: {error}") from None
            print(f"Go download attempt {attempt} failed ({error}); retrying", flush=True)
            time.sleep(5 * attempt)
        else:
            return


def main() -> None:
    """Install the checksum-pinned Go toolchain inside this checkout."""
    root = Path(__file__).resolve().parents[2]
    manifest = json.loads((root / "toolchains/go.json").read_text())
    system = {"Darwin": "darwin", "Linux": "linux"}.get(platform.system())
    arch = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}.get(platform.machine())
    entry = next(
        (value for value in manifest["files"] if (value["os"], value["arch"]) == (system, arch)),
        None,
    )
    if entry is None:
        raise SystemExit("This toolchain manifest supports macOS arm64 and Linux arm64/amd64")
    tools = root / ".tools"
    if tools.is_symlink():
        raise SystemExit("Refusing a symlinked toolchain directory")
    tools.mkdir(exist_ok=True)
    destination = tools / "go"
    if destination.exists() or destination.is_symlink():
        marker = destination / ".gcp-local-sha256"
        if (
            not destination.is_symlink()
            and marker.is_file()
            and marker.read_text().strip() == entry["sha256"]
        ):
            print("Pinned Go toolchain already installed.")
            return
        raise SystemExit(
            "Existing .tools/go does not match this pin; move it aside before installing"
        )
    with tempfile.TemporaryDirectory(prefix="go-install-", dir=tools) as raw:
        temporary = Path(raw)
        archive = temporary / "archive.tar.gz"
        download("https://go.dev/dl/" + entry["filename"], archive)
        if hashlib.sha256(archive.read_bytes()).hexdigest() != entry["sha256"]:
            raise SystemExit("Go archive checksum mismatch")
        with tarfile.open(archive) as source:
            for member in source.getmembers():
                parts = Path(member.name).parts
                if (
                    not parts
                    or parts[0] != "go"
                    or ".." in parts
                    or not (member.isfile() or member.isdir())
                ):
                    raise SystemExit("Unexpected path or file type in Go archive")
            # members are pre-validated against traversal and file types above.
            source.extractall(temporary)  # noqa: S202 -- validated archive members
        unpacked = temporary / "go"
        if (unpacked / "VERSION").read_text().splitlines()[0] != manifest["version"]:
            raise SystemExit("Go archive version mismatch")
        (unpacked / ".gcp-local-sha256").write_text(entry["sha256"] + "\n")
        unpacked.rename(destination)
    print("Installed " + manifest["version"] + " inside .tools/go.")


if __name__ == "__main__":
    main()
