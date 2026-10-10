#!/usr/bin/env python3
"""Install the checksum-pinned Compose diagnostic plugin in an arm64 Linux guest."""

import hashlib
import os
import platform
from pathlib import Path
from urllib.request import urlopen

VERSION = "5.1.3"
SHA256 = "e8105a3e687ea7e0b0f81abe4bf9269c8a2801fb72c2b498b5ff2472bc54145f"
URL = f"https://github.com/docker/compose/releases/download/v{VERSION}/docker-compose-linux-aarch64"


def main():
    if platform.system() != "Linux" or platform.machine() != "aarch64":
        raise SystemExit("Install this diagnostic plugin only in the arm64 Moo task guest")
    path = Path("/usr/local/lib/docker/cli-plugins/docker-compose")
    if path.exists() and hashlib.sha256(path.read_bytes()).hexdigest() == SHA256:
        print("Pinned Compose diagnostic plugin is already installed.")
        return
    if path.exists() or path.is_symlink():
        raise SystemExit(
            "Existing Compose plugin differs; select a fresh task guest instead of overwriting it"
        )
    with urlopen(URL, timeout=60) as response:
        data = response.read(100 * 1024 * 1024 + 1)
    if hashlib.sha256(data).hexdigest() != SHA256:
        raise SystemExit("Compose checksum does not match the pinned release asset")
    path.parent.mkdir(parents=True, exist_ok=True)
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o755), "wb") as output:
        output.write(data)
    print("Installed Compose " + VERSION + " for task-guest diagnostics.")


if __name__ == "__main__":
    main()
