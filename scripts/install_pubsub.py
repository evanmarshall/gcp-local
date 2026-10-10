#!/usr/bin/env python3
"""Install the checksum-pinned official Pub/Sub emulator JAR, without gcloud."""

import hashlib
import io
import json
import tarfile
import tempfile
from pathlib import Path
from urllib.request import urlopen


def main() -> int:
    """Install the checksum-pinned official Pub/Sub emulator JAR."""
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads((root / "toolchains/pubsub.json").read_text())
    directory = root / ".tools/pubsub"
    output = directory / "emulator.jar"
    if (
        output.is_file()
        and hashlib.sha256(output.read_bytes()).hexdigest() == manifest["jar_sha256"]
    ):
        print("Pinned Pub/Sub emulator is already installed.")
        return
    with urlopen(manifest["url"], timeout=60) as response:  # noqa: S310 -- pinned https release URL
        raw = response.read(manifest["size"] + 1)
    if len(raw) != manifest["size"] or hashlib.sha256(raw).hexdigest() != manifest["sha256"]:
        raise SystemExit("Pub/Sub emulator archive failed size/checksum verification")
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        member = archive.getmember(manifest["jar"])
        if not member.isfile() or member.size > 100 * 1024 * 1024:
            raise SystemExit("Unexpected Pub/Sub emulator archive layout")
        jar = archive.extractfile(member).read()
    if hashlib.sha256(jar).hexdigest() != manifest["jar_sha256"]:
        raise SystemExit("Pub/Sub emulator JAR checksum mismatch")
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=directory, delete=False) as handle:
        handle.write(jar)
        candidate = Path(handle.name)
    candidate.chmod(0o644)
    candidate.replace(output)
    print(
        "Installed Pub/Sub emulator "
        + manifest["version"]
        + " in .tools/pubsub/emulator.jar; Java 11+ is required."
    )


if __name__ == "__main__":
    main()
