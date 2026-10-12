#!/usr/bin/env python3
"""Install a checksum-pinned official Pub/Sub or Firestore emulator JAR, without gcloud."""

import argparse
import hashlib
import io
import json
import tarfile
import tempfile
from pathlib import Path
from urllib.request import urlopen

# Download timeout (seconds) and the largest JAR accepted from each archive.
EMULATORS = {
    "pubsub": (60, 100 * 1024 * 1024),
    "firestore": (120, 128 * 1024 * 1024),
}


def main() -> None:
    """Install the selected emulator JAR in .tools/<emulator>/emulator.jar."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("emulator", choices=sorted(EMULATORS))
    name = parser.parse_args().emulator
    timeout, limit = EMULATORS[name]
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads((root / "toolchains" / (name + ".json")).read_text())
    label = manifest["name"]
    directory = root / ".tools" / name
    output = directory / "emulator.jar"
    if (
        output.is_file()
        and hashlib.sha256(output.read_bytes()).hexdigest() == manifest["jar_sha256"]
    ):
        print(f"Pinned {label} is already installed.")
        return
    with urlopen(manifest["url"], timeout=timeout) as response:  # noqa: S310 -- pinned https release URL
        raw = response.read(manifest["size"] + 1)
    if len(raw) != manifest["size"] or hashlib.sha256(raw).hexdigest() != manifest["sha256"]:
        raise SystemExit(f"{label} archive failed size/checksum verification")
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        member = archive.getmember(manifest["jar"])
        if not member.isfile() or member.size > limit:
            raise SystemExit(f"Unexpected {label} archive layout")
        jar = archive.extractfile(member).read()
    if hashlib.sha256(jar).hexdigest() != manifest["jar_sha256"]:
        raise SystemExit(f"{label} JAR checksum mismatch")
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=directory, delete=False) as handle:
        handle.write(jar)
        candidate = Path(handle.name)
    candidate.chmod(0o644)
    candidate.replace(output)
    print(
        f"Installed {label} {manifest['version']} in .tools/{name}/emulator.jar; "
        f"Java {manifest['java_minimum']}+ is required."
    )


if __name__ == "__main__":
    main()
