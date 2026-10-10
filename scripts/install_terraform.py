#!/usr/bin/env python3
"""Install checksum-pinned Terraform and a local Google provider mirror."""

import hashlib
import io
import json
import platform
import tempfile
import zipfile
from pathlib import Path
from urllib.request import urlopen


def main():
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads((root / "toolchains/terraform.json").read_text())
    arch = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64"}.get(platform.machine())
    target = platform.system().lower() + "_" + str(arch)
    tools = root / ".tools/terraform"
    for key in ("terraform", "google"):
        entry = manifest[key]
        archive = entry["archives"].get(target)
        if archive is None:
            raise SystemExit("Unsupported Terraform platform: " + target)
        directory = (
            tools / "bin"
            if key == "terraform"
            else tools / "mirror/registry.terraform.io/hashicorp/google" / entry["version"] / target
        )
        receipt = directory / ".verified.json"
        if receipt.is_file():
            saved = json.loads(receipt.read_text())
            executable = directory / saved["filename"]
            if (
                saved["archive_sha256"] == archive["sha256"]
                and executable.is_file()
                and hashlib.sha256(executable.read_bytes()).hexdigest() == saved["binary_sha256"]
            ):
                continue
        with urlopen(archive["url"], timeout=60) as response:
            data = response.read(300 * 1024 * 1024 + 1)
        if len(data) > 300 * 1024 * 1024 or hashlib.sha256(data).hexdigest() != archive["sha256"]:
            raise SystemExit("Terraform archive failed size/checksum verification")
        directory.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(io.BytesIO(data)) as package:
            prefix = "terraform-provider-google_" if key == "google" else "terraform"
            executables = [
                info
                for info in package.infolist()
                if info.filename.startswith(prefix) and "/" not in info.filename
            ]
            if len(executables) != 1 or executables[0].file_size > 1024 * 1024 * 1024:
                raise SystemExit("Unexpected Terraform archive layout")
            item = executables[0]
            with tempfile.NamedTemporaryFile(dir=directory, delete=False) as output:
                binary = package.read(item)
                output.write(binary)
                candidate = Path(output.name)
            candidate.chmod(0o755)
            candidate.replace(directory / item.filename)
        receipt.write_text(
            json.dumps(
                {
                    "archive_sha256": archive["sha256"],
                    "filename": item.filename,
                    "binary_sha256": hashlib.sha256(binary).hexdigest(),
                },
                indent=2,
            )
            + "\n"
        )
    print("Installed pinned Terraform and Google provider mirror in .tools/terraform.")


if __name__ == "__main__":
    main()
