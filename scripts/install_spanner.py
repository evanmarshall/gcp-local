#!/usr/bin/env python3
"""Install the checksum-pinned official Cloud Spanner emulator without Docker or gcloud.

The emulator binary and its newer libstdc++ come from the per-architecture image
manifest, fetched anonymously from the gcr.io registry API by digest. Every manifest,
layer blob, and extracted file is verified against toolchains/spanner.json.
"""

import argparse
import hashlib
import json
import platform
import shutil
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
from http.client import HTTPResponse
from pathlib import Path
from urllib.parse import urljoin, urlparse

MANIFEST_TYPE = "application/vnd.docker.distribution.manifest.v2+json"
ARCHITECTURES = {
    "x86_64": "linux/amd64",
    "amd64": "linux/amd64",
    "aarch64": "linux/arm64",
    "arm64": "linux/arm64",
}
CHUNK = 1 << 20
MAX_MANIFEST = 64 * 1024


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Surface redirects so the registry token is never forwarded to blob storage."""

    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        """Return no follow-up request so the caller sees the redirect."""
        return


_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect)


def _open(url: str, headers: dict[str, str] | None = None) -> HTTPResponse:
    """Open an https URL with no ambient proxy; follow at most three redirects without headers."""
    for _ in range(4):
        if urlparse(url).scheme != "https":
            raise SystemExit("Refusing a non-https Spanner emulator download")
        # https is enforced above.
        request = urllib.request.Request(url, headers=headers or {})  # noqa: S310
        try:
            return _OPENER.open(request, timeout=60)
        except urllib.error.HTTPError as error:
            if error.code not in {301, 302, 303, 307, 308} or not error.headers.get("Location"):
                raise
            url, headers = urljoin(url, error.headers["Location"]), None
    raise SystemExit("Too many redirects while downloading the Spanner emulator")


def _token(pin: dict) -> str:
    scope = "repository:" + pin["repository"] + ":pull"
    with _open(pin["registry"] + "/v2/token?scope=" + scope) as response:
        return json.loads(response.read(MAX_MANIFEST))["token"]


def _manifest(pin: dict, token: str, digest: str) -> dict:
    url = pin["registry"] + "/v2/" + pin["repository"] + "/manifests/" + digest
    headers = {"Authorization": "Bearer " + token, "Accept": MANIFEST_TYPE}
    with _open(url, headers) as response:
        raw = response.read(MAX_MANIFEST + 1)
    if len(raw) > MAX_MANIFEST or "sha256:" + hashlib.sha256(raw).hexdigest() != digest:
        raise SystemExit("Spanner emulator manifest failed digest verification")
    return json.loads(raw)


def _blob(pin: dict, token: str, layer: str, size: int, directory: Path) -> Path:
    """Download one layer blob into directory and verify its size and digest."""
    url = pin["registry"] + "/v2/" + pin["repository"] + "/blobs/" + layer
    path = directory / layer.removeprefix("sha256:")
    digest, total = hashlib.sha256(), 0
    headers = {"Authorization": "Bearer " + token}
    with _open(url, headers) as response, path.open("wb") as handle:
        while chunk := response.read(CHUNK):
            total += len(chunk)
            if total > size:
                raise SystemExit("Spanner emulator layer exceeds its pinned size")
            digest.update(chunk)
            handle.write(chunk)
    if total != size or "sha256:" + digest.hexdigest() != layer:
        raise SystemExit("Spanner emulator layer failed size/digest verification")
    return path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def _extract(archive_path: Path, entry: dict, target: Path) -> None:
    """Extract one regular member to target after verifying size and checksum."""
    with tarfile.open(archive_path, "r:gz") as archive:
        member = next((m for m in archive if m.name.removeprefix("./") == entry["member"]), None)
        if member is None or not member.isfile() or member.size != entry["size"]:
            raise SystemExit("Unexpected Spanner emulator layer layout for " + entry["member"])
        source = archive.extractfile(member)
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as handle:
            shutil.copyfileobj(source, handle, CHUNK)
            candidate = Path(handle.name)
    if _sha256(candidate) != entry["sha256"]:
        candidate.unlink()
        raise SystemExit("Spanner emulator file checksum mismatch: " + entry["install"])
    candidate.chmod(int(entry["mode"], 8))
    candidate.replace(target)


def _installed(directory: Path, files: list[dict]) -> bool:
    return all(
        (directory / f["install"]).is_file() and _sha256(directory / f["install"]) == f["sha256"]
        for f in files
    )


def main(argv: list[str] | None = None) -> int:
    """Install the pinned emulator files for one Linux architecture."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--platform",
        choices=["linux/amd64", "linux/arm64"],
        help="target platform (default: this Linux host)",
    )
    parser.add_argument("--output", type=Path, help="install directory (default: .tools/spanner)")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    pin = json.loads((root / "toolchains/spanner.json").read_text())
    target = args.platform
    if target is None:
        if sys.platform != "linux":
            raise SystemExit(
                "The Spanner emulator runs only on Linux; use the packaged Docker image or Linux CI"
            )
        target = ARCHITECTURES.get(platform.machine().lower())
        if target is None:
            machine = platform.machine()
            raise SystemExit("Unsupported architecture for the Spanner emulator: " + machine)
    selected = pin["platforms"][target]
    directory = (args.output or root / ".tools/spanner").resolve()
    if _installed(directory, selected["files"]):
        print("Pinned Spanner emulator " + pin["version"] + " is already installed.")
        return 0
    token = _token(pin)
    manifest = _manifest(pin, token, selected["manifest"])
    layers = {layer["digest"]: layer["size"] for layer in manifest["layers"]}
    with tempfile.TemporaryDirectory(prefix="gcp-local-spanner-") as scratch:
        for entry in selected["files"]:
            if layers.get(entry["layer"]) != entry["layer_size"]:
                raise SystemExit(
                    "Pinned layer is absent from the Spanner emulator manifest: " + entry["layer"]
                )
            blob = _blob(pin, token, entry["layer"], entry["layer_size"], Path(scratch))
            _extract(blob, entry, directory / entry["install"])
            blob.unlink()
    print("Installed Spanner emulator " + pin["version"] + " (" + target + ") in " + str(directory))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
