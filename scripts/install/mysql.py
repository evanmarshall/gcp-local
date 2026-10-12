#!/usr/bin/env python3
"""Install the checksum-pinned MySQL Community Server binaries for the Cloud SQL backend.

Each official generic-Linux tarball is verified against toolchains/mysql.json, only
the server and the files it loads are extracted, and the extracted ELF files are
reduced to their loadable segments before the result is checked against its pins
and moved into place.
"""

import argparse
import hashlib
import http.client
import json
import platform
import posixpath
import re
import shutil
import struct
import sys
import tarfile
import tempfile
import time
import urllib.error
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
MAX_TREE = 1 << 30
# The server, the private libraries it links (OpenSSL, protobuf-lite, Abseil), the
# ICU data REGEXP loads, the component its default manifest names, English error
# messages, and character-set definitions. Clients, tools, debug builds, test
# plugins, and the LDAP/Kerberos/FIDO client libraries are not extracted.
KEEP = re.compile(
    r"bin/mysqld"
    r"|lib/plugin/component_reference_cache\.so"
    r"|lib/private/(libssl\.so\.3|libcrypto\.so\.3|libprotobuf-lite\.so\.[0-9.]+|libabsl_[a-z0-9_]+\.so)"
    r"|lib/private/icudt[0-9]+l(/.*)?"
    r"|share/english/errmsg\.sys"
    r"|share/charsets/[A-Za-z0-9_.]+"
)
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def _download(pin: dict, target: Path) -> None:
    """Download one tarball over https with no ambient proxy and verify its digests."""
    if urlparse(pin["url"]).scheme != "https":
        raise SystemExit("Refusing a non-https MySQL download")
    # Retry transient CDN failures, including a response that ends early,
    # a few times before failing the install.
    for attempt in range(4):
        try:
            sha256, md5, total = _fetch(pin, target)
        except (urllib.error.URLError, http.client.HTTPException, ConnectionError, TimeoutError):
            if attempt == 3:
                raise
        else:
            if total == pin["size"] or attempt == 3:
                break
        time.sleep(5 * (attempt + 1))
    if total != pin["size"] or sha256 != pin["sha256"] or md5 != pin["md5"]:
        raise SystemExit("MySQL tarball failed size/digest verification")


def _fetch(pin: dict, target: Path) -> tuple[str, str, int]:
    """Stream one tarball to target, refusing more than its pinned size."""
    sha256, md5, total = hashlib.sha256(), hashlib.md5(usedforsecurity=False), 0
    with _OPENER.open(pin["url"], timeout=120) as response, target.open("wb") as handle:
        while chunk := response.read(CHUNK):
            total += len(chunk)
            if total > pin["size"]:
                raise SystemExit("MySQL tarball exceeds its pinned size")
            sha256.update(chunk)
            md5.update(chunk)
            handle.write(chunk)
    return sha256.hexdigest(), md5.hexdigest(), total


def _member_name(name: str, prefix: str) -> str | None:
    """Return the tree-relative name of a member to extract, or None to skip it."""
    clean = posixpath.normpath(name.removeprefix("./"))
    if clean.startswith(("/", "..")) or not (clean + "/").startswith(prefix + "/"):
        raise SystemExit("Unexpected MySQL archive member: " + name)
    relative = clean[len(prefix) + 1 :]
    return relative if KEEP.fullmatch(relative) else None


def _extract(tarball: Path, prefix: str, directory: Path) -> None:
    """Extract the selected regular files and in-tree relative symlinks."""
    total = 0
    with tarfile.open(tarball, mode="r:xz") as archive:
        for member in archive:
            name = _member_name(member.name, prefix)
            if name is None:
                continue
            target = directory / name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.issym():
                link = posixpath.normpath(posixpath.join(posixpath.dirname(name), member.linkname))
                if member.linkname.startswith("/") or link.startswith(".."):
                    raise SystemExit("Unsafe MySQL archive link: " + member.name)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.symlink_to(member.linkname)
            elif member.isfile():
                total += member.size
                if total > MAX_TREE:
                    raise SystemExit("MySQL archive exceeds the extraction bound")
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as source, target.open("wb") as handle:
                    shutil.copyfileobj(source, handle, CHUNK)
                target.chmod(0o755 if member.mode & 0o111 else 0o644)
            else:
                raise SystemExit("Unexpected MySQL archive member type: " + member.name)


def strip_sections(path: Path) -> None:
    """Keep only the loadable part of a 64-bit little-endian ELF file.

    The generic tarballs ship unstripped binaries: most of mysqld is DWARF debug
    information and symbol tables, which the loader never reads. This truncates the
    file after the last byte any program header covers and removes the section
    header table, as ELFkickers' sstrip does. Every loaded segment keeps its exact
    bytes, so the result is deterministic and its digest can be pinned.
    """
    data = bytearray(path.read_bytes())
    if data[:6] != b"\x7fELF\x02\x01":
        raise SystemExit("Not a 64-bit little-endian ELF file: " + path.name)
    (phoff,) = struct.unpack_from("<Q", data, 0x20)
    phentsize, phnum = struct.unpack_from("<HH", data, 0x36)
    end = phoff + phentsize * phnum
    for index in range(phnum):
        offset, size = struct.unpack_from("<Q16xQ", data, phoff + index * phentsize + 8)
        end = max(end, offset + size)
    if end > len(data):
        raise SystemExit("ELF program headers exceed the file: " + path.name)
    del data[end:]
    struct.pack_into("<Q", data, 0x28, 0)  # e_shoff
    struct.pack_into("<HHH", data, 0x3A, 0, 0, 0)  # e_shentsize, e_shnum, e_shstrndx
    path.write_bytes(bytes(data))


def _strip_tree(directory: Path) -> None:
    for path in sorted(directory.rglob("*")):
        if path.is_file() and not path.is_symlink():
            with path.open("rb") as handle:
                elf = handle.read(4) == b"\x7fELF"
            if elf:
                strip_sections(path)


def _installed(directory: Path, binaries: dict) -> bool:
    return all(
        (directory / name).is_file() and _sha256(directory / name) == digest
        for name, digest in binaries.items()
    )


def install(version: str, pin: dict, directory: Path) -> None:
    """Install one pinned version into directory, replacing it atomically."""
    if _installed(directory, pin["binaries"]):
        print("Pinned MySQL " + version + " is already installed.")
        return
    directory.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="gcp-local-mysql-", dir=directory.parent) as scratch:
        tarball = Path(scratch) / "server.tar.xz"
        _download(pin, tarball)
        tree = Path(scratch) / "tree"
        tree.mkdir()
        _extract(tarball, pin["prefix"], tree)
        tarball.unlink()
        if _sha256(tree / "bin/mysqld") != pin["upstream_mysqld_sha256"]:
            raise SystemExit("Extracted mysqld does not match the upstream pin")
        _strip_tree(tree)
        if not _installed(tree, pin["binaries"]):
            raise SystemExit("Extracted MySQL binaries do not match the pin")
        if directory.exists():
            shutil.rmtree(directory)
        tree.replace(directory)
    print("Installed MySQL " + version + " in " + str(directory))


def main(argv: list[str] | None = None) -> int:
    """Install every pinned version for one Linux architecture."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--platform",
        choices=["linux/amd64", "linux/arm64"],
        help="target platform (default: this Linux host)",
    )
    parser.add_argument("--output", type=Path, help="install root (default: .tools/mysql)")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[2]
    pins = json.loads((root / "toolchains/mysql.json").read_text())
    target = args.platform
    if target is None:
        if sys.platform != "linux":
            raise SystemExit("The Cloud SQL MySQL backend runs only on Linux")
        target = ARCHITECTURES.get(platform.machine().lower())
        if target is None:
            raise SystemExit("Unsupported architecture for MySQL: " + platform.machine())
    output = (args.output or root / ".tools/mysql").resolve()
    for name, entry in pins["versions"].items():
        major = name.removeprefix("MYSQL_").replace("_", ".")
        install(entry["version"], entry["platforms"][target], output / major)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
