#!/usr/bin/env python3
"""Preview or install the portable application skill without overwriting local edits."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
BUNDLE = Path("skills/gcp-local-setup")
DESTINATION = Path(".agents/skills/gcp-local-setup")
STAMP = ".install.json"
START, END = "<!-- gcp-local-setup:start -->", "<!-- gcp-local-setup:end -->"
ENTRY = (START + "\nFor GCP application validation setup, read "
         "[gcp-local-setup](.agents/skills/gcp-local-setup/SKILL.md).\n" + END + "\n")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def safe_path(root, relative):
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("Installation paths must stay inside the application repository")
    path = root
    for part in relative.parts:
        path = path / part
        if path.is_symlink():
            raise ValueError("Refusing a symlink in the installation path")
    return path


def install(repository, *, apply=False):
    root = Path(repository).absolute()
    if root.is_symlink() or not root.is_dir() or not (root / ".git").exists():
        raise ValueError("Select an existing application Git repository root")
    root = root.resolve()
    git_root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], cwd=root, text=True).strip()
    if Path(git_root).resolve() != root:
        raise ValueError("Select the application repository root, not a subdirectory")
    source = ROOT / BUNDLE
    dirty = subprocess.check_output(["git", "status", "--porcelain", "--", str(BUNDLE)], cwd=ROOT, text=True)
    if dirty:
        raise ValueError("Commit the skill bundle before installation so its source revision is reproducible")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    files = {}
    tracked = subprocess.check_output(["git", "ls-files", "-z", "--", str(BUNDLE)], cwd=ROOT).decode().split("\0")
    for relative in sorted(path for path in tracked if path):
        path = safe_path(ROOT, Path(relative))
        files[path.relative_to(source).as_posix()] = path.read_bytes()
    if "SKILL.md" not in files:
        raise ValueError("Incomplete source skill bundle")
    destination = safe_path(root, DESTINATION)
    hashes = {name: digest(data) for name, data in files.items()}
    previous = {}
    if destination.exists():
        stamp = safe_path(root, DESTINATION / STAMP)
        if not stamp.is_file():
            raise ValueError("Existing skill is unmanaged; refusing to overwrite it")
        installed = json.loads(stamp.read_text())
        if not isinstance(installed, dict) or installed.get("version") != 1 or not isinstance(installed.get("files"), dict):
            raise ValueError("Invalid installation manifest; refusing to overwrite the skill")
        previous = installed["files"]
        for name, expected in previous.items():
            path = safe_path(root, DESTINATION / name)
            if not path.is_file() or digest(path.read_bytes()) != expected:
                raise ValueError("Installed skill has local edits; preserve or reconcile them before updating")
        for path in destination.rglob("*"):
            if path.is_symlink():
                raise ValueError("Installed skill contains a symlink")
            if path.is_file() and path.relative_to(destination).as_posix() != STAMP and path.relative_to(destination).as_posix() not in previous:
                raise ValueError("Installed skill contains unmanaged files; refusing to overwrite them")
    agents = safe_path(root, Path("AGENTS.md"))
    original_agents = agents.read_text() if agents.exists() else ""
    if START in original_agents or END in original_agents:
        if original_agents.count(START) != 1 or original_agents.count(END) != 1 or ENTRY not in original_agents:
            raise ValueError("The installed AGENTS.md entry was edited; reconcile it before updating")
        updated_agents = original_agents
    else:
        updated_agents = original_agents + ("\n" if original_agents.endswith("\n") else "\n\n" if original_agents else "") + ENTRY
    manifest = {"version": 1, "source_repository": "https://github.com/evanmarshall/gcp-local",
                "source_revision": revision, "files": hashes}
    writes = {DESTINATION / name: data for name, data in files.items()
              if not safe_path(root, DESTINATION / name).is_file()
              or safe_path(root, DESTINATION / name).read_bytes() != data}
    stamp_content = (json.dumps(manifest, indent=2) + "\n").encode()
    stamp = safe_path(root, DESTINATION / STAMP)
    if not stamp.is_file() or stamp.read_bytes() != stamp_content:
        writes[DESTINATION / STAMP] = stamp_content
    if updated_agents != original_agents:
        writes[Path("AGENTS.md")] = updated_agents.encode()
    removes = [DESTINATION / name for name in previous if name not in files]
    plan = {"mode": "apply" if apply else "preview", "source_revision": revision,
            "write": sorted(str(path) for path in writes), "remove": sorted(str(path) for path in removes)}
    if apply:
        for relative, data in writes.items():
            path = safe_path(root, relative)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        for relative in removes:
            safe_path(root, relative).unlink()
    return plan


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = install(args.repo, apply=args.apply)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, str(error) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
