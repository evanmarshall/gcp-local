#!/usr/bin/env python3
"""Check repository documentation, YAML, and discoverable skill metadata."""

import re
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

import yaml

ROOT = Path(__file__).resolve().parents[2]
# client_matrix lives beside the client runner in scripts/run.
sys.path[:0] = [str(ROOT), str(ROOT / "scripts/run")]
import client_matrix  # noqa: E402 -- needs scripts/run on sys.path
import layout  # noqa: E402 -- sits beside this script in scripts/check

from gcp_local.bootstrap import SERVICES  # noqa: E402 -- needs the repository root on sys.path

ISSUE_LINK = "](" + client_matrix.ISSUES

GIT = shutil.which("git") or "git"
CONTRIBUTING = ".github/CONTRIBUTING.md"


def check_documents(errors: list[str], paths: list[Path]) -> None:
    """Require the root documents and the hygiene of every text file."""
    errors.extend(
        f"Missing required document: {relative}"
        for relative in (
            "AGENTS.md",
            "README.md",
            CONTRIBUTING,
            "docs/design.md",
        )
        if not (ROOT / relative).is_file()
    )
    for path in paths:
        if not path.is_file() or path.suffix not in {".md", ".yml", ".yaml", ".py", ".sh"}:
            continue
        content = path.read_text()
        label = str(path.relative_to(ROOT))
        if not content.endswith("\n"):
            errors.append(f"{label}: missing final newline")
        if re.search(r"[ \t]+$", content, re.MULTILINE):
            errors.append(f"{label}: trailing whitespace")
        check_yaml_and_links(errors, path, content, label)


def check_yaml_and_links(errors: list[str], path: Path, content: str, label: str) -> None:
    """Validate YAML files parse and Markdown relative links resolve."""
    if path.suffix in {".yml", ".yaml"}:
        try:
            yaml.safe_load(content)
        except yaml.YAMLError as exc:
            errors.append(f"{label}: invalid YAML: {exc}")
    if path.suffix != ".md":
        return
    # Exclude fenced examples; inspect relative inline Markdown links.
    prose = re.sub(r"^```.*?^```[^\n]*$", "", content, flags=re.MULTILINE | re.DOTALL)
    for raw in re.findall(r"\[[^\]]*\]\(([^)]+)\)", prose):
        target = raw.strip("<>")
        parsed = urlsplit(target)
        if parsed.scheme or parsed.netloc or not parsed.path:
            continue
        destination = (path.parent / unquote(parsed.path)).resolve()
        if not destination.exists():
            errors.append(f"{label}: broken relative link: {target}")


def check_coverage(errors: list[str]) -> None:
    """Require docs/coverage.md to list every service, recording, and Terraform case."""
    coverage = (ROOT / "docs/coverage.md").read_text()
    rows = {
        match.group(1): match.group(0)
        for match in re.finditer(r"^\| `([a-z]+)` \|.*$", coverage, re.MULTILINE)
    }
    label = "docs/coverage.md"
    errors.extend(f"{label}: no row for service {name}" for name in SERVICES if name not in rows)
    errors.extend(f"{label}: unknown service {name}" for name in rows if name not in SERVICES)
    listed = "\n".join(rows.values())

    def names(directory: str) -> list[str]:
        return sorted(path.name for path in (ROOT / directory).iterdir() if path.is_dir())

    selection = yaml.safe_load((ROOT / "tests/terraform/selection.json").read_text())
    upstream = ROOT / "third_party/terraform-provider-google/manifest.json"
    extracted = yaml.safe_load(upstream.read_text())["functions"]
    tf_label = "tests/terraform/selection.json"
    if set(selection["upstream_test_ids"]) != {n for n in extracted if n.startswith("Test")}:
        errors.append(f"{tf_label}: upstream_test_ids must match the extracted functions")
    errors.extend(
        f"{tf_label}: local case {name} must start with TestLocal"
        for name in selection["local_test_ids"]
        if not name.startswith("TestLocal")
    )
    for kind, required in (
        ("recording", sorted(layout.replay.recordings(ROOT))),
        ("Terraform case", selection["upstream_test_ids"] + selection["local_test_ids"]),
    ):
        errors.extend(
            f"{label}: {kind} {name} missing from the summary table"
            for name in required
            if not re.search(rf"[ ,]{re.escape(name)}[ ,|]", listed)
        )
    errors.extend(
        f"{label}: permission probe {name} not listed"
        for name in names("tests/permission/recordings")
        if name not in coverage
    )
    clients = coverage.split("## Client compatibility", 1)[-1].split("\n## ", 1)[0]
    errors.extend(
        f"{label}: client test {path} does not exist"
        for path in re.findall(r"^\|[^\n]*\| `([^`]+)` \|$", clients, re.MULTILINE)
        if not (ROOT / path).exists()
    )
    check_client_matrix(errors, clients)
    if "docs/coverage.md" not in (ROOT / "README.md").read_text():
        errors.append("README.md: must link docs/coverage.md")


def check_client_matrix(errors: list[str], section: str) -> None:
    """Require the client compatibility tables to list exactly the matrix's combinations."""
    rows, failures = client_matrix.coverage_lines(client_matrix.load())
    lines = set(section.splitlines())
    documented = {
        "row": {line for line in lines if "| `tests/clients/" in line},
        "known failure": {line for line in lines if line.startswith("|") and ISSUE_LINK in line},
    }
    label = "docs/coverage.md"
    for kind, expected in (("row", set(rows)), ("known failure", set(failures))):
        errors.extend(
            f"{label}: missing client {kind}: {line}"
            for line in sorted(expected - documented[kind])
        )
        errors.extend(
            f"{label}: client {kind} not in tests/clients/matrix.json: {line}"
            for line in sorted(documented[kind] - expected)
        )


def check_tooling_index(errors: list[str]) -> None:
    """Require CONTRIBUTING.md to name every script and requirements file by its path.

    Scripts may be named relative to scripts/ (check/repo.py) or in full.
    """
    index = (ROOT / CONTRIBUTING).read_text()
    names = [
        (path, path.relative_to(ROOT / "scripts").as_posix())
        for path in (ROOT / "scripts").rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    ]
    names += [(path, path.relative_to(ROOT).as_posix()) for path in ROOT.glob("requirements/*.txt")]
    errors.extend(
        f"{CONTRIBUTING}: {path.relative_to(ROOT)} is not listed in the tooling index"
        for path, name in sorted(names)
        if not re.search(rf"(?<![\w./-])(?:scripts/)?{re.escape(name)}(?![\w/-])", index)
    )


MAX_TRACKED_BYTES = 1_000_000
# A function or method with a block or one-line body, and a type with a block
# or one-line definition.
GO_FUNCTION = re.compile(
    r"^func (?:\((\w+ )?\*?(\w+)\) )?(\w+)\(([^)]*)\)([^{\n]*)\{(?:\n(.*?)^\}| ([^\n]*) \}$)",
    re.MULTILINE | re.DOTALL,
)
GO_TYPE = re.compile(
    r"^type (\w+) (?:((?:struct|interface) \{)\n(.*?)^\}|([^\n]*)$)", re.MULTILINE | re.DOTALL
)
# Declarations whose text matches across providers but which are each
# package's own, with the reason. A key is a name ("Receiver.name" for
# methods) or "package.name" to exempt one package's copy.
DISTINCT_PER_PROVIDER = {
    "driverOf": "asserts the package's own driver interface",
    "driver": "the package's backend contract; driverOf asserts it",
    "Provider": "each package's exported provider; equal fields, different methods",
    "emulator": "wraps the package's own emulator Config",
    "emulator.Ready": "probes the package's own emulator process",
    "emulator.Reset": "restarts the package's own emulator",
    "emulator.client": "returns the package's own emulator connection",
    "fleet.Reset": "stops the package's own server fleet",
}


def check_tracked_files(errors: list[str], paths: list[Path]) -> None:
    """Reject committed binaries, non-script executables, and large files."""
    modes = subprocess.check_output([GIT, "ls-files", "-s"], cwd=ROOT).decode().splitlines()
    executable = {line.split("\t", 1)[1] for line in modes if line.startswith("100755")}
    for path in paths:
        if not path.is_file():
            continue
        label = str(path.relative_to(ROOT))
        size = path.stat().st_size
        if size > MAX_TRACKED_BYTES:
            errors.append(f"{label}: {size} bytes; keep tracked files under {MAX_TRACKED_BYTES}")
        head = path.read_bytes()[:8192]
        if b"\0" in head:
            errors.append(f"{label}: binary file; build it instead of committing it")
        elif label in executable and not head.startswith(b"#!"):
            errors.append(f"{label}: executable without a shebang")


def go_declarations(source: str) -> list[tuple[str, str]]:
    """Return (label, comparable text) for each Go function, method, and type.

    A method's receiver variable becomes a placeholder, so the same body on
    two packages' receivers compares equal.
    """
    found = []
    for match in GO_FUNCTION.finditer(source):
        receiver, receiver_type, name, params, results, block, line = match.groups()
        body = block if block is not None else line
        if receiver:
            body = re.sub(rf"\b{receiver.strip()}\b", "RECEIVER", body)
        label = f"{receiver_type}.{name}" if receiver_type else name
        found.append((label, f"func {label}({params}){results}{{{body}"))
    for match in GO_TYPE.finditer(source):
        name, opener, block, line = match.groups()
        found.append((name, f"type {name} {opener}{block}" if opener else f"type {name} {line}"))
    return found


def check_provider_duplicates(errors: list[str]) -> None:
    """Fail when an identical function, method, or type is declared in two provider packages.

    Provider packages sit at any depth under internal/providers (databases/spanner,
    storage/xmlapi); exemptions name the package by its directory name.
    """
    providers = ROOT / "internal/providers"
    seen: dict[str, set[str]] = {}
    used: set[str] = set()
    for path in sorted(providers.rglob("*.go")):
        package = path.parent.relative_to(providers).as_posix()
        if package.split("/")[0] == "provider" or path.name.endswith("_test.go"):
            continue
        for label, text in go_declarations(path.read_text()):
            exempt = {label, f"{path.parent.name}.{label}"} & DISTINCT_PER_PROVIDER.keys()
            if exempt:
                used |= exempt
                continue
            seen.setdefault(" ".join(text.split()), set()).add(f"{package}.{label}")
    errors.extend(
        f"scripts/check/repo.py: stale duplicate exemption {key}"
        for key in sorted(DISTINCT_PER_PROVIDER.keys() - used)
    )
    errors.extend(
        "identical declaration in several providers; move it to internal/providers/provider: "
        + ", ".join(sorted(names))
        for names in seen.values()
        if len({name.rsplit(".", 1)[0] for name in names}) > 1
    )


def skill_error(path: Path, content: str) -> str | None:
    """Return the first skill-metadata defect, or None when the skill is sound."""
    match = re.match(r"\A---\n(.*?)\n---\n", content, re.DOTALL)
    metadata = yaml.safe_load(match.group(1)) if match else None
    if not isinstance(metadata, dict):
        raise TypeError("missing YAML frontmatter mapping")
    name = metadata.get("name")
    if name != path.parent.name or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", name or ""):
        raise ValueError("skill name must match its folder and use lowercase hyphenated words")
    if not isinstance(metadata.get("description"), str) or not metadata["description"].strip():
        raise ValueError("missing description")
    if "[TODO:" in content:
        raise ValueError("unfinished skill scaffold")
    ui = yaml.safe_load((path.parent / "agents/openai.yaml").read_text())["interface"]
    if not 25 <= len(ui["short_description"]) <= 64:
        raise ValueError("UI short description must have 25-64 characters")
    if f"${name}" not in ui["default_prompt"]:
        raise ValueError("UI prompt must invoke the skill by name")
    return None


def check_skill(path: Path, errors: list[str]) -> None:
    """Validate one skill's frontmatter, description, and UI interface."""
    label = str(path.relative_to(ROOT))
    try:
        skill_error(path, path.read_text())
    except (ValueError, TypeError, KeyError, OSError, yaml.YAMLError) as exc:
        errors.append(f"{label}: {exc}")


def main() -> int:
    """Check repository documentation, YAML, and discoverable skill metadata."""
    errors: list[str] = []
    files = (
        subprocess.check_output(
            [GIT, "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=ROOT,
        )
        .decode()
        .split("\0")
    )
    paths = [ROOT / name for name in sorted(set(files)) if name]
    check_documents(errors, paths)
    check_tracked_files(errors, paths)
    present = [path for path in paths if path.exists()]
    layout.check_root_layout(errors, ROOT, present)
    layout.check_directory_sizes(errors, ROOT, present)
    layout.check_recordings(errors, ROOT)
    check_provider_duplicates(errors)
    check_coverage(errors)
    check_tooling_index(errors)
    skills = sorted(
        [*(ROOT / ".agents/skills").glob("*/SKILL.md"), *(ROOT / "skills").glob("*/SKILL.md")]
    )
    if not skills:
        errors.append("No repository skills found")
    for path in skills:
        check_skill(path, errors)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Repository checks passed ({len(paths)} files, {len(skills)} skills).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
