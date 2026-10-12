#!/usr/bin/env python3
"""Check repository documentation, YAML, and discoverable skill metadata."""

import re
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gcp_local.bootstrap import SERVICES

GIT = shutil.which("git") or "git"

ROOT = Path(__file__).resolve().parents[1]


def check_documents(errors: list[str], paths: list[Path]) -> None:
    """Require the root documents and the hygiene of every text file."""
    errors.extend(
        f"Missing required document: {relative}"
        for relative in (
            "AGENTS.md",
            "README.md",
            "CONTRIBUTING.md",
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
    for kind, required in (
        ("recording", names("tests/conformance/recordings")),
        ("Terraform case", selection["test_ids"]),
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
    if "docs/coverage.md" not in (ROOT / "README.md").read_text():
        errors.append("README.md: must link docs/coverage.md")


def check_tooling_index(errors: list[str]) -> None:
    """Require CONTRIBUTING.md to name every script and requirements file."""
    index = (ROOT / "CONTRIBUTING.md").read_text()
    paths = [*(ROOT / "scripts").iterdir(), *ROOT.glob("requirements*.txt")]
    errors.extend(
        f"CONTRIBUTING.md: {path.relative_to(ROOT)} is not listed in the tooling index"
        for path in sorted(paths)
        if path.is_file() and not re.search(rf"(?<![\w-]){re.escape(path.name)}\b", index)
    )


MAX_TRACKED_BYTES = 1_000_000
GO_FUNCTION = re.compile(r"^func (\w+)\(([^)]*)\)([^{\n]*)\{\n(.*?)^\}", re.MULTILINE | re.DOTALL)
# Functions whose text matches across providers but whose types are each
# package's own, so they are not the same helper.
DISTINCT_PER_PROVIDER = {"driverOf": "asserts the package's own driver interface"}


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


def check_provider_duplicates(errors: list[str]) -> None:
    """Fail when an identical function is declared in two providers."""
    seen: dict[str, set[str]] = {}
    for path in sorted((ROOT / "internal/providers").glob("*/*.go")):
        if path.parent.name == "provider" or path.name.endswith("_test.go"):
            continue
        for match in GO_FUNCTION.finditer(path.read_text()):
            name, params, results, body = match.groups()
            if name in DISTINCT_PER_PROVIDER:
                continue
            key = " ".join(f"{params}{results}{{{body}".split())
            seen.setdefault(key, set()).add(f"{path.parent.name}.{name}")
    errors.extend(
        "identical function in several providers; move it to internal/providers/provider: "
        + ", ".join(sorted(names))
        for names in seen.values()
        if len({name.split(".")[0] for name in names}) > 1
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
