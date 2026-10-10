#!/usr/bin/env python3
"""Check repository documentation, YAML, and discoverable skill metadata."""

import re
import subprocess
import shutil
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

import yaml

GIT = shutil.which("git") or "git"

ROOT = Path(__file__).resolve().parents[1]


def main():
    errors = []
    files = (
        subprocess.check_output(
            [GIT, "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=ROOT,
        )
        .decode()
        .split("\0")
    )
    paths = [ROOT / name for name in sorted(set(files)) if name]
    for relative in (
        "AGENTS.md",
        "README.md",
        "CONTRIBUTING.md",
        "docs/design.md",
        "docs/roadmap.md",
    ):
        if not (ROOT / relative).is_file():
            errors.append(f"Missing required document: {relative}")

    for path in paths:
        if not path.is_file() or path.suffix not in {".md", ".yml", ".yaml", ".py", ".sh"}:
            continue
        content = path.read_text()
        label = str(path.relative_to(ROOT))
        if not content.endswith("\n"):
            errors.append(f"{label}: missing final newline")
        if re.search(r"[ \t]+$", content, re.MULTILINE):
            errors.append(f"{label}: trailing whitespace")
        if path.suffix in {".yml", ".yaml"}:
            try:
                yaml.safe_load(content)
            except yaml.YAMLError as exc:
                errors.append(f"{label}: invalid YAML: {exc}")
        if path.suffix == ".md":
            # Exclude fenced examples; inspect relative inline Markdown links.
            prose = re.sub(r"^```.*?^```[^\n]*$", "", content, flags=re.MULTILINE | re.DOTALL)
            for target in re.findall(r"\[[^\]]*\]\(([^)]+)\)", prose):
                target = target.strip("<>")
                parsed = urlsplit(target)
                if parsed.scheme or parsed.netloc or not parsed.path:
                    continue
                destination = (path.parent / unquote(parsed.path)).resolve()
                if not destination.exists():
                    errors.append(f"{label}: broken relative link: {target}")

    skills = sorted(
        [*(ROOT / ".agents/skills").glob("*/SKILL.md"), *(ROOT / "skills").glob("*/SKILL.md")]
    )
    if not skills:
        errors.append("No repository skills found")
    for path in skills:
        label = str(path.relative_to(ROOT))
        content = path.read_text()
        match = re.match(r"\A---\n(.*?)\n---\n", content, re.DOTALL)
        try:
            metadata = yaml.safe_load(match.group(1)) if match else None
            if not isinstance(metadata, dict):
                raise ValueError("missing YAML frontmatter mapping")
            name = metadata.get("name")
            if name != path.parent.name or not re.fullmatch(
                r"[a-z0-9]+(?:-[a-z0-9]+)*", name or ""
            ):
                raise ValueError(
                    "skill name must match its folder and use lowercase hyphenated words"
                )
            if (
                not isinstance(metadata.get("description"), str)
                or not metadata["description"].strip()
            ):
                raise ValueError("missing description")
            if "[TODO:" in content:
                raise ValueError("unfinished skill scaffold")
            ui = yaml.safe_load((path.parent / "agents/openai.yaml").read_text())["interface"]
            if not 25 <= len(ui["short_description"]) <= 64:
                raise ValueError("UI short description must have 25–64 characters")
            if f"${name}" not in ui["default_prompt"]:
                raise ValueError("UI prompt must invoke the skill by name")
        except (ValueError, TypeError, KeyError, OSError, yaml.YAMLError) as exc:
            errors.append(f"{label}: {exc}")

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Repository checks passed ({len(paths)} files, {len(skills)} skills).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
