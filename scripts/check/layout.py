"""Repository layout and recording-wiring checks used by scripts/check/repo.py.

Standard library only, so tests/tooling can exercise them without PyYAML.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/run"))
import recordings as replay  # noqa: E402 -- the replay plan lives beside the other runners

# Tracked top-level entries (.agents/skills/repo-layout). Each root file is one
# a tool reads only from the repository root:
ROOT_FILES = {
    ".editorconfig",  # editors look for it at the root
    ".gitignore",  # Git applies it from the root down
    "AGENTS.md",  # agents load it from the root
    "LICENSE",  # GitHub and the wheel's license-files
    "README.md",  # GitHub's landing page and the wheel's readme
    "go.mod",  # the Go module root
    "go.sum",  # must sit next to go.mod
    "moo.toml",  # moo reads it from the project root
    "pyproject.toml",  # pip, hatchling, pytest, and ruff read it from the root
}
ROOT_DIRS = {
    ".agents",
    ".githooks",
    ".github",
    "api",
    "cmd",
    "docs",
    "examples",
    "gcp_local",
    "gcp_validation",
    "internal",
    "packaging",
    "quality",
    "requirements",
    "scripts",
    "skills",
    "tests",
    "third_party",
    "toolchains",
    "tools",
}
# Every directory except the root holds at most this many files and subdirectories.
MAX_CHILDREN = 12
# The CI job that replays every recording, and the entry point it must run.
WORKFLOW = ".github/workflows/repository-checks.yml"
REPLAY = "scripts/check/recordings.sh"
REPLAY_DRIVER = "scripts/run/recordings.py"


def check_root_layout(errors: list[str], root: Path, paths: list[Path]) -> None:
    """Fail when a tracked file or directory is added at the repository root."""
    for path in paths:
        relative = path.relative_to(root)
        top = relative.parts[0]
        if len(relative.parts) == 1 and top not in ROOT_FILES:
            errors.append(f"{top}: new root file; put it in a directory (see ROOT_FILES)")
        elif len(relative.parts) > 1 and top not in ROOT_DIRS:
            errors.append(f"{top}/: new root directory; use an existing one (see ROOT_DIRS)")


def check_directory_sizes(errors: list[str], root: Path, paths: list[Path]) -> None:
    """Fail when a directory below the root has more than MAX_CHILDREN children."""
    children: dict[Path, set[str]] = {}
    for path in paths:
        relative = path.relative_to(root)
        for depth in range(1, len(relative.parts)):
            children.setdefault(Path(*relative.parts[:depth]), set()).add(relative.parts[depth])
    errors.extend(
        f"{directory}/: {len(names)} children; group them so each directory has at most "
        f"{MAX_CHILDREN} (see .agents/skills/repo-layout)"
        for directory, names in sorted(children.items())
        if len(names) > MAX_CHILDREN
    )


def check_recordings(errors: list[str], root: Path) -> None:
    """Require every recording to have a scenario that the CI replay collects.

    A recording at tests/conformance/recordings/<service path>/<id> needs a
    gcp_scenario with that id in a test module under tests/conformance/<service
    path>/, a run in the replay plan, and the replay entry point in CI.
    """
    errors.extend(replay.plan(root)[1])
    workflow = root / WORKFLOW
    if not workflow.is_file() or not re.search(
        rf"^[^#\n]*\bsh {re.escape(REPLAY)}\s*$", workflow.read_text(), re.MULTILINE
    ):
        errors.append(f"{WORKFLOW}: no job runs `sh {REPLAY}`, so recordings are not replayed")
    entry = root / REPLAY
    if not entry.is_file() or REPLAY_DRIVER not in entry.read_text():
        errors.append(f"{REPLAY}: must run {REPLAY_DRIVER}")
