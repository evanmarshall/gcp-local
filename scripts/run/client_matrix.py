"""Load the client matrix and resolve each client's pinned version. Not an entry point.

`tests/clients/matrix.json` declares every (service, client) combination and
whether it passes today. Versions come from the runners' own pins, so the
results and docs/coverage.md always name what actually ran.
"""

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MATRIX = ROOT / "tests/clients/matrix.json"
ISSUES = "https://github.com/evanmarshall/gcp-local/issues/"


def _go_versions() -> dict[str, str]:
    text = (ROOT / "tests/clients/go/go.mod").read_text()
    return {
        name: version.removeprefix("v")
        for name, version in re.findall(r"^\s*(\S+) (v\S+)$", text, re.MULTILINE)
    }


def _node_versions() -> dict[str, str]:
    lock = json.loads((ROOT / "tests/clients/node/package-lock.json").read_text())
    return {
        path.removeprefix("node_modules/"): entry["version"]
        for path, entry in lock["packages"].items()
        if path.startswith("node_modules/") and "/node_modules/" not in path[1:]
    }


def _python_versions(path: Path) -> dict[str, str]:
    versions = {}
    for line in path.read_text().splitlines():
        if line.startswith("-r "):
            versions.update(_python_versions(path.parent / line[3:].strip()))
        elif "==" in line and not line.startswith("#"):
            name, version = line.split("==", 1)
            versions[name.strip().lower()] = version.strip()
    return versions


def _tool_versions() -> dict[str, str]:
    tools = json.loads((ROOT / "toolchains/clients.json").read_text())
    return {name: tools[name]["version"] for name in ("gcloud", "rclone")}


def load() -> dict:
    """Return the matrix with each combination's resolved version and coverage label."""
    matrix = json.loads(MATRIX.read_text())
    pins = {
        "go": _go_versions(),
        "node": _node_versions(),
        "python": _python_versions(ROOT / "tests/clients/python/requirements.txt"),
        "cli": _tool_versions(),
    }
    for combo in matrix["combos"]:
        client = matrix["clients"][combo["client"]]
        versions = pins[client["pins"]]
        package = combo["package"]
        if package not in versions:
            raise ValueError(f"{combo['client']} pins no version of {package}")
        combo["version"] = versions[package]
        language = client["language"]
        combo["label"] = f"{language + ' ' if language else ''}`{package}` {combo['version']}"
        combo["test"] = client["test"]
    return matrix


def coverage_lines(matrix: dict) -> tuple[list[str], list[str]]:
    """Return the coverage table rows for passing combos and the list items for failing ones."""
    rows, failures = [], []
    for combo in matrix["combos"]:
        if combo["expect"] == "pass":
            rows.append(
                f"| {combo['service']} | {combo['label']} | {combo['transport']} "
                f"| `{combo['test']}` |"
            )
        else:
            failures.append(
                f"| {combo['service']} | {combo['label']} | {combo['transport']} "
                f"| {combo['gap']} | [#{combo['issue']}]({ISSUES}{combo['issue']}) |"
            )
    return rows, failures
