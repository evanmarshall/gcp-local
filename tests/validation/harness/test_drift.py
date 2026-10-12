"""The weekly drift plan names real scenarios under scheduled, revalidate-only scopes."""

import json
import runpy
from pathlib import Path

import pytest

from gcp_validation.scope import check_scope

ROOT = Path(__file__).resolve().parents[3]
DRIFT = ROOT / ".github/drift"


def scopes() -> list[Path]:
    """Return every committed drift scope file."""
    return sorted(path for path in DRIFT.rglob("*.json") if path.name != "plan.json")


def test_plan_names_existing_tests_and_scopes() -> None:
    """Every entry's modules and scope exist, and every scope file is used once."""
    plan = runpy.run_path(str(ROOT / "scripts/live/drift.py"))["load_plan"]()
    used = sorted(DRIFT / entry["scope"] for entry in plan if "scope" in entry)
    assert used == scopes()


@pytest.mark.parametrize("path", scopes(), ids=lambda path: path.stem)
def test_scope_is_scheduled_and_revalidate_only(path: Path) -> None:
    """A drift scope authorizes revalidate and cleanup and refuses record."""
    value = json.loads(path.read_text())
    assert value["scheduled_runs"] is True
    fields = {
        "service": value["services"][0],
        "project": value["project"],
        "project_number": value["project_number"],
        "limits": value["limits"],
        "label": path.stem,
    }
    check_scope(value, mode="revalidate", **fields)
    check_scope(value, mode="cleanup", **fields)
    with pytest.raises(ValueError, match="never authorizes record"):
        check_scope(value, mode="record", **fields)


def test_workflow_is_main_only_pinned_and_keeps_credentials_out_of_artifacts() -> None:
    """The drift job runs on main in the gated environment and uploads no credentials."""
    import yaml  # noqa: PLC0415 -- only this test reads workflow YAML

    workflow = yaml.load(
        (ROOT / ".github/workflows/gcp-drift.yml").read_text(),
        Loader=yaml.BaseLoader,  # noqa: S506 -- BaseLoader cannot construct objects
    )
    assert set(workflow["on"]) == {"schedule", "workflow_dispatch"}
    job = workflow["jobs"]["drift"]
    assert job["environment"] == "gcp-conformance"
    assert job["if"] == "github.ref == 'refs/heads/main'"
    for step in job["steps"]:
        if "uses" in step:
            assert len(step["uses"].split("@")[1]) == 40
        if step.get("uses", "").startswith("actions/upload-artifact@"):
            paths = step["with"]["path"].splitlines()
            allowed = (".artifacts/drift/", "~/.local/state/gcp-local/")
            assert all(path.startswith(allowed) for path in paths)
            assert not any("credentials" in path or "gha-creds" in path for path in paths)
