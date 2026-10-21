"""The weekly drift plan names real scenarios under scheduled, revalidate-only scopes."""

import importlib
import json
import runpy
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from gcp_validation.sandbox import Sandbox

ROOT = Path(__file__).resolve().parents[3]
DRIFT = ROOT / ".github/drift"
PLAN = runpy.run_path(str(ROOT / "scripts/live/drift.py"))["load_plan"]()


def scopes() -> list[Path]:
    """Return every committed drift scope file."""
    return sorted(path for path in DRIFT.rglob("*.json") if path.name != "plan.json")


def loader(reference: str) -> Callable[..., Any]:
    """Resolve an entry's `module:attr` or `path.py:attr` scope loader."""
    source, _, attribute = reference.partition(":")
    names = attribute.split(".")
    if source.endswith(".py"):
        value = runpy.run_path(str(ROOT / source))[names[0]]
    else:
        value = getattr(importlib.import_module(source), names[0])
    for name in names[1:]:
        value = getattr(value, name)
    return value


def test_plan_names_existing_tests_and_scopes() -> None:
    """Every entry's modules and scope exist, and every scope file is used once."""
    used = sorted(DRIFT / entry["scope"] for entry in PLAN if "scope" in entry)
    assert used == scopes()


@pytest.mark.parametrize(
    "entry", [entry for entry in PLAN if "scope" in entry], ids=lambda entry: entry["name"]
)
def test_scope_passes_its_scenario_loader_for_revalidate_only(entry: dict) -> None:
    """The scenario's own loader accepts the scope for revalidate and cleanup and refuses record.

    The loader holds the scenario's current limits, so a scope left behind when
    they change fails here instead of in the weekly live run.
    """
    path = DRIFT / entry["scope"]
    value = json.loads(path.read_text())
    assert value["scheduled_runs"] is True
    sandbox = Sandbox(value["project"], value["project_number"], "us-central1", "", {})
    load = loader(entry["loader"])
    load(str(path), sandbox, "revalidate")
    load(str(path), sandbox, "cleanup")
    with pytest.raises(ValueError, match="never authorizes record"):
        load(str(path), sandbox, "record")


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
