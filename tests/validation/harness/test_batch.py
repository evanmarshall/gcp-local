"""The parallel live runner plans every recording and keeps lock-sharing runs apart."""

import argparse
import json
import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
batch = runpy.run_path(str(ROOT / "scripts/live/batch.py"))
replay = batch["replay"]


def scope_tree(tmp_path: Path) -> Path:
    """Write a scope file for every entry, as --write-approved-scopes would."""
    for entry in batch["plan"]():
        if entry.option:
            service, limits = batch["scope_contract"](entry)
            module = Path(entry.module).relative_to("tests/conformance")
            path = tmp_path / module.parent / (module.stem.removeprefix("test_") + ".json")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({"services": [service], "limits": limits}))
    return tmp_path


def test_only_plans_a_named_scenario_without_a_recording(tmp_path: Path) -> None:
    """--only reaches a declared scenario that has no recording yet."""
    module = tmp_path / "tests/conformance/compute/fresh/test_fresh.py"
    module.parent.mkdir(parents=True)
    module.write_text('@pytest.mark.gcp_scenario(id="fresh-scenario")\n')
    assert batch["declared"](tmp_path, {"fresh-scenario", "other"}) == {
        "fresh-scenario": "compute/fresh"
    }


def test_plan_covers_every_recording_once() -> None:
    """Each recording belongs to exactly one planned module."""
    planned = [scenario for entry in batch["plan"]() for scenario in entry.scenarios]
    assert sorted(planned) == sorted(replay.recordings(ROOT))


def test_scope_contracts_match_the_loaders() -> None:
    """Scope names and limits come from the scenario's own loader."""
    contracts = {
        entry.name: batch["scope_contract"](entry) for entry in batch["plan"]() if entry.option
    }
    sys.path.insert(0, str(ROOT))
    from gcp_validation.services.databases import bigquery  # noqa: PLC0415

    assert contracts["databases-bigquery-test_bigquery"] == ("bigquery", bigquery.LIMITS)
    assert contracts["databases-bigquery-test_bigquery_sql"][1] != bigquery.LIMITS
    assert contracts["messaging-pubsub-schemas-test_schemas"][0] == "pubsub-schemas"
    assert contracts["security-secretmanager-test_secrets"][0] == "secretmanager"


def test_lock_sharing_entries_share_a_lane(tmp_path: Path) -> None:
    """Same-service scenarios run in order; project policy writers run alone."""
    lanes, alone = batch["schedule"](batch["plan"](), scope_tree(tmp_path))
    names = {key: [entry.name for entry in members] for key, members in lanes.items()}
    assert len(names["storage"]) >= 10
    assert len(names["cloudsql"]) == 5
    compute = {
        "compute-networking-test_networking",
        "compute-vmconnectivity-test_vmconnectivity",
        "compute-vmserviceaccounts-test_vmserviceaccounts",
    }
    assert set(names["compute"]) == compute
    assert sorted(entry.name for entry in alone) == [
        "logging-test_cloudlogging_conditions", "security-resourcemanager-test_policy"]
    # The read-only hierarchy scenario shares no lock; only project-policy writers run alone.
    laned = [name for members in names.values() for name in members]
    assert "security-resourcemanager-test_policy" not in laned
    hierarchy = "security-resourcemanager-hierarchy-test_hierarchy"
    assert names["resourcemanager-hierarchy"] == [hierarchy]


def test_exclusive_entries_run_after_every_lane(tmp_path: Path, monkeypatch) -> None:
    """Nothing overlaps the exclusive run, and failures are reported."""
    order: list[str] = []

    def fake(entry, _args):
        order.append(entry.name)
        return entry, entry.name != "artifactregistry-test_artifactregistry", 0.0

    run = batch["run"]
    monkeypatch.setitem(run.__globals__, "execute", fake)
    lanes, alone = batch["schedule"](batch["plan"](), scope_tree(tmp_path))
    failed = run(lanes, alone, argparse.Namespace(jobs=4))
    assert order[-1] == "security-resourcemanager-test_policy"
    assert failed == ["artifactregistry-test_artifactregistry"]
