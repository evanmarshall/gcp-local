import json
import runpy
import shutil
from pathlib import Path

import pytest
from storage_server import storage_server

from gcp_validation.authorization import StorageAuthorization
from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, Scenario, SnapshotCase
from gcp_validation.storage import StorageClient, StorageFixture

pytestmark = pytest.mark.harness_loopback
ROOT = Path(__file__).resolve().parents[2]
run_authorization = runpy.run_path(str(ROOT / "tests/conformance/test_storage_authorization.py"))[
    "test_storage_authorization"
]
SCENARIO = Scenario(
    "synthetic-authorization",
    "storage",
    "rest-json-v1",
    "synthetic-contract",
    {"sdk": "synthetic", "provider": "not-applicable", "backend": "synthetic-server"},
)


def execute(endpoint, tmp_path, mode="record", scenario=run_authorization):
    fixture = StorageFixture(
        StorageClient(Target.select("local", project="synthetic-project", endpoint=endpoint)),
        tmp_path / "state",
    )
    case = SnapshotCase(
        SCENARIO, mode, tmp_path / "baselines", tmp_path / "candidates", source="synthetic"
    )
    try:
        fixture.setup()
        scenario(fixture, case)
    finally:
        fixture.cleanup()
        if fixture.manifest["cleanup"] == "passed":
            case.match("cleanup", {"bucket_absent": True, "owned_objects_removed": True})
    return case.finish()


def test_authorization_scenario_records_compares_and_cleans_up(tmp_path):
    with storage_server() as (endpoint, state):
        state["authorization"] = True
        candidate = execute(endpoint, tmp_path)
        destination = tmp_path / "baselines" / SCENARIO.id
        shutil.copytree(candidate, destination)
        path = destination / "provenance.json"
        manifest = json.loads(path.read_text())
        assert manifest["source"] == "synthetic"
        manifest["reviewed"] = True
        path.write_text(json.dumps(manifest))
        assert execute(endpoint, tmp_path, "local") is None
        assert state["bucket"] is None and not state["objects"]
        assert {actor for actor, _, _ in state["requests"]} == {
            "fixture",
            "allowed",
            "denied",
            "anonymous",
        }


@pytest.mark.parametrize("fault", ["denied", "anonymous", "side-effect"])
def test_authorization_detects_access_leaks_and_denied_side_effects(tmp_path, fault):
    with storage_server() as (endpoint, state):
        state["authorization"] = True
        if fault == "side-effect":
            state["authorization_side_effect"] = True
        else:
            state["leaked_access"] = fault
        with pytest.raises(AssertionError):
            execute(endpoint, tmp_path)
        assert state["bucket"] is None and not state["objects"]
        assert not (tmp_path / "candidates").exists()


def test_revocation_timeout_is_inconclusive_and_cleanup_still_runs(tmp_path):
    with storage_server() as (endpoint, state):
        state["authorization"] = True
        fixture = StorageFixture(
            StorageClient(Target.select("local", project="synthetic-project", endpoint=endpoint)),
            tmp_path,
        )
        try:
            fixture.setup()
            fixture.upload("object.txt", b"test").require(200)
            # The policy write succeeds but the data plane still uses an old grant.
            state["leaked_access"] = "allowed"
            iam = StorageAuthorization(fixture)
            iam.set_role("allowed")
            with pytest.raises(Inconclusive, match="transition"):
                iam.wait_read("object.txt", "allowed", 403)
        finally:
            fixture.cleanup()
        assert state["bucket"] is None


def test_policy_transition_preserves_unrelated_bindings_and_etag(tmp_path):
    with storage_server() as (endpoint, state):
        fixture = StorageFixture(
            StorageClient(Target.select("local", project="synthetic-project", endpoint=endpoint)),
            tmp_path,
        )
        try:
            fixture.setup()
            unrelated = {
                "role": "roles/storage.objectViewer",
                "members": ["serviceAccount:unrelated@example.com"],
                "condition": {"expression": "true", "title": "synthetic"},
            }
            state["policy"]["etag"] = "unchanged-etag"
            state["policy"]["bindings"].append(unrelated)
            StorageAuthorization(fixture).set_role("denied", "roles/storage.objectViewer")
            assert state["policy"]["etag"] == "unchanged-etag"
            assert unrelated in state["policy"]["bindings"]
            with pytest.raises(ValueError):
                StorageAuthorization(fixture).set_role("fixture", "roles/storage.objectUser")
        finally:
            fixture.cleanup()


def test_cross_project_scenario_uses_foreign_identity_and_detects_leaks(tmp_path):
    scenario = runpy.run_path(str(ROOT / "tests/conformance/test_storage_isolation.py"))[
        "test_storage_cross_project"
    ]
    with storage_server() as (endpoint, state):
        state["authorization"] = True
        assert execute(endpoint, tmp_path, scenario=scenario).is_dir()
        state["leaked_access"] = "peer"
        with pytest.raises(AssertionError):
            execute(endpoint, tmp_path / "leaked", scenario=scenario)
        assert state["bucket"] is None
