import json
from pathlib import Path
import runpy
import shutil

import pytest

from gcp_validation.config import Target
from gcp_validation.evidence import Divergence, Inconclusive, Scenario, SnapshotCase, Unverified
from gcp_validation.normalization import StorageObservations
from gcp_validation.storage import Response, StorageClient, StorageFixture

from storage_server import storage_server


pytestmark = pytest.mark.harness_loopback
ROOT = Path(__file__).resolve().parents[2]
run_lifecycle = runpy.run_path(str(ROOT / "tests/conformance/test_storage.py"))["test_storage_object_lifecycle"]
SCENARIO = Scenario("synthetic-storage-lifecycle", "storage", "rest-json-v1", "synthetic-contract",
                    {"sdk": "synthetic", "provider": "not-applicable", "backend": "synthetic-server"})


def execute(endpoint, tmp_path, mode="record"):
    client = StorageClient(Target.select("local", project="synthetic-project", endpoint=endpoint))
    fixture = StorageFixture(client, tmp_path / "state")
    case = SnapshotCase(SCENARIO, mode, tmp_path / "baselines", tmp_path / "candidates", source="synthetic")
    try:
        fixture.setup()
        run_lifecycle(fixture, case)
    finally:
        fixture.cleanup()
        if fixture.manifest["cleanup"] == "passed":
            case.match("cleanup", {"bucket_absent": True, "owned_objects_removed": True})
    return case.finish()


def baseline(endpoint, tmp_path):
    candidate = execute(endpoint, tmp_path)
    destination = tmp_path / "baselines" / SCENARIO.id
    shutil.copytree(candidate, destination)
    path = destination / "provenance.json"
    metadata = json.loads(path.read_text())
    assert metadata["source"] == "synthetic"
    metadata["reviewed"] = True
    path.write_text(json.dumps(metadata))
    return destination


def test_same_storage_scenario_records_and_compares_offline(tmp_path, monkeypatch):
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/unselected-secret.json")
    with storage_server() as (endpoint, state):
        destination = baseline(endpoint, tmp_path)
        before = (destination / "case.snapshot.json").read_bytes()
        assert execute(endpoint, tmp_path, "local") is None
        assert (destination / "case.snapshot.json").read_bytes() == before
        assert state["bucket"] is None and state["objects"] == {}
        assert b"synthetic-secret" not in before
        assert b"authorization" not in before
        assert b"gcp-local-000000000001-" not in before
        with pytest.raises(Unverified):
            SnapshotCase(SCENARIO, "local", tmp_path / "baselines", tmp_path / "candidates")


def test_comparison_detects_wrong_error_reason(tmp_path):
    with storage_server() as (endpoint, state):
        baseline(endpoint, tmp_path)
        state["corrupt_reason"] = True
        with pytest.raises(Divergence, match="conditional-write-rejected"):
            execute(endpoint, tmp_path, "local")
        assert state["bucket"] is None


@pytest.mark.parametrize("corruption", ["denied_side_effect", "corrupt_content"])
def test_scenario_detects_side_effects_and_wrong_content(tmp_path, corruption):
    with storage_server() as (endpoint, state):
        state[corruption] = True
        with pytest.raises(AssertionError):
            execute(endpoint, tmp_path)
        assert not (tmp_path / "candidates").exists()
        assert state["bucket"] is None and state["objects"] == {}


def test_failed_cleanup_keeps_owned_manifest_and_no_candidate(tmp_path):
    with storage_server() as (endpoint, state):
        state["fail_cleanup"] = True
        with pytest.raises(AssertionError, match="HTTP 500"):
            execute(endpoint, tmp_path)
        assert not (tmp_path / "candidates").exists()
        manifests = list((tmp_path / "state").glob("*.json"))
        assert len(manifests) == 1
        assert json.loads(manifests[0].read_text())["cleanup"] == "pending"
        state["fail_cleanup"] = False
        client = StorageClient(Target.select("local", project="synthetic-project", endpoint=endpoint))
        recovery = StorageFixture(client, tmp_path / "state", manifest=manifests[0])
        recovery.acquire()
        recovery.cleanup()
        assert state["bucket"] is None
        assert json.loads(manifests[0].read_text())["cleanup"] == "passed"


def test_cleanup_refuses_foreign_ownership(tmp_path):
    with storage_server() as (endpoint, state):
        fixture = StorageFixture(StorageClient(Target.select("local", project="synthetic-project", endpoint=endpoint)), tmp_path)
        fixture.setup()
        state["bucket"]["labels"]["gcp-local-run"] = "different-owner"
        with pytest.raises(ValueError, match="ownership"):
            fixture.cleanup()
        assert state["bucket"] is not None
        state["bucket"]["labels"]["gcp-local-run"] = fixture.manifest["run_id"]
        fixture.cleanup()


def test_resource_lock_and_payload_limits(tmp_path):
    with storage_server() as (endpoint, state):
        client = StorageClient(Target.select("local", project="synthetic-project", endpoint=endpoint))
        first, second = StorageFixture(client, tmp_path), StorageFixture(client, tmp_path)
        try:
            first.setup()
            with pytest.raises(Inconclusive, match="lock"):
                second.setup()
            before = len(state["requests"])
            with pytest.raises(ValueError, match="payload"):
                first.upload("too-large.txt", b"x" * (1048576 + 1))
            assert len(state["requests"]) == before
        finally:
            second.cleanup()
            first.cleanup()


def test_local_transport_refuses_redirect_to_cloud(tmp_path):
    with storage_server() as (endpoint, state):
        state["redirect"] = True
        client = StorageClient(Target.select("local", project="synthetic-project", endpoint=endpoint))
        assert client.request("allowed", "GET", "/storage/v1/b").status == 307
        assert len(state["requests"]) == 1


def test_normalization_preserves_alias_relationships_and_rejects_secrets(tmp_path):
    fixture = StorageFixture(StorageClient(Target.select("local", project="synthetic-project", endpoint="http://127.0.0.1:1234")), tmp_path)
    normalizer = StorageObservations(fixture)
    assert normalizer.reference("etag", "one") == normalizer.reference("etag", "one")
    assert normalizer.reference("etag", "one") != normalizer.reference("etag", "two")
    response = Response(403, b'{"error":{"code":403,"message":"Bearer synthetic-secret"}}', "application/json")
    with pytest.raises(ValueError, match="credential"):
        normalizer.error(response)


def test_missing_baseline_fails_before_storage_mutation(tmp_path):
    with storage_server() as (endpoint, state):
        with pytest.raises(Unverified):
            execute(endpoint, tmp_path, "local")
        assert state["requests"] == []


def test_json_error_with_non_json_content_type_still_selects_public_fields(tmp_path):
    fixture = StorageFixture(StorageClient(Target.select("local", project="synthetic-project", endpoint="http://127.0.0.1:1234")), tmp_path)
    normalizer = StorageObservations(fixture)
    response = Response(403, json.dumps({"error": {
        "code": 403, "message": "storage.objects.create denied", "authorization": "Bearer synthetic-secret",
        "errors": [{"reason": "forbidden", "refresh_token": "synthetic-private-value"}],
    }}).encode(), "text/plain")
    result = normalizer.error(response)
    assert result["format"] == "json" and result["content_type"] == "text/plain"
    assert result["reasons"] == ["forbidden"]
    assert "synthetic" not in json.dumps(result)
