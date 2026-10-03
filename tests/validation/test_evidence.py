from dataclasses import replace
import json
import shutil

import pytest

from gcp_validation.evidence import (
    Divergence, Scenario, SnapshotCase, Unverified, check_observation, sha256,
)


@pytest.fixture
def scenario():
    return Scenario("synthetic-storage", "harness", "synthetic", "fixture-v1", {
        "sdk": "synthetic", "provider": "not-applicable", "backend": "synthetic",
    })


def baseline(tmp_path, scenario, observation=None):
    recorder = SnapshotCase(scenario, "record", tmp_path / "baseline", tmp_path / "candidates",
                            source="synthetic")
    recorder.match("resource", observation or {"status": 403, "permission": "storage.objects.get"})
    candidate = recorder.finish()
    destination = tmp_path / "baseline" / scenario.id
    shutil.copytree(candidate, destination)
    manifest_path = destination / "provenance.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["reviewed"] = True  # Synthetic test fixture only; never cloud provenance.
    manifest_path.write_text(json.dumps(manifest))
    return destination


def test_synthetic_snapshot_compare_and_no_local_updates(tmp_path, scenario, monkeypatch):
    monkeypatch.setenv("SNAPSHOT_UPDATE", "1")
    monkeypatch.setenv("SNAPSHOT_RAW", "1")
    destination = baseline(tmp_path, scenario)
    before = {path.name: path.read_bytes() for path in destination.iterdir()}
    comparison = SnapshotCase(scenario, "local", tmp_path / "baseline", tmp_path / "candidates",
                              source="synthetic")
    comparison.match("resource", {"status": 403, "permission": "storage.objects.get"})
    assert comparison.finish() is None
    assert comparison.last_cloud_validation is None
    assert before == {path.name: path.read_bytes() for path in destination.iterdir()}


def test_real_comparison_refuses_synthetic_evidence(tmp_path, scenario):
    baseline(tmp_path, scenario)
    with pytest.raises(Unverified, match="synthetic"):
        SnapshotCase(scenario, "local", tmp_path / "baseline", tmp_path / "candidates")


def test_wrong_permission_is_a_divergence(tmp_path, scenario):
    baseline(tmp_path, scenario)
    comparison = SnapshotCase(scenario, "local", tmp_path / "baseline", tmp_path / "candidates",
                              source="synthetic")
    comparison.match("resource", {"status": 403, "permission": "storage.objects.create"})
    with pytest.raises(Divergence, match="resource"):
        comparison.finish()


@pytest.mark.parametrize("defect", ["missing", "corrupted", "unreviewed", "revision", "malformed"])
def test_missing_or_untrustworthy_baseline_is_unverified(tmp_path, scenario, defect):
    directory = baseline(tmp_path, scenario)
    path = directory / "provenance.json"
    if defect == "missing":
        path.unlink()
    elif defect == "corrupted":
        (directory / "case.snapshot.json").write_text("{}")
    elif defect == "revision":
        scenario = replace(scenario, revision="changed-contract")
    elif defect == "malformed":
        path.write_text("[]")
    else:
        data = json.loads(path.read_text())
        data["reviewed"] = False
        path.write_text(json.dumps(data))
    with pytest.raises(Unverified):
        SnapshotCase(scenario, "local", tmp_path / "baseline", tmp_path / "candidates",
                     source="synthetic")


def test_removed_observation_does_not_silently_pass(tmp_path, scenario):
    baseline(tmp_path, scenario)
    comparison = SnapshotCase(scenario, "local", tmp_path / "baseline", tmp_path / "candidates",
                              source="synthetic")
    comparison.match("different", {"status": 403})
    with pytest.raises(Divergence, match="labels"):
        comparison.finish()


def test_empty_recording_does_not_write_evidence(tmp_path, scenario):
    recorder = SnapshotCase(scenario, "record", tmp_path / "baseline", tmp_path / "candidates",
                            source="synthetic")
    with pytest.raises(Unverified):
        recorder.finish()
    assert not (tmp_path / "candidates").exists()


def test_candidates_are_unique_unreviewed_and_integrity_bound(tmp_path, scenario):
    directories = []
    for _ in range(2):
        recorder = SnapshotCase(scenario, "record", tmp_path / "baseline", tmp_path / "candidates",
                                source="synthetic")
        recorder.match("ok", {"status": 200})
        directories.append(recorder.finish())
    assert directories[0] != directories[1]
    for directory in directories:
        data = json.loads((directory / "provenance.json").read_text())
        assert data["source"] == "synthetic"
        assert data["reviewed"] is False
        assert data["snapshot_sha256"] == sha256(directory / "case.snapshot.json")
        assert not list(directory.glob("*raw*"))


@pytest.mark.parametrize("secret", [
    {"Authorization": "a secret"}, {"nested": [{"refresh_token": "secret"}]},
    {"Cookie": "secret"}, {"api-key": "secret"}, {"value": "Bearer secret-value"},
    {"value": "-----BEGIN RSA PRIVATE KEY-----"}, {"value": float("nan")},
])
def test_credential_material_never_enters_snapshot(tmp_path, scenario, secret):
    recorder = SnapshotCase(scenario, "record", tmp_path / "baseline", tmp_path / "candidates",
                            source="synthetic")
    with pytest.raises(ValueError) as error:
        recorder.match("response", secret)
    assert "secret-value" not in str(error.value)
    assert recorder.observations == {}
    assert not (tmp_path / "candidates").exists()


def test_public_error_fields_and_relationships_are_retained():
    value = {"resource": "projects/fixture/buckets/b", "parent": "projects/fixture",
             "permission": "storage.objects.get", "reason": "forbidden", "etag_changed": False}
    check_observation(value)
    assert value["permission"] == "storage.objects.get"


def test_unsafe_provenance_is_rejected_before_any_candidate_is_written(tmp_path, scenario):
    recorder = SnapshotCase(scenario, "record", tmp_path / "baseline", tmp_path / "candidates", source="synthetic")
    recorder.match("ok", {"status": 200})
    recorder.context = {"authorization": "synthetic-secret"}
    with pytest.raises(ValueError):
        recorder.finish()
    assert not (tmp_path / "candidates").exists()
