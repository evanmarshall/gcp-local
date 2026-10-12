import json
import runpy
import shutil
from pathlib import Path

import pytest
from storage_server import storage_server

from gcp_validation.config import Target
from gcp_validation.evidence import Scenario, SnapshotCase, Unverified
from gcp_validation.services.storage.client import StorageClient, StorageFixture
from gcp_validation.services.storage.sdk_slice import (
    OwnedBucket,
    require_local_isolation,
    sdk_client,
    upstream_slice,
)

pytest.importorskip("google.cloud.storage")
pytestmark = pytest.mark.harness_loopback
ROOT = Path(__file__).resolve().parents[3]
run_sdk = runpy.run_path(str(ROOT / "tests/conformance/storage/clients/test_storage_sdk.py"))[
    "test_storage_python_sdk"
]
SCENARIO = Scenario(
    "synthetic-sdk",
    "storage",
    "rest-json-v1",
    "synthetic-contract",
    {
        "sdk": "google-cloud-storage/3.10.1",
        "provider": "not-applicable",
        "backend": "synthetic-server",
    },
)


def execute(endpoint, tmp_path, mode="record"):
    tmp_path.mkdir(exist_ok=True)
    fixture = StorageFixture(
        StorageClient(Target.select("local", project="synthetic-project", endpoint=endpoint)),
        tmp_path / "state",
    )
    case = SnapshotCase(
        SCENARIO, mode, tmp_path / "baselines", tmp_path / "candidates", source="synthetic"
    )
    try:
        fixture.setup()
        run_sdk(fixture, case, tmp_path)
    finally:
        fixture.cleanup()
        if fixture.manifest["cleanup"] == "passed":
            case.match("cleanup", {"bucket_absent": True, "owned_objects_removed": True})
    return case.finish()


@pytest.fixture
def controlled_transport(monkeypatch):
    # This scoped stub test is not a public bypass flag for real SDK scenarios.
    monkeypatch.setattr(
        "gcp_validation.services.storage.sdk_slice.require_local_isolation", lambda: None
    )


def test_pinned_sdk_slice_records_compares_and_cleans_up(tmp_path, controlled_transport):
    with storage_server() as (endpoint, state):
        state["sdk"] = True
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


@pytest.mark.parametrize("fault", ["ignore_preconditions", "corrupt_content"])
def test_upstream_assertions_detect_incorrect_protocol(tmp_path, controlled_transport, fault):
    with storage_server() as (endpoint, state):
        state.update(sdk=True, **{fault: True})
        with pytest.raises((AssertionError, pytest.fail.Exception)):
            execute(endpoint, tmp_path)
        assert not (tmp_path / "candidates").exists()
        assert state["bucket"] is None and not state["objects"]


def test_sdk_transport_refuses_cloud_escape_redirects_and_unowned_names(
    tmp_path, controlled_transport
):
    with storage_server() as (endpoint, state):
        state["sdk"] = True
        fixture = StorageFixture(
            StorageClient(Target.select("local", project="synthetic-project", endpoint=endpoint)),
            tmp_path,
        )
        client, session = sdk_client(fixture)
        try:
            fixture.setup()
            before = len(state["requests"])
            with pytest.raises(ValueError, match="endpoint"):
                session.get("https://storage.googleapis.com/storage/v1/b")
            with pytest.raises(ValueError, match="allowlist"):
                OwnedBucket(fixture, client.bucket(fixture.bucket)).blob("outside-scope")
            assert len(state["requests"]) == before
            state["redirect"] = True
            with pytest.raises(ValueError, match="redirect"):
                list(client.list_blobs(fixture.bucket))
            assert len(state["requests"]) == before + 1
        finally:
            state["redirect"] = False
            session.close()
            client.close()
            fixture.cleanup()


def test_local_sdk_requires_process_level_isolation(monkeypatch):
    monkeypatch.delenv("GCP_LOCAL_ISOLATED", raising=False)
    with pytest.raises(Unverified, match="run/isolated"):
        require_local_isolation()


def test_upstream_slice_checks_its_source_pin(tmp_path, monkeypatch):
    manifest, functions = upstream_slice()
    assert len(manifest["tests"]) == 3
    assert all(entry["id"].split("::")[1] in functions for entry in manifest["tests"])
    root = tmp_path / "third_party/python-storage"
    shutil.copytree(ROOT / "third_party/python-storage", root)
    with (root / "storage_slice.py").open("a") as output:
        output.write("\nraise RuntimeError('must never execute')\n")
    monkeypatch.setattr("gcp_validation.services.storage.sdk_slice.ROOT", tmp_path)
    with pytest.raises(ValueError, match="changed"):
        upstream_slice()
