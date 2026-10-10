"""Runner integration tests use synthetic adapters and never contact GCP."""

import json
import os
import shutil
import subprocess
import sys
import textwrap
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

PACKAGE_ROOT = Path(__file__).resolve().parents[2]
SCENARIO = """
@pytest.mark.gcp_scenario(id="synthetic-case", service="harness", transport="synthetic",
    versions={"sdk": "synthetic", "provider": "not-applicable", "backend": "synthetic"})
"""


def run_suite(tmp_path, source, *args, synthetic=False):
    (tmp_path / "pytest.ini").write_text("[pytest]\naddopts = --strict-markers\n")
    (tmp_path / "test_example.py").write_text(textwrap.dedent(source))
    if synthetic:
        (tmp_path / "conftest.py").write_text("""
from functools import partial
import gcp_validation.pytest_plugin as plugin
plugin.SnapshotCase = partial(plugin.SnapshotCase, source="synthetic")
""")
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PACKAGE_ROOT)
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    env.pop("PYTEST_ADDOPTS", None)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "pytest_socket",
            "-p",
            "gcp_validation.pytest_plugin",
            "-q",
            "--gcp-artifacts",
            str(tmp_path / "artifacts"),
            *args,
        ],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
    )
    path = tmp_path / "artifacts/coverage.json"
    report = json.loads(path.read_text()) if path.exists() else None
    return result, report


def test_offline_blocks_network_and_emits_junit_without_cloud_coverage(tmp_path):
    result, report = run_suite(
        tmp_path,
        """
        import socket
        import pytest
        from pytest_socket import SocketBlockedError
        def test_no_ambient_network():
            with pytest.raises(SocketBlockedError):
                socket.socket()
    """,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert report["harness"] == {"pass": 1}
    assert report["conformance"] == {"total": 0, "verified": 0, "outcomes": {}}
    xml = ET.parse(tmp_path / "artifacts/junit.xml")
    assert len(xml.findall(".//testcase")) == 1


def test_offline_scenario_is_unverified_not_a_pass(tmp_path):
    result, report = run_suite(
        tmp_path,
        "import pytest\n"
        + SCENARIO
        + """
def test_cloud():
    raise AssertionError("offline must not execute the cloud scenario")
""",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert report["conformance"]["outcomes"] == {"unverified": 1}
    assert report["conformance"]["verified"] == 0


def test_missing_baseline_fails_before_scenario_execution(tmp_path):
    result, report = run_suite(
        tmp_path,
        "import pytest\n"
        + SCENARIO
        + """
def test_cloud():
    raise AssertionError("must not execute without a baseline")
""",
        "--gcp-mode",
        "local",
        "--gcp-project",
        "local-test",
        "--gcp-endpoint",
        "http://127.0.0.1:8080",
    )
    assert result.returncode != 0
    assert report["conformance"]["outcomes"] == {"unverified": 1}


def test_local_versions_report_without_inventing_verification(tmp_path):
    result, report = run_suite(
        tmp_path,
        "import pytest\n" + SCENARIO + "\ndef test_cloud(): pass\n",
        "--gcp-mode",
        "local",
        "--gcp-project",
        "local-test",
        "--gcp-endpoint",
        "http://127.0.0.1:8080",
        "--gcp-provider-version",
        "test-provider/1",
        "--gcp-backend-version",
        "test-backend/2",
    )
    assert result.returncode != 0  # Still refuses the missing baseline.
    assert report["conformance"]["verified"] == 0
    assert report["results"][0]["scenario"]["versions"] == {
        "sdk": "synthetic",
        "provider": "test-provider/1",
        "backend": "test-backend/2",
    }


def test_incomplete_live_config_fails_before_test_import(tmp_path):
    result, report = run_suite(
        tmp_path,
        """
        from pathlib import Path
        Path("imported").write_text("must not happen")
        def test_cloud(): pass
    """,
        "--gcp-mode",
        "record",
    )
    assert result.returncode == 4
    assert "Live mode requires" in result.stderr
    assert not (tmp_path / "imported").exists()


@pytest.mark.parametrize(
    "args",
    [
        ["--force-enable-socket"],
        ["--allow-hosts", "storage.googleapis.com"],
        ["--allow-unix-socket"],
    ],
)
def test_network_guard_overrides_are_rejected(tmp_path, args):
    result, _ = run_suite(tmp_path, "def test_example(): pass\n", *args)
    assert result.returncode == 4


def test_local_allows_loopback_but_blocks_cloud_addresses(tmp_path):
    result, report = run_suite(
        tmp_path,
        """
        import socket
        import threading
        import pytest
        from pytest_socket import SocketConnectBlockedError
        def test_loopback():
            with socket.socket() as server:
                server.bind(("127.0.0.1", 0))
                server.listen(1)
                def accept():
                    client, _ = server.accept()
                    with client:
                        client.sendall(b"ok")
                thread = threading.Thread(target=accept, daemon=True)
                thread.start()
                with socket.create_connection(server.getsockname(), timeout=2) as client:
                    assert client.recv(2) == b"ok"
                thread.join(2)
            with socket.socket() as client:
                with pytest.raises(SocketConnectBlockedError):
                    client.connect(("169.254.169.254", 80))
    """,
        "--gcp-mode",
        "local",
        "--gcp-project",
        "local-test",
        "--gcp-endpoint",
        "http://127.0.0.1:8080",
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_report_distinguishes_nonpassing_outcomes(tmp_path):
    result, report = run_suite(
        tmp_path,
        """
        import pytest
        from gcp_validation.evidence import Divergence, Unsupported, Unverified, Inconclusive
        def test_divergence(): raise Divergence("different status")
        def test_unsupported(): raise Unsupported("not implemented")
        def test_unverified(): raise Unverified("no recording")
        def test_inconclusive(): raise Inconclusive("propagation timeout")
        def test_skip(): pytest.skip("not selected")
        def test_error(): raise RuntimeError("broken fixture")
    """,
    )
    assert result.returncode != 0
    assert report["harness"] == {
        "divergence": 1,
        "unsupported": 1,
        "unverified": 1,
        "inconclusive": 1,
        "skipped": 1,
        "error": 1,
    }
    assert report["conformance"]["verified"] == 0


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_recording_waits_for_cleanup_and_never_claims_synthetic_cloud_evidence(
    tmp_path, cleanup_fails
):
    credentials = tmp_path / "synthetic-credentials.json"
    credentials.write_text("{}")
    source = """
import pytest
@pytest.fixture
def resource():
    yield
    CLEANUP
""".replace("CLEANUP", 'raise RuntimeError("cleanup failed")' if cleanup_fails else "pass")
    source += (
        SCENARIO
        + """
def test_cloud(resource, gcp_snapshot):
    gcp_snapshot.match("resource", {"status": 200})
"""
    )
    result, report = run_suite(
        tmp_path,
        source,
        "--gcp-mode",
        "record",
        "--gcp-project",
        "sandbox-test",
        "--gcp-credentials-file",
        str(credentials),
        "--gcp-allow-live",
        "--gcp-live-scope",
        "synthetic harness; no cloud",
        synthetic=True,
    )
    candidates = list((tmp_path / "artifacts").glob("candidates/*/provenance.json"))
    if cleanup_fails:
        assert result.returncode != 0
        assert candidates == []
        assert report["conformance"]["outcomes"] == {"error": 1}
    else:
        assert result.returncode == 0, result.stdout + result.stderr
        assert len(candidates) == 1
        assert json.loads(candidates[0].read_text())["source"] == "synthetic"
        assert report["conformance"]["outcomes"] == {"unverified": 1}
    assert report["conformance"]["verified"] == 0


def test_full_synthetic_record_review_compare_and_corruption(tmp_path):
    credentials = tmp_path / "synthetic-credentials.json"
    credentials.write_text("{}")
    observation = tmp_path / "observed-status"
    observation.write_text("200")
    source = (
        "import pytest\nfrom pathlib import Path\n"
        + SCENARIO
        + """
def test_cloud(gcp_snapshot):
    gcp_snapshot.match("resource", {"status": int(Path("observed-status").read_text())})
"""
    )
    result, _ = run_suite(
        tmp_path,
        source,
        "--gcp-mode",
        "record",
        "--gcp-project",
        "sandbox-test",
        "--gcp-credentials-file",
        str(credentials),
        "--gcp-allow-live",
        "--gcp-live-scope",
        "synthetic harness; no cloud",
        synthetic=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    candidate = next((tmp_path / "artifacts/candidates").iterdir())
    destination = tmp_path / "baselines/synthetic-case"
    shutil.copytree(candidate, destination)
    provenance = destination / "provenance.json"
    data = json.loads(provenance.read_text())
    assert data["source"] == "synthetic"
    data["reviewed"] = True
    provenance.write_text(json.dumps(data))
    before = {path.name: path.read_bytes() for path in destination.iterdir()}
    local = [
        "--gcp-mode",
        "local",
        "--gcp-project",
        "local-test",
        "--gcp-endpoint",
        "http://127.0.0.1:8080",
        "--gcp-baselines",
        str(destination.parent),
    ]
    result, report = run_suite(tmp_path, source, *local, synthetic=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert report["conformance"]["outcomes"] == {"pass": 1}
    assert report["conformance"]["verified"] == 0
    observation.write_text("403")
    result, report = run_suite(tmp_path, source, *local, synthetic=True)
    assert result.returncode != 0
    assert report["conformance"]["outcomes"] == {"divergence": 1}
    assert "Snapshot mismatch in: resource" in result.stdout
    assert before == {path.name: path.read_bytes() for path in destination.iterdir()}


def test_profile_requires_live_opt_in_before_reading_or_importing(tmp_path):
    result, _ = run_suite(
        tmp_path, 'raise AssertionError("must not import")', "--gcp-profile", "/does/not/exist"
    )
    assert result.returncode == 4
    assert "profiles require an explicit live mode" in result.stderr


def test_contract_dependency_changes_invalidate_evidence(tmp_path):
    dependency = tmp_path / "adapter.py"
    dependency.write_text("contract = 1\n")
    credentials = tmp_path / "synthetic.json"
    credentials.write_text("{}")
    marker = SCENARIO.replace(
        'id="synthetic-case",', 'id="synthetic-case", contract_files=("adapter.py",),'
    )
    source = (
        "import pytest\n"
        + marker
        + """
def test_cloud(gcp_snapshot):
    gcp_snapshot.match("resource", {"status": 200})
"""
    )
    result, _ = run_suite(
        tmp_path,
        source,
        "--gcp-mode",
        "record",
        "--gcp-project",
        "sandbox-test",
        "--gcp-credentials-file",
        str(credentials),
        "--gcp-allow-live",
        "--gcp-live-scope",
        "synthetic only",
        synthetic=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    candidate = next((tmp_path / "artifacts/candidates").iterdir())
    destination = tmp_path / "baselines/synthetic-case"
    shutil.copytree(candidate, destination)
    manifest = destination / "provenance.json"
    data = json.loads(manifest.read_text())
    data["reviewed"] = True
    assert "adapter.py" in data["context"]["contract_files"]
    manifest.write_text(json.dumps(data))
    dependency.write_text("contract = 2\n")
    result, report = run_suite(
        tmp_path,
        source,
        "--gcp-mode",
        "local",
        "--gcp-project",
        "local-test",
        "--gcp-endpoint",
        "http://127.0.0.1:8080",
        "--gcp-baselines",
        str(destination.parent),
        synthetic=True,
    )
    assert result.returncode != 0
    assert report["conformance"]["outcomes"] == {"unverified": 1}


def test_contract_dependencies_cannot_escape_repository(tmp_path):
    marker = SCENARIO.replace(
        'id="synthetic-case",', 'id="synthetic-case", contract_files=("../secret",),'
    )
    result, _ = run_suite(tmp_path, "import pytest\n" + marker + "def test_cloud(): pass\n")
    assert result.returncode == 4
    assert "repository-relative" in result.stderr
