"""Local ADC creation must not target cloud endpoints or overwrite credentials."""

import json
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]


def command(destination, endpoint="http://127.0.0.1:4566"):
    return [
        sys.executable,
        str(ROOT / "scripts/dev/create_local_identity.py"),
        "--directory",
        str(destination),
        "--project",
        "synthetic-project",
        "--principal",
        "serviceAccount:app@synthetic-project.iam.gserviceaccount.com",
        "--endpoint",
        endpoint,
    ]


def test_private_local_bundle_and_no_overwrite(tmp_path):
    destination = tmp_path / "identity"
    result = subprocess.run(command(destination), text=True, capture_output=True, check=False)
    assert result.returncode == 0, result.stderr
    config = json.loads((destination / "server.json").read_text())
    adc = json.loads((destination / "adc.json").read_text())
    subject = (destination / "subject.txt").read_text()
    assert subject == config["principals"][0]["subject_token"]
    assert subject not in result.stdout and subject not in result.stderr
    assert adc["token_url"] == "http://127.0.0.1:4566/v1/token"
    assert adc["token_info_url"] == "http://127.0.0.1:4566/v1/introspect"
    assert adc["credential_source"]["file"] == str(destination / "subject.txt")
    assert stat.S_IMODE(destination.stat().st_mode) == 0o700
    before = {path.name: path.read_bytes() for path in destination.iterdir()}
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in destination.iterdir())
    result = subprocess.run(command(destination), capture_output=True, check=False)
    assert result.returncode != 0
    assert before == {path.name: path.read_bytes() for path in destination.iterdir()}


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://sts.googleapis.com",
        "http://localhost:4566",
        "http://127.0.0.1",
        "http://user:pass@127.0.0.1:4566",
    ],
)
def test_rejects_nonlocal_or_ambiguous_endpoint(tmp_path, endpoint):
    destination = tmp_path / "identity"
    result = subprocess.run(command(destination, endpoint), capture_output=True, check=False)
    assert result.returncode != 0 and not destination.exists()
