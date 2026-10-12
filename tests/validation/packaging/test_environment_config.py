"""A saved environment must not redirect readiness or take over another project."""

import importlib.util
import json
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "local_env", Path(__file__).resolve().parents[3] / "scripts/dev/local_env.py"
)
local_env = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local_env)


@pytest.fixture
def environment(tmp_path):
    directory = tmp_path / "environment"
    directory.mkdir(mode=0o700)
    local_env.initialize(directory, 14566, "synthetic-project")
    return directory


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("port", "80@storage.googleapis.com"),
        ("port", 0),
        ("compose_project", "someone-else"),
        ("directory", "/unowned"),
    ],
)
def test_rejects_modified_environment_target(environment, field, value):
    path = environment / "environment.json"
    manifest = json.loads(path.read_text())
    manifest[field] = value
    local_env.private_json(path, manifest)
    with pytest.raises(ValueError):
        local_env.load(environment)


def test_cleanup_can_load_ownership_when_credentials_are_broken(environment):
    (environment / "identity/adc.json").unlink()
    with pytest.raises(FileNotFoundError):
        local_env.load(environment)
    assert local_env.load(environment, check_credentials=False)["project"] == "synthetic-project"
