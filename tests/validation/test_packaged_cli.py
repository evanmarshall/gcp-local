"""The packaged CLI's image selection, generated state, and in-image launcher."""

import json
import re

import pytest

from gcp_local import IMAGE_REPOSITORY, __version__, bootstrap, container, environment


@pytest.fixture
def initialized(tmp_path):
    directory = tmp_path / "environment"
    directory.mkdir(mode=0o700)
    value = bootstrap.initialize(directory, 14566, "synthetic-project", workloads=["worker"])
    return directory, value


def test_image_selection_precedence(monkeypatch):
    monkeypatch.delenv("GCP_LOCAL_IMAGE", raising=False)
    image, version = environment.select_image(None)
    assert image is None
    assert re.fullmatch(r"source-[0-9a-f]{16}", version)
    monkeypatch.setenv("GCP_LOCAL_IMAGE", "registry.example/gcp-local:1.2.3")
    assert environment.select_image(None) == ("registry.example/gcp-local:1.2.3", None)
    assert environment.select_image("gcp-local:local") == ("gcp-local:local", None)
    monkeypatch.delenv("GCP_LOCAL_IMAGE")
    monkeypatch.setattr(environment, "checkout", lambda: None)
    assert environment.select_image(None) == (IMAGE_REPOSITORY + ":" + __version__, None)


@pytest.mark.parametrize("image", ["--help", "Gcp:1", "a b", "x" * 300])
def test_rejects_unsafe_image_references(image):
    with pytest.raises(ValueError, match="image reference"):
        environment.select_image(image)


def test_generated_state_grants_every_service_and_terraform_adc(initialized):
    directory, value = initialized
    assert re.fullmatch(r"[1-9][0-9]{11}", value["project_number"])
    assert value["project_number"] == bootstrap.project_number("synthetic-project")
    iam = json.loads((directory / "iam.json").read_text())
    assert iam["resources"][0]["number"] == value["project_number"]
    bindings = next(iter(iam["policies"].values()))["bindings"]
    assert [binding["role"] for binding in bindings] == bootstrap.DEVELOPER_ROLES
    developer = "serviceAccount:developer@synthetic-project.iam.gserviceaccount.com"
    assert all(binding["members"] == [developer] for binding in bindings)
    server = json.loads((directory / "identity/server.json").read_text())
    assert [p["subject"] for p in server["principals"]] == [
        developer,
        "serviceAccount:worker@synthetic-project.iam.gserviceaccount.com",
    ]
    adc = json.loads((directory / "identity/adc.json").read_text())
    terraform = json.loads((directory / "identity/terraform-adc.json").read_text())
    assert "universe_domain" not in adc
    assert terraform == {**adc, "universe_domain": "gcp-local.invalid"}
    assert adc["credential_source"]["file"] == str(directory / "identity/subject.txt")
    assert bootstrap.load(directory)["workloads"] == ["worker"]
    env = environment.application_env(value)
    assert env["GCP_LOCAL_TERRAFORM_CREDENTIALS"] == str(directory / "identity/terraform-adc.json")
    assert env["GCP_LOCAL_PROJECT_NUMBER"] == value["project_number"]


def test_recorded_paths_follow_the_host_directory(tmp_path):
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    host = tmp_path / "host/environment"
    bootstrap.initialize(state, 14566, "synthetic-project", mode="standalone", recorded=host)
    adc = json.loads((state / "identity/adc.json").read_text())
    assert adc["credential_source"]["file"] == str(host / "identity/subject.txt")
    assert bootstrap.load(state, recorded=host)["mode"] == "standalone"
    with pytest.raises(ValueError, match="ownership"):
        bootstrap.load(state)


def test_older_environments_are_refused_but_can_be_stopped(initialized):
    directory, value = initialized
    bootstrap.private_json(directory / "environment.json", {**value, "schema": 1})
    with pytest.raises(ValueError, match="older gcp-local"):
        bootstrap.load(directory)
    assert environment.legacy(directory)["compose_project"] == value["compose_project"]


@pytest.mark.parametrize("workload", ["Worker", "w", "bad_name", "x" * 31])
def test_rejects_invalid_workloads(tmp_path, workload):
    directory = tmp_path / "environment"
    directory.mkdir(mode=0o700)
    with pytest.raises(ValueError, match="Workload"):
        bootstrap.initialize(directory, 14566, "synthetic-project", workloads=[workload])


def test_service_flag_selects_every_reported_provider():
    flagged = {"cloudkms" if name == "kms" else name for name in bootstrap.SERVICE_FLAG.split(",")}
    assert flagged == set(bootstrap.SERVICES)
    assert sorted(bootstrap.SERVICES) == bootstrap.SERVICES


def test_launcher_flags(monkeypatch, tmp_path):
    monkeypatch.setenv("GCP_LOCAL_PORT", "15566")
    monkeypatch.delenv("GCP_LOCAL_RUN_DOCKER_SOCKET", raising=False)
    arguments = container.core_arguments(tmp_path / "identity.json", tmp_path / "iam.json")
    assert arguments[0] == "/gcp-local"
    assert "-listen=0.0.0.0:15566" in arguments
    assert "-services=" + bootstrap.SERVICE_FLAG in arguments
    assert (
        "-run-process=gcp-local/workflow:latest="
        "/usr/local/bin/python3 -I /opt/gcp-local/workflow/worker.py"
    ) in arguments
    assert not any(argument.startswith("-run-docker-socket") for argument in arguments)
    monkeypatch.setenv("GCP_LOCAL_PORT", "80")
    with pytest.raises(SystemExit):
        container.port()


def test_standalone_requires_the_host_directory(monkeypatch):
    monkeypatch.delenv("GCP_LOCAL_HOST_DIRECTORY", raising=False)
    with pytest.raises(SystemExit, match="GCP_LOCAL_HOST_DIRECTORY"):
        container.standalone()
