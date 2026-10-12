"""The packaged CLI's image selection, generated state, and in-image launcher."""

import json
import os
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
    monkeypatch.delenv("GCP_LOCAL_GRPC_TLS_PORT", raising=False)
    monkeypatch.delenv("GCP_LOCAL_RUN_DOCKER_SOCKET", raising=False)
    arguments = container.core_arguments(
        tmp_path / "identity.json", tmp_path / "iam.json", tmp_path / "tls"
    )
    assert arguments[0] == "/gcp-local"
    assert "-listen=0.0.0.0:15566" in arguments
    assert "-grpc-tls-listen=0.0.0.0:15584" in arguments
    assert f"-grpc-tls-dir={tmp_path / 'tls'}" in arguments
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


def test_data_ports_default_after_the_grpc_port(initialized):
    directory, value = initialized
    assert value["data_ports"] == [14568, 14583]
    assert bootstrap.load(directory)["data_ports"] == [14568, 14583]
    assert environment.application_env(value)["GCP_LOCAL_DATA_PORTS"] == "14568-14583"


def test_data_ports_are_configurable_and_bounded(tmp_path):
    directory = tmp_path / "environment"
    directory.mkdir(mode=0o700)
    value = bootstrap.initialize(directory, 14566, "synthetic-project", data_ports=(25000, 25003))
    assert bootstrap.data_port_range(value) == "25000-25003"
    for ports in [
        (14566, 14570),
        (14560, 14567),
        (14580, 14590),
        (25000, 25064),
        (25010, 25000),
        (80, 90),
    ]:
        with pytest.raises(ValueError, match="data ports"):
            bootstrap.check_data_ports(ports, 14566, 14567, 14584)
    with pytest.raises(ValueError, match="FIRST-LAST"):
        bootstrap.parse_data_ports("6379")


def test_manifests_without_data_ports_use_the_default(initialized):
    directory, value = initialized
    del value["data_ports"]
    bootstrap.private_json(directory / "environment.json", value)
    assert bootstrap.load(directory)["data_ports"] == [14568, 14583]


def test_launcher_publishes_the_data_port_range(monkeypatch, tmp_path):
    monkeypatch.setenv("GCP_LOCAL_PORT", "15566")
    monkeypatch.delenv("GCP_LOCAL_GRPC_TLS_PORT", raising=False)
    monkeypatch.delenv("GCP_LOCAL_DATA_PORTS", raising=False)
    arguments = container.core_arguments(
        tmp_path / "identity.json", tmp_path / "iam.json", tmp_path / "tls"
    )
    assert "-data-ports=15568-15583" in arguments
    assert "-data-listen=0.0.0.0" in arguments
    monkeypatch.setenv("GCP_LOCAL_DATA_PORTS", "20000-20007")
    assert container.data_ports() == (20000, 20007)
    monkeypatch.setenv("GCP_LOCAL_DATA_PORTS", "4560-4570")
    with pytest.raises(SystemExit, match="GCP_LOCAL_DATA_PORTS"):
        container.data_ports()


def test_launcher_adds_the_running_uid_to_passwd(tmp_path):
    image, target = tmp_path / "passwd.image", tmp_path / "passwd"
    image.write_text("root:x:0:0:root:/root:/bin/bash\n")
    container.write_passwd(image, target)
    lines = target.read_text().splitlines()
    assert lines[0] == "root:x:0:0:root:/root:/bin/bash"
    assert lines[1].split(":")[2:4] == [str(os.getuid()), str(os.getgid())]
    container.write_passwd(image, target)
    assert len(target.read_text().splitlines()) == 2


def test_launcher_docker_mode_requires_its_label_and_network(monkeypatch, tmp_path):
    monkeypatch.setenv("GCP_LOCAL_PORT", "15566")
    monkeypatch.setenv("GCP_LOCAL_RUN_DOCKER_SOCKET", "/var/run/docker.sock")
    monkeypatch.delenv("GCP_LOCAL_RUN_ENVIRONMENT", raising=False)
    monkeypatch.delenv("GCP_LOCAL_RUN_DOCKER_NETWORK", raising=False)
    with pytest.raises(SystemExit, match="GCP_LOCAL_RUN_ENVIRONMENT"):
        container.core_arguments(
            tmp_path / "identity.json", tmp_path / "iam.json", tmp_path / "tls"
        )
    monkeypatch.setenv("GCP_LOCAL_RUN_ENVIRONMENT", "gcp-local-0123456789abcdef")
    monkeypatch.setenv("GCP_LOCAL_RUN_DOCKER_NETWORK", "bad network")
    with pytest.raises(SystemExit, match="GCP_LOCAL_RUN_DOCKER_NETWORK"):
        container.core_arguments(
            tmp_path / "identity.json", tmp_path / "iam.json", tmp_path / "tls"
        )
    monkeypatch.setenv("GCP_LOCAL_RUN_DOCKER_NETWORK", "gcp-local-0123456789abcdef_backend")
    arguments = container.core_arguments(
        tmp_path / "identity.json", tmp_path / "iam.json", tmp_path / "tls"
    )
    assert arguments[-3:] == [
        "-run-docker-socket=/var/run/docker.sock",
        "-run-docker-network=gcp-local-0123456789abcdef_backend",
        "-run-docker-environment=gcp-local-0123456789abcdef",
    ]
    # Every image runs as a container: no process mapping that could shadow one.
    assert not any(argument.startswith("-run-process") for argument in arguments)


class Recorder:
    """Capture the docker commands the CLI runs."""

    def __init__(self, listed: str = "") -> None:
        self.calls: list[tuple[list[str], dict[str, str]]] = []
        self.listed = listed

    def __call__(self, command, **kwargs):
        self.calls.append((list(command), kwargs.get("env") or {}))
        stdout = self.listed if "ps" in command else ""
        return environment.subprocess.CompletedProcess(command, 0, stdout, "")


def docker_mode(value):
    return {**value, "cloud_run_docker": True, "docker_socket_gid": 991}


def test_compose_applies_the_docker_override_only_when_enabled(monkeypatch, initialized):
    _, value = initialized
    recorder = Recorder()
    monkeypatch.setattr(environment.subprocess, "run", recorder)
    environment.compose({**value, "image": "gcp-local:test"}, "config")
    environment.compose(docker_mode({**value, "image": "gcp-local:test"}), "config")
    (plain, plain_env), (enabled, enabled_env) = recorder.calls
    assert not any(part.endswith("compose.run.yaml") for part in plain)
    assert "GCP_LOCAL_RUN_ENVIRONMENT" not in plain_env
    assert any(part.endswith("compose.run.yaml") for part in enabled)
    assert enabled_env["GCP_LOCAL_RUN_ENVIRONMENT"] == value["compose_project"]
    assert enabled_env["GCP_LOCAL_DOCKER_GID"] == "991"


def test_down_removes_only_this_environments_revisions(monkeypatch, initialized):
    directory, value = initialized
    recorder = Recorder(listed="abc123\ndef456\n")
    monkeypatch.setattr(environment.subprocess, "run", recorder)
    args = environment.parser().parse_args(["down"])
    environment.run(args, {**value, "image": "gcp-local:test"}, directory)
    commands = [command[1:] for command, _ in recorder.calls]
    label = "label=gcp-local.environment=" + value["compose_project"]
    listings = [command for command in commands if command[0] == "ps"]
    assert listings
    assert all(command[-1] == label for command in listings)
    removals = [index for index, command in enumerate(commands) if command[0] == "rm"]
    down = next(index for index, command in enumerate(commands) if "down" in command)
    # Removed before Compose deletes the network and again after the core stops.
    assert removals[0] < down < removals[-1]
    assert commands[removals[0]] == ["rm", "--force", "abc123", "def456"]


def test_reset_refuses_to_change_cloud_run_docker_mode(initialized):
    directory, value = initialized
    args = environment.parser().parse_args(["--cloud-run-docker", "reset"])
    with pytest.raises(ValueError, match="Cloud Run Docker mode with up"):
        environment.start(args, value, directory)


def test_manifests_with_an_invalid_docker_mode_are_refused(initialized):
    directory, value = initialized
    for mode in (
        {"cloud_run_docker": "yes"},
        {"cloud_run_docker": True, "docker_socket_gid": None},
    ):
        bootstrap.private_json(directory / "environment.json", {**value, **mode})
        with pytest.raises(ValueError, match="Invalid environment"):
            bootstrap.load(directory)
    bootstrap.private_json(directory / "environment.json", docker_mode(value))
    assert bootstrap.load(directory)["docker_socket_gid"] == 991


def test_env_names_the_in_network_endpoint_in_docker_mode(initialized):
    _, value = initialized
    assert "GCP_LOCAL_RUN_ENDPOINT" not in environment.application_env(value)
    env = environment.application_env(docker_mode(value))
    assert env["GCP_LOCAL_RUN_ENDPOINT"] == "http://gcp-local:14566"
    assert env["GCP_LOCAL_RUN_GRPC_ENDPOINT"] == "gcp-local:4567"


def test_grpc_tls_port_follows_the_data_range(initialized):
    directory, value = initialized
    assert value["grpc_tls_port"] == 14584
    tls = directory / "tls"
    assert tls.is_dir() and tls.stat().st_mode & 0o777 == 0o700
    env = environment.application_env(value)
    assert env["GCP_LOCAL_GRPC_TLS_ENDPOINT"] == "127.0.0.1:14584"
    assert env["GCP_LOCAL_GRPC_TLS_CA"] == str(directory / "tls/ca.pem")


def test_manifests_without_a_grpc_tls_port_use_the_default(initialized):
    directory, value = initialized
    del value["grpc_tls_port"]
    bootstrap.private_json(directory / "environment.json", value)
    assert bootstrap.load(directory)["grpc_tls_port"] == 14584
    bootstrap.private_json(directory / "environment.json", {**value, "grpc_tls_port": 14567})
    with pytest.raises(ValueError, match="gRPC TLS port"):
        bootstrap.load(directory)


def test_load_refuses_shared_tls_material(initialized):
    directory, _ = initialized
    key = directory / "tls/server-key.pem"
    key.write_text("private")
    key.chmod(0o644)
    with pytest.raises(ValueError, match="owner-only"):
        bootstrap.load(directory)
    key.chmod(0o600)
    assert bootstrap.load(directory)["grpc_tls_port"] == 14584


def test_launcher_grpc_tls_port_is_bounded(monkeypatch):
    monkeypatch.setenv("GCP_LOCAL_PORT", "15566")
    monkeypatch.setenv("GCP_LOCAL_GRPC_TLS_PORT", "20000")
    assert container.grpc_tls_port() == 20000
    for value in ("80", "4567", "15566"):
        monkeypatch.setenv("GCP_LOCAL_GRPC_TLS_PORT", value)
        with pytest.raises(SystemExit, match="GCP_LOCAL_GRPC_TLS_PORT"):
            container.grpc_tls_port()
