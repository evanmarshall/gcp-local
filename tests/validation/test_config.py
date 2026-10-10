import pytest

from gcp_validation.config import Target


def test_offline_ignores_ambient_cloud_identity(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/production/credentials.json")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "production-project")
    assert Target.select() == Target()


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://storage.googleapis.com",
        "http://169.254.169.254:80",
        "http://example.com:8080",
        "http://127.0.0.1",
        "http://127.0.0.1:0",
        "http://user:secret@127.0.0.1:8080",
        "http://127.0.0.1:8080/api",
        "http://127.0.0.1:8080?token=secret",
        "file:///tmp/server",
        "http://127.0.0.1:invalid",
    ],
)
def test_local_rejects_unsafe_or_ambiguous_endpoint(endpoint):
    with pytest.raises(ValueError):
        Target.select("local", project="local-test", endpoint=endpoint)


def test_local_target_cannot_carry_real_credentials():
    with pytest.raises(ValueError, match="real credentials"):
        Target.select(
            "local",
            project="local-test",
            endpoint="http://127.0.0.1:8080",
            credentials_file="/any/file",
        )


def test_local_requires_explicit_project_and_keeps_ipv6():
    with pytest.raises(ValueError, match="project"):
        Target.select("local", endpoint="http://127.0.0.1:8080")
    target = Target.select("local", project="local-test", endpoint="http://[::1]:8080/")
    assert target.endpoint == "http://[::1]:8080"
    assert target.credentials_file is None


@pytest.mark.parametrize("missing", ["project", "credentials_file", "live_scope", "allow_live"])
def test_live_requires_every_scope_input(tmp_path, missing):
    path = tmp_path / "synthetic.json"
    path.write_text("{}")
    inputs = dict(
        project="sandbox-test",
        credentials_file=str(path),
        live_scope="synthetic configuration test; no cloud execution",
        allow_live=True,
    )
    inputs.pop(missing)
    with pytest.raises(ValueError, match="Live mode requires"):
        Target.select("record", **inputs)


def test_live_preserves_only_explicit_credential_selection(tmp_path):
    path = tmp_path / "synthetic.json"
    path.write_text("{}")
    target = Target.select(
        "revalidate",
        project="sandbox-test",
        credentials_file=str(path),
        live_scope="synthetic config only",
        allow_live=True,
    )
    assert target.credentials_file == path.resolve()
    assert target.endpoint is None


def test_offline_rejects_target_flags():
    with pytest.raises(ValueError):
        Target.select("offline", project="something")
