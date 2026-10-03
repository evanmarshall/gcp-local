import json
from pathlib import Path

import pytest

from gcp_validation.sandbox import Sandbox, inspect_credentials, main


@pytest.fixture
def profile(tmp_path):
    project = "synthetic-sandbox"
    fields = {"GCP_PROJECT_ID": project, "GCP_PROJECT_NUMBER": "123456789012",
              "GCP_REGION": "us-central1", "GCP_TEST_LIVE_SCOPE": '"Synthetic data only"'}
    for alias, prefix in (("allowed", "GCP_TEST"), ("fixture", "GCP_TEST_FIXTURE"),
                          ("denied", "GCP_TEST_DENIED")):
        email = f"synthetic-{alias}@{project}.iam.gserviceaccount.com"
        path = tmp_path / (alias + ".json")
        path.write_text(json.dumps({
            "type": "impersonated_service_account", "quota_project_id": project,
            "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/" + email + ":generateAccessToken",
            "source_credentials": {"type": "authorized_user", "client_id": "synthetic",
                                   "client_secret": "synthetic-secret", "refresh_token": "synthetic-token"},
        }))
        path.chmod(0o600)
        fields[prefix + "_SERVICE_ACCOUNT"] = email
        fields[prefix + "_CREDENTIALS_FILE"] = str(path)
    env = tmp_path / ".env"
    env.write_text("".join(f"{key}={value}\n" for key, value in fields.items()))
    return env


def test_offline_doctor_uses_only_selected_files(profile, monkeypatch, capsys):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/does/not/exist")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "production-project")
    main(["doctor", "--profile", str(profile)])
    result = capsys.readouterr().out
    assert json.loads(result)["mode"] == "offline"
    assert "synthetic-token" not in result and "synthetic-secret" not in result
    assert "production-project" not in result


def test_profile_does_not_execute_shell(profile, tmp_path):
    sentinel = tmp_path / "should-not-exist"
    with profile.open("a") as f:
        f.write(f"UNUSED='$(touch {sentinel})'\n")
    assert Sandbox.load(profile).project == "synthetic-sandbox"
    assert not sentinel.exists()


@pytest.mark.parametrize("line", [
    "GCP_PROJECT_ID=other-project", "GCP_TEST_MAX_BUCKETS=2", "GCP_TEST_MAX_OBJECTS=11",
    "GCP_TEST_MAX_OBJECT_BYTES=1048577", "GCP_TEST_MAX_OBJECTS=0", "export BAD=value",
])
def test_reject_duplicate_or_unbounded_profile(profile, line):
    with profile.open("a") as f:
        f.write(line + "\n")
    with pytest.raises(ValueError):
        Sandbox.load(profile)


@pytest.mark.parametrize("mutation", ["project", "target", "endpoint", "type", "public", "malformed"])
def test_reject_wrong_or_unsafe_credentials(profile, mutation):
    sandbox = Sandbox.load(profile)
    identity = sandbox.identities["allowed"]
    path = identity.credentials_file
    data = json.loads(path.read_text())
    if mutation == "project":
        data["quota_project_id"] = "unrelated-project"
    elif mutation == "target":
        data["service_account_impersonation_url"] = data["service_account_impersonation_url"].replace("allowed", "fixture")
    elif mutation == "endpoint":
        data["source_credentials"]["token_uri"] = "https://example.com/steal"
    elif mutation == "type":
        data["type"] = "external_account"
    elif mutation == "public":
        path.chmod(0o644)
    path.write_text("secret malformed input" if mutation == "malformed" else json.dumps(data))
    with pytest.raises(ValueError) as caught:
        inspect_credentials(identity, sandbox.project)
    assert "synthetic-token" not in str(caught.value)
    assert "secret malformed input" not in str(caught.value)


def test_distinct_callers_and_no_relative_credential_paths(profile):
    original = profile.read_text()
    profile.write_text(original.replace("synthetic-denied@", "synthetic-allowed@"))
    with pytest.raises(ValueError, match="distinct"):
        Sandbox.load(profile)
    profile.write_text(original.replace(str(profile.parent) + "/allowed.json", "allowed.json"))
    with pytest.raises(ValueError, match="absolute"):
        Sandbox.load(profile)


def test_live_doctor_requires_explicit_flag(profile, monkeypatch, capsys):
    refreshed = []
    monkeypatch.setattr("gcp_validation.sandbox.load_credentials", lambda identity, project: identity.email)
    monkeypatch.setattr("gcp_validation.sandbox.refresh_credentials", refreshed.append)
    main(["doctor", "--profile", str(profile)])
    assert refreshed == []
    main(["doctor", "--profile", str(profile), "--allow-live"])
    assert len(refreshed) == 3
    assert json.loads(capsys.readouterr().out.split("}\n", 1)[-1])["mode"] == "live-auth"
