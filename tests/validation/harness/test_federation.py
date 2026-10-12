import json
import runpy
from pathlib import Path
from urllib.parse import urlencode

import pytest
import yaml

from gcp_validation.sandbox import Identity, inspect_credentials, load_credentials

ROOT = Path(__file__).resolve().parents[3]
PROVIDER = (
    "projects/123456789012/locations/global/"
    "workloadIdentityPools/local-github/providers/manual-storage"
)
PROJECT = "synthetic-sandbox"


@pytest.fixture
def wif(tmp_path, monkeypatch):
    endpoint = "https://pipelines.actions.githubusercontent.com/synthetic/_apis/token?api-version=2"
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_URL", endpoint)
    identity = Identity(
        "synthetic-allowed@" + PROJECT + ".iam.gserviceaccount.com",
        tmp_path / "credentials.json",
        PROVIDER,
    )
    data = {
        "type": "external_account",
        "audience": "//iam.googleapis.com/" + PROVIDER,
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "token_url": "https://sts.googleapis.com/v1/token",
        "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
        + identity.email
        + ":generateAccessToken",
        "credential_source": {
            "url": endpoint
            + "&"
            + urlencode({"audience": "https://iam.googleapis.com/" + PROVIDER}),
            "headers": {"Authorization": "Bearer synthetic-runner-secret"},
            "format": {"type": "json", "subject_token_field_name": "value"},
        },
    }
    identity.credentials_file.write_text(json.dumps(data))
    identity.credentials_file.chmod(0o600)
    return identity, data


def test_typed_wif_load_is_offline_and_uses_explicit_identity(wif):
    identity, _ = wif
    credential = load_credentials(identity, PROJECT)
    assert credential.service_account_email == identity.email
    assert credential.quota_project_id == PROJECT
    assert not credential.valid


def mutate_wif(mutation: str, identity: Identity, data: dict) -> Identity:
    """Apply one credential-confusion mutation and return the identity to test."""
    mutations = {
        "provider": lambda d: d.update(audience=d["audience"] + "-different"),
        "target": lambda d: d.update(
            service_account_impersonation_url=d["service_account_impersonation_url"].replace(
                "allowed", "fixture"
            )
        ),
        "token-url": lambda d: d.update(token_url="https://example.com/steal"),
        "source-host": lambda d: d["credential_source"].update(
            url=d["credential_source"]["url"].replace(
                "pipelines.actions.githubusercontent.com", "example.com"
            )
        ),
        "source-path": lambda d: d["credential_source"].update(
            url=d["credential_source"]["url"].replace("/synthetic/", "/foreign/")
        ),
        "source-audience": lambda d: d["credential_source"].update(
            url=d["credential_source"]["url"] + "-different"
        ),
        "file": lambda d: d["credential_source"].update(file="/unsafe/credential-source"),
        "executable": lambda d: d["credential_source"].update(
            executable="/unsafe/credential-source"
        ),
        "unknown-field": lambda d: d.update(token_info_url="https://example.com/steal"),
        "quota": lambda d: d.update(quota_project_id="unrelated-project"),
        "universe": lambda d: d.update(universe_domain="example.com"),
        "headers": lambda d: d["credential_source"].update(headers=None),
    }
    if mutation == "no-provider":
        return Identity(identity.email, identity.credentials_file)
    if mutation in mutations:
        mutations[mutation](data)
    return identity


@pytest.mark.parametrize(
    "mutation",
    [
        "provider",
        "target",
        "token-url",
        "source-host",
        "source-path",
        "source-audience",
        "file",
        "executable",
        "unknown-field",
        "quota",
        "universe",
        "headers",
        "no-provider",
    ],
)


def test_wif_rejects_credential_confusion_before_network(wif, mutation):
    identity, data = wif
    identity = mutate_wif(mutation, identity, data)
    identity.credentials_file.write_text(json.dumps(data))
    with pytest.raises(ValueError) as caught:
        load_credentials(identity, PROJECT)
    assert "synthetic-runner-secret" not in str(caught.value)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("GITHUB_ACTIONS", "false"),
        ("GITHUB_EVENT_NAME", "pull_request"),
        ("GITHUB_EVENT_NAME", "pull_request_target"),
        ("GITHUB_REF", "refs/heads/feature"),
        ("ACTIONS_ID_TOKEN_REQUEST_URL", "https://example.com/token"),
    ],
)
def test_wif_rejects_untrusted_execution_context(wif, monkeypatch, key, value):
    identity, _ = wif
    monkeypatch.setenv(key, value)
    with pytest.raises(ValueError):
        inspect_credentials(identity, PROJECT)


def test_provisioning_plan_has_no_caller_storage_grants_or_ambient_project():
    plan = runpy.run_path(str(ROOT / "scripts/live/plan_sandbox.py"))["plan"]
    result = plan(PROJECT, "123456789012", "setup@example.com", "owner/repo", "1234", "5678")
    steps = result["steps"]
    for step in steps:
        assert "--project=" + PROJECT in step["command"]
        assert "--account=setup@example.com" in step["command"]
        command = " ".join(step["command"])
        if "--role=roles/storage." in command:
            assert "--member=serviceAccount:gcp-local-fixtures@" in command
    provider = next(step for step in steps if "create-oidc" in step["command"])
    condition = provider["expected"]["attributeCondition"]
    for claim in (
        "repository_id == '1234'",
        "repository_owner_id == '5678'",
        "ref == 'refs/heads/main'",
        "environment == 'gcp-conformance'",
        "event_name == 'workflow_dispatch'",
        "workflow_ref == 'owner/repo/.github/workflows/cloud-conformance.yml@refs/heads/main'",
    ):
        assert "assertion." + claim in condition
    assert "billing" not in " ".join(" ".join(step["command"]) for step in steps)
    with pytest.raises(ValueError):
        plan(PROJECT, "123456789012", "setup@example.com", "owner/repo' || true", "1234", "5678")


def test_ci_workflow_is_manual_and_artifacts_exclude_credentials():

    # BaseLoader never constructs Python objects; the raw mapping is the point.
    workflow = yaml.load(
        (ROOT / ".github/workflows/cloud-conformance.yml").read_text(),
        Loader=yaml.BaseLoader,  # noqa: S506 -- BaseLoader cannot construct objects
    )
    assert set(workflow["on"]) == {"workflow_dispatch"}
    job = workflow["jobs"]["storage"]
    assert job["environment"] == "gcp-conformance"
    assert "refs/heads/main" in job["if"] and "workflow_dispatch" in job["if"]
    assert workflow["concurrency"]["cancel-in-progress"] == "false"
    for step in job["steps"]:
        if step.get("uses", "").startswith("actions/upload-artifact@"):
            paths = step["with"]["path"].splitlines()
            assert all(
                path.startswith((".artifacts/cloud/", "~/.local/state/gcp-local/"))
                for path in paths
            )
            assert not any("credentials" in path or "gha-creds" in path for path in paths)
        if "uses" in step:
            assert len(step["uses"].split("@")[1]) == 40


def test_ci_profile_is_private_explicit_and_never_embeds_credential_contents(
    wif, tmp_path, monkeypatch, capsys
):
    _, original = wif
    workspace = tmp_path / "checkout"
    workspace.mkdir()
    monkeypatch.setenv("GITHUB_WORKSPACE", str(workspace))
    for name, value in {
        "GCP_PROJECT_ID": PROJECT,
        "GCP_PROJECT_NUMBER": "123456789012",
        "GCP_REGION": "us-central1",
        "GCP_TEST_LIVE_SCOPE": "Storage only; synthetic scope",
        "GCP_TEST_WIF_PROVIDER": PROVIDER,
    }.items():
        monkeypatch.setenv(name, value)
    for alias, prefix in (
        ("allowed", "GCP_TEST"),
        ("fixture", "GCP_TEST_FIXTURE"),
        ("denied", "GCP_TEST_DENIED"),
    ):
        email = "synthetic-" + alias + "@" + PROJECT + ".iam.gserviceaccount.com"
        data = json.loads(json.dumps(original))
        data["service_account_impersonation_url"] = (
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
            + email
            + ":generateAccessToken"
        )
        path = workspace / (alias + ".json")
        path.write_text(json.dumps(data))
        monkeypatch.setenv(prefix + "_SERVICE_ACCOUNT", email)
        monkeypatch.setenv(prefix + "_CREDENTIALS_FILE", str(path))
    prepare = runpy.run_path(str(ROOT / "scripts/live/prepare_ci_profile.py"))["main"]
    output = tmp_path / "live.env"
    prepare(["--output", str(output)])
    assert output.stat().st_mode & 0o777 == 0o600
    assert "synthetic-runner-secret" not in output.read_text() + capsys.readouterr().out
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in workspace.glob("*.json"))
    with pytest.raises(FileExistsError):
        prepare(["--output", str(output)])
    with pytest.raises(SystemExit):
        prepare(["--output", str(workspace / "unsafe.env")])
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")
    with pytest.raises(SystemExit):
        prepare(["--output", str(tmp_path / "pr.env")])
    assert not (tmp_path / "pr.env").exists()
