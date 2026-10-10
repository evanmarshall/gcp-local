"""Portable bundle installation and diagnostics, using isolated application repositories."""

import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

PYTHON3 = shutil.which("python3") or "python3"
GIT = shutil.which("git") or "git"

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "agent_installer", ROOT / "scripts/install_agent_skill.py"
)
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


def git(root, *args):
    return subprocess.run(
        [
            GIT,
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            *args,
        ],
        cwd=root,
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()


def tree(root):
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file() and ".git" not in path.relative_to(root).parts and not path.is_symlink()
    }


@pytest.fixture
def repos(tmp_path, monkeypatch):
    source, app = tmp_path / "source", tmp_path / "application"
    source.mkdir()
    app.mkdir()
    git(app, "init", "-q")
    (app / "AGENTS.md").write_text("Keep the application's existing instructions.\n")
    (app / ".agents/skills/another-skill").mkdir(parents=True)
    (app / ".agents/skills/another-skill/SKILL.md").write_text("Unrelated skill\n")
    shutil.copytree(ROOT / "skills/gcp-local-setup", source / "skills/gcp-local-setup")
    (source / ".gitignore").write_text(".env\n")
    git(source, "init", "-q")
    git(source, "add", ".")
    git(source, "commit", "-qm", "Synthetic skill source.")
    monkeypatch.setattr(installer, "ROOT", source)
    return source, app


def test_preview_then_install_preserves_application_and_is_idempotent(repos):
    source, app = repos
    before = tree(app)
    plan = installer.install(app)
    assert plan["mode"] == "preview" and plan["write"]
    assert before == tree(app)
    installer.install(app, apply=True)
    assert (app / "AGENTS.md").read_text().startswith(before["AGENTS.md"].decode())
    assert (app / ".agents/skills/another-skill/SKILL.md").read_bytes() == before[
        ".agents/skills/another-skill/SKILL.md"
    ]
    copied = tree(app / installer.DESTINATION)
    assert set(copied) == set(tree(source / installer.BUNDLE)) | {installer.STAMP}
    stamp = json.loads(copied[installer.STAMP])
    assert stamp["source_revision"] == git(source, "rev-parse", "HEAD")
    installed = tree(app)
    assert installer.install(app, apply=True)["write"] == []
    assert installed == tree(app)


def test_update_previews_and_preserves_user_instructions(repos):
    source, app = repos
    installer.install(app, apply=True)
    agents_before = (app / "AGENTS.md").read_bytes()
    guide = source / installer.BUNDLE / "references/profiles.md"
    guide.write_text(guide.read_text() + "\nA synthetic documentation update.\n")
    with pytest.raises(ValueError, match="Commit"):
        installer.install(app)
    git(source, "add", ".")
    git(source, "commit", "-qm", "Update fixture guide.")
    assert str(installer.DESTINATION / "references/profiles.md") in installer.install(app)["write"]
    installer.install(app, apply=True)
    assert (app / "AGENTS.md").read_bytes() == agents_before
    assert installer.install(app)["write"] == []


@pytest.mark.parametrize("path", ["SKILL.md", "references/profiles.md", "scripts/inspect_repo.py"])
def test_local_edits_are_never_overwritten(repos, path):
    _, app = repos
    installer.install(app, apply=True)
    file = app / installer.DESTINATION / path
    file.write_text(file.read_text() + "\nLocal customization.\n")
    before = tree(app)
    with pytest.raises(ValueError, match="local edits"):
        installer.install(app, apply=True)
    assert tree(app) == before


@pytest.mark.parametrize("path", [".agents", "AGENTS.md", ".agents/skills/gcp-local-setup"])
def test_symlink_escapes_are_rejected(repos, tmp_path, path):
    _, app = repos
    destination = app / path
    if destination.is_dir():
        shutil.rmtree(destination)
    elif destination.exists():
        destination.unlink()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "sentinel").write_text("untouched")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        installer.install(app, apply=True)
    assert (outside / "sentinel").read_text() == "untouched"
    assert len(list(outside.iterdir())) == 1


def test_unmanaged_skill_and_edited_discovery_entry_are_preserved(repos):
    _, app = repos
    destination = app / installer.DESTINATION
    destination.mkdir()
    (destination / "SKILL.md").write_text("User's skill")
    with pytest.raises(ValueError, match="unmanaged"):
        installer.install(app, apply=True)
    shutil.rmtree(destination)
    installer.install(app, apply=True)
    agents = app / "AGENTS.md"
    agents.write_text(
        agents.read_text().replace(
            "For GCP application validation", "Customized application validation"
        )
    )
    with pytest.raises(ValueError, match="AGENTS.md entry"):
        installer.install(app, apply=True)


def test_ignored_source_secrets_do_not_enter_copied_bundle(repos):
    source, app = repos
    (source / installer.BUNDLE / ".env").write_text("TOKEN=synthetic-secret")
    installer.install(app, apply=True)
    assert not (app / installer.DESTINATION / ".env").exists()
    assert all(b"synthetic-secret" not in data for data in tree(app).values())


def test_bundle_operates_after_source_checkout_is_removed(repos, tmp_path):
    source, app = repos
    (app / "package.json").write_text(
        json.dumps(
            {
                "dependencies": {"@google-cloud/storage": "^7"},
                "scripts": {"test": "touch MUST_NOT_RUN; SECRET=synthetic-secret npm test"},
            }
        )
    )
    (app / "requirements.txt").write_text("google-cloud-pubsub==2.0\n")
    (app / "main.tf").write_text('resource "google_storage_bucket" "example" {}\n')
    (app / "app.py").write_text('raise RuntimeError("MUST_NOT_IMPORT")\n# STORAGE_EMULATOR_HOST\n')
    (app / ".env").write_text("TOKEN=synthetic-secret")
    installer.install(app, apply=True)
    moved = tmp_path / "relocated-app"
    shutil.move(app, moved)
    shutil.rmtree(source)
    block_network = (
        "import runpy,socket,sys; "
        "socket.socket=lambda *a,**k: (_ for _ in ()).throw(RuntimeError('network forbidden')); "
        "sys.argv=sys.argv[1:]; runpy.run_path(sys.argv[0],run_name='__main__')"
    )
    command = [
        sys.executable,
        "-I",
        "-c",
        block_network,
        str(moved / installer.DESTINATION / "scripts/inspect_repo.py"),
        "--repo",
        str(moved),
        "--output",
        ".gcp-local/compatibility.json",
        "--doctor",
    ]
    result = subprocess.run(command, capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 2, result.stderr
    report = json.loads(result.stdout)
    assert report == json.loads((moved / ".gcp-local/compatibility.json").read_text())
    assert set(report["languages"]) >= {"python", "javascript", "terraform"}
    assert {entry["name"] for entry in report["services"]} == {"storage", "pubsub"}
    assert "google_storage_bucket" in report["terraform_resource_types"]
    assert report["endpoint_variable_references"] == ["STORAGE_EMULATOR_HOST"]
    assert report["test_command_candidates"] == [
        {"cwd": ".", "command": "npm run test", "execution": "not-run"}
    ]
    assert report["profiles"]["local"]["status"] == "unavailable"
    assert not (moved / "MUST_NOT_RUN").exists()
    assert "synthetic-secret" not in result.stdout


def test_inspector_does_not_follow_links_or_overwrite_unrelated_reports(repos, tmp_path):
    _, app = repos
    external = tmp_path / "external.py"
    external.write_text("import google.cloud.bigquery\n")
    (app / "linked.py").symlink_to(external)
    installer.install(app, apply=True)
    script = app / installer.DESTINATION / "scripts/inspect_repo.py"
    output = app / "report.json"
    output.write_text('{"user":"data"}')
    result = subprocess.run(
        [sys.executable, "-I", str(script), "--repo", str(app), "--output", "report.json"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 1 and "unrelated report" in result.stderr
    assert output.read_text() == '{"user":"data"}'
    result = subprocess.run(
        [sys.executable, "-I", str(script), "--repo", str(app)],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert json.loads(result.stdout)["services"] == []
