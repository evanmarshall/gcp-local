import subprocess
import shutil
import tempfile
import unittest
from pathlib import Path

GIT = shutil.which("git") or "git"
PYTHON3 = shutil.which("python3") or "python3"

CHECK = Path(__file__).resolve().parents[2] / "scripts/check_history.py"
HOOK = Path(__file__).resolve().parents[2] / ".githooks/pre-push"
ZERO = "0" * 40


class HistoryPolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        self.git("init", "-b", "main")
        self.git("config", "user.name", "History Test")
        self.git("config", "user.email", "history@example.invalid")
        self.base = self.commit("base")

    def git(self, *args):
        return subprocess.check_output(
            [GIT, *args],
            cwd=self.repo,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()

    def commit(self, name):
        (self.repo / name).write_text(name + "\n")
        self.git("add", name)
        self.git("commit", "-m", name)
        return self.git("rev-parse", "HEAD")

    def push_check(self, local, remote, ref="refs/heads/main"):
        return subprocess.run(
            [PYTHON3, str(CHECK), "--pre-push"],
            cwd=self.repo,
            input=f"refs/heads/feature {local} {ref} {remote}\n",
            text=True,
            capture_output=True,
        )

    def test_fast_forward_is_allowed(self):
        result = self.push_check(self.commit("next"), self.base)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_divergent_main_is_rejected(self):
        remote = self.commit("remote")
        self.git("checkout", "-b", "feature", self.base)
        result = self.push_check(self.commit("local"), remote)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("fast-forward", result.stderr)

    def test_merge_commit_is_rejected_even_on_new_branch(self):
        self.git("checkout", "-b", "feature")
        self.commit("feature")
        self.git("checkout", "main")
        self.commit("main")
        self.git("merge", "--no-ff", "feature", "-m", "merge")
        result = self.push_check(self.git("rev-parse", "HEAD"), ZERO, "refs/heads/new")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Merge commits", result.stderr)

    def test_deleting_main_is_rejected(self):
        self.assertNotEqual(self.push_check(ZERO, self.base).returncode, 0)

    def test_deleting_feature_is_allowed(self):
        self.assertEqual(self.push_check(ZERO, self.base, "refs/heads/feature").returncode, 0)

    def test_missing_remote_commit_requests_fetch(self):
        result = self.push_check(self.base, "1" * 40)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("fetch and retry", result.stderr)

    def test_new_main_is_allowed(self):
        self.assertEqual(self.push_check(self.base, ZERO).returncode, 0)

    def test_hook_is_invoked_by_git_push(self):
        bare = self.repo / "remote.git"
        self.git("init", "--bare", str(bare))
        scripts = self.repo / "scripts"
        scripts.mkdir()
        (scripts / "check_history.py").write_text(CHECK.read_text())
        hooks = self.repo / ".githooks"
        hooks.mkdir()
        hook = hooks / "pre-push"
        hook.write_text(HOOK.read_text())
        hook.chmod(0o755)
        self.git("config", "core.hooksPath", ".githooks")
        self.git("remote", "add", "origin", str(bare))
        self.git("push", "origin", "main")
        result = subprocess.run(
            [GIT, "push", "origin", ":main"],
            cwd=self.repo,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Deleting main", result.stderr)


if __name__ == "__main__":
    unittest.main()
