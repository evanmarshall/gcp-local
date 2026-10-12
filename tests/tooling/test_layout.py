import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts/check"))

import layout

SCENARIO = 'pytestmark = pytest.mark.gcp_scenario(id="{id}", service="x")\n'
WORKFLOW = "jobs:\n  recordings:\n    steps:\n      - run: sh scripts/check/recordings.sh\n"


class Tree:
    """A throwaway repository tree with one wired-in recording."""

    def __init__(self, root):
        self.root = Path(root)
        self.write(
            "tests/conformance/databases/spanner/test_spanner.py",
            SCENARIO.format(id="spanner-admin"),
        )
        self.record("databases/spanner", "spanner-admin")
        self.write(layout.WORKFLOW, WORKFLOW)
        self.write(layout.REPLAY, "python3 scripts/run/recordings.py\n")

    def write(self, name, text=""):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def record(self, service, scenario):
        self.write(f"tests/conformance/recordings/{service}/{scenario}/provenance.json", "{}")
        self.write(f"tests/conformance/recordings/{service}/{scenario}/case.snapshot.json", "{}")

    def recording_errors(self):
        errors = []
        layout.check_recordings(errors, self.root)
        return errors


class RootLayoutTest(unittest.TestCase):
    def errors(self, *names):
        root = Path("/repo")
        errors = []
        layout.check_root_layout(errors, root, [root / name for name in names])
        return errors

    def test_allowed_root_entries_pass(self):
        self.assertEqual(self.errors("README.md", "go.mod", "docs/design.md", "scripts/go.sh"), [])

    def test_new_root_file_fails(self):
        self.assertEqual(
            self.errors("Dockerfile"),
            ["Dockerfile: new root file; put it in a directory (see ROOT_FILES)"],
        )

    def test_new_root_directory_fails(self):
        self.assertIn("misc/: new root directory", self.errors("misc/notes.txt")[0])


class DirectorySizeTest(unittest.TestCase):
    def errors(self, names):
        root = Path("/repo")
        errors = []
        layout.check_directory_sizes(errors, root, [root / name for name in names])
        return errors

    def test_twelve_children_pass(self):
        names = [f"docs/page{i}.md" for i in range(11)] + ["docs/services/storage.md"]
        self.assertEqual(self.errors(names), [])

    def test_thirteenth_child_fails(self):
        names = [f"docs/page{i}.md" for i in range(12)] + ["docs/services/storage.md"]
        self.assertEqual(len(self.errors(names)), 1)
        self.assertIn("docs/: 13 children", self.errors(names)[0])

    def test_nested_directories_are_checked(self):
        names = [f"internal/iam/roles/storage/role{i}.json" for i in range(13)]
        self.assertIn("internal/iam/roles/storage/: 13 children", self.errors(names)[0])

    def test_root_is_exempt(self):
        self.assertEqual(self.errors([f"file{i}" for i in range(20)]), [])


class RecordingWiringTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.tree = Tree(self.directory.name)

    def tearDown(self):
        self.directory.cleanup()

    def test_wired_recording_passes(self):
        self.assertEqual(self.tree.recording_errors(), [])

    def test_recording_without_scenario_fails(self):
        self.tree.record("databases/spanner", "spanner-orphan")
        errors = self.tree.recording_errors()
        self.assertEqual(len(errors), 1)
        self.assertIn("spanner-orphan: no scenario with this id", errors[0])

    def test_scenario_in_another_service_does_not_count(self):
        self.tree.write(
            "tests/conformance/storage/test_storage.py", SCENARIO.format(id="storage-x")
        )
        self.tree.record("databases/bigtable", "storage-x")
        self.assertIn("storage-x: no scenario with this id", self.tree.recording_errors()[0])

    def test_pubsub_recording_without_harness_test_fails(self):
        self.tree.write(
            "tests/conformance/messaging/pubsub/new/test_new.py", SCENARIO.format(id="pubsub-new")
        )
        self.tree.write(
            "tests/conformance/messaging/pubsub/new/conftest.py",
            "identities = pytestconfig.getoption(\"gcp_pubsub_identities\")\n",
        )
        self.tree.record("messaging/pubsub", "pubsub-new")
        errors = self.tree.recording_errors()
        self.assertEqual(len(errors), 1)
        self.assertIn("pubsub-new: no Pub/Sub harness test replays this recording", errors[0])

    def test_pubsub_rest_scenario_replays_in_the_rest_run(self):
        module = "tests/conformance/messaging/pubsub/rest/test_rest.py"
        self.tree.write(module, SCENARIO.format(id="pubsub-rest"))
        self.tree.record("messaging/pubsub", "pubsub-rest")
        self.assertEqual(self.tree.recording_errors(), [])
        runs = {run.name: run for run in layout.replay.plan(self.tree.root)[0]}
        self.assertIn(module, runs["rest"].paths)

    def test_replay_missing_from_ci_fails(self):
        self.tree.write(
            layout.WORKFLOW, "jobs:\n  other:\n    steps:\n      - run: sh scripts/check/go.sh\n"
        )
        self.assertIn(
            "no job runs `sh scripts/check/recordings.sh`", self.tree.recording_errors()[0]
        )

    def test_commented_out_replay_fails(self):
        self.tree.write(layout.WORKFLOW, "      # - run: sh scripts/check/recordings.sh\n")
        self.assertEqual(len(self.tree.recording_errors()), 1)

    def test_entry_point_must_run_the_driver(self):
        self.tree.write(layout.REPLAY, "echo skipped\n")
        self.assertIn("must run scripts/run/recordings.py", self.tree.recording_errors()[0])


class ReplayCoverageTest(unittest.TestCase):
    """The CI gate: verified IDs in the replay reports must equal the recording IDs."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.tree = Tree(self.directory.name)
        self.tree.record("storage", "storage-lifecycle")

    def tearDown(self):
        self.directory.cleanup()

    def report(self, run, *rows):
        results = [
            {"scenario": {"id": scenario}, "status": status, "verified": status == "pass"}
            for scenario, status in rows
        ]
        self.tree.write(
            f"{layout.replay.ARTIFACTS}/{run}/coverage.json", json.dumps({"results": results})
        )

    def test_all_verified_passes(self):
        self.report("storage", ("storage-lifecycle", "pass"))
        self.report("rest", ("spanner-admin", "pass"))
        self.assertEqual(layout.replay.verify(self.tree.root), [])

    def test_skipped_recording_fails(self):
        self.report("storage", ("storage-lifecycle", "pass"))
        self.report("rest", ("spanner-admin", "skipped"))
        self.assertEqual(
            layout.replay.verify(self.tree.root),
            ["spanner-admin: not verified; outcomes: skipped (rest)"],
        )

    def test_recording_never_run_fails(self):
        self.report("storage", ("storage-lifecycle", "pass"))
        self.assertEqual(
            layout.replay.verify(self.tree.root), ["spanner-admin: not verified; outcomes: not run"]
        )

    def test_verified_scenario_without_recording_fails(self):
        self.report("storage", ("storage-lifecycle", "pass"), ("storage-gone", "pass"))
        self.report("rest", ("spanner-admin", "pass"))
        self.assertEqual(
            layout.replay.verify(self.tree.root), ["storage-gone: verified but has no recording"]
        )


if __name__ == "__main__":
    unittest.main()
