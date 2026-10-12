import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts/run"))

import client_matrix
import clients as run_clients


def combo(expect, match="known gap"):
    return {
        "service": "storage",
        "client": "fake",
        "package": "fake-client",
        "version": "1.0.0",
        "transport": "JSON REST",
        "expect": expect,
        "issue": 1 if expect == "xfail" else None,
        "match": match,
    }


def script(code):
    return [sys.executable, "-c", f"import sys; {code}"]


class RunComboTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.logs = Path(temporary.name)

    def run_combo(self, entry, code):
        return run_clients.run_combo(entry, script(code), {}, self.logs)

    def test_declared_pass_that_passes(self):
        result = self.run_combo(combo("pass"), "sys.exit(0)")
        self.assertEqual((result["outcome"], result["reason"]), ("pass", ""))

    def test_declared_pass_that_fails_reports_the_last_line(self):
        result = self.run_combo(combo("pass"), "print('step one'); sys.exit('create: boom')")
        self.assertEqual((result["outcome"], result["reason"]), ("fail", "create: boom"))
        self.assertIn("step one", (self.logs / "fake-storage.log").read_text())

    def test_expected_failure_on_its_gap(self):
        result = self.run_combo(combo("xfail"), "sys.exit('upload: known gap')")
        self.assertEqual(result["outcome"], "xfail")

    def test_expected_failure_that_passes_is_an_error(self):
        result = self.run_combo(combo("xfail"), "sys.exit(0)")
        self.assertEqual(result["outcome"], "xpass")

    def test_expected_failure_with_a_new_error_is_a_failure(self):
        result = self.run_combo(combo("xfail"), "sys.exit('upload: something else')")
        self.assertEqual(result["outcome"], "fail")
        self.assertIn("known gap", result["reason"])


class MatrixTests(unittest.TestCase):
    def test_every_combination_resolves_a_pinned_version(self):
        matrix = client_matrix.load()
        for entry in matrix["combos"]:
            self.assertIn(entry["client"], matrix["clients"])
            self.assertRegex(entry["version"], r"^\d+\.\d+")
            if entry["expect"] == "xfail":
                self.assertTrue(entry["match"] and entry["gap"] and entry["issue"], entry)
            else:
                self.assertEqual(entry["expect"], "pass")

    def test_coverage_lines_split_passing_and_failing_pairs(self):
        rows, failures = client_matrix.coverage_lines(client_matrix.load())
        self.assertTrue(all(row.endswith("` |") for row in rows))
        self.assertTrue(all("/issues/" in line for line in failures))


if __name__ == "__main__":
    unittest.main()
