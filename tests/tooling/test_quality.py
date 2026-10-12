"""The quality baselines can shrink but never grow."""

import collections
import runpy
import unittest
from pathlib import Path

quality = runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts/check/quality.py"))


class BaselineRatchetTest(unittest.TestCase):
    """A baseline may lose findings or move them between files, never add any."""

    def counts(self, *rows: tuple[str, str, int]) -> collections.Counter:
        """Build a rule/file counter."""
        return collections.Counter({(rule, name): count for rule, name, count in rows})

    def test_growth_is_reported_per_rule(self) -> None:
        """A rule whose total rises is growth."""
        before = self.counts(("E501", "a.py", 2))
        after = self.counts(("E501", "a.py", 2), ("E501", "b.py", 1), ("D103", "a.py", 1))
        self.assertEqual(quality["growth"](after, before), ["D103: 0 -> 1", "E501: 2 -> 3"])

    def test_moving_and_shrinking_are_allowed(self) -> None:
        """Findings that move files or disappear don't count."""
        before = self.counts(("E501", "old/a.py", 3))
        after = self.counts(("E501", "new/a.py", 2))
        self.assertEqual(quality["growth"](after, before), [])


if __name__ == "__main__":
    unittest.main()
