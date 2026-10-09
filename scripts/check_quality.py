#!/usr/bin/env python3
"""Run the repository quality gate: strict Go/Python lint plus dead-code analysis.

Every analyzer is version-pinned (toolchains/quality.json, requirements-quality.txt),
so the gate is reproducible on any host. Known lint findings live in the checked-in
baselines under quality/, keyed by rule and file; the gate fails on any finding that is
not baselined and on any baseline entry that has gone stale, so a baseline can only
shrink deliberately. Dead code has no baseline: every unreachable function must be
deleted or listed with a reason in quality/dead-code-go.txt, and every unused Python
definition must be deleted or named in quality/vulture-whitelist.py.
"""

import argparse
import collections
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
QUALITY = ROOT / "quality"
GO_BASELINE = QUALITY / "baseline-go.txt"
PYTHON_BASELINE = QUALITY / "baseline-python.txt"
GO_DEAD_ALLOWLIST = QUALITY / "dead-code-go.txt"
VULTURE_WHITELIST = QUALITY / "vulture-whitelist.py"
TOOLS = ROOT / ".tools/quality/bin"
GO_PROGRAMS = ["./cmd/gcp-local", "./tools/..."]
PYTHON_SOURCES = ("gcp_validation", "scripts", "skills", "tests", "examples", "conftest.py")


def run(command):
    return subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)


def relative(name):
    path = Path(name)
    return str(path.relative_to(ROOT)) if path.is_absolute() else name


def go_lint():
    report = ROOT / ".artifacts/quality/go-lint.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    result = run([str(TOOLS / "golangci-lint"), "run", "-c", ".golangci.yml",
                  "--output.json.path", str(report), "--output.text.path="])
    if not report.is_file():
        raise SystemExit(f"golangci-lint produced no report:\n{result.stdout}{result.stderr}")
    findings = collections.Counter()
    for issue in json.loads(report.read_text())["Issues"]:
        findings[(issue.get("FromLinter") or issue["Linter"], relative(issue["Pos"]["Filename"]))] += 1
    return findings


def python_lint():
    result = run([sys.executable, "-m", "ruff", "check", "--output-format", "json", "."])
    if result.returncode not in (0, 1):
        raise SystemExit(f"ruff failed:\n{result.stdout}{result.stderr}")
    findings = collections.Counter()
    for issue in json.loads(result.stdout or "[]"):
        findings[(issue["code"], relative(issue["filename"]))] += 1
    return findings


def go_dead_code():
    result = run([str(TOOLS / "deadcode"), *GO_PROGRAMS])
    dead = set()
    for line in result.stdout.splitlines():
        if "unreachable func: " in line:
            location, name = line.split("unreachable func: ", 1)
            dead.add(f"{relative(location.split(':')[0])}: {name.strip()}")
    return sorted(dead)


def python_dead_code():
    sources = [str(ROOT / name) for name in PYTHON_SOURCES if (ROOT / name).exists()]
    result = run([sys.executable, "-m", "vulture", "--min-confidence", "100",
                  *sources, str(VULTURE_WHITELIST)])
    return sorted({line.strip() for line in result.stdout.splitlines() if line.strip()})


def read_counts(path):
    counts = collections.Counter()
    if path.is_file():
        for line in path.read_text().splitlines():
            if line.strip() and not line.startswith("#"):
                count, rule, name = line.split(None, 2)
                counts[(rule, name)] = int(count)
    return counts


def write_counts(path, counts):
    rows = sorted(f"{count}\t{rule}\t{name}" for (rule, name), count in counts.items() if count)
    path.write_text("".join(f"{row}\n" for row in rows))


def read_reasons(path):
    entries = []
    if path.is_file():
        for line in path.read_text().splitlines():
            entry, _, reason = line.partition("#")
            if entry.strip():
                entries.append((entry.strip(), reason.strip()))
    return entries


def compare(current, baseline, label):
    failures = False
    for (rule, name), count in sorted(current.items()):
        if count > baseline.get((rule, name), 0):
            print(f"new {label} finding: {name}: {rule} "
                  f"({count} now, {baseline.get((rule, name), 0)} baselined)", file=sys.stderr)
            failures = True
    for (rule, name), count in sorted(baseline.items()):
        if count > current.get((rule, name), 0):
            print(f"stale {label} baseline entry: {name}: {rule} "
                  "(rerun scripts/check_quality.py --update-baseline)", file=sys.stderr)
            failures = True
    return failures


def summarize(counts, label, rules=6):
    print(f"{label}: {sum(counts.values())} baselined finding(s) in {len(counts)} rule/file pair(s)")
    totals = collections.Counter()
    for (rule, _), count in counts.items():
        totals[rule] += count
    for rule, count in totals.most_common(rules):
        print(f"  {rule}: {count}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--update-baseline", action="store_true",
                        help="rewrite the checked-in lint baselines from the current findings")
    arguments = parser.parse_args()

    go = go_lint()
    python = python_lint()
    unreachable = go_dead_code()
    unused = python_dead_code()

    if arguments.update_baseline:
        write_counts(GO_BASELINE, go)
        write_counts(PYTHON_BASELINE, python)
        print(f"Baseline rewritten: {sum(go.values())} Go and {sum(python.values())} Python finding(s).")
        return 0

    failures = compare(go, read_counts(GO_BASELINE), "Go")
    failures |= compare(python, read_counts(PYTHON_BASELINE), "Python")

    allowed = {entry for entry, _ in read_reasons(GO_DEAD_ALLOWLIST)}
    for entry, reason in read_reasons(GO_DEAD_ALLOWLIST):
        if reason == "":
            print(f"{GO_DEAD_ALLOWLIST.name}: {entry} has no reason", file=sys.stderr)
            failures = True
    for name in unreachable:
        if name not in allowed:
            print(f"unreachable Go function: {name}", file=sys.stderr)
            failures = True
    for line in unused:
        print(f"unused Python definition: {line}", file=sys.stderr)
        failures = True

    summarize(go, "Go lint")
    summarize(python, "Python lint")
    print(f"Dead code: {len(unreachable)} unreachable Go function(s), "
          f"{len(unused)} unused Python definition(s)")
    if failures:
        print("Quality gate failed. Delete the dead code, fix the finding, or record why the "
              "finding is deliberate before rerunning.", file=sys.stderr)
        return 1
    print("Quality gate passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
