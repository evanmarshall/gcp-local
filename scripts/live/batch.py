#!/usr/bin/env python3
"""Record or revalidate many scenarios against the sandbox in parallel.

Plans one pytest invocation per scenario module from the recordings tree (the
same mapping the replay check uses), plus any new scenario --only names.
Modules that take the same sandbox lock (one per service) share a lane and
run in order; up to --jobs lanes run at once, and the modules in EXCLUSIVE
run alone afterwards. Each run gets its own artifacts directory and log under
--artifacts. Slow scenarios are bound by GCP creating instances, so
overlapping lanes cuts wall time without changing cost.

Scope files mirror the module path under --scopes: the scope for
tests/conformance/databases/bigquery/test_bigquery_sql.py is
databases/bigquery/bigquery_sql.json. Storage scenarios take their scope from
the profile. Estimate cost and confirm authorization before a live run
(docs/contributing/gcp-sandbox.md); --dry-run prints the plan without running.
"""

import argparse
import importlib
import importlib.util
import json
import re
import runpy
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("recordings", ROOT / "scripts/run/recordings.py")
replay = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(replay)

# These rewrite the project IAM policy; concurrent policy writes from other
# scenarios' temporary grants would fail on the policy etag. The deny policy
# scenarios attach deny policies to the project, which apply to every
# concurrent caller.
EXCLUSIVE = frozenset({
    "tests/conformance/security/resourcemanager/test_policy.py",
    "tests/conformance/logging/test_cloudlogging_conditions.py",
    "tests/conformance/security/resourcemanager/versions/test_versions.py",
    "tests/conformance/security/iam/deny/test_deny_lifecycle.py",
    "tests/conformance/security/iam/deny/test_deny_enforcement.py",
})
# Scope services of the compute scenarios, which share one lane.
COMPUTE_SCOPES = frozenset({"networking", "vmconnectivity", "vmserviceaccounts", "computelists"})
# The scope option each scenario directory's conftest reads, most specific first.
OPTIONS = ("gcp_pubsub_scope", "gcp_secretmanager_scope", "gcp_live_scope_file")


# Scope contracts whose service name and limits aren't the module's SERVICE and LIMITS.
SCOPE_CONTRACTS = {
    "gcp_validation.services.messaging.pubsub": ("pubsub_scope", "pubsub", "LIMITS"),
    "gcp_validation.services.messaging.pubsub_delivery": (
        None, "pubsub-delivery", "DELIVERY_LIMITS",
    ),
    "gcp_validation.services.messaging.pubsub_schemas": (None, "pubsub-schemas", "SCHEMA_LIMITS"),
    "gcp_validation.services.security.secretmanager.secrets": (None, "secretmanager", "LIMITS"),
}


@dataclass(frozen=True)
class Entry:
    """One pytest invocation: a scenario module and the recordings it covers."""

    module: str
    scenarios: tuple[str, ...]
    option: str | None

    @property
    def name(self) -> str:
        """A short, unique label for logs and artifacts."""
        return self.module.removeprefix("tests/conformance/").removesuffix(".py").replace("/", "-")


def scope_option(module: Path) -> str | None:
    """Return the scope option the module's fixtures read, or None for profile scopes."""
    sources = [module, *sorted(module.parent.glob("conftest.py"))]
    text = "".join(path.read_text() for path in sources)
    return next(("--" + o.replace("_", "-") for o in OPTIONS if o in text), None)


def declared(root: Path, only: set[str]) -> dict[str, str]:
    """Map each named scenario ID without a recording to its service path."""
    found = {}
    for module in sorted((root / replay.CONFORMANCE).rglob("test_*.py")):
        for scenario in re.findall(r"""\bid\s*=\s*["']([a-z0-9-]+)["']""", module.read_text()):
            if scenario in only:
                service = module.parent.relative_to(root / replay.CONFORMANCE)
                found[scenario] = service.as_posix()
    return found


def plan(root: Path = ROOT, only: set[str] | None = None) -> list[Entry]:
    """Group every recording, and each new scenario --only names, by its scenario module."""
    modules: dict[str, list[str]] = {}
    scenarios = replay.recordings(root)
    if only:
        scenarios = {**declared(root, only - set(scenarios)), **scenarios}
    for scenario, service in scenarios.items():
        module = replay.scenario_module(root, scenario, service)
        if module is None:
            raise SystemExit(f"recording {scenario} has no scenario module under {service}")
        modules.setdefault(module.relative_to(root).as_posix(), []).append(scenario)
    entries = [
        Entry(module, tuple(sorted(ids)), scope_option(root / module))
        for module, ids in sorted(modules.items())
    ]
    if only:
        entries = [e for e in entries if only & {e.name, e.module, *e.scenarios}]
    return entries


def scope_file(entry: Entry, scopes: Path) -> Path:
    """Return an entry's scope file, mirroring its module path under the scopes directory."""
    module = Path(entry.module).relative_to("tests/conformance")
    path = scopes / module.parent / (module.stem.removeprefix("test_") + ".json")
    if not path.is_file():
        raise SystemExit(f"{entry.name}: missing scope file {path}")
    return path


def scope_contract(entry: Entry) -> tuple[str, dict[str, int]]:
    """Return the service name and fixed limits the entry's scope loader requires."""
    conftest = (ROOT / entry.module).parent / "conftest.py"
    imports = re.findall(r"from (gcp_validation\.services\.[\w.]+) import", conftest.read_text())
    name = imports[-1]
    sibling, service, limits = SCOPE_CONTRACTS.get(name, (None, None, "LIMITS"))
    module = importlib.import_module(name.rsplit(".", 1)[0] + "." + sibling if sibling else name)
    # A scenario with its own limits (BigQuery SQL) declares LIMITS in the test module.
    own = re.search(r"^LIMITS = ", (ROOT / entry.module).read_text(), re.MULTILINE)
    source = runpy.run_path(str(ROOT / entry.module)) if own else vars(module)
    return service or source.get("SERVICE") or module.SERVICE, dict(source[limits])


def write_scopes(entries: list[Entry], scopes: Path, profile: str) -> None:
    """Write the approved scope file for every entry that takes one."""
    sys.path.insert(0, str(ROOT))
    from gcp_validation.sandbox import Sandbox  # noqa: PLC0415 -- needs ROOT on the path
    from gcp_validation.scope import scope_document  # noqa: PLC0415

    sandbox = Sandbox.load(profile)
    for entry in entries:
        if entry.option is None:
            continue
        module = Path(entry.module).relative_to("tests/conformance")
        path = scopes / module.parent / (module.stem.removeprefix("test_") + ".json")
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        service, limits = scope_contract(entry)
        document = scope_document(service, sandbox.project, sandbox.project_number, limits,
                                  approved=True)
        path.write_text(json.dumps(document, indent=2) + "\n")
        print(f"wrote {path}")


def lane(entry: Entry, scopes: Path) -> str:
    """Return the sandbox lock an entry takes; entries sharing one must not overlap."""
    if entry.option is None:
        return "storage"
    # The Secret Manager scenarios share the project's secrets and one lock.
    if entry.option == "--gcp-secretmanager-scope":
        return "secretmanager"
    if entry.option == "--gcp-live-scope-file":
        service = json.loads(scope_file(entry, scopes).read_text())["services"][0]
        # The compute scenarios create VPC networks; together they'd exceed the quota.
        if service in COMPUTE_SCOPES:
            return "compute"
        # A Storage scenario with its own scope (notifications) still takes
        # the Storage fixture's one-bucket lock.
        return "storage" if service.startswith("storage") else service
    return entry.name


def command(entry: Entry, args: argparse.Namespace) -> list[str]:
    """Build one entry's pytest command."""
    arguments = [sys.executable, "-m", "pytest", entry.module, "-q", "--tb=short",
                 "--gcp-mode", args.mode, f"--gcp-profile={args.profile}", "--gcp-allow-live",
                 f"--gcp-artifacts={Path(args.artifacts) / entry.name}"]
    if entry.option:
        arguments.append(f"{entry.option}={scope_file(entry, Path(args.scopes))}")
    return arguments


def execute(entry: Entry, args: argparse.Namespace) -> tuple[Entry, bool, float]:
    """Run one entry with its own log; return whether it passed and how long it took."""
    log = Path(args.artifacts) / f"{entry.name}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with log.open("w") as output:
        result = subprocess.run(command(entry, args), cwd=ROOT, stdout=output,
                                stderr=subprocess.STDOUT, check=False)
    return entry, result.returncode == 0, time.monotonic() - started


def schedule(entries: list[Entry], scopes: Path) -> tuple[dict[str, list[Entry]], list[Entry]]:
    """Split entries into lanes keyed by sandbox lock, plus the ones that run alone."""
    lanes: dict[str, list[Entry]] = {}
    for entry in entries:
        if entry.module not in EXCLUSIVE:
            lanes.setdefault(lane(entry, scopes), []).append(entry)
    return lanes, [e for e in entries if e.module in EXCLUSIVE]


def run(lanes: dict[str, list[Entry]], alone: list[Entry], args: argparse.Namespace) -> list[str]:
    """Run lanes in parallel, then the exclusive entries; return the failed names."""
    failed: list[str] = []

    def run_lane(members: list[Entry]) -> None:
        for entry, passed, seconds in (execute(e, args) for e in members):
            print(f"{'PASS' if passed else 'FAIL'} {entry.name} ({seconds:.0f}s)", flush=True)
            if not passed:
                failed.append(entry.name)

    with ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
        list(pool.map(run_lane, lanes.values()))
    run_lane(alone)
    return failed


def parse(argv: list[str] | None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--mode", choices=("record", "revalidate"), required=True)
    parser.add_argument("--profile", required=True, help="Explicit sandbox .env")
    parser.add_argument("--scopes", required=True, help="Directory of approved scope files")
    parser.add_argument("--artifacts", default=".artifacts/live")
    parser.add_argument("--jobs", type=int, default=4, help="Concurrent lanes; quotas cap this")
    parser.add_argument("--only", nargs="*", help="Entry names, modules, or scenario IDs")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--write-approved-scopes",
        action="store_true",
        help="Write approved scope files into --scopes and exit; only for an authorized run",
    )
    args = parser.parse_args(argv)
    args.artifacts = str(Path(args.artifacts) / args.mode)
    return args


def main(argv: list[str] | None = None) -> int:
    """Plan, then write scopes, print the plan, or run the batch."""
    args = parse(argv)
    entries = plan(only=set(args.only) if args.only else None)
    if args.write_approved_scopes:
        write_scopes(entries, Path(args.scopes), args.profile)
        return 0
    for entry in entries:
        command(entry, args)  # fail before anything runs when a scope file is missing
    lanes, alone = schedule(entries, Path(args.scopes))
    if args.dry_run:
        for key, members in sorted(lanes.items()):
            print(f"lane {key}: {', '.join(e.name for e in members)}")
        for entry in alone:
            print(f"alone: {entry.name}")
        return 0
    failed = run(lanes, alone, args)
    total = sum(len(members) for members in lanes.values()) + len(alone)
    print(f"{total - len(failed)} passed, {len(failed)} failed; logs in {args.artifacts}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
