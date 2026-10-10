"""pytest integration for target guards, snapshot lifecycle, and honest reporting."""

import hashlib
import json
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import pytest_socket

from .config import Target
from .evidence import (
    Divergence,
    Inconclusive,
    Scenario,
    SnapshotCase,
    Unsupported,
    Unverified,
    now,
    sha256,
)
from .sandbox import Sandbox

STATUS = {
    Divergence: "divergence",
    Unsupported: "unsupported",
    Unverified: "unverified",
    Inconclusive: "inconclusive",
}


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register the GCP target and scope options."""
    group = parser.getgroup("GCP validation")
    group.addoption(
        "--gcp-mode", choices=("offline", "local", "record", "revalidate"), default="offline"
    )
    group.addoption("--gcp-project")
    group.addoption("--gcp-profile", help="Explicit sandbox .env; live modes only")
    group.addoption("--gcp-endpoint")
    group.addoption("--gcp-credentials-file")
    group.addoption("--gcp-live-scope")
    group.addoption("--gcp-allow-live", action="store_true")
    group.addoption("--gcp-baselines", default="tests/conformance/recordings")
    group.addoption("--gcp-artifacts", default=".artifacts/validation")
    group.addoption(
        "--gcp-provider-version", help="Explicit local implementation version for reports"
    )
    group.addoption("--gcp-backend-version", help="Explicit local backend version for reports")
    group.addoption(
        "--gcp-pubsub-scope", help="Separately approved bounded Pub/Sub scope JSON; live modes only"
    )
    group.addoption(
        "--gcp-pubsub-identities", help="Explicit synthetic ADC bundle for isolated local Pub/Sub"
    )


def network_policy(config: pytest.Config, *, loopback: bool = False) -> None:
    """Install the socket guard: loopback only for synthetic harness tests."""
    pytest_socket.enable_socket()
    target = config.gcp_target
    if target.mode in {"record", "revalidate"}:
        return
    if loopback or target.mode == "local":
        hosts = {"127.0.0.1", "::1"}
        if target.endpoint:
            hosts.add(urlsplit(target.endpoint).hostname)
        pytest_socket.socket_allow_hosts(sorted(hosts))
    else:
        pytest_socket.disable_socket()


@pytest.hookimpl(tryfirst=True)
def select_target(config: pytest.Config) -> None:
    """Validate the option combination and select the GCP target on the config."""
    profile_path = config.getoption("gcp_profile")
    config.gcp_sandbox = None
    if profile_path:
        if config.getoption("gcp_mode") not in {"record", "revalidate"} or not config.getoption(
            "gcp_allow_live"
        ):
            raise ValueError("Sandbox profiles require an explicit live mode and --gcp-allow-live")
        config.gcp_sandbox = Sandbox.load(profile_path)
    profile = config.gcp_sandbox
    config.gcp_target = Target.select(
        config.getoption("gcp_mode"),
        project=config.getoption("gcp_project") or (profile.project if profile else None),
        endpoint=config.getoption("gcp_endpoint"),
        credentials_file=config.getoption("gcp_credentials_file")
        or (profile.identities["allowed"].credentials_file if profile else None),
        live_scope=config.getoption("gcp_live_scope") or (profile.live_scope if profile else None),
        allow_live=config.getoption("gcp_allow_live"),
    )
    versions = [
        config.getoption("gcp_provider_version"),
        config.getoption("gcp_backend_version"),
    ]
    if config.getoption("gcp_pubsub_scope") and config.gcp_target.mode not in {
        "record",
        "revalidate",
    }:
        raise ValueError("Pub/Sub cloud scope is only valid in explicit live modes")
    if config.getoption("gcp_pubsub_identities") and config.gcp_target.mode != "local":
        raise ValueError("Synthetic Pub/Sub identities are only valid in local mode")
    if any(versions) and (
        config.gcp_target.mode != "local" or not all(value and value.strip() for value in versions)
    ):
        raise ValueError("Supply both implementation versions in local mode only")
    if any(
        config.getoption(option, default=False)
        for option in (
            "force_enable_socket",
            "allow_hosts",
            "allow_unix_socket",
        )
    ):
        raise ValueError("Use the GCP target options instead of overriding network guards")
    if config.pluginmanager.hasplugin("localstack_snapshot.pytest.snapshot"):
        raise ValueError(
            "Use the GCP snapshot adapter, not the AWS-oriented upstream pytest plugin"
        )


def pytest_configure(config: pytest.Config) -> None:
    """Select the GCP target and prepare markers and reporting state."""
    try:
        select_target(config)
    except (OSError, ValueError) as exc:
        raise pytest.UsageError(str(exc)) from None


def scenario_revision(config: pytest.Config, item: pytest.Item, metadata: dict) -> Scenario:
    """Bind the scenario's contract revision to its module and contract file hashes."""
    files = metadata.pop("contract_files", ())
    revision = sha256(item.path)
    if files:
        root = Path(config.rootpath).resolve()
        for name in files:
            path = (root / name).resolve()
            if Path(name).is_absolute() or not path.is_relative_to(root) or not path.is_file():
                raise ValueError("Contract files must be repository-relative files")
            item.gcp_contract_files[name] = sha256(path)
        revision = hashlib.sha256(
            json.dumps(
                {"scenario_module": revision, "contract_files": item.gcp_contract_files},
                sort_keys=True,
            ).encode()
        ).hexdigest()
    return Scenario(**metadata, revision=revision)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Validate scenario markers and socket overrides before collection ends."""
    ids = set()
    for item in items:
        item.gcp_reports = []
        item.gcp_status = None
        item.gcp_case = None
        item.gcp_scenario = None
        item.gcp_contract_files = {}
        if (
            any(item.get_closest_marker(name) for name in ("enable_socket", "allow_hosts"))
            or "socket_enabled" in item.fixturenames
        ):
            raise pytest.UsageError(
                "Use harness_loopback or an explicit GCP mode, not socket overrides"
            )
        marker = item.get_closest_marker("gcp_scenario")
        if marker:
            if item.get_closest_marker("harness_loopback"):
                raise pytest.UsageError(
                    "Cloud scenarios cannot be labeled as synthetic harness tests"
                )
            metadata = dict(marker.kwargs)
            if config.getoption("gcp_provider_version"):
                metadata["versions"] = dict(
                    metadata["versions"],
                    provider=config.getoption("gcp_provider_version"),
                    backend=config.getoption("gcp_backend_version"),
                )
            try:
                item.gcp_scenario = scenario_revision(config, item, metadata)
            except (ValueError, TypeError) as exc:
                raise pytest.UsageError(f"Invalid gcp_scenario metadata: {exc}") from None
            if item.gcp_scenario.id in ids:
                raise pytest.UsageError("Each collected cloud scenario needs a unique ID")
            ids.add(item.gcp_scenario.id)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    """Build the scenario evidence case for the selected target."""
    network_policy(item.config, loopback=bool(item.get_closest_marker("harness_loopback")))
    if item.gcp_scenario:
        mode = item.config.gcp_target.mode
        if mode == "offline":
            item.gcp_status = ("unverified", "No local or real-GCP target selected")
            pytest.skip(item.gcp_status[1])
        item.gcp_case = SnapshotCase(
            item.gcp_scenario,
            mode,
            item.config.getoption("gcp_baselines"),
            item.config.gcp_artifacts / "candidates",
        )
        item.gcp_case.context["contract_files"] = item.gcp_contract_files


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_teardown(item: pytest.Item) -> None:
    """Restore the default socket guard after each test."""
    yield
    network_policy(item.config)


@pytest.fixture
def gcp_target(pytestconfig: pytest.Config) -> Target:
    """Service adapters must use this explicit target, never ambient ADC."""
    return pytestconfig.gcp_target


@pytest.fixture
def gcp_snapshot(request: pytest.FixtureRequest) -> SnapshotCase:
    """Expose the scenario evidence case to the test body."""
    if request.node.gcp_case is None:
        raise ValueError("gcp_snapshot requires a gcp_scenario marker and explicit target")
    return request.node.gcp_case


def finalize_case(item: pytest.Item, report: pytest.TestReport) -> object:
    """Finish the evidence case on teardown and record the resulting status."""
    candidate = None
    try:
        candidate = item.gcp_case.finish()
        if candidate:
            item.gcp_status = ("unverified", "Cloud candidate written; review required")
    except Exception as exc:  # noqa: BLE001 -- every failure mode maps to a report status
        status = next((s for cls, s in STATUS.items() if isinstance(exc, cls)), "error")
        item.gcp_status = (
            status,
            str(exc) if status != "error" else "Snapshot finalization failed",
        )
        report.outcome = "failed"
        report.longrepr = item.gcp_status[1]
    return candidate


def status_of(item: pytest.Item, report: pytest.TestReport, candidate: object) -> tuple[str, str]:
    """Choose the report row's status, with cleanup failure never passing."""
    if item.gcp_status:
        status, reason = item.gcp_status
    elif any(part.failed for part in item.gcp_reports):
        status, reason = "error", "Test or cleanup failed"
    elif any(part.skipped for part in item.gcp_reports):
        status, reason = "skipped", "Skipped or expected failure; not coverage"
    else:
        status, reason = "pass", "Completed"
    # Cleanup failure overrides a prior skip/inconclusive result and is never a pass.
    if report.failed and report.when == "teardown":
        status, reason = "error", "Cleanup failed"
    scenario = item.gcp_scenario
    case = item.gcp_case
    row = {
        "test": item.nodeid,
        "kind": "conformance" if scenario else "harness",
        "status": status,
        "reason": reason,
        "verified": bool(
            scenario and status == "pass" and case and case.manifest and case.source == "gcp"
        ),
        "scenario": asdict(scenario) if scenario else None,
        "last_cloud_validation": case.last_cloud_validation if case else None,
        "baseline_versions": case.manifest["scenario"].get("versions")
        if case and case.manifest
        else None,
        "candidate": str(candidate) if candidate else None,
    }
    item.config.gcp_results.append(row)
    report.user_properties.extend([("validation_kind", row["kind"]), ("validation_status", status)])
    return status, reason


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[object]) -> None:
    """Record per-phase outcomes and finalize the evidence case on teardown."""
    outcome = yield
    report = outcome.get_result()
    if call.excinfo:
        for exception, status in STATUS.items():
            if isinstance(call.excinfo.value, exception):
                item.gcp_status = (status, str(call.excinfo.value))
                break
    item.gcp_reports.append(report)
    if report.when != "teardown":
        return
    candidate = finalize_case(item, report) if item.gcp_case else None
    status_of(item, report, candidate)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Write the validation coverage report and the cloud evidence ledger."""
    config = session.config
    rows = config.gcp_results
    report = {
        "generated_at": now(),
        "mode": config.gcp_target.mode,
        "pytest_exit_status": int(exitstatus),
        "harness": dict(Counter(row["status"] for row in rows if row["kind"] == "harness")),
        "conformance": {
            "total": sum(row["kind"] == "conformance" for row in rows),
            "verified": sum(row["verified"] for row in rows),
            "outcomes": dict(
                Counter(row["status"] for row in rows if row["kind"] == "conformance")
            ),
        },
        "results": rows,
    }
    config.gcp_artifacts.mkdir(parents=True, exist_ok=True)
    (config.gcp_artifacts / "coverage.json").write_text(json.dumps(report, indent=2) + "\n")


def pytest_unconfigure(config: pytest.Config) -> None:  # noqa: ARG001 -- hook contract
    """Release plugin resources."""
    pytest_socket.enable_socket()
