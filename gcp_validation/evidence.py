"""Thin, GCP-independent adapter for LocalStack's pinned snapshot library."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

from localstack_snapshot.snapshots import SnapshotAssertionError, SnapshotSession


class Unverified(AssertionError):  # noqa: N818 -- domain vocabulary, not a defect type
    """Required cloud evidence is missing or unsuitable."""


class Divergence(AssertionError):  # noqa: N818 -- domain vocabulary, not a defect type
    """An observation differs from the reviewed baseline."""


class Unsupported(AssertionError):  # noqa: N818 -- domain vocabulary, not a defect type
    """The tested behavior is explicitly outside current support."""


class Inconclusive(AssertionError):  # noqa: N818 -- domain vocabulary, not a defect type
    """The run could not establish the behavior, for example due to propagation."""


def now() -> str:
    """Return the current UTC time as an ISO string for provenance records."""
    return datetime.now(UTC).isoformat()


def sha256(path: Path) -> str:
    """Digest a file for provenance binding."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


@dataclass(frozen=True)
class Scenario:
    """A cloud conformance scenario's identity and recorded contract revision."""

    id: str
    service: str
    transport: str
    revision: str
    versions: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Reject malformed scenario metadata before any recording starts."""
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", self.id):
            raise ValueError("Scenario IDs must be lowercase hyphenated names")
        if not all((self.service, self.transport, self.revision)):
            raise ValueError("Scenario service, transport, and revision are required")
        if set(self.versions) != {"sdk", "provider", "backend"}:
            raise ValueError(
                "Declare sdk, provider, and backend versions, "
                "using 'not-applicable' where appropriate"
            )
        if not all(isinstance(v, str) and v.strip() for v in self.versions.values()):
            raise ValueError("Version declarations must be nonempty strings")


SECRET_KEYS = {
    "authorization",
    "proxyauthorization",
    "cookie",
    "setcookie",
    "password",
    "accesstoken",
    "refreshtoken",
    "idtoken",
    "privatekey",
    "clientsecret",
    "apikey",
    "xgoogapikey",
    "credentials",
}


def load_provenance(baseline: Path) -> tuple[dict[str, object], str]:
    """Read a baseline's provenance record, converting any defect to Unverified."""
    try:
        manifest = json.loads((baseline / "provenance.json").read_text())
        snapshot_hash = sha256(baseline / "case.snapshot.json")
    except (OSError, ValueError):
        raise Unverified("Missing or unreadable baseline evidence") from None
    return manifest, snapshot_hash
    if not isinstance(manifest, dict):
        raise Unverified("Baseline provenance is not a mapping")
    recorded_at = manifest.get("recorded_at")
    stamp = None
    if isinstance(recorded_at, str):
        try:
            stamp = datetime.fromisoformat(recorded_at)
        except ValueError:
            stamp = None
    if stamp is None or stamp.tzinfo is None:
        raise Unverified("Baseline recording date is missing or lacks a timezone")
    try:
        Scenario(**manifest["scenario"])
    except (TypeError, ValueError, KeyError):
        raise Unverified("Baseline provenance does not describe a scenario") from None
    return manifest


def check_observation(value: object) -> None:
    """Reject common secret fields before they enter snapshot memory or disk.

    Scenarios must select public response fields; this is not a raw HTTP scrubber.
    """
    if isinstance(value, dict):
        check_mapping(value)
    elif isinstance(value, list):
        for child in value:
            check_observation(child)
    elif isinstance(value, str):
        check_text(value)
    elif value is not None and not isinstance(value, (bool, int, float)):
        # JSON-shaped, not a Python object: ValueError, not TypeError.
        raise ValueError("Observations must contain JSON values, not SDK/HTTP objects")
    json.dumps(value, allow_nan=False)


def check_mapping(value: dict[str, object]) -> None:
    """Reject secret-bearing mapping keys and recurse into the values."""
    for key, child in value.items():
        if not isinstance(key, str):
            # JSON keys are strings; a non-string key is a caller type error.
            raise TypeError("Observation keys must be strings")
        if re.sub(r"[^a-z]", "", key.lower()) in SECRET_KEYS:
            raise ValueError("Observation contains a credential-bearing field")
        check_observation(child)


def check_text(value: str) -> None:
    """Reject credential material and re-check embedded JSON strings."""
    if re.search(r"(?i)\bBearer\s+\S+|-----BEGIN .*PRIVATE KEY-----", value):
        raise ValueError("Observation contains credential material")
    if not value.lstrip().startswith(("{", "[")):
        return
    try:
        decoded = json.loads(value)
    except ValueError:
        return
    # The upstream snapshot library also parses JSON-valued strings.
    check_observation(decoded)


class SnapshotCase:
    """One scenario's evidence session against a recorded baseline directory."""

    def __init__(
        self,
        scenario: Scenario,
        mode: str,
        baseline_dir: Path,
        candidate_dir: Path,
        *,
        source: str = "gcp",
    ) -> None:
        """Bind one scenario to its baseline directory and comparison mode."""
        self.scenario, self.mode, self.source = scenario, mode, source
        self.candidate_dir = Path(candidate_dir)
        self.observations = {}
        self.last_cloud_validation = None
        self.baseline = Path(baseline_dir) / scenario.id
        self.manifest = None
        self.context = {}
        if mode not in {"local", "record", "revalidate"}:
            raise ValueError("Snapshots require an explicit comparison or recording mode")
        if source not in {"gcp", "synthetic"}:
            raise ValueError("Unknown evidence source")
        if mode != "record":
            manifest, snapshot_hash = load_provenance(self.baseline)
            expected = asdict(scenario)
            # SDK/provider/backend versions can differ locally: report both. The
            # scenario revision binds the behavior contract, not emulator versions.
            observed = manifest.get("scenario", {})
            if (
                manifest.get("source") != source
                or manifest.get("reviewed") is not True
                or manifest.get("cleanup") != "passed"
                or manifest.get("snapshot_sha256") != snapshot_hash
                or any(
                    observed.get(k) != expected[k]
                    for k in ("id", "service", "transport", "revision")
                )
            ):
                raise Unverified(
                    "Baseline is unreviewed, stale, synthetic, or has mismatched provenance"
                )
            self.manifest = manifest
            self.last_cloud_validation = manifest["recorded_at"] if source == "gcp" else None

    def match(self, label: str, observation: object) -> None:
        """Record one observation under a fresh, hyphenated label."""
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", label):
            raise ValueError("Snapshot labels must be lowercase hyphenated names")
        if label in self.observations:
            raise ValueError("Duplicate snapshot label")
        check_observation(observation)
        self.observations[label] = json.loads(json.dumps(observation, allow_nan=False))

    def finish(self) -> Path | None:
        """Invoke only after all test and cleanup phases succeeded."""
        check_observation(self.context)
        if not self.observations:
            raise Unverified("Scenario completed without any snapshot observations")
        recording = self.mode == "record"
        directory = (
            self.candidate_dir / f"{self.scenario.id}-{uuid4().hex}" if recording else self.baseline
        )
        if recording:
            directory.mkdir(parents=True, exist_ok=False)
        session = SnapshotSession(
            base_file_path=str(directory / "case"),
            scope_key=self.scenario.id,
            update=recording,
            verify=True,
            raw=False,
        )
        if not recording and set(session.recorded_state or {}) != set(self.observations):
            raise Divergence("Observed snapshot labels differ from the reviewed baseline")
        for label, observation in self.observations.items():
            session.match(label, observation)
        try:
            session._assert_all()  # noqa: SLF001 -- the pinned snapshot API is private by design
        except SnapshotAssertionError as exc:
            changed = ", ".join(sorted(result.key for result in exc.result if not result))
            raise Divergence(f"Snapshot mismatch in: {changed}") from None
        if not recording:
            if self.mode == "revalidate" and self.source == "gcp":
                self.last_cloud_validation = now()
            return None
        # The upstream pytest plugin persists on teardown even after failures.
        # The GCP plugin calls us only after successful cleanup, without that plugin.
        session._persist_state()  # noqa: SLF001 -- the pinned snapshot API is private by design
        persisted = json.loads((directory / "case.snapshot.json").read_text())
        if persisted[self.scenario.id]["recorded-content"] != session.observed_state:
            raise OSError("Snapshot persistence failed")
        manifest = {
            "source": self.source,
            "reviewed": False,
            "cleanup": "passed",
            "recorded_at": now(),
            "scenario": asdict(self.scenario),
            "snapshot_sha256": sha256(directory / "case.snapshot.json"),
            "runner_versions": {
                name: version(name) for name in ("pytest", "pytest-socket", "localstack-snapshot")
            },
        }
        manifest["context"] = self.context
        (directory / "provenance.json").write_text(json.dumps(manifest, indent=2) + "\n")
        return directory
