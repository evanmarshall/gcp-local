"""Thin, GCP-independent adapter for LocalStack's pinned snapshot library."""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
from importlib.metadata import version
import json
from pathlib import Path
import re
from uuid import uuid4

from localstack_snapshot.snapshots import SnapshotAssertionError, SnapshotSession


class Unverified(AssertionError):
    """Required cloud evidence is missing or unsuitable."""


class Divergence(AssertionError):
    """An observation differs from the reviewed baseline."""


class Unsupported(AssertionError):
    """The tested behavior is explicitly outside current support."""


class Inconclusive(AssertionError):
    """The run could not establish the behavior, for example due to propagation."""


def now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


@dataclass(frozen=True)
class Scenario:
    id: str
    service: str
    transport: str
    revision: str
    versions: dict = field(default_factory=dict)

    def __post_init__(self):
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", self.id):
            raise ValueError("Scenario IDs must be lowercase hyphenated names")
        if not all((self.service, self.transport, self.revision)):
            raise ValueError("Scenario service, transport, and revision are required")
        if set(self.versions) != {"sdk", "provider", "backend"}:
            raise ValueError("Declare sdk, provider, and backend versions, using 'not-applicable' where appropriate")
        if not all(isinstance(v, str) and v.strip() for v in self.versions.values()):
            raise ValueError("Version declarations must be nonempty strings")


SECRET_KEYS = {
    "authorization", "proxyauthorization", "cookie", "setcookie", "password",
    "accesstoken", "refreshtoken", "idtoken", "privatekey", "clientsecret",
    "apikey", "xgoogapikey", "credentials",
}


def check_observation(value):
    """Reject common secret fields before they enter snapshot memory or disk.

    Scenarios must select public response fields; this is not a raw HTTP scrubber.
    """
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError("Observation keys must be strings")
            if re.sub(r"[^a-z]", "", key.lower()) in SECRET_KEYS:
                raise ValueError("Observation contains a credential-bearing field")
            check_observation(child)
    elif isinstance(value, list):
        for child in value:
            check_observation(child)
    elif isinstance(value, str):
        if re.search(r"(?i)\bBearer\s+\S+|-----BEGIN .*PRIVATE KEY-----", value):
            raise ValueError("Observation contains credential material")
        if value.lstrip().startswith(("{", "[")):
            try:
                decoded = json.loads(value)
            except ValueError:
                pass
            else:
                # The upstream snapshot library also parses JSON-valued strings.
                check_observation(decoded)
    elif value is not None and not isinstance(value, (bool, int, float)):
        raise ValueError("Observations must contain JSON values, not SDK/HTTP objects")
    json.dumps(value, allow_nan=False)


class SnapshotCase:
    def __init__(self, scenario, mode, baseline_dir, candidate_dir, *, source="gcp"):
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
            try:
                manifest = json.loads((self.baseline / "provenance.json").read_text())
                if not isinstance(manifest, dict):
                    raise ValueError("Invalid provenance mapping")
                snapshot_hash = sha256(self.baseline / "case.snapshot.json")
                if datetime.fromisoformat(manifest["recorded_at"]).tzinfo is None:
                    raise ValueError("Recording date must include its timezone")
                Scenario(**manifest["scenario"])
            except (OSError, ValueError, KeyError, TypeError):
                raise Unverified("Missing or invalid baseline evidence") from None
            expected = asdict(scenario)
            # SDK/provider/backend versions can differ locally: report both. The
            # scenario revision binds the behavior contract, not emulator versions.
            observed = manifest.get("scenario", {})
            if (manifest.get("source") != source or manifest.get("reviewed") is not True
                    or manifest.get("cleanup") != "passed"
                    or manifest.get("snapshot_sha256") != snapshot_hash
                    or any(observed.get(k) != expected[k] for k in ("id", "service", "transport", "revision"))):
                raise Unverified("Baseline is unreviewed, stale, synthetic, or has mismatched provenance")
            self.manifest = manifest
            self.last_cloud_validation = manifest["recorded_at"] if source == "gcp" else None

    def match(self, label, observation):
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", label):
            raise ValueError("Snapshot labels must be lowercase hyphenated names")
        if label in self.observations:
            raise ValueError("Duplicate snapshot label")
        check_observation(observation)
        self.observations[label] = json.loads(json.dumps(observation, allow_nan=False))

    def finish(self):
        """Invoke only after all test and cleanup phases succeeded."""
        check_observation(self.context)
        if not self.observations:
            raise Unverified("Scenario completed without any snapshot observations")
        recording = self.mode == "record"
        directory = self.candidate_dir / f"{self.scenario.id}-{uuid4().hex}" if recording else self.baseline
        if recording:
            directory.mkdir(parents=True, exist_ok=False)
        session = SnapshotSession(base_file_path=str(directory / "case"),
                                  scope_key=self.scenario.id, update=recording, verify=True, raw=False)
        if not recording and set(session.recorded_state or {}) != set(self.observations):
            raise Divergence("Observed snapshot labels differ from the reviewed baseline")
        for label, observation in self.observations.items():
            session.match(label, observation)
        try:
            session._assert_all()
        except SnapshotAssertionError as exc:
            changed = ", ".join(sorted(result.key for result in exc.result if not result))
            raise Divergence(f"Snapshot mismatch in: {changed}") from None
        if not recording:
            if self.mode == "revalidate" and self.source == "gcp":
                self.last_cloud_validation = now()
            return None
        # The upstream pytest plugin persists on teardown even after failures.
        # The GCP plugin calls us only after successful cleanup, without that plugin.
        session._persist_state()
        persisted = json.loads((directory / "case.snapshot.json").read_text())
        if persisted[self.scenario.id]["recorded-content"] != session.observed_state:
            raise OSError("Snapshot persistence failed")
        manifest = {
            "source": self.source, "reviewed": False, "cleanup": "passed",
            "recorded_at": now(), "scenario": asdict(self.scenario),
            "snapshot_sha256": sha256(directory / "case.snapshot.json"),
            "runner_versions": {name: version(name) for name in (
                "pytest", "pytest-socket", "localstack-snapshot")},
        }
        manifest["context"] = self.context
        (directory / "provenance.json").write_text(json.dumps(manifest, indent=2) + "\n")
        return directory
