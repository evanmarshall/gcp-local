"""Bounded Cloud KMS rotation-schedule scenario, not yet recorded against GCP.

GCP rotates on schedule only after a rotationPeriod of at least one day, so the run
records the schedule's round-trip, its patches, and its refusals, and the
destroyScheduledDuration a destroy honors. It creates one labeled symmetric key in the
shared ring and destroys its versions; refused creates leave nothing behind.
"""

import argparse
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import SnapshotCase, check_observation, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.security.cloudkms import LIVE_ENDPOINT, LOCATION, RING, version_of
from gcp_validation.services.security.refusals import outcome

SERVICE = "cloudkms"
LIMITS = {"max_key_rings": 1, "max_crypto_keys": 1, "max_key_versions": 1, "max_run_seconds": 600}
USABLE = {"ENABLED", "DISABLED"}
DAY = 86400
OK = 200


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the separately approved live scope for this scenario."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


def stamp(moment: datetime) -> str:
    """Render a whole-second UTC timestamp."""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def same_time(returned: str | None, sent: str) -> bool:
    """Report whether a returned timestamp names the sent instant, in any precision."""
    return bool(returned) and datetime.fromisoformat(returned) == datetime.fromisoformat(sent)


class RotationFixture(OwnedRun):
    """One labeled ENCRYPT_DECRYPT key per run; cleanup destroys its usable versions."""

    def __init__(self, client: RestClient, state_dir: Path, *, manifest: str | None = None) -> None:
        """Bind the run manifest, key name, and ownership labels."""
        super().__init__(
            client,
            state_dir,
            SERVICE,
            LIMITS["max_run_seconds"],
            manifest=manifest,
            check=lambda v: v.get("key") == "gr-" + v["run_id"][:12],
        )
        self.manifest.setdefault("key", "gr-" + self.manifest["run_id"][:12])
        self.ring_path = f"/v1/projects/{self.project}/locations/{LOCATION}/keyRings/{RING}"
        self.key_path = f"{self.ring_path}/cryptoKeys/{self.manifest['key']}"
        self.labels = {"managed-by": "gcp-local", "gcp-local-run": self.manifest["run_id"]}

    def call(
        self, method: str, suffix: str = "", *, query: dict | None = None, body: dict | None = None
    ) -> Response:
        """Send one request to the run's key as the fixture."""
        return self.client.request(
            "fixture",
            method,
            self.key_path + suffix,
            query=query,
            body=body,
            timeout=self.remaining(),
        )

    def create(self, key: str, body: dict) -> Response:
        """Create one key in the shared ring."""
        return self.client.request(
            "fixture",
            "POST",
            f"{self.ring_path}/cryptoKeys",
            query={"cryptoKeyId": key},
            body={"labels": self.labels, **body},
            timeout=self.remaining(),
        )

    def setup(self) -> None:
        """Ensure the shared ring exists."""
        self.acquire()
        self.client.verify_project()
        ring = self.client.request("fixture", "GET", self.ring_path)
        if ring.status == 404:
            created = self.client.request(
                "fixture",
                "POST",
                self.ring_path.rsplit("/", 1)[0],
                query={"keyRingId": RING},
                body={},
            )
            if created.status not in {200, 409}:
                raise ValueError(f"Shared key ring create returned HTTP {created.status}")
        elif ring.status != OK:
            raise ValueError(f"Shared key ring read returned HTTP {ring.status}")
        self.manifest["attempted"] = True
        self.save()  # Creation may succeed even if the response is lost.

    def cleanup(self) -> None:
        """Keys persist in GCP; destroy every owned version that is still usable."""
        if not self.manifest["attempted"]:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            key = self.call("GET")
            if key.status == OK:
                if key.json().get("labels") != self.labels:
                    raise ValueError("Refusing KMS cleanup: key ownership does not match")
                for version in self.versions():
                    if version["state"] in USABLE:
                        number = version_of(version["name"])
                        self.call("POST", f"/cryptoKeyVersions/{number}:destroy", body={})
                if any(version["state"] in USABLE for version in self.versions()):
                    raise ValueError("Owned key versions remain usable after cleanup")
            elif key.status != 404:
                raise ValueError(f"Owned key read returned HTTP {key.status}")
            self.manifest.update(cleanup="passed", finished_at=now())
            self.save()
        finally:
            self.release()

    def versions(self) -> list[dict]:
        """List the owned key's versions."""
        return self.call("GET", "/cryptoKeyVersions").json().get("cryptoKeyVersions", [])


def schedule(observe: Observations, response: Response, sent_next: str | None) -> dict:
    """Select a key's schedule; the next time becomes a check against what was sent."""
    value = response.json()
    selected = {
        **observe.select(response, ("purpose", "rotationPeriod", "destroyScheduledDuration")),
        "next_rotation_echoed": None
        if sent_next is None
        else same_time(value.get("nextRotationTime"), sent_next),
        "next_rotation_text": None
        if sent_next is None
        else value.get("nextRotationTime", "").removeprefix(sent_next[:19]),
        "primary": version_of(value.get("primary", {}).get("name")),
    }
    check_observation(selected)
    return selected


def run_scenario(fixture: RotationFixture, snapshot: SnapshotCase) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations(
        {
            fixture.manifest["key"]: "<key>",
            fixture.manifest["run_id"]: "<run>",
            RING: "<ring>",
            fixture.project: "<project>",
            client.number: "<project-number>",
        }
    )
    spare = fixture.manifest["key"] + "-refused"
    tomorrow = stamp(datetime.now(UTC) + timedelta(days=1))
    refusals = {
        "purpose-missing": {"rotationPeriod": f"{DAY}s"},
        "period-without-next": {"purpose": "ENCRYPT_DECRYPT", "rotationPeriod": f"{DAY}s"},
        "period-too-short": {
            "purpose": "ENCRYPT_DECRYPT",
            "rotationPeriod": "3600s",
            "nextRotationTime": tomorrow,
        },
        "asymmetric-rotation": {
            "purpose": "ASYMMETRIC_SIGN",
            "versionTemplate": {"algorithm": "EC_SIGN_P256_SHA256"},
            "rotationPeriod": f"{DAY}s",
            "nextRotationTime": tomorrow,
        },
    }
    for label, body in refusals.items():
        refused = fixture.create(spare, body)
        if refused.status == OK:
            fixture.client.request(
                "fixture",
                "POST",
                f"{fixture.ring_path}/cryptoKeys/{spare}/cryptoKeyVersions/1:destroy",
                body={},
                timeout=fixture.remaining(),
            )
            raise AssertionError(f"{label}: GCP created the key; its version is now destroyed")
        snapshot.match(label, outcome(observe, refused))
    created = fixture.create(
        fixture.manifest["key"],
        {
            "purpose": "ENCRYPT_DECRYPT",
            "rotationPeriod": f"{DAY}s",
            "nextRotationTime": tomorrow,
            "destroyScheduledDuration": f"{DAY}s",
        },
    )
    if created.status != OK:
        raise AssertionError(f"Key create returned HTTP {created.status}")
    snapshot.match("scheduled-created", schedule(observe, created, tomorrow))
    snapshot.match("scheduled-read", schedule(observe, fixture.call("GET"), tomorrow))
    later = stamp(datetime.now(UTC) + timedelta(days=2))

    def patch(mask: str, body: dict) -> Response:
        return fixture.call("PATCH", query={"updateMask": mask}, body=body)

    snapshot.match(
        "period-patched",
        schedule(observe, patch("rotationPeriod", {"rotationPeriod": f"{2 * DAY}s"}), tomorrow),
    )
    snapshot.match(
        "next-patched",
        schedule(observe, patch("nextRotationTime", {"nextRotationTime": later}), later),
    )
    snapshot.match("next-cleared-alone", outcome(observe, patch("nextRotationTime", {})))
    snapshot.match(
        "schedule-cleared", schedule(observe, patch("rotationPeriod,nextRotationTime", {}), None)
    )
    destroyed = fixture.call("POST", "/cryptoKeyVersions/1:destroy", body={})
    value = destroyed.json()
    delay = None
    if value.get("destroyTime"):
        delay = (datetime.fromisoformat(value["destroyTime"]) - datetime.now(UTC)).total_seconds()
    snapshot.match(
        "destroy-honors-duration",
        {
            **observe.select(destroyed, ("state",)),
            "destroy_in_a_day": delay is not None and abs(delay - DAY) < 300,
        },
    )


def main(argv: list[str] | None = None) -> None:
    """Destroy the owned key versions after an interrupted live run."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("command", choices=["cleanup"])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--allow-live", action="store_true", required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox, "cleanup")
    target = Target.select(
        "record",
        project=sandbox.project,
        credentials_file=sandbox.identities["allowed"].credentials_file,
        live_scope=sandbox.live_scope,
        allow_live=args.allow_live,
    )
    client = RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    state = Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE
    RotationFixture(client, state, manifest=args.manifest).cleanup()
    sys.stdout.write("Owned KMS rotation cleanup passed.\n")


if __name__ == "__main__":
    main()
