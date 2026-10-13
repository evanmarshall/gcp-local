"""Bounded Secret Manager lifecycle-policy and notification scenarios, not yet recorded.

``secretmanager-secret-policies`` round-trips user-managed replication, topics,
rotation, ttl and expireTime, version aliases, and delayed destruction, with their
refusals. ``secretmanager-rotation-events`` waits for the SECRET_ROTATE notification
GCP publishes at nextRotationTime and for a ttl expiry, and pulls every notification
from one subscription. Both need the project's Secret Manager service agent
(``gcloud beta services identity create --service=secretmanager.googleapis.com``);
the fixture grants it ``roles/pubsub.publisher`` on the run's topic only.
"""

import argparse
import base64
import json
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, SnapshotCase, check_observation, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.security.refusals import outcome

SERVICE = "secretmanager"
LIMITS = {
    "max_secrets": 4,
    "max_versions": 4,
    "max_topics": 1,
    "max_subscriptions": 1,
    "max_run_seconds": 1200,
}
LIVE_ENDPOINT = "https://secretmanager.googleapis.com"
PUBSUB_ENDPOINT = "https://pubsub.googleapis.com"
REPLICAS = ["us-east1", "us-central1"]
# GCP's minimum lead for nextRotationTime is 300s; the margin covers request latency.
ROTATION_LEAD = 310
EXPIRY_TTL = 120
EVENT_WAIT = 600
OK = 200


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the separately approved live scope for these scenarios."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


def stamp(moment: datetime) -> str:
    """Render a whole-second UTC timestamp."""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse(text: str | None) -> datetime | None:
    """Parse an RFC 3339 timestamp, or None."""
    return datetime.fromisoformat(text) if text else None


def seconds_between(later: str | None, earlier: str | None) -> float | None:
    """Return the seconds between two timestamps, or None when either is missing."""
    end, start = parse(later), parse(earlier)
    return (end - start).total_seconds() if end and start else None


class PoliciesFixture(OwnedRun):
    """One topic, one pull subscription, and up to four secrets named for the run."""

    def __init__(
        self,
        client: RestClient,
        pubsub: RestClient,
        state_dir: Path,
        *,
        manifest: str | None = None,
    ) -> None:
        """Bind the run manifest and the owned resource paths."""
        super().__init__(
            client,
            state_dir,
            SERVICE,
            LIMITS["max_run_seconds"],
            manifest=manifest,
            check=lambda v: v.get("name") == "gl-sm-" + v["run_id"][:12],
        )
        self.pubsub = pubsub
        self.manifest.setdefault("name", "gl-sm-" + self.manifest["run_id"][:12])
        self.manifest.setdefault("versions_added", 0)
        self.name = self.manifest["name"]
        self.topic = f"projects/{self.project}/topics/{self.name}"
        self.subscription = f"projects/{self.project}/subscriptions/{self.name}"
        self.secrets = [f"projects/{self.project}/secrets/{self.name}-{i}" for i in range(4)]
        self.labels = {"managed-by": "gcp-local", "gcp-local-run": self.manifest["run_id"]}
        self.agent = (
            f"serviceAccount:service-{client.number}@gcp-sa-secretmanager.iam.gserviceaccount.com"
        )

    def secret(
        self,
        index: int,
        method: str,
        suffix: str = "",
        *,
        actor: str = "fixture",
        query: dict | None = None,
        body: dict | None = None,
    ) -> Response:
        """Call one owned secret."""
        return self.client.request(
            actor,
            method,
            "/v1/" + self.secrets[index] + suffix,
            query=query,
            body=body,
            timeout=self.remaining(),
        )

    def create(self, index: int, body: dict) -> Response:
        """Create one owned secret with the run's labels."""
        return self.client.request(
            "fixture",
            "POST",
            f"/v1/projects/{self.project}/secrets",
            query={"secretId": self.secrets[index].rsplit("/", 1)[1]},
            body={"labels": self.labels, **body},
            timeout=self.remaining(),
        )

    def refuse(self, body: dict) -> Response:
        """Try a create GCP may refuse on the spare secret; delete it if GCP accepted it."""
        response = self.create(3, body)
        if response.status == OK:
            self.secret(3, "DELETE")
        return response

    def add_version(self, index: int, payload: bytes) -> Response:
        """Add one counted version."""
        if self.manifest["versions_added"] >= LIMITS["max_versions"]:
            raise Inconclusive("Secret version limit reached")
        self.manifest["versions_added"] += 1
        self.save()
        return self.secret(
            index,
            "POST",
            ":addVersion",
            body={"payload": {"data": base64.b64encode(payload).decode()}},
        )

    def pubsub_call(
        self, method: str, name: str, suffix: str = "", body: dict | None = None
    ) -> Response:
        """Call the run's topic or subscription."""
        return self.pubsub.request(
            "fixture", method, "/v1/" + name + suffix, body=body, timeout=self.remaining()
        )

    def setup(self) -> Response:
        """Create the topic and subscription; return the topic create response."""
        self.acquire()
        self.client.verify_project()
        self.manifest["attempted"] = True
        self.save()  # Creation may succeed even if the response is lost.
        topic = self.pubsub_call("PUT", self.topic, body={"labels": self.labels})
        if topic.status != OK:
            raise Inconclusive(f"Topic create returned HTTP {topic.status}")
        subscription = self.pubsub_call(
            "PUT", self.subscription, body={"topic": self.topic, "ackDeadlineSeconds": 10}
        )
        if subscription.status != OK:
            raise Inconclusive(f"Subscription create returned HTTP {subscription.status}")
        return topic

    def grant_agent(self) -> Response:
        """Grant the Secret Manager service agent publish on the run's topic only."""
        policy = self.pubsub_call("GET", self.topic, ":getIamPolicy")
        granted = self.pubsub_call(
            "POST",
            self.topic,
            ":setIamPolicy",
            body={
                "policy": {
                    "etag": policy.json().get("etag", ""),
                    "bindings": [{"role": "roles/pubsub.publisher", "members": [self.agent]}],
                }
            },
        )
        if granted.status == 400:
            raise Inconclusive(
                "The Secret Manager service agent does not exist; create it with "
                "gcloud beta services identity create --service=secretmanager.googleapis.com"
            )
        return granted

    def until_created(self, index: int, body: dict) -> Response:
        """Create a secret with topics, retrying while the agent's grant propagates (live only)."""
        return self.wait_until(lambda: self.create(index, body), {400, 403}, 180)

    def pull(self) -> list[dict]:
        """Pull and acknowledge what the subscription holds now."""
        pulled = self.pubsub_call(
            "POST", self.subscription, ":pull", body={"maxMessages": 10, "returnImmediately": True}
        )
        received = pulled.json().get("receivedMessages", [])
        if received:
            self.pubsub_call(
                "POST",
                self.subscription,
                ":acknowledge",
                body={"ackIds": [item["ackId"] for item in received]},
            )
        return [item["message"] for item in received]

    def owned(self, response: Response) -> None:
        """Refuse cleanup of a secret the manifest doesn't own."""
        if response.json().get("labels") != self.labels:
            raise ValueError("Refusing Secret Manager cleanup: secret ownership does not match")

    def cleanup(self) -> None:
        """Delete the owned secrets, subscription, and topic, then confirm they are gone."""
        if not self.manifest["attempted"]:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            for index in range(len(self.secrets)):
                current = self.secret(index, "GET")
                if current.status == OK:
                    self.owned(current)
                    self.secret(index, "DELETE")
                if self.secret(index, "GET").status != 404:
                    raise ValueError("Owned secret remains after deletion")
            for name in (self.subscription, self.topic):
                self.pubsub_call("DELETE", name)
                if self.pubsub_call("GET", name).status != 404:
                    raise ValueError("Owned Pub/Sub resource remains after deletion")
            self.manifest.update(cleanup="passed", finished_at=now())
            self.save()
        finally:
            self.release()


def observer(fixture: PoliciesFixture) -> Observations:
    """Alias the run's identifiers."""
    client = fixture.client
    aliases = {secret: f"<secret-{i}>" for i, secret in enumerate(fixture.secrets)}
    aliases.update(
        {
            secret.replace(
                "projects/" + fixture.project + "/", "projects/" + client.number + "/", 1
            ): f"<secret-{i}-by-number>"
            for i, secret in enumerate(fixture.secrets)
        }
    )
    aliases.update(
        {
            fixture.topic: "<topic>",
            fixture.subscription: "<subscription>",
            fixture.agent: "<agent>",
            fixture.manifest["run_id"]: "<run>",
            fixture.project: "<project>",
            client.number: "<project-number>",
        }
    )
    aliases.update({email: "<" + actor + ">" for actor, email in client.emails.items()})
    return Observations(aliases)


def policy_view(observe: Observations, response: Response, sent_next: str | None = None) -> dict:
    """Select a secret's lifecycle fields; times become checks against what was sent."""
    value = response.json()
    rotation = value.get("rotation", {})
    selected = {
        **observe.select(
            response, ("replication", "topics", "versionAliases", "versionDestroyTtl")
        ),
        "rotation_fields": sorted(rotation),
        "rotation_period": rotation.get("rotationPeriod"),
        "next_rotation_echoed": None
        if sent_next is None
        else rotation.get("nextRotationTime") == sent_next,
        "expire_after_create": seconds_between(value.get("expireTime"), value.get("createTime"))
        is not None,
    }
    check_observation(selected)
    return selected


def run_policies(fixture: PoliciesFixture, snapshot: SnapshotCase) -> None:
    """Observe the lifecycle-policy round-trips and refusals."""
    observe = observer(fixture)
    topics = [{"name": fixture.topic}]
    soon = datetime.now(UTC) + timedelta(hours=1)
    rotation = {"nextRotationTime": stamp(soon), "rotationPeriod": "3600s"}
    auto = {"automatic": {}}
    snapshot.match(
        "topics-without-publisher",
        outcome(observe, fixture.refuse({"replication": auto, "topics": topics})),
    )
    snapshot.match(
        "missing-topic",
        outcome(
            observe,
            fixture.refuse({"replication": auto, "topics": [{"name": fixture.topic + "-missing"}]}),
        ),
    )
    snapshot.match("agent-granted", observe.select(fixture.grant_agent(), ("bindings",)))
    refusals = {
        "rotation-without-topics": {"replication": auto, "rotation": rotation},
        "rotation-too-soon": {
            "replication": auto,
            "topics": topics,
            "rotation": {"nextRotationTime": stamp(datetime.now(UTC) + timedelta(seconds=60))},
        },
        "rotation-short-period": {
            "replication": auto,
            "topics": topics,
            "rotation": {**rotation, "rotationPeriod": "60s"},
        },
        "rotation-period-alone": {
            "replication": auto,
            "topics": topics,
            "rotation": {"rotationPeriod": "3600s"},
        },
        "expire-in-past": {"replication": auto, "expireTime": "2020-01-01T00:00:00Z"},
        "destroy-ttl-short": {"replication": auto, "versionDestroyTtl": "60s"},
        "aliases-without-versions": {"replication": auto, "versionAliases": {"current": "1"}},
        "empty-replicas": {"replication": {"userManaged": {}}},
    }
    for label, body in refusals.items():
        snapshot.match(label, outcome(observe, fixture.refuse(body)))
    managed = {"userManaged": {"replicas": [{"location": location} for location in REPLICAS]}}
    created = fixture.until_created(
        0,
        {
            "replication": managed,
            "topics": topics,
            "rotation": rotation,
            "ttl": "7200s",
            "versionDestroyTtl": "86400s",
        },
    )
    if created.status != OK:
        raise AssertionError(f"Secret create returned HTTP {created.status}")
    ttl = seconds_between(created.json().get("expireTime"), created.json().get("createTime"))
    snapshot.match(
        "policies-created",
        {
            **policy_view(observe, created, rotation["nextRotationTime"]),
            "expire_is_ttl": ttl is not None and abs(ttl - 7200) < 60,
        },
    )
    snapshot.match(
        "policies-read",
        policy_view(observe, fixture.secret(0, "GET"), rotation["nextRotationTime"]),
    )
    added = [fixture.add_version(0, payload) for payload in (b"first", b"second")]
    snapshot.match(
        "version-replication", [observe.select(item, ("replicationStatus",)) for item in added]
    )
    observe_aliases(fixture, snapshot, observe)
    observe_updates(fixture, snapshot, observe, managed)
    observe_delayed_destroy(fixture, snapshot, observe)


def observe_aliases(
    fixture: PoliciesFixture, snapshot: SnapshotCase, observe: Observations
) -> None:
    """Set an alias, then read and access by it; refused aliases change nothing."""

    def alias(aliases: dict) -> Response:
        return fixture.secret(
            0, "PATCH", query={"updateMask": "versionAliases"}, body={"versionAliases": aliases}
        )

    snapshot.match("alias-set", policy_view(observe, alias({"current": "1"})))
    accessed = fixture.secret(0, "GET", "/versions/current:access")
    snapshot.match(
        "alias-access",
        {"http_status": accessed.status, "name": observe.text(accessed.json().get("name", ""))},
    )
    read = fixture.secret(0, "GET", "/versions/current")
    snapshot.match("alias-get", observe.select(read, ("name", "state")))
    snapshot.match(
        "alias-disable",
        outcome(observe, fixture.secret(0, "POST", "/versions/current:disable", body={})),
    )
    snapshot.match("alias-reserved", outcome(observe, alias({"latest": "1"})))
    snapshot.match("alias-missing-version", outcome(observe, alias({"current": "9"})))


def observe_updates(
    fixture: PoliciesFixture, snapshot: SnapshotCase, observe: Observations, managed: dict
) -> None:
    """Patch replication, expiration, topics, and rotation."""

    def patch(mask: str, body: dict) -> Response:
        return fixture.secret(0, "PATCH", query={"updateMask": mask}, body=body)

    snapshot.match(
        "replication-to-automatic",
        outcome(observe, patch("replication", {"replication": {"automatic": {}}})),
    )
    snapshot.match(
        "replication-same", outcome(observe, patch("replication", {"replication": managed}))
    )
    snapshot.match(
        "replication-fewer-replicas",
        outcome(
            observe,
            patch(
                "replication",
                {"replication": {"userManaged": {"replicas": [{"location": REPLICAS[0]}]}}},
            ),
        ),
    )
    expire = stamp(datetime.now(UTC) + timedelta(hours=3))
    expired = patch("expire_time", {"expireTime": expire})
    snapshot.match(
        "expire-time-set",
        {
            **policy_view(observe, expired),
            "expire_echoed": expired.json().get("expireTime") == expire,
        },
    )
    snapshot.match("expiration-cleared", policy_view(observe, patch("ttl", {})))
    snapshot.match("topics-removed-with-rotation", outcome(observe, patch("topics", {})))
    snapshot.match("rotation-cleared", policy_view(observe, patch("rotation", {})))


def observe_delayed_destroy(
    fixture: PoliciesFixture, snapshot: SnapshotCase, observe: Observations
) -> None:
    """Destroy a version under versionDestroyTtl, destroy it again, and cancel by enabling."""
    destroyed = fixture.secret(0, "POST", "/versions/2:destroy", body={})
    delay = seconds_between(destroyed.json().get("scheduledDestroyTime"), stamp(datetime.now(UTC)))
    snapshot.match(
        "destroy-scheduled",
        {
            **observe.select(destroyed, ("state",)),
            "scheduled_in_a_day": delay is not None and abs(delay - 86400) < 300,
        },
    )
    snapshot.match(
        "destroy-scheduled-again",
        outcome(observe, fixture.secret(0, "POST", "/versions/2:destroy", body={})),
    )
    enabled = fixture.secret(0, "POST", "/versions/2:enable", body={})
    snapshot.match("destroy-cancelled", observe.select(enabled, ("state",)))


def run_events(fixture: PoliciesFixture, snapshot: SnapshotCase) -> None:
    """Wait for SECRET_ROTATE and a ttl expiry; record every notification the topic got."""
    observe = observer(fixture)
    snapshot.match("agent-granted", observe.select(fixture.grant_agent(), ("bindings",)))
    topics = [{"name": fixture.topic}]
    expiring = fixture.until_created(
        1, {"replication": {"automatic": {}}, "topics": topics, "ttl": f"{EXPIRY_TTL}s"}
    )
    snapshot.match("expiring-created", policy_view(observe, expiring))
    next_rotation = stamp(datetime.now(UTC) + timedelta(seconds=ROTATION_LEAD))
    rotating = fixture.create(
        2,
        {
            "replication": {"automatic": {}},
            "topics": topics,
            "rotation": {"nextRotationTime": next_rotation},
        },
    )
    snapshot.match("rotating-created", policy_view(observe, rotating, next_rotation))
    snapshot.match("version-added", observe.select(fixture.add_version(2, b"value"), ("state",)))
    messages, deadline = [], time.monotonic() + ROTATION_LEAD + EVENT_WAIT
    while time.monotonic() < min(deadline, fixture.deadline - 60):
        messages += fixture.pull()
        kinds = {message.get("attributes", {}).get("eventType") for message in messages}
        if {"SECRET_ROTATE", "SECRET_DELETE"} <= kinds:
            break
        time.sleep(10)
    expired = fixture.secret(1, "GET")
    snapshot.match("expired", {"http_status": expired.status})
    after = fixture.secret(2, "GET").json().get("rotation", {})
    snapshot.match("rotation-after-notification", {"fields": sorted(after)})
    snapshot.match("notifications", notifications(observe, messages))


def notifications(observe: Observations, messages: list[dict]) -> list[dict]:
    """Summarize each notification: its type, attributes (times dropped), and data field names."""
    summary = []
    for message in messages:
        attributes = message.get("attributes", {})
        data = base64.b64decode(message.get("data", "")) or b"{}"
        try:
            fields = sorted(json.loads(data))
        except ValueError:
            fields = ["<not-json>"]
        summary.append(
            {
                "event_type": attributes.get("eventType"),
                "attributes": {
                    key: observe.text(value)
                    for key, value in sorted(attributes.items())
                    if "time" not in key.lower()
                },
                "attribute_names": sorted(attributes),
                "data_fields": fields,
            }
        )
    summary.sort(
        key=lambda item: (
            item["attributes"].get("secretId", ""),
            item["event_type"] or "",
            item["attributes"].get("versionId", ""),
        )
    )
    check_observation(summary)
    return summary


def main(argv: list[str] | None = None) -> None:
    """Delete the owned secrets, subscription, and topic after an interrupted live run."""
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
    pubsub = RestClient(target, PUBSUB_ENDPOINT, sandbox=sandbox, scope=scope)
    state = Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE
    PoliciesFixture(client, pubsub, state, manifest=args.manifest).cleanup()
    sys.stdout.write("Owned Secret Manager policy cleanup passed.\n")


if __name__ == "__main__":
    main()
