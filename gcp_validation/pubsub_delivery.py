"""Bounded Pub/Sub delivery-feature scenario: dead-letter, ordering, filter, exactly-once.

Not recorded against GCP. Live runs need a separately approved scope whose
services entry is ``pubsub-delivery`` (``scripts/plan_pubsub.py --scenario delivery``).
"""

import fcntl
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, Self
from uuid import uuid4

from .evidence import Inconclusive, now
from .pubsub import PubSubClient, RPCFailure
from .pubsub_scope import ACTORS, private_json
from .sandbox import Sandbox

DELIVERY_LIMITS = {
    "max_topics": 2,
    "max_subscriptions": 6,
    "max_messages": 20,
    "max_message_bytes": 1024,
    "max_run_seconds": 900,
}
# Owned resources, created in this order and deleted in reverse.
RESOURCES = (
    ("topic", "events"),
    ("topic", "dead-letters"),
    ("subscription", "ordered"),
    ("subscription", "filtered"),
    ("subscription", "exact"),
    ("subscription", "dead-lettered"),
    ("subscription", "collected"),
)
MAX_ATTEMPTS = 5


class Snapshot(Protocol):
    """The observation sink: a SnapshotCase or a local preparation recorder."""

    def match(self, label: str, observation: object) -> None:
        """Record one observation."""


@dataclass(frozen=True)
class DeliveryScope:
    """A separately approved live scope for this scenario."""

    project: str
    project_number: str

    @classmethod
    def load(cls, filename: str | None, sandbox: Sandbox | None) -> Self:
        """Load the scope file and require it to match the fixed contract."""
        if not filename or sandbox is None:
            raise ValueError("Pub/Sub delivery requires a separately approved --gcp-pubsub-scope")
        value = json.loads(Path(filename).read_text())
        if value != delivery_scope(sandbox.project, sandbox.project_number, approved=True):
            raise ValueError(
                "Pub/Sub delivery scope must match the approved project, limits, and cleanup"
            )
        return cls(sandbox.project, sandbox.project_number)

    def evidence(self) -> dict[str, Any]:
        """Scope facts recorded with the evidence."""
        return {
            "services": ["pubsub-delivery"],
            "location": "global",
            "limits": dict(DELIVERY_LIMITS),
            "cleanup_required": True,
            "identities": list(ACTORS),
        }


def delivery_scope(project: str, project_number: str, *, approved: bool) -> dict[str, Any]:
    """Return the scope document; the planner writes it unapproved."""
    return {
        "schema": 1,
        "approved": approved,
        "services": ["pubsub-delivery"],
        "project": project,
        "project_number": project_number,
        "location": "global",
        "limits": DELIVERY_LIMITS,
        "cleanup_required": True,
        "scheduled_runs": False,
    }


class DeliveryFixture:
    """Owns the scenario's topics and subscriptions through a private manifest."""

    def __init__(self, client: PubSubClient, state_dir: Path, project_number: str) -> None:
        """Prepare an ownership manifest; nothing is created until setup."""
        self.client, self.project, self.number = client, client.target.project, project_number
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.deadline = time.monotonic() + DELIVERY_LIMITS["max_run_seconds"]
        run = uuid4().hex
        self.manifest = {
            "schema": 1,
            "kind": "pubsub-delivery",
            "project": self.project,
            "project_number": project_number,
            "run_id": run,
            "attempted": [],
            "publish_attempts": 0,
            "cleanup": "pending",
            "started_at": now(),
        }
        self.path = self.state_dir / (run + ".json")
        self.labels = {"managed-by": "gcp-local", "gcp-local-run": run}
        self.lock = None

    def name(self, kind: str, short: str) -> str:
        """Return the owned resource name for one scenario resource."""
        return f"projects/{self.project}/{kind}s/gcp-local-{short}-{self.manifest['run_id'][:12]}"

    def call(self, kind: str, method: str, request: dict, *, actor: str = "fixture") -> Any:  # noqa: ANN401 -- SDK response types vary per method
        """One bounded RPC; failures become RPCFailure."""
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise Inconclusive(
                "Pub/Sub delivery scenario exceeded its deadline; cleanup is still required"
            )
        return self.client.call(actor, kind, method, request, timeout=min(10, remaining))

    def remember(self, name: str) -> None:
        """Persist intent before a create whose response may be lost."""
        self.manifest["attempted"].append(name)
        private_json(self.path, self.manifest)

    def setup(self) -> dict[str, Any]:
        """Create every owned resource and return the created messages by short name."""
        self.lock = (self.state_dir / "pubsub-delivery.lock").open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            self.lock = None
            raise Inconclusive("Another Pub/Sub delivery run holds this project lock") from None
        self.client.verify_project()
        events, dead = self.name("topic", "events"), self.name("topic", "dead-letters")
        bodies = {
            "ordered": {
                "topic": events,
                "enable_message_ordering": True,
                "ack_deadline_seconds": 10,
            },
            "filtered": {"topic": events, "filter": 'attributes.kind = "match"'},
            "exact": {
                "topic": events,
                "enable_exactly_once_delivery": True,
                "ack_deadline_seconds": 10,
            },
            "dead-lettered": {
                "topic": events,
                "ack_deadline_seconds": 10,
                "filter": 'attributes.kind = "poison"',
                "dead_letter_policy": {
                    "dead_letter_topic": dead,
                    "max_delivery_attempts": MAX_ATTEMPTS,
                },
            },
            "collected": {"topic": dead},
        }
        created = {}
        for kind, short in RESOURCES:
            name = self.name(kind, short)
            self.remember(name)
            request = {"name": name, "labels": self.labels, **bodies.get(short, {})}
            created[short] = self.call(kind, "create_" + kind, request)
        return created

    def publish(self, topic: str, payload: bytes, **fields: object) -> None:
        """Publish one bounded message."""
        if len(payload) > DELIVERY_LIMITS["max_message_bytes"]:
            raise ValueError("Payload exceeds the bounded scope")
        if self.manifest["publish_attempts"] >= DELIVERY_LIMITS["max_messages"]:
            raise Inconclusive("Pub/Sub delivery publish limit reached")
        self.manifest["publish_attempts"] += 1
        private_json(self.path, self.manifest)
        self.call("topic", "publish", {"topic": topic, "messages": [{"data": payload, **fields}]})

    def pull(self, subscription: str, *, wait: float) -> list:
        """Pull until messages arrive or wait elapses; an empty list is a valid outcome."""
        deadline = min(self.deadline, time.monotonic() + wait)
        request = {"subscription": subscription, "max_messages": 10, "return_immediately": True}
        while True:
            try:
                response = self.call("subscription", "pull", request)
                if response.received_messages:
                    return list(response.received_messages)
            except RPCFailure as error:
                if error.status != "DEADLINE_EXCEEDED":
                    raise
            if time.monotonic() >= deadline:
                return []
            time.sleep(0.5)

    def ack(self, subscription: str, received: list) -> None:
        """Acknowledge a batch."""
        if received:
            ids = [m.ack_id for m in received]
            self.call("subscription", "acknowledge", {"subscription": subscription, "ack_ids": ids})

    def nack(self, subscription: str, received: list) -> None:
        """Nack a batch with a zero ack deadline."""
        if received:
            request = {
                "subscription": subscription,
                "ack_ids": [m.ack_id for m in received],
                "ack_deadline_seconds": 0,
            }
            self.call("subscription", "modify_ack_deadline", request)

    def grant_agent(self, kind: str, resource: str, role: str) -> None:
        """Grant the project's Pub/Sub service agent a role on one resource."""
        agent = f"serviceAccount:service-{self.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
        policy = self.call(kind, "get_iam_policy", {"resource": resource})
        bindings = [{"role": b.role, "members": list(b.members)} for b in policy.bindings]
        bindings.append({"role": role, "members": [agent]})
        policy = {"version": 1, "etag": policy.etag, "bindings": bindings}
        self.call(kind, "set_iam_policy", {"resource": resource, "policy": policy})

    def cleanup(self) -> None:
        """Delete owned resources in reverse creation order, verifying ownership labels."""
        try:
            for name in reversed(self.manifest["attempted"]):
                kind = "subscription" if "/subscriptions/" in name else "topic"
                try:
                    resource = self.client.call("fixture", kind, "get_" + kind, {kind: name})
                except RPCFailure as error:
                    if error.status == "NOT_FOUND":
                        continue
                    raise
                if dict(resource.labels) != self.labels:
                    raise ValueError(
                        "Refusing Pub/Sub cleanup: ownership does not match the manifest"
                    )
                self.client.call("fixture", kind, "delete_" + kind, {kind: name})
            self.manifest.update(cleanup="passed", finished_at=now())
            private_json(self.path, self.manifest)
        finally:
            if self.lock:
                self.lock.close()
                self.lock = None


def subscription_shape(subscription: Any) -> dict[str, Any]:  # noqa: ANN401 -- SDK Subscription
    """Select the public delivery fields of a created subscription."""
    shape = {
        "enable_message_ordering": subscription.enable_message_ordering,
        "enable_exactly_once_delivery": subscription.enable_exactly_once_delivery,
        "filter": subscription.filter,
    }
    if subscription.dead_letter_policy.dead_letter_topic:
        shape["dead_letter_policy"] = {
            "dead_letter_topic": "<topic:dead-letters>",
            "max_delivery_attempts": subscription.dead_letter_policy.max_delivery_attempts,
        }
    return shape


def data(received: list) -> list[str]:
    """Message payloads of a batch."""
    return [m.message.data.decode() for m in received]


def ordering(fixture: DeliveryFixture, subscription: str, topic: str, snapshot: Snapshot) -> None:
    """Key order on first delivery and after a nack of the first batch."""
    for value in ("1", "2", "3"):
        fixture.publish(topic, value.encode(), ordering_key="key", attributes={"kind": "ordered"})
    first = fixture.pull(subscription, wait=30)
    fixture.nack(subscription, first)
    acked, deadline = [], time.monotonic() + 120
    while len(set(acked)) < 3 and time.monotonic() < deadline:
        batch = fixture.pull(subscription, wait=10)
        acked.extend(data(batch))
        fixture.ack(subscription, batch)
    observation = {
        "first_delivery_starts_with": data(first[:1]),
        "redelivered_first": acked[:1],
        "key_order": list(dict.fromkeys(acked)),
    }
    snapshot.match("ordered-delivery", observation)


def filtering(fixture: DeliveryFixture, subscription: str, topic: str, snapshot: Snapshot) -> None:
    """Only matching attributes are delivered; an unparsable filter is invalid."""
    fixture.publish(topic, b"match", attributes={"kind": "match"})
    delivered, deadline = [], time.monotonic() + 30
    while "match" not in delivered and time.monotonic() < deadline:
        batch = fixture.pull(subscription, wait=5)
        delivered.extend(data(batch))
        fixture.ack(subscription, batch)
    snapshot.match("filtered-delivery", {"delivered": sorted(set(delivered))})
    invalid = fixture.name("subscription", "bad-filter")
    fixture.remember(invalid)
    request = {
        "name": invalid,
        "topic": topic,
        "labels": fixture.labels,
        "filter": "attributes.kind = =",
    }
    try:
        fixture.call("subscription", "create_subscription", request)
    except RPCFailure as error:
        snapshot.match("invalid-filter", {"grpc_status": error.status})
        return
    raise AssertionError("An unparsable filter was accepted")


def exactly_once(fixture: DeliveryFixture, subscription: str, snapshot: Snapshot) -> None:
    """Observe a successful first ack and the result of a repeated ack."""
    # Imported here so the offline planner can import this module without the SDK.
    from google.api_core.exceptions import GoogleAPICallError  # noqa: PLC0415

    received = fixture.pull(subscription, wait=30)
    if not received:
        raise Inconclusive("Exactly-once subscription received nothing")
    fixture.ack(subscription, received[:1])
    client = fixture.client.clients["fixture"]["subscription"]
    request = {"subscription": subscription, "ack_ids": [received[0].ack_id]}
    try:
        client.acknowledge(request=request, retry=None, timeout=10)
        second = {"grpc_status": "OK"}
    except GoogleAPICallError as error:
        second = {
            "grpc_status": error.grpc_status_code.name,
            "reason": error.reason,
            "ack_results": sorted(set((error.metadata or {}).values())),
        }
    fixture.ack(subscription, received[1:])
    snapshot.match("exactly-once-second-ack", second)


def dead_letter_ungranted(
    fixture: DeliveryFixture, names: dict[str, str], snapshot: Snapshot
) -> None:
    """Without the service agent's grants, a nacked message is never forwarded."""
    subscription = names["dead-lettered"]
    fixture.publish(names["events"], b"poison-ungranted", attributes={"kind": "poison"})
    attempts = []
    for _ in range(MAX_ATTEMPTS + 2):
        batch = fixture.pull(subscription, wait=30)
        if not batch:
            break
        attempts.extend(m.delivery_attempt for m in batch)
        fixture.nack(subscription, batch)
    observation = {
        "redelivered_past_limit": len(attempts) >= MAX_ATTEMPTS + 2,
        "delivery_attempts": sorted(set(attempts)),
        "forwarded": bool(fixture.pull(names["collected"], wait=5)),
    }
    snapshot.match("dead-letter-ungranted", observation)
    fixture.ack(subscription, fixture.pull(subscription, wait=5))


def dead_letter_granted(
    fixture: DeliveryFixture, names: dict[str, str], snapshot: Snapshot
) -> None:
    """With publisher on the dead-letter topic and subscriber on the source, it is forwarded."""
    subscription, collected = names["dead-lettered"], names["collected"]
    fixture.grant_agent("topic", names["dead-letters"], "roles/pubsub.publisher")
    fixture.grant_agent("subscription", subscription, "roles/pubsub.subscriber")
    fixture.publish(names["events"], b"poison-granted", attributes={"kind": "poison"})
    attempts, forwarded, deadline = [], [], time.monotonic() + 300
    while not forwarded and time.monotonic() < deadline:
        batch = fixture.pull(subscription, wait=5)
        attempts.extend(m.delivery_attempt for m in batch if m.message.data == b"poison-granted")
        fixture.nack(subscription, batch)
        forwarded = fixture.pull(collected, wait=1)
    if not forwarded:
        raise Inconclusive(f"No dead-letter forwarding observed; attempts {attempts}")
    message = forwarded[0].message
    fixture.ack(collected, forwarded)
    observation = {
        "data": message.data.decode(),
        "attribute_names": sorted(
            k for k in message.attributes if k.startswith("CloudPubSubDeadLetter")
        ),
        "source_delivery_count": message.attributes.get("CloudPubSubDeadLetterSourceDeliveryCount"),
        "first_attempt": min((a for a in attempts if a), default=0),
    }
    snapshot.match("dead-letter-granted", observation)


def run_delivery_scenario(fixture: DeliveryFixture, snapshot: Snapshot) -> None:
    """Run the same observations in local, recording, and revalidation modes."""
    created = fixture.setup()
    shapes = {
        s: subscription_shape(created[s]) for s in ("ordered", "filtered", "exact", "dead-lettered")
    }
    snapshot.match("subscriptions", shapes)
    names = {short: fixture.name(kind, short) for kind, short in RESOURCES}
    request = {"subscription": names["ordered"], "max_messages": 1, "return_immediately": True}
    try:
        fixture.call("subscription", "pull", request, actor="denied")
    except RPCFailure as error:
        snapshot.match("denied-pull", {"grpc_status": error.status})
    else:
        raise AssertionError("Denied caller pulled")
    ordering(fixture, names["ordered"], names["events"], snapshot)
    filtering(fixture, names["filtered"], names["events"], snapshot)
    exactly_once(fixture, names["exact"], snapshot)
    dead_letter_ungranted(fixture, names, snapshot)
    dead_letter_granted(fixture, names, snapshot)
