"""Bounded Pub/Sub REST data-plane recording: publish, pull, modifyAckDeadline, acknowledge.

The data-plane requests mirror what gcloud 588.0.0 sends, as traced against the
conformance server: ``topics publish`` posts ``{"messages": [{"attributes": {},
"data": ...}]}``, ``subscriptions pull --auto-ack`` posts ``{"maxMessages": 1,
"returnImmediately": false}`` and then ``{"ackIds": [...]}``, each with ``alt=json``.
"""

import argparse
import base64
import sys
import time
from collections.abc import Callable
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, SnapshotCase, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = "pubsub-rest"
LIMITS = {
    "max_topics": 1,
    "max_subscriptions": 1,
    "max_messages": 4,
    "max_message_bytes": 1024,
    "max_run_seconds": 900,
}
LIVE_ENDPOINT = "https://pubsub.googleapis.com"
PAYLOAD = b"gcp-local rest data plane"
HTTP_ERROR = 400

Resource = Callable[..., Response]


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the separately approved live scope for this scenario."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class DataPlaneFixture(OwnedRun):
    """One topic and one pull subscription named for the run."""

    def __init__(self, client: RestClient, state_dir: Path, *, manifest: str | None = None) -> None:
        """Bind the run manifest and the topic and subscription paths."""
        super().__init__(
            client,
            state_dir,
            SERVICE,
            LIMITS["max_run_seconds"],
            manifest=manifest,
            check=lambda v: v.get("name") == "gl-" + v["run_id"][:12],
        )
        self.manifest.setdefault("name", "gl-" + self.manifest["run_id"][:12])
        self.manifest.setdefault("messages", 0)
        self.name = self.manifest["name"]
        self.topic_path = f"/v1/projects/{self.project}/topics/{self.name}"
        self.sub_path = f"/v1/projects/{self.project}/subscriptions/{self.name}"

    def send(self, path: str, method: str, suffix: str, actor: str, body: dict | None) -> Response:
        """Send one request; custom methods carry gcloud's alt=json."""
        query = {"alt": "json"} if suffix.startswith(":") else None
        return self.client.request(
            actor, method, path + suffix, body=body, query=query, timeout=self.remaining()
        )

    def topic(
        self, method: str, suffix: str = "", *, actor: str = "fixture", body: dict | None = None
    ) -> Response:
        """Send one request to the run's topic."""
        return self.send(self.topic_path, method, suffix, actor, body)

    def sub(
        self, method: str, suffix: str = "", *, actor: str = "fixture", body: dict | None = None
    ) -> Response:
        """Send one request to the run's subscription."""
        return self.send(self.sub_path, method, suffix, actor, body)

    def publish(self, *, actor: str = "fixture", extra: dict | None = None) -> Response:
        """Publish one bounded message the way gcloud does; refusals cost nothing."""
        if self.manifest["messages"] >= LIMITS["max_messages"]:
            raise ValueError("Message limit reached")
        message = {"attributes": {}, "data": base64.b64encode(PAYLOAD).decode()}
        body = {**(extra or {}), "messages": [message]}
        response = self.topic("POST", ":publish", actor=actor, body=body)
        if response.status == 200:
            self.manifest["messages"] += 1
            self.save()
        return response

    def pull(self, *, actor: str = "fixture") -> Response:
        """Pull one message as gcloud does, retrying an empty answer while live."""
        deadline = time.monotonic() + (60 if self.client.sandbox else 0)
        body = {"maxMessages": 1, "returnImmediately": False}
        while True:
            response = self.sub("POST", ":pull", actor=actor, body=body)
            if (
                response.status != 200
                or response.json().get("receivedMessages")
                or time.monotonic() >= deadline
            ):
                return response
            time.sleep(2)

    def setup(self) -> tuple[Response, Response]:
        """Create the run's topic and subscription after writing the manifest."""
        self.acquire()
        self.client.verify_project()
        self.manifest["attempted"] = True
        self.save()  # Creation may succeed even if the response is lost.
        topic = self.topic("PUT", body={})
        if topic.status != 200:
            raise Inconclusive(f"Topic create returned HTTP {topic.status}")
        name = f"projects/{self.project}/topics/{self.name}"
        sub = self.sub("PUT", body={"topic": name, "ackDeadlineSeconds": 10})
        return topic, sub

    def cleanup(self) -> None:
        """Delete only the owned subscription and topic, then confirm both are gone."""
        if not self.manifest["attempted"]:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            for path, kind in ((self.sub_path, "subscriptions"), (self.topic_path, "topics")):
                self.remove(path, kind)
            self.manifest.update(cleanup="passed", finished_at=now())
            self.save()
        finally:
            self.release()

    def remove(self, path: str, kind: str) -> None:
        """Delete one owned resource if it exists and confirm it is absent."""
        current = self.client.request("fixture", "GET", path)
        if current.status == 200:
            if not current.json().get("name", "").endswith(f"/{kind}/{self.name}"):
                raise ValueError("Refusing Pub/Sub cleanup: ownership does not match the manifest")
            deleted = self.client.request("fixture", "DELETE", path)
            if deleted.status not in {200, 404}:
                raise ValueError(f"Owned {kind} delete returned HTTP {deleted.status}")
        elif current.status != 404:
            raise ValueError(f"Owned {kind} read returned HTTP {current.status}")
        if self.client.request("fixture", "GET", path).status != 404:
            raise ValueError(f"Owned {kind} remains after deletion")


def outcome(observe: Observations, response: Response, fields: tuple[str, ...] = ()) -> dict:
    """Return an error's code and status, or the selected success fields."""
    if response.status >= HTTP_ERROR:
        return observe.error(response)
    return observe.select(response, fields)


def delivery(observe: Observations, response: Response, published_id: str | None) -> dict:
    """Describe one pulled message without its ack ID or timestamps."""
    value = response.json()
    received = value.get("receivedMessages", [])
    first = received[0] if received else {}
    message = first.get("message", {})
    return {
        "http_status": response.status,
        "fields": sorted(value),
        "count": len(received),
        "received_fields": sorted(first),
        "message_fields": sorted(message),
        "data": base64.b64decode(message.get("data", "")).decode(),
        "attributes": observe.value(message.get("attributes")),
        "same_message_id": message.get("messageId") == published_id,
    }


def grant(fixture: DataPlaneFixture, resource: Resource, role: str) -> Response:
    """Bind one role for the allowed identity on a topic or subscription."""
    member = "serviceAccount:" + fixture.client.emails["allowed"]
    policy = {"bindings": [{"role": role, "members": [member]}]}
    return resource("POST", ":setIamPolicy", body={"policy": policy})


def created_observations(observe: Observations, topic: Response, sub: Response) -> dict:
    """Select only the create fields this scenario depends on.

    The control-plane response shape is outside the scenario's data-plane claim.
    """
    fields = ("name", "topic", "ackDeadlineSeconds")
    return {
        "topic-created": {
            "http_status": topic.status,
            "name": observe.text(topic.json().get("name", "")),
        },
        "subscription-created": {
            "http_status": sub.status,
            **{field: observe.value(sub.json().get(field)) for field in fields},
        },
    }


def ack_id(response: Response) -> str:
    """Return the ack ID of the first pulled message."""
    return response.json()["receivedMessages"][0]["ackId"]


def run_scenario(
    fixture: DataPlaneFixture, snapshot: SnapshotCase, created: tuple[Response, Response]
) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations(
        {
            fixture.name: "<name>",
            fixture.manifest["run_id"]: "<run>",
            fixture.project: "<project>",
            client.number: "<project-number>",
            **{email: "<" + actor + ">" for actor, email in client.emails.items()},
        }
    )
    for label, observation in created_observations(observe, *created).items():
        snapshot.match(label, observation)
    authorize_and_deliver(fixture, snapshot, observe)
    probe_requests(fixture, snapshot, observe)
    snapshot.match("subscription-deleted", {"http_status": fixture.sub("DELETE").status})
    # GCP accepted a publish sent just after the topic delete while this
    # scenario was drafted, so a post-delete publish is not a stable observation.
    snapshot.match("topic-deleted", {"http_status": fixture.topic("DELETE").status})


def authorize_and_deliver(
    fixture: DataPlaneFixture, snapshot: SnapshotCase, observe: Observations
) -> None:
    """Grant publisher then subscriber, and deliver, nack, redeliver, and ack one message."""
    snapshot.match("denied-publish", observe.error(fixture.publish(actor="denied")))
    granted = grant(fixture, fixture.topic, "roles/pubsub.publisher")
    snapshot.match("publisher-granted", observe.select(granted, ("bindings",)))
    published = fixture.wait_until(lambda: fixture.publish(actor="allowed"), {403}, 180)
    ids = published.json().get("messageIds", [])
    snapshot.match("published", {**observe.select(published), "message_ids": len(ids)})
    first_id = ids[0] if ids else None
    snapshot.match("publisher-cannot-pull", observe.error(fixture.pull(actor="allowed")))
    granted = grant(fixture, fixture.sub, "roles/pubsub.subscriber")
    snapshot.match("subscriber-granted", observe.select(granted, ("bindings",)))
    pulled = fixture.wait_until(lambda: fixture.pull(actor="allowed"), {403}, 180)
    snapshot.match("pulled", delivery(observe, pulled, first_id))
    acks = {"ackIds": [ack_id(pulled)]}
    snapshot.match("denied-pull", observe.error(fixture.pull(actor="denied")))
    denied = fixture.sub("POST", ":acknowledge", actor="denied", body=acks)
    snapshot.match("denied-acknowledge", observe.error(denied))
    nack = {**acks, "ackDeadlineSeconds": 0}
    nacked = fixture.sub("POST", ":modifyAckDeadline", actor="allowed", body=nack)
    snapshot.match("nacked", {"http_status": nacked.status, "body": nacked.json()})
    redelivered = fixture.pull(actor="allowed")
    snapshot.match("redelivered", delivery(observe, redelivered, first_id))
    acks = {"ackIds": [ack_id(redelivered)]}
    long = fixture.sub(
        "POST", ":modifyAckDeadline", actor="allowed", body={**acks, "ackDeadlineSeconds": 601}
    )
    snapshot.match("deadline-out-of-range", outcome(observe, long))
    acked = fixture.sub("POST", ":acknowledge", actor="allowed", body=acks)
    snapshot.match("acknowledged", {"http_status": acked.status, "body": acked.json()})


def probe_requests(
    fixture: DataPlaneFixture, snapshot: SnapshotCase, observe: Observations
) -> None:
    """Observe an empty pull, a pull without maxMessages, and a body naming the topic."""
    immediate = {"maxMessages": 1, "returnImmediately": True}
    empty = fixture.sub("POST", ":pull", actor="allowed", body=immediate)
    snapshot.match("empty-pull", {"http_status": empty.status, "body": empty.json()})
    unbounded = fixture.sub("POST", ":pull", body={"returnImmediately": True})
    snapshot.match("pull-without-max-messages", outcome(observe, unbounded))
    named = fixture.publish(extra={"topic": f"projects/{fixture.project}/topics/{fixture.name}"})
    snapshot.match("publish-body-names-topic", outcome(observe, named))


def main(argv: list[str] | None = None) -> None:
    """Delete only the owned topic and subscription after an interrupted run."""
    parser = argparse.ArgumentParser(
        description="Delete only the owned topic and subscription after an interrupted run"
    )
    parser.add_argument("command", choices=["cleanup"])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--allow-live", action="store_true", required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox, 'cleanup')
    target = Target.select(
        "record",
        project=sandbox.project,
        credentials_file=sandbox.identities["allowed"].credentials_file,
        live_scope=sandbox.live_scope,
        allow_live=args.allow_live,
    )
    client = RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    state = Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE
    DataPlaneFixture(client, state, manifest=args.manifest).cleanup()
    sys.stdout.write("Owned Pub/Sub cleanup passed.\n")


if __name__ == "__main__":
    main()
