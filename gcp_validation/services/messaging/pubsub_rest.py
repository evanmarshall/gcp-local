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
from gcp_validation.errors import detailed
from gcp_validation.evidence import Inconclusive, SnapshotCase, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = "pubsub-rest"
LIMITS = {
    "max_topics": 2,
    "max_subscriptions": 2,
    "max_messages": 4,
    "max_message_bytes": 1024,
    "max_run_seconds": 900,
}
LIVE_ENDPOINT = "https://pubsub.googleapis.com"
PAYLOAD = b"gcp-local rest data plane"
HTTP_ERROR = 400

Resource = Callable[..., Response]
# ErrorInfo metadata that differs on every denial; the first also encodes the project ID.
VOLATILE_METADATA = ("error_info_id", "troubleshooter_url")


def denial(observe: Observations, response: Response) -> dict:
    """Select an error with its message and details, without per-request ErrorInfo metadata."""
    selected = detailed(observe, response)
    for detail in selected["details"]:
        for key in VOLATILE_METADATA:
            detail.get("metadata", {}).pop(key, None)
    return selected


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the separately approved live scope for this scenario."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class DataPlaneFixture(OwnedRun):
    """Two topics and two pull subscriptions named for the run.

    The second pair's name sorts before the first's, so list order is observable.
    """

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
        self.second = "gk-" + self.manifest["run_id"][:12]

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
            for name in (self.name, self.second):
                for kind in ("subscriptions", "topics"):
                    self.remove(f"/v1/projects/{self.project}/{kind}/{name}", kind, name)
            self.manifest.update(cleanup="passed", finished_at=now())
            self.save()
        finally:
            self.release()

    def remove(self, path: str, kind: str, name: str) -> None:
        """Delete one owned resource if it exists and confirm it is absent."""
        current = self.client.request("fixture", "GET", path)
        if current.status == 200:
            if not current.json().get("name", "").endswith(f"/{kind}/{name}"):
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
            fixture.second: "<second>",
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
    conditional_policy(fixture, snapshot, observe)
    probe_requests(fixture, snapshot, observe)
    observe_lists(fixture, snapshot, observe)
    observe_decoding(fixture, snapshot, observe)
    snapshot.match("subscription-deleted", {"http_status": fixture.sub("DELETE").status})
    # GCP accepted a publish sent just after the topic delete while this
    # scenario was drafted, so a post-delete publish is not a stable observation.
    snapshot.match("topic-deleted", {"http_status": fixture.topic("DELETE").status})


def authorize_and_deliver(
    fixture: DataPlaneFixture, snapshot: SnapshotCase, observe: Observations
) -> None:
    """Grant publisher then subscriber, and deliver, nack, redeliver, and ack one message."""
    snapshot.match("denied-publish", denial(observe, fixture.publish(actor="denied")))
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


CONDITION = {
    "title": "gcp-local-conformance",
    "description": "Expires in 2100",
    "expression": 'request.time < timestamp("2100-01-01T00:00:00Z")',
}


def read_policy(fixture: DataPlaneFixture, version: int | None) -> Response:
    """Read the topic policy as gcloud does, requesting a version when one is given."""
    query = {"alt": "json"}
    if version is not None:
        query["options.requestedPolicyVersion"] = str(version)
    return fixture.client.request(
        "fixture", "GET", fixture.topic_path + ":getIamPolicy", query=query,
        timeout=fixture.remaining(),
    )


def policy_view(observe: Observations, response: Response) -> dict:
    """Describe a policy's version and bindings; a _withcond_ role suffix is opaque."""
    if response.status >= HTTP_ERROR:
        return observe.error(response)
    value = response.json()
    bindings = []
    for binding in value.get("bindings", []):
        role, marker, suffix = binding["role"].partition("_withcond_")
        shape = {"length": len(suffix), "hex": all(c in "0123456789abcdef" for c in suffix)}
        bindings.append({
            "role": role,
            "withcond_suffix": shape if marker else None,
            "members": sorted(observe.text(member) for member in binding["members"]),
            "condition": binding.get("condition"),
        })
    return {
        "http_status": response.status,
        "version": value.get("version"),
        "bindings": sorted(
            bindings, key=lambda item: (item["role"], item["condition"] is not None)
        ),
    }


def conditional_policy(
    fixture: DataPlaneFixture, snapshot: SnapshotCase, observe: Observations
) -> None:
    """Try conditional bindings on the topic and subscription, and read the policy versions."""
    member = "serviceAccount:" + fixture.client.emails["allowed"]
    current = read_policy(fixture, 3)
    if current.status != 200:
        raise Inconclusive(f"Topic getIamPolicy returned HTTP {current.status}")
    kept = current.json().get("bindings", [])
    conditional = {"role": "roles/pubsub.viewer", "members": [member], "condition": CONDITION}
    policy = {"bindings": [*kept, conditional], "etag": current.json().get("etag", "")}
    rejected = fixture.topic("POST", ":setIamPolicy", body={"policy": {**policy, "version": 1}})
    snapshot.match("condition-needs-version-3", observe.error(rejected))
    written = fixture.topic("POST", ":setIamPolicy", body={"policy": {**policy, "version": 3}})
    snapshot.match("topic-condition-refused", policy_view(observe, written))
    only = {"bindings": [conditional], "version": 3}
    sub = fixture.sub("POST", ":setIamPolicy", body={"policy": only})
    snapshot.match("subscription-condition-refused", policy_view(observe, sub))
    # A version 3 write without conditions, then reads at each requested version.
    unconditional = {"bindings": kept, "etag": current.json().get("etag", ""), "version": 3}
    rewritten = fixture.topic("POST", ":setIamPolicy", body={"policy": unconditional})
    snapshot.match("unconditional-v3-written", policy_view(observe, rewritten))
    snapshot.match("read-v3", policy_view(observe, read_policy(fixture, 3)))
    snapshot.match("read-v1", policy_view(observe, read_policy(fixture, 1)))
    snapshot.match("read-unversioned", policy_view(observe, read_policy(fixture, None)))


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


def observe_lists(
    fixture: DataPlaneFixture, snapshot: SnapshotCase, observe: Observations
) -> None:
    """Create the second topic and subscription, then observe list order and paging.

    Other runs may share the sandbox project; only the owned pair's order is recorded.
    """
    second_topic = f"/v1/projects/{fixture.project}/topics/{fixture.second}"
    second_sub = f"/v1/projects/{fixture.project}/subscriptions/{fixture.second}"
    topic = fixture.send(second_topic, "PUT", "", "fixture", {})
    if topic.status != 200:
        raise Inconclusive(f"Second topic create returned HTTP {topic.status}")
    name = f"projects/{fixture.project}/topics/{fixture.second}"
    sub = fixture.send(second_sub, "PUT", "", "fixture", {"topic": name, "ackDeadlineSeconds": 10})
    if sub.status != 200:
        raise Inconclusive(f"Second subscription create returned HTTP {sub.status}")
    owned = {fixture.name, fixture.second}
    for kind in ("topics", "subscriptions"):
        path = f"/v1/projects/{fixture.project}/{kind}"

        def listing(path: str = path, **query: str) -> Response:
            return fixture.client.request(
                "fixture", "GET", path, query=query or None, timeout=fixture.remaining()
            )

        everything = listing()
        items = everything.json().get(kind, [])
        names = [item.get("name", "").rsplit("/", 1)[-1] for item in items]
        first = listing(pageSize="1")
        token = first.json().get("nextPageToken", "")
        second = listing(pageSize="1", pageToken=token)
        # Topics list in name order; subscriptions in an order GCP doesn't
        # document (neither name nor creation), so only membership is recorded.
        listed = [observe.text(n) for n in names if n in owned]
        snapshot.match(f"{kind}-list", {
            "http_status": everything.status, "fields": sorted(everything.json()),
            **({"owned_order": listed} if kind == "topics" else {"owned": sorted(listed)}),
            "first_status": first.status, "first_count": len(first.json().get(kind, [])),
            "first_has_next": bool(token), "second_status": second.status,
            "second_count": len(second.json().get(kind, [])),
        })
        snapshot.match(f"{kind}-list-invalid-token",
                       denial(observe, listing(pageToken="not-a-page-token")))
        snapshot.match(f"{kind}-list-negative-size", denial(observe, listing(pageSize="-1")))
    deletes = (("second-subscription-deleted", second_sub), ("second-topic-deleted", second_topic))
    for label, path in deletes:
        snapshot.match(label, {"http_status": fixture.client.request(
            "fixture", "DELETE", path, timeout=fixture.remaining()).status})


def observe_decoding(
    fixture: DataPlaneFixture, snapshot: SnapshotCase, observe: Observations
) -> None:
    """Observe refused topic bodies: an unknown name and a mistyped value.

    Each reuses the second pair's deleted name; a refused creation leaves nothing.
    """
    path = f"/v1/projects/{fixture.project}/topics/{fixture.second}"
    unknown = fixture.send(path, "PUT", "", "fixture", {"unknownField": "x"})
    snapshot.match("unknown-field", denial(observe, unknown))
    mistyped = fixture.send(path, "PUT", "", "fixture", {"labels": "x"})
    snapshot.match("mistyped-field", denial(observe, mistyped))


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
