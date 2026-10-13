"""The Pub/Sub topic and subscription a Storage notification scenario publishes to.

The Storage bucket comes from the profile's Storage scope; the topic and
subscription need this separately approved scope. Cloud Storage publishes as
the project's Storage service agent, so the scenario grants that agent
roles/pubsub.publisher on the topic; cleanup removes the grant and deletes the
subscription and the topic.
"""

import base64
import time
from pathlib import Path

from gcp_validation.evidence import Inconclusive, now
from gcp_validation.live_rest import LiveScope, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = "storage-notifications"
LIMITS = {"max_topics": 1, "max_subscriptions": 1, "max_messages": 20, "max_run_seconds": 900}
LIVE_ENDPOINT = "https://pubsub.googleapis.com"
# GCP delivers notifications asynchronously, usually within seconds.
DELIVERY_SECONDS = 90


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the separately approved live scope for the topic and subscription."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class NotificationTopic(OwnedRun):
    """One topic and one pull subscription named for the run."""

    def __init__(self, client: RestClient, state_dir: Path) -> None:
        """Bind the run manifest and the topic and subscription paths."""
        super().__init__(client, state_dir, SERVICE, LIMITS["max_run_seconds"])
        self.name = "gl-sn-" + self.manifest["run_id"][:12]
        self.manifest.update(name=self.name, received=0)
        self.topic = f"projects/{self.project}/topics/{self.name}"
        self.topic_path = "/v1/" + self.topic
        self.sub_path = f"/v1/projects/{self.project}/subscriptions/{self.name}"

    def setup(self) -> None:
        """Create the topic and subscription after writing the manifest."""
        self.acquire()
        self.client.verify_project()
        self.manifest["attempted"] = True
        self.save()  # Creation may succeed even if the response is lost.
        if self.client.request("fixture", "PUT", self.topic_path, body={}).status != 200:
            raise Inconclusive("Topic create failed")
        subscription = {"topic": self.topic, "ackDeadlineSeconds": 10}
        if self.client.request("fixture", "PUT", self.sub_path, body=subscription).status != 200:
            raise Inconclusive("Subscription create failed")

    def grant(self, member: str | None) -> Response:
        """Replace the topic policy with one publisher binding, or none."""
        bindings = [{"role": "roles/pubsub.publisher", "members": [member]}] if member else []
        return self.client.request(
            "fixture",
            "POST",
            self.topic_path + ":setIamPolicy",
            body={"policy": {"bindings": bindings}},
            timeout=self.remaining(),
        )

    def pull(self, count: int) -> list[dict]:
        """Pull and acknowledge until count distinct messages arrive, then catch late extras."""
        messages, deadline = {}, time.monotonic() + (DELIVERY_SECONDS if self.client.sandbox else 5)
        settle = None
        while time.monotonic() < deadline and (settle is None or time.monotonic() < settle):
            response = self.client.request(
                "fixture",
                "POST",
                self.sub_path + ":pull",
                body={"maxMessages": 10, "returnImmediately": True},
                timeout=self.remaining(),
            )
            if response.status != 200:
                raise Inconclusive(f"Pull returned HTTP {response.status}")
            received = response.json().get("receivedMessages", [])
            if received:
                self.client.request(
                    "fixture",
                    "POST",
                    self.sub_path + ":acknowledge",
                    body={"ackIds": [item["ackId"] for item in received]},
                    timeout=self.remaining(),
                ).json()
            for item in received:
                messages.setdefault(item["message"]["messageId"], item["message"])
            self.manifest["received"] = len(messages)
            if self.manifest["received"] > LIMITS["max_messages"]:
                raise ValueError("Message count exceeds the bounded scenario scope")
            if len(messages) >= count and settle is None:
                # Notifications are at-least-once and unordered; catch late extras.
                settle = time.monotonic() + (5 if self.client.sandbox else 0.5)
            time.sleep(1 if not received else 0)
        return list(messages.values())

    def cleanup(self) -> None:
        """Remove the grant, then delete the subscription and topic and confirm both are gone."""
        if not self.manifest["attempted"]:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            if self.client.request("fixture", "GET", self.topic_path).status == 200:
                self.grant(None)
            for path in (self.sub_path, self.topic_path):
                current = self.client.request("fixture", "GET", path)
                if current.status == 200:
                    if not current.json().get("name", "").endswith("/" + self.name):
                        raise ValueError(
                            "Refusing Pub/Sub cleanup: ownership does not match the manifest"
                        )
                    if self.client.request("fixture", "DELETE", path).status not in {200, 404}:
                        raise ValueError("Owned Pub/Sub delete failed")
                elif current.status != 404:
                    raise ValueError("Owned Pub/Sub read failed")
                if self.client.request("fixture", "GET", path).status != 404:
                    raise ValueError("Owned Pub/Sub resource remains after deletion")
            self.manifest.update(cleanup="passed", finished_at=now())
            self.save()
        finally:
            self.release()


def decoded(message: dict) -> bytes:
    """Return a pulled message's payload bytes."""
    return base64.b64decode(message.get("data", ""))
