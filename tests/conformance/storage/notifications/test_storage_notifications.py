"""Pub/Sub notifications for object changes, published as the project's Storage service agent.

A notification configuration needs the Storage service agent to hold
pubsub.topics.publish on the topic; without it GCP refuses the configuration.
One configuration filters by event type and object name prefix and sends the
JSON object resource with custom attributes; the others send every event with
no payload. Messages are matched by event type and object, not by order:
delivery is asynchronous and at least once.
"""

import json
import platform
import time

import pytest

from gcp_validation.evidence import Inconclusive
from gcp_validation.services.storage.normalization import StorageObservations
from gcp_validation.services.storage.notifications import decoded

CONTRACT = (
    "gcp_validation/services/storage/client.py",
    "gcp_validation/services/storage/normalization.py",
    "gcp_validation/services/storage/notifications.py",
    "gcp_validation/live_rest.py",
    "gcp_validation/sandbox.py",
    "gcp_validation/federation.py",
    "tests/conformance/storage/conftest.py",
    "tests/conformance/storage/notifications/conftest.py",
)
VERSIONS = {
    "sdk": "stdlib-http/" + platform.python_version() + ";google-auth/2.56.2",
    "provider": "not-applicable",
    "backend": "not-applicable",
}
GRANT_SECONDS = 180
FILTERED = {
    "payload_format": "JSON_API_V1",
    "event_types": ["OBJECT_FINALIZE", "OBJECT_METADATA_UPDATE", "OBJECT_DELETE"],
    "custom_attributes": {"source": "gcp-local"},
    "object_name_prefix": "n-",
}
PAYLOAD_FIELDS = ("kind", "name", "contentType", "storageClass")


class Shapes:
    """Select the public fields of configurations and messages, with references for IDs."""

    def __init__(self, storage, topic):
        self.observed = StorageObservations(storage)
        self.observed.aliases[topic.name] = "<topic>"

    def config_path(self, text):
        """Return a configuration path with its numeric ID replaced by its reference."""
        prefix, _, config = text.rpartition("/notificationConfigs/")
        if not prefix:
            return text
        return self.observed.text(
            prefix + "/notificationConfigs/" + self.observed.reference("config", config)
        )

    def config(self, response):
        """Return a configuration's fields, or the refusal."""
        if response.status >= 400:
            return self.observed.error(response)
        if response.status == 204:
            return {"status": 204}
        value = response.json()
        link = value.get("selfLink", "").split("/storage/v1/", 1)[-1]
        return {
            "status": response.status,
            "fields": sorted(value),
            "kind": value.get("kind"),
            "id": self.observed.reference("config", value.get("id")),
            "topic": self.observed.text(value.get("topic", "")),
            "payload_format": value.get("payload_format"),
            "event_types": value.get("event_types"),
            "custom_attributes": value.get("custom_attributes"),
            "object_name_prefix": value.get("object_name_prefix"),
            "self_link_path": self.config_path(link),
        }

    def message(self, message):
        """Return a pulled message's attributes and payload shape."""
        attributes = dict(message.get("attributes", {}))
        payload = decoded(message)
        shape = {
            "attributes": sorted(attributes),
            "eventType": attributes.get("eventType"),
            "payloadFormat": attributes.get("payloadFormat"),
            "objectId": attributes.get("objectId"),
            "bucketId": self.observed.text(attributes.get("bucketId", "")),
            "notificationConfig": self.config_path(attributes.get("notificationConfig", "")),
            "source": attributes.get("source"),
            "has_payload": bool(payload),
        }
        for key in ("objectGeneration", "overwroteGeneration", "overwrittenByGeneration"):
            if key in attributes:
                shape[key] = self.observed.reference("generation", attributes[key])
        if attributes.get("payloadFormat") == "JSON_API_V1":
            value = json.loads(payload)
            selected = {key: value.get(key) for key in PAYLOAD_FIELDS}
            selected["generation"] = self.observed.reference("generation", value.get("generation"))
            selected["has_links"] = "selfLink" in value and "mediaLink" in value
            shape["payload"] = selected
        return shape


def delivered(topic, shapes, count):
    """Pull count messages and describe them in a stable order."""
    messages = [shapes.message(message) for message in topic.pull(count)]
    return sorted(
        messages,
        key=lambda shape: (shape["eventType"], shape["notificationConfig"], shape["objectId"]),
    )


def service_agent(storage, shapes, gcp_snapshot):
    """Record the service agent and its denial; return the agent's email."""
    path = f"/storage/v1/projects/{storage.project}/serviceAccount"
    agent = storage.client.request("fixture", "GET", path)
    email = agent.require(200)["email_address"]
    shapes.observed.aliases[email] = "<storage-agent>"
    named = f"service-{storage.project_number}@gs-project-accounts.iam.gserviceaccount.com"
    gcp_snapshot.match(
        "service-agent",
        {
            "fields": sorted(agent.json()),
            "kind": agent.json().get("kind"),
            "email": shapes.observed.text(email),
            "named_by_number": email == named,
        },
    )
    refused = shapes.observed.error(storage.client.request("denied", "GET", path))
    # GCP appends a troubleshooter link with a per-request error ID.
    refused["message"] = refused["message"].split(" Remediate access", 1)[0]
    gcp_snapshot.match("service-agent-denied", refused)
    return email


def configure(storage, topic, shapes, gcp_snapshot, email):
    """Record configuration refusals, creation, listing, and reads; return the listing."""
    configs = storage.path + "/notificationConfigs"

    def create(body, actor="fixture"):
        return storage.client.request(actor, "POST", configs, body=body)

    full_topic = "//pubsub.googleapis.com/" + topic.topic
    absent = "//pubsub.googleapis.com/projects/" + storage.project + "/topics/gl-absent"
    unfiltered = {"topic": full_topic, "payload_format": "NONE"}
    gcp_snapshot.match("create-without-grant", shapes.config(create(unfiltered)))
    missing_topic = create({**unfiltered, "topic": absent})
    gcp_snapshot.match("create-missing-topic", shapes.config(missing_topic))
    gcp_snapshot.match("grant", {"status": topic.grant("serviceAccount:" + email).status})
    deadline = time.monotonic() + (GRANT_SECONDS if storage.sandbox else 0)
    filtered = create({"topic": full_topic, **FILTERED})
    while filtered.status != 200 and time.monotonic() < deadline:
        time.sleep(10)
        filtered = create({"topic": full_topic, **FILTERED})
    if filtered.status != 200:
        raise Inconclusive("The publisher grant did not propagate within the deadline")
    gcp_snapshot.match("create-filtered", shapes.config(filtered))
    short = create({"topic": topic.topic, "payload_format": "NONE"})
    gcp_snapshot.match("create-unfiltered-short-topic", shapes.config(short))
    for label, body in {
        "payload-unknown": {"topic": full_topic, "payload_format": "XML"},
        "event-unknown": {**unfiltered, "event_types": ["OBJECT_EXPLODE"]},
        "topic-malformed": {"topic": "not-a-topic", "payload_format": "NONE"},
        "topic-missing": {"payload_format": "NONE"},
    }.items():
        gcp_snapshot.match(label, shapes.config(create(body)))
    gcp_snapshot.match("create-denied", shapes.config(create(unfiltered, actor="denied")))
    listing = storage.client.request("fixture", "GET", configs).require(200)
    ids = sorted(shapes.observed.reference("config", item["id"]) for item in listing["items"])
    gcp_snapshot.match("list", {"kind": listing.get("kind"), "ids": ids})
    read = storage.client.request("fixture", "GET", configs + "/" + filtered.json()["id"])
    gcp_snapshot.match("get", shapes.config(read))
    missing = storage.client.request("fixture", "GET", configs + "/999999")
    gcp_snapshot.match("get-missing", shapes.config(missing))
    return listing, filtered.json()["id"]


@pytest.mark.gcp_scenario(
    id="storage-notifications",
    service="storage",
    transport="rest-json-v1",
    versions=VERSIONS,
    contract_files=CONTRACT,
)
def test_storage_notifications(storage, topic, gcp_snapshot):
    shapes = Shapes(storage, topic)
    email = service_agent(storage, shapes, gcp_snapshot)
    listing, filtered_id = configure(storage, topic, shapes, gcp_snapshot, email)
    configs = storage.path + "/notificationConfigs"

    first = storage.upload("n-1", b"notified").require(200)
    shapes.observed.reference("generation", first["generation"])
    gcp_snapshot.match("finalize", delivered(topic, shapes, 3))
    storage.upload("other", b"other").require(200)
    gcp_snapshot.match("finalize-unfiltered", delivered(topic, shapes, 2))
    metadata = {"metadata": {"k": "v"}}
    storage.client.request("allowed", "PATCH", storage.path + "/o/n-1", body=metadata).require(200)
    gcp_snapshot.match("metadata-update", delivered(topic, shapes, 3))
    second = storage.upload("n-1", b"replaced", generation=first["generation"]).require(200)
    gcp_snapshot.match("overwrite", delivered(topic, shapes, 6))
    storage.delete("n-1", second["generation"]).require(204)
    gcp_snapshot.match("delete", delivered(topic, shapes, 3))

    for config in listing["items"]:
        label = "deleted-" + shapes.observed.reference("config", config["id"]).strip("<>")
        deleted = storage.client.request("fixture", "DELETE", configs + "/" + config["id"])
        gcp_snapshot.match(label, shapes.config(deleted))
    after = storage.client.request("fixture", "GET", configs + "/" + filtered_id)
    gcp_snapshot.match("deleted-get", shapes.config(after))
    other = storage.read("other").require(200)
    storage.delete("other", other["generation"]).require(204)
    gcp_snapshot.match("after-configs-deleted", delivered(topic, shapes, 0))
