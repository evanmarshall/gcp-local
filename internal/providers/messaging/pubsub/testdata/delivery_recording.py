"""Run the delivery scenario locally, then compare it with its GCP recording."""

import json
import os
import subprocess
import sys
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.services.messaging.pubsub import PubSubClient
from gcp_validation.services.messaging.pubsub_delivery import DeliveryFixture, run_delivery_scenario


class Observations:
    def __init__(self):
        self.values = {}

    def match(self, name, value):
        self.values[name] = value


endpoint, identities, state, number = sys.argv[1:5]
target = Target.select("local", project="project-a", endpoint=endpoint)
client = PubSubClient(target, identities=identities)
observations = Observations()
try:
    fixture = DeliveryFixture(client, state, number)
    try:
        run_delivery_scenario(fixture, observations)
    finally:
        fixture.cleanup()
    assert fixture.manifest["cleanup"] == "passed"
    listing = {"project": "projects/project-a", "page_size": 2}
    assert not client.call("fixture", "topic", "list_topics", listing).topics
    assert not client.call("fixture", "subscription", "list_subscriptions", listing).subscriptions
finally:
    client.close()
root = Path(__file__).resolve().parents[5]
artifacts = root / ".artifacts/recordings/pubsub-delivery"
# Compare against the reviewed GCP recording. A missing or stale baseline
# skips in pytest, so the coverage report must show the scenario verified.
subprocess.run(
    [
        sys.executable,
        "-m",
        "pytest",
        "tests/conformance/messaging/pubsub/delivery",
        "-q",
        "--gcp-mode",
        "local",
        "--gcp-project",
        target.project,
        "--gcp-endpoint",
        target.endpoint,
        "--gcp-pubsub-identities",
        identities,
        "--gcp-artifacts",
        str(artifacts),
    ],
    cwd=root,
    env={**os.environ, "GCP_LOCAL_PUBSUB_PROJECT_NUMBER": number},
    check=True,
    timeout=120,
)
coverage = json.loads((artifacts / "coverage.json").read_text())["conformance"]
assert coverage["total"] == coverage["verified"] == 1, coverage
print(
    json.dumps(
        {
            "kind": "local-scenario-comparison",
            "gcp_execution": False,
            "cloud_fidelity": "verified-against-recording",
            "observations": observations.values,
        }
    )
)
