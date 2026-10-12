"""Run the prepared delivery scenario locally; never create a fake cloud baseline."""

import json
import os
import subprocess
import sys
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.pubsub import PubSubClient
from gcp_validation.pubsub_delivery import DeliveryFixture, run_delivery_scenario


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
root = Path(__file__).resolve().parents[4]
# Comparison needs a reviewed GCP baseline; until one exists this run only
# prepares the scenario and claims no cloud fidelity.
baseline = root / "tests/conformance/recordings/pubsub-delivery-features/provenance.json"
if baseline.exists():
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/conformance/pubsub_delivery",
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
            str(root / ".artifacts/pubsub-delivery-conformance"),
        ],
        cwd=root,
        env={**os.environ, "GCP_LOCAL_PUBSUB_PROJECT_NUMBER": number},
        check=True,
        timeout=120,
    )
print(
    json.dumps(
        {
            "kind": "local-scenario-preparation",
            "gcp_execution": False,
            "cloud_fidelity": "unverified" if not baseline.exists() else "see-conformance-report",
            "observations": observations.values,
        }
    )
)
