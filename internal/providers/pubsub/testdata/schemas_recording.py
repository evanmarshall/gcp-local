"""Run the schema scenario locally, then compare it with its GCP recording."""

import json
import os
import subprocess
import sys
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.pubsub import PubSubClient
from gcp_validation.pubsub_schemas import SchemaFixture, run_schema_scenario


class Observations:
    def __init__(self):
        self.values = {}

    def match(self, name, value):
        self.values[name] = value


endpoint, identities, state = sys.argv[1:4]
target = Target.select("local", project="project-a", endpoint=endpoint)
client = PubSubClient(target, identities=identities)
observations = Observations()
try:
    fixture = SchemaFixture(client, state)
    try:
        run_schema_scenario(fixture, observations)
    finally:
        fixture.cleanup()
    assert fixture.manifest["cleanup"] == "passed"
    listing = {"project": "projects/project-a", "page_size": 2}
    assert not client.call("fixture", "topic", "list_topics", listing).topics
    assert not client.call("fixture", "subscription", "list_subscriptions", listing).subscriptions
    schemas = {"parent": "projects/project-a", "page_size": 2}
    assert not client.call("fixture", "schema", "list_schemas", schemas).schemas
finally:
    client.close()
print(json.dumps({"observations": observations.values}, indent=1))
root = Path(__file__).resolve().parents[4]
artifacts = root / ".artifacts/pubsub-schemas-conformance"
# Compare against the reviewed GCP recording. A missing or stale baseline
# skips in pytest, so the coverage report must show the scenario verified.
subprocess.run(
    [
        sys.executable,
        "-m",
        "pytest",
        "tests/conformance/pubsub_schemas",
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
    env=os.environ,
    check=True,
    timeout=300,
)
coverage = json.loads((artifacts / "coverage.json").read_text())["conformance"]
assert coverage["total"] == coverage["verified"] == 1, coverage
print(
    json.dumps(
        {
            "kind": "local-scenario-comparison",
            "gcp_execution": False,
            "cloud_fidelity": "verified-against-recording",
        }
    )
)
