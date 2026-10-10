"""Run the prepared cloud scenario locally; never create a fake cloud baseline."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.pubsub import PubSubClient, PubSubFixture, run_scenario


class Observations:
    def __init__(self):
        self.names = []

    def match(self, name, value):
        self.names.append(name)


target = Target.select("local", project="project-a", endpoint=sys.argv[1])
client = PubSubClient(target, identities=sys.argv[2])
observations = Observations()
try:
    fixture = PubSubFixture(client, sys.argv[3])
    try:
        fixture.setup()
        run_scenario(fixture, observations)
    finally:
        fixture.cleanup()
    assert fixture.manifest["cleanup"] == "passed"
    assert not client.call(
        "fixture", "topic", "list_topics", {"project": "projects/project-a", "page_size": 2}
    ).topics
    assert not client.call(
        "fixture",
        "subscription",
        "list_subscriptions",
        {"project": "projects/project-a", "page_size": 2},
    ).subscriptions
finally:
    client.close()
root = Path(__file__).resolve().parents[4]
# A real baseline is required for comparison; the preparation run above never
# writes snapshots or claims GCP evidence. Adding an approved baseline enables
# this exact isolated comparison in the existing CI job.
baseline = root / "tests/conformance/recordings/pubsub-pull-authorization/provenance.json"
if baseline.exists():
    digest = hashlib.sha256()
    for directory in ("api/generated", "internal", "cmd"):
        for path in sorted((root / directory).rglob("*.go")):
            digest.update(path.relative_to(root).as_posix().encode() + b"\0" + path.read_bytes())
    for name in ("go.mod", "go.sum"):
        digest.update((root / name).read_bytes())
    backend = json.loads((root / "toolchains/pubsub.json").read_text())
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/conformance/pubsub",
            "-q",
            "--gcp-mode",
            "local",
            "--gcp-project",
            target.project,
            "--gcp-endpoint",
            target.endpoint,
            "--gcp-pubsub-identities",
            sys.argv[2],
            "--gcp-artifacts",
            str(root / ".artifacts/pubsub-conformance"),
            "--gcp-provider-version",
            "gcp-local-source-" + digest.hexdigest(),
            "--gcp-backend-version",
            backend["version"] + ";sha256:" + backend["jar_sha256"],
        ],
        cwd=root,
        check=True,
        timeout=35,
    )
print(
    json.dumps(
        {
            "kind": "local-recorder-smoke",
            "gcp_execution": False,
            "cloud_fidelity": "unverified" if not baseline.exists() else "see-conformance-report",
            "observations": observations.names,
            "cleanup": fixture.manifest["cleanup"],
        }
    )
)
