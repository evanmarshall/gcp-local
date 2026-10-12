import os
from pathlib import Path

import pytest

from gcp_validation.services.messaging.pubsub import PubSubClient
from gcp_validation.services.messaging.pubsub_delivery import DeliveryFixture, DeliveryScope


@pytest.fixture
def pubsub_delivery(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == "local":
        state = pytestconfig.gcp_artifacts / "pubsub-delivery-resources"
        # The local project's number names its Pub/Sub service agent.
        number = os.environ["GCP_LOCAL_PUBSUB_PROJECT_NUMBER"]
    else:
        option = pytestconfig.getoption("gcp_pubsub_scope")
        scope = DeliveryScope.load(option, sandbox, gcp_target.mode)
        state = Path.home() / ".local/state/gcp-local" / sandbox.project / "pubsub-delivery"
        number = scope.project_number
        gcp_snapshot.context["scope"] = scope.evidence()
    client = PubSubClient(
        gcp_target,
        sandbox=sandbox,
        scope=scope,
        identities=pytestconfig.getoption("gcp_pubsub_identities"),
    )
    try:
        fixture = DeliveryFixture(client, state, number)
        try:
            yield fixture
        finally:
            fixture.cleanup()
            if fixture.manifest["cleanup"] == "passed":
                gcp_snapshot.match("cleanup", {"owned_resources_absent": True})
    finally:
        client.close()
