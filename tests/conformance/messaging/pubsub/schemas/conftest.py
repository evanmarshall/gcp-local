from pathlib import Path

import pytest

from gcp_validation.services.messaging.pubsub import PubSubClient
from gcp_validation.services.messaging.pubsub_schemas import SchemaFixture, SchemaScope


@pytest.fixture
def pubsub_schemas(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == "local":
        state = pytestconfig.gcp_artifacts / "pubsub-schema-resources"
    else:
        option = pytestconfig.getoption("gcp_pubsub_scope")
        scope = SchemaScope.load(option, sandbox, gcp_target.mode)
        state = Path.home() / ".local/state/gcp-local" / sandbox.project / "pubsub-schemas"
        gcp_snapshot.context["scope"] = scope.evidence()
    client = PubSubClient(
        gcp_target,
        sandbox=sandbox,
        scope=scope,
        identities=pytestconfig.getoption("gcp_pubsub_identities"),
    )
    try:
        fixture = SchemaFixture(client, state)
        try:
            yield fixture
        finally:
            fixture.cleanup()
            if fixture.manifest["cleanup"] == "passed":
                gcp_snapshot.match("cleanup", {"owned_resources_absent": True})
    finally:
        client.close()
