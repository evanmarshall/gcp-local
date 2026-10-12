from pathlib import Path

import pytest

from gcp_validation.live_rest import RestClient
from gcp_validation.pubsub_rest import LIVE_ENDPOINT, SERVICE, DataPlaneFixture, load_scope


@pytest.fixture
def pubsubrest(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == "local":
        state = pytestconfig.gcp_artifacts / "pubsubrest-resources"
    else:
        scope = load_scope(pytestconfig.getoption("gcp_live_scope_file"), sandbox)
        state = Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE
        gcp_snapshot.context["scope"] = scope.evidence()
    client = RestClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    fixture = DataPlaneFixture(client, state)
    try:
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if fixture.manifest["cleanup"] == "passed":
            gcp_snapshot.match("cleanup", {"topic_and_subscription_absent": True})
