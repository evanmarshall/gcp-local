from pathlib import Path

import pytest

from gcp_validation.live_rest import RestClient
from gcp_validation.services.security.secretmanager.policies import (
    LIVE_ENDPOINT,
    PUBSUB_ENDPOINT,
    SERVICE,
    PoliciesFixture,
    load_scope,
)


@pytest.fixture
def secretmanager_policies(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == "local":
        state = pytestconfig.gcp_artifacts / "secretmanager-policies-resources"
    else:
        scope = load_scope(pytestconfig.getoption("gcp_live_scope_file"), sandbox, gcp_target.mode)
        state = Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE
        gcp_snapshot.context["scope"] = scope.evidence()
    fixture = PoliciesFixture(
        RestClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope),
        RestClient(gcp_target, PUBSUB_ENDPOINT, sandbox=sandbox, scope=scope),
        state,
    )
    try:
        fixture.setup()
        yield fixture
    finally:
        fixture.cleanup()
        if fixture.manifest["cleanup"] == "passed":
            gcp_snapshot.match("cleanup", {"secrets_topic_and_subscription_absent": True})
