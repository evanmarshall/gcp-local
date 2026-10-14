from pathlib import Path

import pytest

from gcp_validation.live_rest import RestClient
from gcp_validation.runner.rpc import GrpcClient
from gcp_validation.services.security.secretmanager.transports import (
    LIVE_ENDPOINT,
    LIVE_HOST,
    SERVICE,
    TransportsFixture,
    load_scope,
)


@pytest.fixture
def secretmanager_transports(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == "local":
        state = pytestconfig.gcp_artifacts / "secretmanager-transports-resources"
    else:
        scope = load_scope(pytestconfig.getoption("gcp_live_scope_file"), sandbox, gcp_target.mode)
        state = Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE
        gcp_snapshot.context["scope"] = scope.evidence()
    client = RestClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    fixture = TransportsFixture(client, GrpcClient(client, LIVE_HOST), state)
    try:
        fixture.setup()
        yield fixture
    finally:
        fixture.cleanup()
        if fixture.manifest["cleanup"] == "passed":
            gcp_snapshot.match("cleanup", {"secrets_absent": True})
