from pathlib import Path

import pytest

from gcp_validation.live_rest import RestClient
from gcp_validation.services.security.projectiam_versions import (
    LIVE_ENDPOINT,
    LOCK,
    VersionFixture,
    load_scope,
)


@pytest.fixture
def projectiamversions(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == "local":
        state = pytestconfig.gcp_artifacts / "projectiamversions-resources"
    else:
        scope = load_scope(pytestconfig.getoption("gcp_live_scope_file"), sandbox, gcp_target.mode)
        state = Path.home() / ".local/state/gcp-local" / sandbox.project / LOCK
        gcp_snapshot.context["scope"] = scope.evidence()
    fixture = VersionFixture(
        RestClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope), state
    )
    try:
        original = fixture.setup()
        yield fixture, original
    finally:
        fixture.cleanup()
        if fixture.manifest["cleanup"] == "passed":
            gcp_snapshot.match("cleanup", {"version_1_without_conditions_or_audit_configs": True})
