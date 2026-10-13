from pathlib import Path

import pytest

from gcp_validation.services.compute.lists_iam import build, load_scope


@pytest.fixture
def listsiam(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == "local":
        state = pytestconfig.gcp_artifacts / "listsiam-resources"
    else:
        scope = load_scope(pytestconfig.getoption("gcp_live_scope_file"), sandbox, gcp_target.mode)
        state = Path.home() / ".local/state/gcp-local" / sandbox.project / "networking"
        gcp_snapshot.context["scope"] = scope.evidence()
    fixture = build(gcp_target, sandbox, scope, state)
    try:
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if fixture.manifest["cleanup"] == "passed":
            gcp_snapshot.match("cleanup", {"owned_resources_absent": True})
