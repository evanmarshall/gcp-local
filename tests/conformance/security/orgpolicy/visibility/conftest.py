import pytest

from gcp_validation.services.security.orgpolicy.visibility import (
    LIVE_ENDPOINT,
    OrgPolicyClient,
    load_scope,
)


@pytest.fixture
def orgpolicy(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode != 'local':
        scope = load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox, gcp_target.mode)
        gcp_snapshot.context['scope'] = scope.evidence()
    client = OrgPolicyClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    client.verify_project()
    return client
