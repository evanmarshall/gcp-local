"""A project custom role granted on a service account, through delete, undelete, and patch."""

from pathlib import Path

import pytest

from gcp_validation.live_rest import RestClient
from gcp_validation.services.security.iam import customroles
from gcp_validation.services.security.iam.customroles import CustomRoleFixture, run_scenario

# This module's scope contract differs from the directory's service accounts.
SERVICE = customroles.SERVICE
LIMITS = customroles.LIMITS


@pytest.fixture
def custom_role(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'customrole-resources'
    else:
        option = pytestconfig.getoption('gcp_live_scope_file')
        scope = customroles.load_scope(option, sandbox, gcp_target.mode)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
        gcp_snapshot.context['scope'] = scope.evidence()
    client = RestClient(gcp_target, customroles.LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    fixture = CustomRoleFixture(client, state)
    try:
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if fixture.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'role_inactive': True, 'account_absent': True})


@pytest.mark.gcp_scenario(
    id='iam-custom-role-lifecycle', service='iam', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/security/iam/customroles.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
    ),
)
def test_custom_role_lifecycle(custom_role, gcp_snapshot):
    fixture, created = custom_role
    run_scenario(fixture, gcp_snapshot, created)
