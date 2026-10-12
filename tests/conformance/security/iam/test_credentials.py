"""IAM Credentials impersonation, delegation, lifetime, and signing against two owned accounts."""

from pathlib import Path

import pytest

from gcp_validation.live_rest import RestClient
from gcp_validation.services.security import iamcredentials
from gcp_validation.services.security.iamcredentials import CredentialsFixture, run_scenario

SERVICE = iamcredentials.SERVICE
LIMITS = iamcredentials.LIMITS


@pytest.fixture
def credentials(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'iamcredentials-resources'
    else:
        option = pytestconfig.getoption('gcp_live_scope_file')
        scope = iamcredentials.load_scope(option, sandbox, gcp_target.mode)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
        gcp_snapshot.context['scope'] = scope.evidence()
    client = RestClient(gcp_target, iamcredentials.IAM_ENDPOINT, sandbox=sandbox, scope=scope)
    fixture = CredentialsFixture(client, state)
    try:
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if fixture.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'accounts_absent': True})


@pytest.mark.gcp_scenario(
    id='iamcredentials-impersonation', service='iam', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/security/iamcredentials.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
    ),
)
def test_impersonation(credentials, gcp_snapshot):
    fixture, created = credentials
    run_scenario(fixture, gcp_snapshot, created)
