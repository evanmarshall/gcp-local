from pathlib import Path

import pytest

from gcp_validation.services.security.secretmanager.secrets import (
    LiveScope,
    SecretManagerClient,
    SecretManagerFixture,
)


@pytest.fixture
def secretmanager(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'secretmanager-resources'
    else:
        option = pytestconfig.getoption('gcp_secretmanager_scope')
        scope = LiveScope.load(option, sandbox, gcp_target.mode)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / 'secretmanager'
        gcp_snapshot.context['scope'] = scope.evidence()
    fixture = SecretManagerFixture(SecretManagerClient(gcp_target, sandbox=sandbox, scope=scope), state)
    try:
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if fixture.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'secret_absent': True})
