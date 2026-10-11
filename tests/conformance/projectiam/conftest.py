from pathlib import Path

import pytest

from gcp_validation.live_rest import RestClient
from gcp_validation.projectiam import LIVE_ENDPOINT, SERVICE, ProjectPolicyFixture, load_scope


@pytest.fixture
def projectiam(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'projectiam-resources'
    else:
        scope = load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
        gcp_snapshot.context['scope'] = scope.evidence()
    fixture = ProjectPolicyFixture(RestClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope), state)
    try:
        initial = fixture.setup()
        yield fixture, initial
    finally:
        fixture.cleanup()
        if fixture.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'scenario_binding_absent': True, 'prior_bindings_preserved': True})
