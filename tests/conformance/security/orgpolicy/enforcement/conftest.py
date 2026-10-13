from pathlib import Path

import pytest

from gcp_validation.services.security.orgpolicy.enforcement import (
    LIVE_ENDPOINT,
    EnforcementClient,
    EnforcementRun,
    load_scope,
)


@pytest.fixture
def enforcement(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'orgpolicy-resources'
    else:
        scope = load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox, gcp_target.mode)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project
        gcp_snapshot.context['scope'] = scope.evidence()
    client = EnforcementClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    run = EnforcementRun(client, state)
    try:
        run.setup()
        yield run
    finally:
        run.cleanup()
        if run.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'bucket_absent': True, 'account_absent': True})
