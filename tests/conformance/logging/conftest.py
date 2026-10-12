from pathlib import Path

import pytest

from gcp_validation.services.cloudlogging import (
    LIVE_ENDPOINT,
    SERVICE,
    LogFixture,
    LoggingClient,
    load_scope,
)


@pytest.fixture
def cloudlogging(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'logging-resources'
    else:
        scope = load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox, gcp_target.mode)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
        gcp_snapshot.context['scope'] = scope.evidence()
    client = LoggingClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    fixture = LogFixture(client, state)
    try:
        fixture.setup()
        yield fixture
    finally:
        fixture.cleanup()
        if fixture.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'log_deleted': True})
