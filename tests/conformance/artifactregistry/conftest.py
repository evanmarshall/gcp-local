from pathlib import Path

import pytest

from gcp_validation.live_rest import RestClient
from gcp_validation.services.artifactregistry import (
    LIVE_ENDPOINT,
    SERVICE,
    RepositoryFixture,
    load_scope,
)


@pytest.fixture
def artifactregistry(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'artifactregistry-resources'
    else:
        scope = load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox, gcp_target.mode)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
        gcp_snapshot.context['scope'] = scope.evidence()
    fixture = RepositoryFixture(RestClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope), state)
    try:
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if fixture.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'repository_absent': True})
