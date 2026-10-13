from pathlib import Path

import pytest

from gcp_validation.services.databases.cloudsql import client_for
from gcp_validation.services.databases.cloudsql_depth import (
    SCENARIOS,
    SERVICE,
    DepthFixture,
    load_scope,
    specs,
)


@pytest.fixture
def cloudsql_depth(gcp_snapshot, gcp_target, pytestconfig, request):
    """Own the instances of the requesting module's scenario (its KIND and LIMITS)."""
    kind, limits = request.module.KIND, request.module.LIMITS
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / f'cloudsql-{kind}-resources'
    else:
        scope = load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox,
                           gcp_target.mode, limits)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
        gcp_snapshot.context['scope'] = scope.evidence()
    fixture = DepthFixture(client_for(gcp_target, sandbox=sandbox, scope=scope), state, limits,
                           suffixes=SCENARIOS[kind][0])
    try:
        created = fixture.setup(specs(kind, fixture))
        yield fixture, created
    finally:
        fixture.cleanup()
        if fixture.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'instances_absent': True})
