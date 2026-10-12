from pathlib import Path

import pytest

from gcp_validation.services.compute.networking import clients
from gcp_validation.services.compute.vm_connectivity import SERVICE, VmFixture, load_scope


@pytest.fixture
def vmconnectivity(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'vmconnectivity-resources'
    else:
        scope = load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
        gcp_snapshot.context['scope'] = scope.evidence()
    compute, management = clients(gcp_target, sandbox=sandbox, scope=scope)
    fixture = VmFixture(compute, management, state)
    try:
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if fixture.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'owned_resources_absent': True})
