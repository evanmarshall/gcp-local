from pathlib import Path

import pytest

from gcp_validation.cloudscheduler import SERVICE, JobsFixture, clients, load_scope


@pytest.fixture
def cloudscheduler(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'cloudscheduler-resources'
    else:
        scope = load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
        gcp_snapshot.context['scope'] = scope.evidence()
    scheduler, pubsub = clients(gcp_target, sandbox, scope)
    fixture = JobsFixture(scheduler, pubsub, state)
    try:
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if fixture.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'jobs_absent': True, 'topic_absent': True})
