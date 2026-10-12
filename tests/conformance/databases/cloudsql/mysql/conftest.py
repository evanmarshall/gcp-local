from pathlib import Path

import pytest

from gcp_validation.services.databases.cloudsql import SERVICE, client_for
from gcp_validation.services.databases.cloudsql_mysql import MySQLInstanceFixture, load_scope


@pytest.fixture
def cloudsql_mysql(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'cloudsql-mysql-resources'
    else:
        scope = load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
        gcp_snapshot.context['scope'] = scope.evidence()
    fixture = MySQLInstanceFixture(client_for(gcp_target, sandbox=sandbox, scope=scope), state)
    try:
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if fixture.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'instance_absent': True})
