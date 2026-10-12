from pathlib import Path

import pytest

from gcp_validation.services.databases.bigquery import (
    LIVE_ENDPOINT,
    SERVICE,
    BigQueryClient,
    DatasetFixture,
    load_scope,
)


@pytest.fixture
def bigquery(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'bigquery-resources'
    else:
        scope = load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
        gcp_snapshot.context['scope'] = scope.evidence()
    fixture = DatasetFixture(BigQueryClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope), state)
    try:
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if fixture.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'dataset_absent': True})
