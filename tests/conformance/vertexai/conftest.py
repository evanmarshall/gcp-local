from pathlib import Path

import pytest

from gcp_validation.live_rest import RestClient
from gcp_validation.services.vertexai import LIVE_ENDPOINT, SERVICE, InferenceRun, load_scope


@pytest.fixture
def vertexai(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'vertexai-runs'
    else:
        scope = load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
        gcp_snapshot.context['scope'] = scope.evidence()
    run = InferenceRun(RestClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope), state)
    try:
        run.setup()
        yield run
    finally:
        run.cleanup()
