from pathlib import Path

import pytest

from gcp_validation.services.security.iam import denyenforcement, denypolicies


def deny_client(gcp_snapshot, gcp_target, pytestconfig, module):
    """Return the scenario's client and state directory, with its scope evidence."""
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / (module.SERVICE + '-resources')
    else:
        scope = module.load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox,
                                  gcp_target.mode)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / module.SERVICE
        gcp_snapshot.context['scope'] = scope.evidence()
    client = denypolicies.DenyClient(gcp_target, denypolicies.LIVE_ENDPOINT, sandbox=sandbox,
                                     scope=scope)
    return client, state


@pytest.fixture
def deny_lifecycle(gcp_snapshot, gcp_target, pytestconfig):
    client, state = deny_client(gcp_snapshot, gcp_target, pytestconfig, denypolicies)
    run = denypolicies.DenyRun(client, state)
    try:
        run.setup()
        yield run
    finally:
        run.cleanup()
        if run.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'policies_absent': True})


@pytest.fixture
def deny_enforcement(gcp_snapshot, gcp_target, pytestconfig):
    client, state = deny_client(gcp_snapshot, gcp_target, pytestconfig, denyenforcement)
    run = denyenforcement.EnforcementRun(client, state)
    try:
        run.setup()
        yield run
    finally:
        run.cleanup()
        if run.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'policies_absent': True, 'role_inactive': True,
                                           'account_absent': True})
