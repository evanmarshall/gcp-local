"""A project deny policy overrides a custom-role grant; an exemption and a group apply."""

import pytest

from gcp_validation.services.security.iam import denyenforcement

# This module's scope contract (scripts/live/batch.py reads them here).
SERVICE = denyenforcement.SERVICE
LIMITS = denyenforcement.LIMITS


@pytest.mark.gcp_scenario(
    id='iam-deny-policy-enforcement', service='iam', transport='rest-v2',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/security/iam/denyenforcement.py',
        'gcp_validation/services/security/iam/denypolicies.py',
        'gcp_validation/services/security/refusals.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/security/iam/deny/conftest.py',
    ),
)
def test_deny_policy_enforcement(deny_enforcement, gcp_snapshot):
    denyenforcement.run_enforcement(deny_enforcement, gcp_snapshot)
