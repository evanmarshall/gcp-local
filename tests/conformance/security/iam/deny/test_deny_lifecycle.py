"""IAM v2 deny policies on the sandbox project: create, read, list, update, delete, refusals."""

import pytest

from gcp_validation.services.security.iam import denypolicies

# This module's scope contract (scripts/live/batch.py reads them here).
SERVICE = denypolicies.SERVICE
LIMITS = denypolicies.LIMITS


@pytest.mark.gcp_scenario(
    id='iam-deny-policy-lifecycle', service='iam', transport='rest-v2',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/security/iam/denypolicies.py',
        'gcp_validation/services/security/refusals.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/security/iam/deny/conftest.py',
    ),
)
def test_deny_policy_lifecycle(deny_lifecycle, gcp_snapshot):
    denypolicies.run_lifecycle(deny_lifecycle, gcp_snapshot)
