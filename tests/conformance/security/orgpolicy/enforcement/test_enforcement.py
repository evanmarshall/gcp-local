"""The sandbox organization's constraints refuse a bucket, a policy member, and a key.

Each refusal comes after IAM allows the request.
"""

import pytest

from gcp_validation.services.security.orgpolicy.enforcement import run_scenario


@pytest.mark.gcp_scenario(
    id='orgpolicy-constraint-enforcement', service='orgpolicy', transport='rest-json-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/security/orgpolicy/enforcement.py',
        'gcp_validation/services/security/orgpolicy/visibility.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/security/orgpolicy/enforcement/conftest.py',
    ),
)
def test_constraint_enforcement(enforcement, gcp_snapshot):
    run_scenario(enforcement, gcp_snapshot)
