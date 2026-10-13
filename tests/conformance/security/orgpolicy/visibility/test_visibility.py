"""Read-only: the sandbox organization's policies as the project's callers see them."""

import pytest

from gcp_validation.services.security.orgpolicy.visibility import run_visibility


@pytest.mark.gcp_scenario(
    id='orgpolicy-effective-visibility', service='orgpolicy', transport='rest-v2',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/security/orgpolicy/visibility.py',
        'gcp_validation/services/security/hierarchy.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/security/orgpolicy/visibility/conftest.py',
    ),
)
def test_effective_visibility(orgpolicy, gcp_snapshot):
    run_visibility(orgpolicy, gcp_snapshot)
