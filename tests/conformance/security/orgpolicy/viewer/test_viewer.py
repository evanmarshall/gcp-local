"""Read-only, with roles/orgpolicy.policyViewer granted to the allowed identity for the run."""

import pytest

from gcp_validation.services.security.orgpolicy.visibility import run_viewer


@pytest.mark.gcp_scenario(
    id='orgpolicy-project-viewer', service='orgpolicy', transport='rest-v2',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/security/orgpolicy/visibility.py',
        'gcp_validation/services/security/hierarchy.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/security/orgpolicy/viewer/conftest.py',
    ),
)
def test_project_viewer(orgpolicy, gcp_snapshot):
    run_viewer(orgpolicy, gcp_snapshot)
