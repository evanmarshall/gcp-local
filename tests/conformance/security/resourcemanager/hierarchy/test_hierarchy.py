"""Read-only: the hierarchy as callers without organization roles see it."""

import pytest

from gcp_validation.services.security.hierarchy import run_scenario


@pytest.mark.gcp_scenario(
    id='resourcemanager-hierarchy-visibility', service='resourcemanager', transport='rest-v3',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/security/hierarchy.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/security/resourcemanager/hierarchy/conftest.py',
    ),
)
def test_hierarchy_visibility(hierarchy, gcp_snapshot):
    run_scenario(hierarchy, gcp_snapshot)
