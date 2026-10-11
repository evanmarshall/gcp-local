"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.projectiam import run_scenario


@pytest.mark.gcp_scenario(
    id='project-iam-policy-binding', service='resourcemanager', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable', 'backend': 'not-applicable'},
    contract_files=('gcp_validation/projectiam.py', 'gcp_validation/live_rest.py',
                    'gcp_validation/sandbox.py', 'tests/conformance/projectiam/conftest.py'),
)
def test_policy_binding(projectiam, gcp_snapshot):
    fixture, initial = projectiam
    run_scenario(fixture, gcp_snapshot, initial)
