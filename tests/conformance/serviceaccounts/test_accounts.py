"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.serviceaccounts import run_scenario


@pytest.mark.gcp_scenario(
    id='serviceaccount-lifecycle-authorization', service='iam', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable', 'backend': 'not-applicable'},
    contract_files=('gcp_validation/serviceaccounts.py', 'gcp_validation/live_rest.py',
                    'gcp_validation/sandbox.py', 'tests/conformance/serviceaccounts/conftest.py'),
)
def test_lifecycle_authorization(serviceaccounts, gcp_snapshot):
    fixture, created = serviceaccounts
    run_scenario(fixture, gcp_snapshot, created)
