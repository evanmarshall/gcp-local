"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.secretmanager import run_scenario


@pytest.mark.gcp_scenario(
    id='secretmanager-access-lifecycle', service='secretmanager', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable', 'backend': 'not-applicable'},
    contract_files=('gcp_validation/secretmanager.py', 'gcp_validation/sandbox.py',
                    'tests/conformance/secretmanager/conftest.py'),
)
def test_access_lifecycle(secretmanager, gcp_snapshot):
    fixture, created = secretmanager
    run_scenario(fixture, gcp_snapshot, created)
