"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.services.security.cloudkms import run_scenario


@pytest.mark.gcp_scenario(
    id='cloudkms-symmetric-key-lifecycle', service='cloudkms', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable', 'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/security/cloudkms.py',
        'gcp_validation/services/security/refusals.py',
        'gcp_validation/errors.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/security/cloudkms/symmetric/conftest.py',
    ),
)
def test_cloudkms(cloudkms, gcp_snapshot):
    fixture, created = cloudkms
    run_scenario(fixture, gcp_snapshot, created)
