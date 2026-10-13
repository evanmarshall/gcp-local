"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.services.security.cloudkms_rotation import run_scenario


@pytest.mark.gcp_scenario(
    id="cloudkms-rotation-schedule",
    service="cloudkms",
    transport="rest-v1",
    versions={
        "sdk": "urllib;google-auth/2.56.2",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/services/security/cloudkms_rotation.py",
        "gcp_validation/services/security/cloudkms.py",
        "gcp_validation/services/security/refusals.py",
        "gcp_validation/live_rest.py",
        "gcp_validation/sandbox.py",
        "tests/conformance/security/cloudkms/rotation/conftest.py",
    ),
)
def test_cloudkms_rotation(cloudkms_rotation, gcp_snapshot):
    run_scenario(cloudkms_rotation, gcp_snapshot)
