"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.cloudkms_asymmetric import run_scenario


@pytest.mark.gcp_scenario(
    id="cloudkms-asymmetric-mac-keys",
    service="cloudkms",
    transport="rest-v1",
    versions={
        "sdk": "urllib;google-auth/2.56.2",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/cloudkms_asymmetric.py",
        "gcp_validation/cloudkms.py",
        "gcp_validation/live_rest.py",
        "gcp_validation/sandbox.py",
        "tests/conformance/cloudkmsasymmetric/conftest.py",
    ),
)
def test_cloudkms_asymmetric(cloudkms_asymmetric, gcp_snapshot):
    fixture, created = cloudkms_asymmetric
    run_scenario(fixture, gcp_snapshot, created)
