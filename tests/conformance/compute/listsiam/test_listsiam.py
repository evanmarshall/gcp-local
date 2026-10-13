"""Compute list options, operations methods, and subnetwork IAM versions and conditions."""

import pytest

from gcp_validation.services.compute.lists_iam import run_scenario


@pytest.mark.gcp_scenario(
    id="compute-lists-operations-iam",
    service="compute",
    transport="rest-v1",
    versions={
        "sdk": "urllib;google-auth/2.56.2",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/services/compute/lists_iam.py",
        "gcp_validation/services/compute/vm_connectivity.py",
        "gcp_validation/services/compute/networking.py",
        "gcp_validation/live_rest.py",
        "gcp_validation/live_paths.py",
        "gcp_validation/sandbox.py",
        "tests/conformance/compute/listsiam/conftest.py",
    ),
)
def test_listsiam(listsiam, gcp_snapshot):
    fixture, created = listsiam
    run_scenario(fixture, gcp_snapshot, created)
