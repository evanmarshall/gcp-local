"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.vm_connectivity import run_scenario


@pytest.mark.gcp_scenario(
    id='vm-connectivity-nat-peering', service='networkmanagement', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable', 'backend': 'not-applicable'},
    contract_files=('gcp_validation/vm_connectivity.py', 'gcp_validation/networking.py', 'gcp_validation/live_rest.py',
                    'gcp_validation/live_paths.py', 'gcp_validation/sandbox.py',
                    'tests/conformance/vmconnectivity/conftest.py'),
)
def test_vmconnectivity(vmconnectivity, gcp_snapshot):
    fixture, created = vmconnectivity
    run_scenario(fixture, gcp_snapshot, created)
