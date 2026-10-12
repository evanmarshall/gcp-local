"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.memorystore import run_scenario


@pytest.mark.gcp_scenario(
    id='memorystore-redis-instance-admin', service='redis', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=('gcp_validation/memorystore.py', 'gcp_validation/networking.py', 'gcp_validation/live_paths.py', 'gcp_validation/live_rest.py',
                    'gcp_validation/sandbox.py', 'tests/conformance/memorystore/conftest.py'),
)
def test_memorystore(memorystore, gcp_snapshot):
    fixture, created = memorystore
    run_scenario(fixture, gcp_snapshot, created)
