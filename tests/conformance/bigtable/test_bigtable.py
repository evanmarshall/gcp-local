"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.bigtable import run_scenario


@pytest.mark.gcp_scenario(
    id='bigtable-instance-table-admin', service='bigtable', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable', 'backend': 'not-applicable'},
    contract_files=('gcp_validation/bigtable.py', 'gcp_validation/live_rest.py', 'gcp_validation/live_paths.py',
                    'gcp_validation/sandbox.py', 'tests/conformance/bigtable/conftest.py'),
)
def test_bigtable(bigtable, gcp_snapshot):
    fixture, created = bigtable
    run_scenario(fixture, gcp_snapshot, created)
