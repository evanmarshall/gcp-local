"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.spanner import run_scenario


@pytest.mark.gcp_scenario(
    id='spanner-instance-database-admin', service='spanner', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable', 'backend': 'not-applicable'},
    contract_files=('gcp_validation/spanner.py', 'gcp_validation/live_rest.py',
                    'gcp_validation/sandbox.py', 'tests/conformance/spanner/conftest.py'),
)
def test_spanner(spanner, gcp_snapshot):
    fixture, created = spanner
    run_scenario(fixture, gcp_snapshot, created)
