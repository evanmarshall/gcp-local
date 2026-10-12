"""Not recorded: this scenario stays unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.services.databases.spanner_data import run_scenario


@pytest.mark.gcp_scenario(
    id='spanner-data-plane-rest', service='spanner', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/databases/spanner_data.py',
        'gcp_validation/services/databases/spanner.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/databases/spanner/data/conftest.py',
    ),
)
def test_spanner_data(spanner_data, gcp_snapshot):
    fixture, created = spanner_data
    run_scenario(fixture, gcp_snapshot, created)
