"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.services.databases.bigquery import run_scenario


@pytest.mark.gcp_scenario(
    id='bigquery-dataset-rows-query', service='bigquery', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable', 'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/databases/bigquery.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/databases/bigquery/conftest.py',
    ),
)
def test_bigquery(bigquery, gcp_snapshot):
    fixture, created = bigquery
    run_scenario(fixture, gcp_snapshot, created)
