"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.services.databases.cloudsql import run_scenario


@pytest.mark.gcp_scenario(
    id='cloudsql-postgres-instance-admin', service='cloudsql', transport='rest-v1beta4',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/databases/cloudsql.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/databases/cloudsql/conftest.py',
    ),
)
def test_cloudsql(cloudsql, gcp_snapshot):
    fixture, created = cloudsql
    run_scenario(fixture, gcp_snapshot, created)
