"""Cloud SQL for PostgreSQL REGIONAL availability, TLS, client certificates, networks, flags."""

import pytest

from gcp_validation.services.databases.cloudsql_depth import run_connectivity

KIND = 'connectivity'
LIMITS = {'max_instances': 1, 'max_ssl_certs': 1, 'max_run_seconds': 4500}


@pytest.mark.gcp_scenario(
    id='cloudsql-postgres-connectivity', service='cloudsql', transport='rest-v1beta4',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/databases/cloudsql_depth.py',
        'gcp_validation/services/databases/cloudsql.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/databases/cloudsql/connectivity/conftest.py',
    ),
)
def test_cloudsql_connectivity(cloudsql_depth, gcp_snapshot):
    fixture, created = cloudsql_depth
    run_connectivity(fixture, gcp_snapshot, created)
