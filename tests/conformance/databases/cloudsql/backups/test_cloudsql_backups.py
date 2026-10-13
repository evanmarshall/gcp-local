"""Cloud SQL for PostgreSQL backups: configuration, an on-demand run, restores, deletion."""

import pytest

from gcp_validation.services.databases.cloudsql_depth import run_backups

KIND = 'backups'
LIMITS = {'max_instances': 2, 'max_backup_runs': 1, 'max_run_seconds': 5400}


@pytest.mark.gcp_scenario(
    id='cloudsql-postgres-backups', service='cloudsql', transport='rest-v1beta4',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/databases/cloudsql_depth.py',
        'gcp_validation/services/databases/cloudsql.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/databases/cloudsql/backups/conftest.py',
    ),
)
def test_cloudsql_backups(cloudsql_depth, gcp_snapshot):
    fixture, created = cloudsql_depth
    run_backups(fixture, gcp_snapshot, created)
