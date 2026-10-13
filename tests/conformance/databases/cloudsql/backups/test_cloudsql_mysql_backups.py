"""Cloud SQL for MySQL backups with binary logging, database flags, and an in-place restore."""

import pytest

from gcp_validation.services.databases.cloudsql_depth import run_mysql

KIND = 'mysql'
LIMITS = {'max_instances': 1, 'max_backup_runs': 1, 'max_run_seconds': 4500}


@pytest.mark.gcp_scenario(
    id='cloudsql-mysql-backups-flags', service='cloudsql', transport='rest-v1beta4',
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
def test_cloudsql_mysql_backups(cloudsql_depth, gcp_snapshot):
    fixture, created = cloudsql_depth
    run_mysql(fixture, gcp_snapshot, created)
