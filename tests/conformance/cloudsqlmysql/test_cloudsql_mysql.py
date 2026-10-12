"""Cloud SQL for MySQL admin scenario: instance, databases with charset and collation, a user."""

import pytest

from gcp_validation.cloudsql_mysql import run_scenario


@pytest.mark.gcp_scenario(
    id='cloudsql-mysql-instance-admin', service='cloudsql', transport='rest-v1beta4',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=('gcp_validation/cloudsql_mysql.py', 'gcp_validation/cloudsql.py',
                    'gcp_validation/live_rest.py', 'gcp_validation/sandbox.py',
                    'tests/conformance/cloudsqlmysql/conftest.py'),
)
def test_cloudsql_mysql(cloudsql_mysql, gcp_snapshot):
    fixture, created = cloudsql_mysql
    run_scenario(fixture, gcp_snapshot, created)
