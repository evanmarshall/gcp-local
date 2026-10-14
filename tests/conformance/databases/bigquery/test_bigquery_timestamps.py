"""BigQuery reads TIMESTAMP strings without a zone as UTC: one query of literals, no table.

The fixture identity's standing roles/bigquery.admin runs the query; the owned dataset is
created and deleted only to hold the run's lock and manifest. Literal queries scan no bytes.
"""

import pytest

from gcp_validation.services.databases.bigquery import query_result

# Each column reads one way a string becomes a TIMESTAMP, or a TIMESTAMP a string. Under
# America/Los_Angeles every UTC-sensitive column would be 8 hours (or a day) off.
QUERY = """SELECT
  UNIX_SECONDS(TIMESTAMP '2020-01-01') AS literal_date,
  UNIX_SECONDS(TIMESTAMP '2020-01-01 12:00:00') AS literal_time,
  UNIX_SECONDS(CAST('2020-01-01 12:00:00' AS TIMESTAMP)) AS cast_time,
  UNIX_SECONDS(CAST('2020-01-01' AS TIMESTAMP)) AS cast_date,
  UNIX_SECONDS(TIMESTAMP('2020-01-01')) AS function_date,
  UNIX_SECONDS(TIMESTAMP('2020-01-01 12:00:00')) AS function_time,
  UNIX_SECONDS(TIMESTAMP_TRUNC(TIMESTAMP '2020-01-01 23:30:00', DAY)) AS trunc_day,
  DATE(TIMESTAMP '2020-01-01 23:30:00') AS date_before_midnight,
  DATE(TIMESTAMP '2020-01-02 00:30:00') AS date_after_midnight,
  CAST(TIMESTAMP '2020-01-01 12:00:00' AS STRING) AS as_string,
  TIMESTAMP '2020-01-01 12:00:00+00' = '2020-01-01 12:00:00' AS coerced_equal,
  UNIX_SECONDS(TIMESTAMP '2020-01-01 12:00:00 America/New_York') AS explicit_zone"""


@pytest.mark.gcp_scenario(
    id='bigquery-timestamp-zones', service='bigquery', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/databases/bigquery.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/databases/bigquery/conftest.py',
    ),
)
def test_bigquery_timestamps(bigquery, gcp_snapshot):
    fixture, created = bigquery
    if created.status != 200:
        raise AssertionError(f'Dataset create returned HTTP {created.status}')
    fixture.owned(created)
    fixture.manifest['queries'] += 1
    fixture.save()
    # A cached result omits totalBytesBilled and totalSlotMs, so every run computes it.
    response = fixture.client.request(
        'fixture', 'POST', f'{fixture.base}/queries', timeout=fixture.remaining(),
        body={'query': QUERY, 'useLegacySql': False, 'useQueryCache': False, 'location': 'US',
              'timeoutMs': 20000})
    # The result names no project, dataset, or job id: only field names, schema, and rows.
    gcp_snapshot.match('timestamp-zones', query_result(response))
