"""BigQuery requests as dbt-bigquery 1.12 sends them.

Request shapes come from dbt-bigquery 1.12.0rc1 (google-cloud-bigquery 3.46.2)
run through a logging proxy: every statement is a jobs.insert whose
configuration carries jobTimeoutMs, labels, dryRun false, and an INTERACTIVE
Standard SQL query, followed by getQueryResults. dbt creates its dataset with
CREATE SCHEMA IF NOT EXISTS, lists routines with the dataset's tables, reads
the destination table of a CTAS and of a SELECT for their row counts, merges
incremental models from a subquery, and stages snapshots in a table that
expires through the expiration_timestamp option.
"""

import time
from pathlib import Path
from typing import Any

import pytest

from gcp_validation.evidence import Inconclusive, SnapshotCase
from gcp_validation.live_rest import LiveScope, Observations, Response
from gcp_validation.services.databases.bigquery import (
    LIVE_ENDPOINT,
    BigQueryClient,
    DatasetFixture,
    rows_of,
)

SERVICE = 'bigquery'
LIMITS = {'max_datasets': 1, 'max_tables': 3, 'max_rows': 10, 'max_queries': 12,
          'max_run_seconds': 600}
# dbt's fallback job_execution_timeout: one day.
JOB_TIMEOUT_MS = '86400000'


def load_scope(filename: str, sandbox: Any, mode: str) -> LiveScope:
    """Load the approved scope file for this scenario's limits."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class DbtFixture(DatasetFixture):
    """The shared owned dataset, with dbt's jobs.insert body and this scenario's query budget."""

    def job(self, name: str, statement: str, *, actor: str = 'fixture') -> Response:
        """Insert one query job as dbt does; {ds} names the owned dataset."""
        if self.manifest['queries'] >= LIMITS['max_queries']:
            raise ValueError('Query limit reached')
        self.manifest['queries'] += 1
        self.save()
        query = statement.replace('{ds}', f'`{self.project}`.`{self.dataset}`')
        body = {
            'jobReference': {'jobId': f'{self.dataset}_{name}', 'projectId': self.project,
                             'location': 'US'},
            'configuration': {
                'query': {'useLegacySql': False, 'priority': 'INTERACTIVE', 'query': query},
                'labels': {'dbt_invocation_id': self.manifest['run_id']},
                'dryRun': False,
                'jobTimeoutMs': JOB_TIMEOUT_MS,
            },
        }
        return self.client.request(actor, 'POST', f'{self.base}/jobs',
                                   query={'prettyPrint': 'false'}, body=body,
                                   timeout=self.remaining())

    def done(self, name: str) -> Response:
        """Wait for a job as the Python client does, then read the finished job."""
        results = f'{self.base}/queries/{self.dataset}_{name}'
        deadline = time.monotonic() + (60 if self.client.sandbox else 0)
        while True:
            waited = self.client.request('fixture', 'GET', results, timeout=self.remaining(),
                                         query={'location': 'US', 'timeoutMs': 10000})
            if waited.json().get('jobComplete') or time.monotonic() >= deadline:
                break
        if not waited.json().get('jobComplete'):
            raise Inconclusive('Query job did not finish within the deadline')
        return self.client.request('fixture', 'GET', f'{self.base}/jobs/{self.dataset}_{name}',
                                   query={'location': 'US'}, timeout=self.remaining())


def job_shape(observe: Observations, response: Response, *,
              finished: bool = True) -> dict[str, Any]:
    """Select what dbt reads from a job: its destination, and when finished its DDL statistics.

    A jobs.insert answer may be RUNNING or DONE, and a running job carries no
    DDL statistics yet, so an unfinished answer records only what both carry.
    """
    value = response.json()
    query = value.get('configuration', {}).get('query', {})
    statistics = value.get('statistics', {}).get('query', {})
    destination = query.get('destinationTable')
    shape = {
        'http_status': response.status,
        'configuration_fields': sorted(value.get('configuration', {})),
        'jobTimeoutMs': value.get('configuration', {}).get('jobTimeoutMs'),
        'query_fields': sorted(query),
        'writeDisposition': query.get('writeDisposition'),
        'statementType': statistics.get('statementType'),
        'destination': None if destination is None else {
            'fields': sorted(destination),
            'project': observe.value(destination.get('projectId')),
            # GCP names anonymous result datasets "_" plus a hash, and their tables "anon" plus one.
            'anonymous_dataset': destination.get('datasetId', '').startswith('_'),
            'anonymous_table': destination.get('tableId', '').startswith('anon'),
            'value': None if destination.get('datasetId', '').startswith('_')
            else observe.value(destination),
        },
    }
    if finished:
        shape.update({
            'state': value.get('status', {}).get('state'),
            'error': observe.value(value.get('status', {}).get('errorResult')),
            'ddlOperationPerformed': statistics.get('ddlOperationPerformed'),
            'ddlTargetTable': observe.value(statistics.get('ddlTargetTable')),
            'ddlTargetDataset': observe.value(statistics.get('ddlTargetDataset')),
        })
    return shape


def table_shape(observe: Observations, response: Response) -> dict[str, Any]:
    """Select a tables.get answer: its row count, type, and expiration."""
    value = response.json()
    if response.status != 200:
        return observe.error(response)
    shape = {'http_status': response.status, 'type': value.get('type'),
             'numRows': value.get('numRows'), 'schema': value.get('schema'),
             'expires': 'expirationTime' in value}
    if 'expirationTime' in value:
        lifetime = int(value['expirationTime']) - int(value['creationTime'])
        shape['lifetime_hours'] = round(lifetime / 3_600_000)
    return shape


def first_row(response: Response) -> list[Any]:
    """Return the first result row of a finished job."""
    rows = rows_of(response.json())
    return rows[0] if rows else []


def schema(fixture: DbtFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """CREATE SCHEMA IF NOT EXISTS on an absent and on an existing dataset."""
    deleted = fixture.call('DELETE', query={'deleteContents': 'true'})
    if deleted.status not in {200, 204}:
        raise AssertionError(f'Dataset delete returned HTTP {deleted.status}')
    statement = 'create schema if not exists {ds}'
    inserted = fixture.job('schema', statement)
    snapshot.match('create-schema', job_shape(observe, inserted, finished=False))
    snapshot.match('create-schema-done', job_shape(observe, fixture.done('schema')))
    created = fixture.call('GET')
    snapshot.match('schema-dataset', observe.select(created, ('location', 'datasetReference')))
    # Restore the labels cleanup checks ownership by; dbt sets none.
    restored = fixture.call('PATCH', body={'labels': fixture.labels})
    if restored.status != 200:
        raise AssertionError(f'Dataset label restore returned HTTP {restored.status}')
    fixture.owned(restored)
    fixture.job('schema_again', statement)
    snapshot.match('create-schema-exists', job_shape(observe, fixture.done('schema_again')))


def models(fixture: DbtFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """A table model, a data test, and the destination tables dbt reads row counts from."""
    snapshot.match('routines-empty', observe.select(fixture.call('GET', '/routines')))
    denied = fixture.call('GET', '/routines', actor='denied')
    snapshot.match('routines-denied', observe.error(denied))
    ctas = ('create or replace table {ds}.`orders` OPTIONS() as ('
            "select * from unnest([struct(1 as order_id, 'shipped' as status),"
            " (2, 'placed'), (3, 'shipped')]))")
    snapshot.match('ctas', job_shape(observe, fixture.job('ctas', ctas), finished=False))
    done = fixture.done('ctas')
    snapshot.match('ctas-done', job_shape(observe, done))
    destination = done.json()['configuration']['query'].get('destinationTable') or {}
    snapshot.match('ctas-destination', table_shape(observe, fixture.call(
        'GET', f'/tables/{destination.get("tableId", "orders")}')))
    # dbt's second run replaces every table model.
    fixture.job('ctas_again', ctas)
    snapshot.match('ctas-replace-done', job_shape(observe, fixture.done('ctas_again')))
    test = ('select count(*) as failures, count(*) != 0 as should_warn,'
            ' count(*) != 0 as should_error from (select order_id from {ds}.`orders`'
            ' where order_id is null) dbt_internal_test')
    snapshot.match('select', job_shape(observe, fixture.job('select', test), finished=False))
    done = fixture.done('select')
    snapshot.match('select-done', job_shape(observe, done))
    anonymous = done.json()['configuration']['query'].get('destinationTable')
    if anonymous is None:
        snapshot.match('select-destination', None)
        return
    path = (f'/bigquery/v2/projects/{anonymous["projectId"]}/datasets/{anonymous["datasetId"]}'
            f'/tables/{anonymous["tableId"]}')
    for actor in ('fixture', 'denied'):
        read = fixture.client.request(actor, 'GET', path, timeout=fixture.remaining())
        snapshot.match(f'select-destination-{actor}', table_shape(observe, read))


def incremental(fixture: DbtFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """dbt's merge strategy and its snapshot staging table."""
    merge = ('merge into {ds}.`orders` as DBT_INTERNAL_DEST using ('
             "select * from unnest([struct(2 as order_id, 'shipped' as status), (4, 'placed')])"
             ' where order_id >= (select min(order_id) from {ds}.`orders`)'
             ') as DBT_INTERNAL_SOURCE'
             ' on ((DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id))'
             ' when matched then update set `order_id` = DBT_INTERNAL_SOURCE.`order_id`,'
             ' `status` = DBT_INTERNAL_SOURCE.`status`'
             ' when not matched then insert (`order_id`, `status`) values (`order_id`, `status`)')
    fixture.job('merge', merge)
    # The local engine reports no row count for MERGE, so only the job is compared.
    snapshot.match('merge-done', job_shape(observe, fixture.done('merge')))
    fixture.job('merged_rows', 'select order_id, status from {ds}.`orders` order by order_id')
    fixture.done('merged_rows')
    results = f'{fixture.base}/queries/{fixture.dataset}_merged_rows'
    rows = fixture.client.request('fixture', 'GET', results, query={'location': 'US'},
                                  timeout=fixture.remaining())
    snapshot.match('merged-rows', {'http_status': rows.status, 'rows': rows_of(rows.json())})
    staging = ('create or replace table {ds}.`orders__dbt_tmp` OPTIONS('
               ' expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)'
               ' ) as (select order_id from {ds}.`orders`)')
    fixture.job('staging', staging)
    snapshot.match('staging-done', job_shape(observe, fixture.done('staging')))
    table = fixture.call('GET', '/tables/orders__dbt_tmp')
    snapshot.match('staging-table', table_shape(observe, table))
    created, expires = (int(table.json().get(key, 0)) for key in ('creationTime', 'expirationTime'))
    hours = (expires - created) / 3_600_000
    # The expression is evaluated when the statement runs, a moment before the table's creationTime.
    if not 11.9 < hours <= 12:
        raise AssertionError(f'Staging table expires {hours} hours after creation, not 12')


def run_scenario(fixture: DbtFixture, snapshot: SnapshotCase, created: Response) -> None:
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.dataset: '<dataset>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>',
                            **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    if created.status != 200:
        raise AssertionError(f'Dataset create returned HTTP {created.status}')
    fixture.owned(created)
    schema(fixture, snapshot, observe)
    models(fixture, snapshot, observe)
    incremental(fixture, snapshot, observe)


@pytest.fixture
def bigquery_dbt(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'bigquery-dbt-resources'
    else:
        scope = load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox, gcp_target.mode)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
        gcp_snapshot.context['scope'] = scope.evidence()
    client = BigQueryClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    fixture = DbtFixture(client, state)
    try:
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if fixture.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'dataset_absent': True})


@pytest.mark.gcp_scenario(
    id='bigquery-dbt-requests', service='bigquery', transport='rest-v1',
    versions={'sdk': 'dbt-bigquery/1.12.0rc1;google-cloud-bigquery/3.46.2 request shapes',
              'provider': 'not-applicable', 'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/databases/bigquery.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
    ),
)
def test_bigquery_dbt(bigquery_dbt, gcp_snapshot):
    fixture, created = bigquery_dbt
    run_scenario(fixture, gcp_snapshot, created)
