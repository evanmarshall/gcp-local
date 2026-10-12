"""Bounded BigQuery GoogleSQL recording: DDL, DML, joins, parameters, and per-table reads.

Every table comes from DDL and DML jobs, not streaming inserts, so DML runs
without streaming-buffer limits and reads need no propagation wait. The fixture
identity holds roles/bigquery.dataOwner and roles/bigquery.jobUser on the
project; the allowed identity holds roles/bigquery.jobUser on the project and,
once the scenario grants it, roles/bigquery.dataViewer on one table only.
"""

from pathlib import Path
from typing import Any

import pytest

from gcp_validation.bigquery import LIVE_ENDPOINT, BigQueryClient, DatasetFixture, rows_of
from gcp_validation.evidence import SnapshotCase
from gcp_validation.live_rest import LiveScope, Observations, Response, RestClient

SERVICE = 'bigquery'
LIMITS = {'max_datasets': 1, 'max_tables': 5, 'max_rows': 10, 'max_queries': 30,
          'max_run_seconds': 600}


def load_scope(filename: str, sandbox: Any) -> LiveScope:
    """Load the approved scope file for this scenario's limits."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox)


class SQLFixture(DatasetFixture):
    """The shared owned dataset, with this scenario's query budget and request options."""

    def sql(self, statement: str, *, actor: str = 'fixture', **options: Any) -> Response:
        """Send one jobs.query, counted against this scenario's query limit."""
        if self.manifest['queries'] >= LIMITS['max_queries']:
            raise ValueError('Query limit reached')
        self.manifest['queries'] += 1
        self.save()
        # Statements name the owned dataset as {ds}.
        body = {'query': statement.replace('{ds}', self.dataset), 'useLegacySql': False,
                'location': 'US', 'timeoutMs': 30000, **options}
        return self.client.request(actor, 'POST', f'{self.base}/queries',
                                   timeout=self.remaining(), body=body)


def result(response: Response) -> dict[str, Any]:
    """Select a jobs.query result: its keys, statement type, schema, rows, and DML count."""
    value = response.json()
    return {'http_status': response.status, 'fields': sorted(value),
            'statementType': value.get('statementType'), 'jobComplete': value.get('jobComplete'),
            'totalRows': value.get('totalRows'),
            'numDmlAffectedRows': value.get('numDmlAffectedRows'),
            'schema': value.get('schema'), 'rows': rows_of(value)}


def scalar(name: str, kind: str, value: str) -> dict[str, Any]:
    """One scalar query parameter; name is empty for a positional one."""
    parameter = {'parameterType': {'type': kind}, 'parameterValue': {'value': value}}
    if name:
        parameter['name'] = name
    return parameter


def build(fixture: SQLFixture, snapshot: SnapshotCase) -> None:
    """Create the tables with DDL and fill them with DML."""
    snapshot.match('create-table', result(fixture.sql(
        'CREATE TABLE {ds}.people (id INT64, name STRING, active BOOL)')))
    snapshot.match('insert', result(fixture.sql(
        "INSERT INTO {ds}.people (id, name, active) VALUES (1, 'ada', TRUE), (2, 'grace', FALSE),"
        " (3, 'linus', TRUE)")))
    snapshot.match('create-table-as-select', result(fixture.sql(
        'CREATE TABLE {ds}.orders AS SELECT * FROM UNNEST([STRUCT(1 AS person, 10.5 AS total),'
        ' (1, 4.5), (3, 7.0)])')))
    snapshot.match('create-updates', result(fixture.sql(
        "CREATE TABLE {ds}.updates AS SELECT 1 AS id, 'ada!' AS name UNION ALL SELECT 4, 'ken'")))


def read(fixture: SQLFixture, snapshot: SnapshotCase) -> None:
    """Joins, aggregates, a CTE with a subquery, functions, parameters, and a script."""
    snapshot.match('join-aggregate', result(fixture.sql(
        'SELECT p.name, COUNT(*) AS n, SUM(o.total) AS total FROM {ds}.people p'
        ' JOIN {ds}.orders o ON o.person = p.id GROUP BY p.name HAVING SUM(o.total) > 1'
        ' ORDER BY p.name')))
    snapshot.match('cte-subquery-functions', result(fixture.sql(
        'WITH big AS (SELECT person FROM {ds}.orders WHERE total > 5)'
        ' SELECT UPPER(name) AS upper_name, LENGTH(name) AS len FROM {ds}.people'
        ' WHERE id IN (SELECT person FROM big) ORDER BY id LIMIT 5')))
    snapshot.match('named-parameters', result(fixture.sql(
        'SELECT name FROM {ds}.people WHERE id = @id AND active = @active', parameterMode='NAMED',
        queryParameters=[scalar('id', 'INT64', '3'), scalar('active', 'BOOL', 'true')])))
    snapshot.match('positional-parameters', result(fixture.sql(
        'SELECT name FROM {ds}.people WHERE name = ? OR id = ? ORDER BY id',
        parameterMode='POSITIONAL',
        queryParameters=[scalar('', 'STRING', 'ada'), scalar('', 'INT64', '2')])))
    snapshot.match('default-dataset', result(fixture.sql(
        'SELECT COUNT(*) AS n FROM people',
        defaultDataset={'projectId': fixture.project, 'datasetId': fixture.dataset})))
    snapshot.match('script', result(fixture.sql(
        'DECLARE x INT64 DEFAULT 1; SET x = x + 1; SELECT name FROM {ds}.people WHERE id = x')))


def change(fixture: SQLFixture, snapshot: SnapshotCase) -> None:
    """UPDATE, DELETE, MERGE, and a view over the changed rows."""
    snapshot.match('update', result(fixture.sql(
        'UPDATE {ds}.people SET active = NOT active WHERE id = 2')))
    snapshot.match('delete', result(fixture.sql('DELETE FROM {ds}.people WHERE id = 3')))
    merged = fixture.sql(
        'MERGE {ds}.people T USING {ds}.updates S ON T.id = S.id'
        ' WHEN MATCHED THEN UPDATE SET name = S.name'
        ' WHEN NOT MATCHED THEN INSERT (id, name, active) VALUES (S.id, S.name, FALSE)')
    # The local engine reports no row count for MERGE, so only its outcome is compared.
    snapshot.match('merge', {'http_status': merged.status,
                             'statementType': merged.json().get('statementType')})
    snapshot.match('rows-after-dml', result(fixture.sql(
        'SELECT id, name, active FROM {ds}.people ORDER BY id')))
    snapshot.match('create-view', result(fixture.sql(
        'CREATE VIEW {ds}.active_people AS SELECT name FROM {ds}.people WHERE active')))
    snapshot.match('query-view', result(fixture.sql(
        'SELECT name FROM {ds}.active_people ORDER BY name')))


def authorize(fixture: SQLFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """A table-level reader can read its table, not a second table, and cannot write."""
    policy = fixture.call('POST', '/tables/people:getIamPolicy', body={})
    granted = fixture.call('POST', '/tables/people:setIamPolicy', body={'policy': {
        'etag': policy.json().get('etag', ''), 'bindings': [
            *policy.json().get('bindings', []),
            {'role': 'roles/bigquery.dataViewer',
             'members': ['serviceAccount:' + fixture.client.emails['allowed']]}]}})
    snapshot.match('reader-granted', {'http_status': granted.status})
    allowed = fixture.wait_until(lambda: fixture.sql(
        'SELECT name FROM {ds}.people ORDER BY id', actor='allowed'), {403}, 180)
    snapshot.match('reader-reads-its-table', result(allowed))
    snapshot.match('reader-denied-second-table', observe.error(fixture.sql(
        'SELECT p.name, o.total FROM {ds}.people p JOIN {ds}.orders o ON o.person = p.id',
        actor='allowed')))
    snapshot.match('reader-denied-dml', observe.error(fixture.sql(
        'DELETE FROM {ds}.people WHERE TRUE', actor='allowed')))
    snapshot.match('denied-identity-query', observe.error(fixture.sql(
        'SELECT name FROM {ds}.people', actor='denied')))


def run_scenario(fixture: SQLFixture, snapshot: SnapshotCase, created: Response) -> None:
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.dataset: '<dataset>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>',
                            **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    if created.status != 200:
        raise AssertionError(f'Dataset create returned HTTP {created.status}')
    fixture.owned(created)
    build(fixture, snapshot)
    table = fixture.call('GET', '/tables/people')
    snapshot.match('ddl-table', observe.select(table, ('schema', 'type')))
    read(fixture, snapshot)
    change(fixture, snapshot)
    listed = fixture.call('GET', '/tables/people/data')
    # Row order is unspecified without ORDER BY, so the rows are compared as a set.
    snapshot.match('tabledata', {'http_status': listed.status,
                                 'rows': sorted(rows_of(listed.json()))})
    authorize(fixture, snapshot, observe)
    snapshot.match('missing-table', observe.error(fixture.sql('SELECT 1 FROM {ds}.absent')))
    snapshot.match('unknown-column', observe.error(fixture.sql('SELECT nope FROM {ds}.people')))
    snapshot.match('drop-view', result(fixture.sql('DROP VIEW {ds}.active_people')))
    snapshot.match('drop-table', result(fixture.sql('DROP TABLE {ds}.updates')))
    snapshot.match('dropped-table', observe.error(fixture.call('GET', '/tables/updates')))


JOB_USER = 'roles/bigquery.jobUser'


def local_job_user(target: Any, *, grant: bool) -> None:
    """Grant or revoke the allowed identity's project-level job user role locally.

    A live run needs the operator to grant it for the run's duration, as for the
    fixture's roles; the local run stands in for that grant through the
    emulator's project IAM and revokes it after, so other scenarios that expect
    the allowed identity to lack it still see that.
    """
    client = RestClient(target, 'https://cloudresourcemanager.googleapis.com')
    path = f'/v1/projects/{target.project}'
    policy = client.request('fixture', 'POST', path + ':getIamPolicy', body={}).json()
    member = 'serviceAccount:' + client.emails['allowed']
    current = policy.get('bindings', [])
    bindings = [b for b in current if b.get('role') != JOB_USER]
    members = next((b['members'] for b in current if b.get('role') == JOB_USER), [])
    members = [m for m in members if m != member] + ([member] if grant else [])
    if members:
        bindings.append({'role': JOB_USER, 'members': members})
    updated = client.request('fixture', 'POST', path + ':setIamPolicy', body={'policy': {
        'etag': policy.get('etag', ''), 'bindings': bindings}})
    if updated.status != 200:
        raise AssertionError(f'Local job user grant returned HTTP {updated.status}')


@pytest.fixture
def bigquery_sql(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'bigquery-sql-resources'
    else:
        scope = load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
        gcp_snapshot.context['scope'] = scope.evidence()
    client = BigQueryClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    fixture = SQLFixture(client, state)
    try:
        if gcp_target.mode == 'local':
            local_job_user(gcp_target, grant=True)
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if gcp_target.mode == 'local':
            local_job_user(gcp_target, grant=False)
        if fixture.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'dataset_absent': True})


@pytest.mark.gcp_scenario(
    id='bigquery-sql-engine', service='bigquery', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=('gcp_validation/bigquery.py', 'gcp_validation/live_rest.py',
                    'gcp_validation/sandbox.py'),
)
def test_bigquery_sql(bigquery_sql, gcp_snapshot):
    fixture, created = bigquery_sql
    run_scenario(fixture, gcp_snapshot, created)
