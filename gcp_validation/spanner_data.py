"""Bounded Spanner data-plane recording over the v1 REST session methods.

It reuses the admin scenario's owned 100-processing-unit instance and cleanup,
adds one database, writes rows with a single-use commit and DML, reads them
back with read and executeSql, and checks that an ungranted caller is denied.
"""

from typing import Any

from .evidence import SnapshotCase
from .live_rest import Observations, Response
from .spanner import TABLE, InstanceFixture

DATABASE = 'gl_data'


def rows(response: Response) -> dict[str, Any]:
    """Select result rows and column names; values are proto3 JSON strings."""
    value = response.json()
    fields = value.get('metadata', {}).get('rowType', {}).get('fields', [])
    return {'http_status': response.status, 'columns': [field.get('name') for field in fields],
            'rows': value.get('rows', [])}


def write_rows(fixture: InstanceFixture, snapshot: SnapshotCase, path: str,
               observe: Observations) -> None:
    """Insert two rows with a single-use commit, then update one with DML."""
    commit = fixture.call('POST', path + ':commit', body={
        'singleUseTransaction': {'readWrite': {}},
        'mutations': [{'insert': {'table': 'Items', 'columns': ['Id', 'Name'],
                                  'values': [['a', 'alpha'], ['b', 'beta']]}}]})
    snapshot.match('mutations-committed', observe.select(commit, ()))
    snapshot.match('read-rows', rows(fixture.call('POST', path + ':read', body={
        'table': 'Items', 'columns': ['Id', 'Name'], 'keySet': {'all': True}})))
    begin = fixture.call('POST', path + ':beginTransaction', body={'options': {'readWrite': {}}})
    snapshot.match('transaction-begun', observe.select(begin, ()))
    transaction = begin.json().get('id', '')
    dml = fixture.call('POST', path + ':executeSql', body={
        'sql': "UPDATE Items SET Name = 'ALPHA' WHERE Id = 'a'",
        'transaction': {'id': transaction}, 'seqno': '1'})
    count = dml.json().get('stats', {}).get('rowCountExact')
    snapshot.match('dml-count', {'http_status': dml.status, 'rowCountExact': count})
    committed = fixture.call('POST', path + ':commit', body={'transactionId': transaction})
    snapshot.match('dml-committed', observe.select(committed, ()))


def run_scenario(fixture: InstanceFixture, snapshot: SnapshotCase, created: Response) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.instance: '<instance>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>'})
    fixture.finish(created)
    database = fixture.finish(fixture.call('POST', '/databases', body={
        'createStatement': f'CREATE DATABASE `{DATABASE}`', 'extraStatements': [TABLE]}))
    snapshot.match('database-ready',
                   {'http_status': database.status, 'done': database.json().get('done')})
    sessions = f'/databases/{DATABASE}/sessions'
    session = fixture.call('POST', sessions, body={})
    snapshot.match('session-created', observe.select(session, ()))
    path = sessions + '/' + session.json()['name'].split('/sessions/', 1)[1]

    denied = fixture.call('POST', sessions, actor='denied', body={})
    snapshot.match('denied-session', observe.error(denied))
    denied = fixture.call('POST', path + ':executeSql', actor='denied',
                          body={'sql': 'SELECT Id FROM Items'})
    snapshot.match('denied-query', observe.error(denied))

    write_rows(fixture, snapshot, path, observe)
    snapshot.match('query-rows', rows(fixture.call('POST', path + ':executeSql', body={
        'sql': 'SELECT Id, Name FROM Items ORDER BY Id'})))

    deleted = fixture.call('DELETE', path)
    snapshot.match('session-deleted', {'http_status': deleted.status, 'body': deleted.json()})
    dropped = fixture.call('DELETE', f'/databases/{DATABASE}')
    snapshot.match('database-dropped', {'http_status': dropped.status, 'body': dropped.json()})
