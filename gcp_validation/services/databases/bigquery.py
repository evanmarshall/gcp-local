"""Bounded BigQuery dataset/table/row/query recording with explicit identities and owned cleanup."""

import argparse
import json
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = 'bigquery'
LIMITS = {'max_datasets': 1, 'max_tables': 1, 'max_rows': 4, 'max_queries': 4, 'max_run_seconds': 600}
LIVE_ENDPOINT = 'https://bigquery.googleapis.com'
SCHEMA = {'fields': [{'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
                     {'name': 'n', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                     {'name': 'ok', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}]}
ROWS = [{'insertId': 'r1', 'json': {'name': 'alpha', 'n': 1, 'ok': True}},
        {'insertId': 'r2', 'json': {'name': 'beta', 'n': 2, 'ok': False}}]


def load_scope(filename, sandbox, mode: str):
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class BigQueryClient(RestClient):
    """The shared transport, for BigQuery's /bigquery/v2 path family."""

    def request(self, actor, method, path, *, query=None, body=None, timeout=30):
        if actor not in self.emails:
            raise ValueError('Unknown scenario identity')
        if not path.startswith('/bigquery/v2/projects/' + self.target.project + '/'):
            raise ValueError('Select an explicit BigQuery path in the selected project')
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.token(actor)}
        if self.target.mode != 'local':
            headers['x-goog-user-project'] = self.target.project
        data = json.dumps(body).encode() if body is not None else (b'' if method == 'POST' else None)
        url = self.endpoint + path + ('?' + urlencode(query) if query else '')
        try:
            response = self.opener.open(Request(url, data=data, headers=headers, method=method), timeout=timeout)
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError):
            raise Inconclusive('Transport failed; no cloud fallback is available') from None
        with response:
            return Response(response.code, response.read(1 << 20))


class DatasetFixture(OwnedRun):
    """One labeled dataset holding one table; cleanup deletes the dataset and its contents."""

    def __init__(self, client, state_dir, *, manifest=None):
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('dataset') == 'gl_' + v['run_id'][:12])
        self.manifest.setdefault('dataset', 'gl_' + self.manifest['run_id'][:12])
        self.manifest.setdefault('queries', 0)
        self.dataset = self.manifest['dataset']
        self.base = f'/bigquery/v2/projects/{self.project}'
        self.labels = {'managed-by': 'gcp-local', 'gcp-local-run': self.manifest['run_id']}

    def call(self, method, suffix='', *, actor='fixture', body=None, query=None):
        return self.client.request(actor, method, f'{self.base}/datasets/{self.dataset}{suffix}',
                                   body=body, query=query, timeout=self.remaining())

    def query(self, sql, *, actor='fixture', dry_run=False):
        if self.manifest['queries'] >= LIMITS['max_queries']:
            raise ValueError('Query limit reached')
        self.manifest['queries'] += 1
        self.save()
        return self.client.request(actor, 'POST', f'{self.base}/queries', timeout=self.remaining(), body={
            'query': sql, 'useLegacySql': False, 'dryRun': dry_run, 'location': 'US', 'timeoutMs': 20000})

    def setup(self):
        self.acquire()
        self.client.verify_project()
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.client.request('fixture', 'POST', f'{self.base}/datasets', timeout=self.remaining(), body={
            'datasetReference': {'projectId': self.project, 'datasetId': self.dataset},
            'location': 'US', 'labels': self.labels})

    def owned(self, response):
        if response.json().get('labels') != self.labels:
            raise ValueError('Refusing BigQuery cleanup: dataset ownership does not match the manifest')

    def cleanup(self):
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            path = f'{self.base}/datasets/{self.dataset}'
            current = self.client.request('fixture', 'GET', path)
            if current.status == 200:
                self.owned(current)
                deleted = self.client.request('fixture', 'DELETE', path, query={'deleteContents': 'true'})
                if deleted.status not in {200, 204, 404}:
                    raise ValueError(f'Owned dataset delete returned HTTP {deleted.status}')
            elif current.status != 404:
                raise ValueError(f'Owned dataset read returned HTTP {current.status}')
            if self.client.request('fixture', 'GET', path).status != 404:
                raise ValueError('Owned dataset remains after deletion')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()


def rows_of(value):
    return [[cell.get('v') for cell in row.get('f', [])] for row in value.get('rows', [])]


def query_result(response):
    value = response.json()
    return {'http_status': response.status, 'fields': sorted(value), 'jobComplete': value.get('jobComplete'),
            'totalRows': value.get('totalRows'), 'schema': value.get('schema'), 'rows': rows_of(value),
            'job_reference_fields': sorted(value.get('jobReference', {}))}


def run_scenario(fixture, snapshot, created):
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.dataset: '<dataset>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>',
                            **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    if created.status != 200:
        raise AssertionError(f'Dataset create returned HTTP {created.status}')
    fixture.owned(created)
    snapshot.match('dataset-created', observe.select(created, ('id', 'datasetReference', 'labels', 'location')))
    snapshot.match('denied-dataset-read', observe.error(fixture.call('GET', actor='denied')))
    table = fixture.call('POST', '/tables', body={
        'tableReference': {'projectId': fixture.project, 'datasetId': fixture.dataset, 'tableId': 't1'},
        'schema': SCHEMA})
    snapshot.match('table-created', observe.select(table, ('id', 'schema', 'type')))
    inserted = fixture.call('POST', '/tables/t1/insertAll', body={'kind': 'bigquery#tableDataInsertAllRequest', 'rows': ROWS})
    snapshot.match('rows-inserted', observe.select(inserted, ('kind', 'insertErrors')))
    listed = fixture.call('GET', '/tables/t1/data')
    deadline = time.monotonic() + (60 if client.sandbox else 0)
    while len(rows_of(listed.json())) < len(ROWS) and time.monotonic() < deadline:
        time.sleep(5)  # Streamed rows can take a moment to become readable.
        listed = fixture.call('GET', '/tables/t1/data')
    # totalRows lags while streamed rows sit in the buffer, so only rows are compared.
    snapshot.match('rows-listed', {'http_status': listed.status, 'rows': sorted(rows_of(listed.json()))})
    sql = f'SELECT name, n FROM `{fixture.project}.{fixture.dataset}.t1` WHERE n = 1'
    snapshot.match('query', query_result(fixture.query(sql)))
    dry = fixture.query(sql, dry_run=True).json()
    snapshot.match('dry-run', {'fields': sorted(dry), 'jobComplete': dry.get('jobComplete'),
                               'bytes_processed_present': 'totalBytesProcessed' in dry})
    # Dataset IAM methods are allowlisted in GCP; access is granted per table.
    snapshot.match('dataset-iam-unavailable', observe.error(fixture.call('POST', ':getIamPolicy', body={})))
    policy = fixture.call('POST', '/tables/t1:getIamPolicy', body={})
    snapshot.match('table-policy-read', observe.select(policy, ('bindings',)))
    granted = fixture.call('POST', '/tables/t1:setIamPolicy', body={'policy': {
        'etag': policy.json().get('etag', ''), 'bindings': policy.json().get('bindings', []) + [
            {'role': 'roles/bigquery.dataViewer', 'members': ['serviceAccount:' + client.emails['allowed']]}]}})
    snapshot.match('viewer-granted', {'http_status': granted.status, 'viewer_bound': any(
        b['role'] == 'roles/bigquery.dataViewer' and 'serviceAccount:' + client.emails['allowed'] in b['members']
        for b in granted.json().get('bindings', []))})
    allowed = fixture.wait_until(lambda: fixture.call('GET', '/tables/t1/data', actor='allowed'), {403}, 180)
    snapshot.match('viewer-reads-rows', {'http_status': allowed.status, 'rows': sorted(rows_of(allowed.json()))})
    snapshot.match('viewer-cannot-insert', observe.error(fixture.call(
        'POST', '/tables/t1/insertAll', actor='allowed', body={'rows': [{'json': {'name': 'no'}}]})))
    snapshot.match('viewer-cannot-query', observe.error(fixture.query(sql, actor='allowed')))
    snapshot.match('delete-non-empty', observe.error(fixture.call('DELETE')))
    deleted = fixture.call('DELETE', query={'deleteContents': 'true'})
    snapshot.match('dataset-deleted', {'http_status': deleted.status, 'body_bytes': len(deleted.body)})
    snapshot.match('absent', observe.error(fixture.call('GET')))


def main(argv=None):
    parser = argparse.ArgumentParser(description='Delete only the owned dataset after an interrupted run')
    parser.add_argument('command', choices=['cleanup'])
    parser.add_argument('--profile', required=True)
    parser.add_argument('--scope', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--allow-live', action='store_true', required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox, 'cleanup')
    target = Target.select('record', project=sandbox.project, credentials_file=sandbox.identities['allowed'].credentials_file,
                           live_scope=sandbox.live_scope, allow_live=args.allow_live)
    client = BigQueryClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    DatasetFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                   manifest=args.manifest).cleanup()
    print('Owned BigQuery cleanup passed.')


if __name__ == '__main__':
    main()
