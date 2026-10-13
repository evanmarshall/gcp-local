"""Bounded Spanner instance/database admin recording.

The instance bills while it exists, so the scenario uses the smallest regional size and
deletes it at the end.
"""

import argparse
import sys
import time
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.errors import detailed
from gcp_validation.evidence import Inconclusive, SnapshotCase, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = 'spanner'
LIMITS = {'max_instances': 1, 'max_processing_units': 100, 'max_databases': 2,
          'max_run_seconds': 900}
LIVE_ENDPOINT = 'https://spanner.googleapis.com'
CONFIG = 'regional-us-central1'
TABLE = 'CREATE TABLE Items (Id STRING(36) NOT NULL, Name STRING(64)) PRIMARY KEY (Id)'
COLUMN = 'ALTER TABLE Items ADD COLUMN Note STRING(64)'
EXTRA = 'CREATE TABLE Extra (Id STRING(36) NOT NULL) PRIMARY KEY (Id)'
HTTP_ERROR = 400


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the separately approved live scope for this scenario."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class InstanceFixture(OwnedRun):
    """One labeled 100-processing-unit instance; cleanup deletes it with its database."""

    def __init__(self, client: RestClient, state_dir: Path, *, manifest: str | None = None) -> None:
        """Bind the run manifest, the instance name, and its ownership labels."""
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('instance') == 'gl-' + v['run_id'][:12])
        self.manifest.setdefault('instance', 'gl-' + self.manifest['run_id'][:12])
        self.instance = self.manifest['instance']
        self.base = f'/v1/projects/{self.project}/instances'
        self.labels = {'managed-by': 'gcp-local', 'gcp-local-run': self.manifest['run_id']}

    def call(self, method: str, suffix: str = '', *, actor: str = 'fixture',
             body: dict | None = None, query: dict | None = None) -> Response:
        """Send one request to the run's instance or a resource under it."""
        return self.client.request(actor, method, f'{self.base}/{self.instance}{suffix}',
                                   body=body, query=query, timeout=self.remaining())

    def create_instance(self) -> Response:
        """Create the run's instance at the smallest size."""
        return self.client.request('fixture', 'POST', self.base, timeout=self.remaining(), body={
            'instanceId': self.instance, 'instance': {
                'config': f'projects/{self.project}/instanceConfigs/{CONFIG}',
                'displayName': 'gcp-local conformance',
                'processingUnits': LIMITS['max_processing_units'], 'labels': self.labels}})

    def finish(self, response: Response) -> Response:
        """Poll a long-running operation to completion; return the final operation."""
        operation = response.json()
        while response.status == 200 and not operation.get('done'):
            time.sleep(3 if self.client.sandbox else 0)
            name = operation['name'].split('/', 2)[2]  # drop "projects/<project>"
            response = self.client.request('fixture', 'GET', f'/v1/projects/{self.project}/{name}',
                                           timeout=self.remaining())
            operation = response.json()
        return response

    def setup(self) -> Response:
        """Create the instance after writing the manifest; refuse a sandbox that holds one."""
        self.acquire()
        self.client.verify_project()
        listing = self.client.request('fixture', 'GET', self.base)
        if listing.status != 200:
            raise Inconclusive(f'Instance listing returned HTTP {listing.status}')
        if listing.json().get('instances'):
            raise Inconclusive(
                'Spanner sandbox already holds an instance; refusing to add billable capacity')
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.create_instance()

    def cleanup(self) -> None:
        """Delete only the owned instance, with its databases, and confirm it is gone."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            path = f'{self.base}/{self.instance}'
            current = self.client.request('fixture', 'GET', path)
            if current.status == 200:
                if current.json().get('labels') != self.labels:
                    raise ValueError(
                        'Refusing Spanner cleanup: instance ownership does not match the manifest')
                deleted = self.client.request('fixture', 'DELETE', path)
                if deleted.status not in {200, 404}:
                    raise ValueError(f'Owned instance delete returned HTTP {deleted.status}')
            elif current.status != 404:
                raise ValueError(f'Owned instance read returned HTTP {current.status}')
            if self.client.request('fixture', 'GET', path).status != 404:
                raise ValueError('Owned instance remains after deletion')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()


def operation(response: Response) -> dict:
    """Describe an operation's shape and outcome without its name or timestamps."""
    value = response.json()
    result = value.get('response', {})
    return {'http_status': response.status, 'fields': sorted(value), 'done': value.get('done'),
            'metadata_type': value.get('metadata', {}).get('@type'),
            'response_type': result.get('@type'),
            'response_fields': sorted(result), 'error': value.get('error', {}).get('code')}


def run_scenario(fixture: InstanceFixture, snapshot: SnapshotCase, created: Response) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({
        fixture.instance: '<instance>', fixture.manifest['run_id']: '<run>',
        fixture.project: '<project>', client.number: '<project-number>',
        **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    snapshot.match('instance-operation', operation(fixture.finish(created)))
    instance = fixture.call('GET')
    snapshot.match('instance-read', observe.select(instance, (
        'name', 'config', 'processingUnits', 'state', 'labels', 'displayName')))
    snapshot.match('denied-read', detailed(observe, fixture.call('GET', actor='denied')))
    policy = fixture.call('POST', ':getIamPolicy', body={})
    snapshot.match('policy-read', observe.select(policy, ('bindings',)))
    viewer = {'role': 'roles/spanner.viewer',
              'members': ['serviceAccount:' + client.emails['allowed']]}
    granted = fixture.call('POST', ':setIamPolicy', body={
        'policy': {'etag': policy.json().get('etag', ''), 'bindings': [viewer]}})
    snapshot.match('viewer-granted', observe.select(granted, ('bindings',)))
    allowed = fixture.wait_until(lambda: fixture.call('GET', actor='allowed'), {403}, 180)
    snapshot.match('viewer-read', {'http_status': allowed.status})
    snapshot.match('viewer-cannot-create-database', detailed(observe, fixture.call(
        'POST', '/databases', actor='allowed',
        body={'createStatement': 'CREATE DATABASE `denied`'})))
    snapshot.match('unknown-field', detailed(observe, fixture.call(
        'POST', '/databases',
        body={'createStatement': 'CREATE DATABASE `refused`', 'unknownField': 1})))
    database = fixture.finish(fixture.call('POST', '/databases', body={
        'createStatement': 'CREATE DATABASE `gl_db`', 'extraStatements': [TABLE]}))
    snapshot.match('database-operation', operation(database))
    read = fixture.call('GET', '/databases/gl_db')
    snapshot.match('database-read', observe.select(read, ('name', 'state', 'databaseDialect')))
    ddl = fixture.call('GET', '/databases/gl_db/ddl')
    snapshot.match('ddl', observe.select(ddl, ('statements',)))
    altered = fixture.finish(fixture.call('PATCH', '/databases/gl_db/ddl',
                                          body={'statements': [COLUMN]}))
    snapshot.match('ddl-update-operation', operation(altered))
    ddl = fixture.call('GET', '/databases/gl_db/ddl')
    snapshot.match('ddl-after-update', observe.select(ddl, ('statements',)))
    observe_lists(fixture, snapshot, observe)
    conditional_grant(fixture, snapshot, observe, viewer)
    snapshot.match('duplicate-instance', observe.error(fixture.create_instance()))
    dropped = fixture.call('DELETE', '/databases/gl_db')
    snapshot.match('database-dropped', {'http_status': dropped.status, 'body': dropped.json()})
    snapshot.match('database-absent', observe.error(fixture.call('GET', '/databases/gl_db')))
    deleted = fixture.call('DELETE')
    snapshot.match('instance-deleted', {'http_status': deleted.status, 'body': deleted.json()})
    snapshot.match('instance-absent', observe.error(fixture.call('GET')))


def observe_lists(fixture: InstanceFixture, snapshot: SnapshotCase,
                  observe: Observations) -> None:
    """Observe database list order, paging, page refusals, and the empty backups list.

    A second database is created that sorts first. The instance is run-owned, so every
    listed database is the run's.
    """
    snapshot.match('second-database-operation', operation(fixture.finish(fixture.call(
        'POST', '/databases', body={'createStatement': 'CREATE DATABASE `gl_ab`'}))))
    listed = fixture.call('GET', '/databases')
    snapshot.match('database-list', {
        'http_status': listed.status, 'fields': sorted(listed.json()),
        'names': [observe.text(item.get('name', ''))
                  for item in listed.json().get('databases', [])]})
    first = fixture.call('GET', '/databases', query={'pageSize': '1'})
    token = first.json().get('nextPageToken', '')
    second = fixture.call('GET', '/databases', query={'pageSize': '1', 'pageToken': token})
    snapshot.match('database-list-paged', {
        'first_status': first.status, 'first_count': len(first.json().get('databases', [])),
        'first_has_next': bool(token), 'second_status': second.status,
        'second_count': len(second.json().get('databases', [])),
        'second_has_next': bool(second.json().get('nextPageToken'))})
    snapshot.match('list-invalid-token', detailed(observe, fixture.call(
        'GET', '/databases', query={'pageToken': 'not-a-page-token'})))
    negative = fixture.call('GET', '/databases', query={'pageSize': '-1'})
    snapshot.match('list-negative-size', detailed(observe, negative)
                   if negative.status >= HTTP_ERROR else {
                       'http_status': negative.status,
                       'count': len(negative.json().get('databases', [])),
                       'has_next': bool(negative.json().get('nextPageToken'))})
    backups = fixture.call('GET', '/backups')
    snapshot.match('backup-list', {'http_status': backups.status, 'body': backups.json()})


def conditional_grant(fixture: InstanceFixture, snapshot: SnapshotCase,
                      observe: Observations, viewer: dict) -> None:
    """Grant databaseAdmin on the instance under a condition only gl_db satisfies.

    The condition reads resource.type, .name, and .service; the allowed caller may then
    change gl_db's schema but not gl_ab's.
    """
    database = f'projects/{fixture.project}/instances/{fixture.instance}/databases/gl_db'
    condition = {'title': 'gcp-local-conformance', 'expression': (
        'resource.type == "spanner.googleapis.com/Database" && '
        f'resource.name == "{database}" && resource.service == "spanner.googleapis.com"')}
    current = fixture.call('POST', ':getIamPolicy',
                           body={'options': {'requestedPolicyVersion': 3}})
    admin = {'role': 'roles/spanner.databaseAdmin', 'members': viewer['members'],
             'condition': condition}
    written = fixture.call('POST', ':setIamPolicy', body={'policy': {
        'version': 3, 'etag': current.json().get('etag', ''), 'bindings': [viewer, admin]}})
    bindings = written.json().get('bindings', [])
    snapshot.match('conditional-granted', {
        'http_status': written.status, 'version': written.json().get('version'),
        'roles': sorted(b['role'] for b in bindings),
        'conditions': sorted(sorted(b.get('condition', {})) for b in bindings)})
    body = {'statements': [EXTRA]}
    matched = fixture.wait_until(
        lambda: fixture.call('PATCH', '/databases/gl_db/ddl', actor='allowed', body=body),
        {403}, 240)
    snapshot.match('condition-matched', operation(fixture.finish(matched)))
    snapshot.match('condition-not-matched', detailed(observe, fixture.call(
        'PATCH', '/databases/gl_ab/ddl', actor='allowed', body=body)))


def main(argv: list[str] | None = None) -> None:
    """Delete only the owned instance after an interrupted run."""
    parser = argparse.ArgumentParser(
        description='Delete only the owned instance after an interrupted run')
    parser.add_argument('command', choices=['cleanup'])
    parser.add_argument('--profile', required=True)
    parser.add_argument('--scope', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--allow-live', action='store_true', required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox, 'cleanup')
    target = Target.select('record', project=sandbox.project,
                           credentials_file=sandbox.identities['allowed'].credentials_file,
                           live_scope=sandbox.live_scope, allow_live=args.allow_live)
    client = RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    InstanceFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                    manifest=args.manifest).cleanup()
    sys.stdout.write('Owned Spanner cleanup passed.\n')


if __name__ == '__main__':
    main()
