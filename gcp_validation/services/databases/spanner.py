"""Bounded Spanner instance/database admin recording. The instance bills while it exists,
so the scenario uses the smallest regional size and deletes it at the end."""

import argparse
import time
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = 'spanner'
LIMITS = {'max_instances': 1, 'max_processing_units': 100, 'max_databases': 1, 'max_run_seconds': 900}
LIVE_ENDPOINT = 'https://spanner.googleapis.com'
CONFIG = 'regional-us-central1'
TABLE = 'CREATE TABLE Items (Id STRING(36) NOT NULL, Name STRING(64)) PRIMARY KEY (Id)'
COLUMN = 'ALTER TABLE Items ADD COLUMN Note STRING(64)'


def load_scope(filename, sandbox):
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox)


class InstanceFixture(OwnedRun):
    """One labeled 100-processing-unit instance; cleanup deletes it with its database."""

    def __init__(self, client, state_dir, *, manifest=None):
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('instance') == 'gl-' + v['run_id'][:12])
        self.manifest.setdefault('instance', 'gl-' + self.manifest['run_id'][:12])
        self.instance = self.manifest['instance']
        self.base = f'/v1/projects/{self.project}/instances'
        self.labels = {'managed-by': 'gcp-local', 'gcp-local-run': self.manifest['run_id']}

    def call(self, method, suffix='', *, actor='fixture', body=None, query=None):
        return self.client.request(actor, method, f'{self.base}/{self.instance}{suffix}',
                                   body=body, query=query, timeout=self.remaining())

    def create_instance(self):
        return self.client.request('fixture', 'POST', self.base, timeout=self.remaining(), body={
            'instanceId': self.instance, 'instance': {
                'config': f'projects/{self.project}/instanceConfigs/{CONFIG}', 'displayName': 'gcp-local conformance',
                'processingUnits': LIMITS['max_processing_units'], 'labels': self.labels}})

    def finish(self, response):
        """Poll a long-running operation to completion; return the final operation."""
        operation = response.json()
        while response.status == 200 and not operation.get('done'):
            time.sleep(3 if self.client.sandbox else 0)
            name = operation['name'].split('/', 2)[2]  # drop "projects/<project>"
            response = self.client.request('fixture', 'GET', f'/v1/projects/{self.project}/{name}',
                                           timeout=self.remaining())
            operation = response.json()
        return response

    def setup(self):
        self.acquire()
        self.client.verify_project()
        listing = self.client.request('fixture', 'GET', self.base)
        if listing.status != 200:
            raise Inconclusive(f'Instance listing returned HTTP {listing.status}')
        if listing.json().get('instances'):
            raise Inconclusive('Spanner sandbox already holds an instance; refusing to add billable capacity')
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.create_instance()

    def cleanup(self):
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
                    raise ValueError('Refusing Spanner cleanup: instance ownership does not match the manifest')
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


def operation(response):
    value = response.json()
    result = value.get('response', {})
    return {'http_status': response.status, 'fields': sorted(value), 'done': value.get('done'),
            'metadata_type': value.get('metadata', {}).get('@type'), 'response_type': result.get('@type'),
            'response_fields': sorted(result), 'error': value.get('error', {}).get('code')}


def run_scenario(fixture, snapshot, created):
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.instance: '<instance>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>',
                            **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    snapshot.match('instance-operation', operation(fixture.finish(created)))
    instance = fixture.call('GET')
    snapshot.match('instance-read', observe.select(instance, ('name', 'config', 'processingUnits', 'state', 'labels',
                                                              'displayName')))
    snapshot.match('denied-read', observe.error(fixture.call('GET', actor='denied')))
    policy = fixture.call('POST', ':getIamPolicy', body={})
    snapshot.match('policy-read', observe.select(policy, ('bindings',)))
    granted = fixture.call('POST', ':setIamPolicy', body={'policy': {'etag': policy.json().get('etag', ''), 'bindings': [
        {'role': 'roles/spanner.viewer', 'members': ['serviceAccount:' + client.emails['allowed']]}]}})
    snapshot.match('viewer-granted', observe.select(granted, ('bindings',)))
    allowed = fixture.wait_until(lambda: fixture.call('GET', actor='allowed'), {403}, 180)
    snapshot.match('viewer-read', {'http_status': allowed.status})
    snapshot.match('viewer-cannot-create-database', observe.error(fixture.call(
        'POST', '/databases', actor='allowed', body={'createStatement': 'CREATE DATABASE `denied`'})))
    database = fixture.finish(fixture.call('POST', '/databases', body={
        'createStatement': 'CREATE DATABASE `gl_db`', 'extraStatements': [TABLE]}))
    snapshot.match('database-operation', operation(database))
    read = fixture.call('GET', '/databases/gl_db')
    snapshot.match('database-read', observe.select(read, ('name', 'state', 'databaseDialect')))
    snapshot.match('ddl', observe.select(fixture.call('GET', '/databases/gl_db/ddl'), ('statements',)))
    altered = fixture.finish(fixture.call('PATCH', '/databases/gl_db/ddl', body={'statements': [COLUMN]}))
    snapshot.match('ddl-update-operation', operation(altered))
    snapshot.match('ddl-after-update', observe.select(fixture.call('GET', '/databases/gl_db/ddl'), ('statements',)))
    snapshot.match('duplicate-instance', observe.error(fixture.create_instance()))
    dropped = fixture.call('DELETE', '/databases/gl_db')
    snapshot.match('database-dropped', {'http_status': dropped.status, 'body': dropped.json()})
    snapshot.match('database-absent', observe.error(fixture.call('GET', '/databases/gl_db')))
    deleted = fixture.call('DELETE')
    snapshot.match('instance-deleted', {'http_status': deleted.status, 'body': deleted.json()})
    snapshot.match('instance-absent', observe.error(fixture.call('GET')))


def main(argv=None):
    parser = argparse.ArgumentParser(description='Delete only the owned instance after an interrupted run')
    parser.add_argument('command', choices=['cleanup'])
    parser.add_argument('--profile', required=True)
    parser.add_argument('--scope', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--allow-live', action='store_true', required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox)
    target = Target.select('record', project=sandbox.project, credentials_file=sandbox.identities['allowed'].credentials_file,
                           live_scope=sandbox.live_scope, allow_live=args.allow_live)
    client = RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    InstanceFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                    manifest=args.manifest).cleanup()
    print('Owned Spanner cleanup passed.')


if __name__ == '__main__':
    main()
