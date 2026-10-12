"""Bounded Bigtable admin recording. The cluster bills while it exists, so the scenario
uses one HDD node and deletes the instance at the end."""

import argparse
import time
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, now
from gcp_validation.live_paths import PrefixClient
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun
from gcp_validation.sandbox import Sandbox

SERVICE = 'bigtable'
LIMITS = {'max_instances': 1, 'max_clusters': 1, 'max_nodes': 1, 'max_tables': 1, 'max_run_seconds': 900}
LIVE_ENDPOINT = 'https://bigtableadmin.googleapis.com'
ZONE = 'us-central1-b'


def load_scope(filename, sandbox, mode: str):
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class AdminClient(PrefixClient):
    """Admin paths, plus this project's operations under /v2/operations/projects/P/."""

    prefix = '/v2'

    def request(self, actor, method, path, **options):
        if path.startswith(f'/v2/operations/projects/{self.target.project}/') and method == 'GET':
            self.prefix = '/v2/operations'
            try:
                return super().request(actor, method, path, **options)
            finally:
                self.prefix = '/v2'
        return super().request(actor, method, path, **options)


class InstanceFixture(OwnedRun):
    """One labeled instance with a single one-node HDD cluster."""

    def __init__(self, client, state_dir, *, manifest=None):
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('instance') == 'gl-' + v['run_id'][:12])
        self.manifest.setdefault('instance', 'gl-' + self.manifest['run_id'][:12])
        self.instance = self.manifest['instance']
        self.cluster = self.instance + '-c1'
        self.base = f'/v2/projects/{self.project}/instances'
        self.labels = {'managed-by': 'gcp-local', 'gcp-local-run': self.manifest['run_id']}

    def call(self, method, suffix='', *, actor='fixture', body=None, query=None):
        return self.client.request(actor, method, f'{self.base}/{self.instance}{suffix}',
                                   body=body, query=query, timeout=self.remaining())

    def create_instance(self):
        return self.client.request('fixture', 'POST', self.base, timeout=self.remaining(), body={
            'parent': f'projects/{self.project}', 'instanceId': self.instance,
            'instance': {'displayName': 'gcp-local conformance', 'type': 'PRODUCTION', 'labels': self.labels},
            'clusters': {self.cluster: {'location': f'projects/{self.project}/locations/{ZONE}',
                                        'serveNodes': LIMITS['max_nodes'], 'defaultStorageType': 'HDD'}}})

    def finish(self, response):
        """Poll a long-running operation to completion; return the final operation."""
        operation = response.json()
        while response.status == 200 and not operation.get('done'):
            time.sleep(3 if self.client.sandbox else 0)
            response = self.client.request('fixture', 'GET', '/v2/' + operation['name'], timeout=self.remaining())
            operation = response.json()
        return response

    def setup(self):
        self.acquire()
        self.client.verify_project()
        listing = self.client.request('fixture', 'GET', self.base)
        if listing.status != 200:
            raise Inconclusive(f'Instance listing returned HTTP {listing.status}')
        if listing.json().get('instances'):
            raise Inconclusive('Bigtable sandbox already holds an instance; refusing to add billable capacity')
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
                    raise ValueError('Refusing Bigtable cleanup: instance ownership does not match the manifest')
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
    observe = Observations({fixture.cluster: '<cluster>', fixture.instance: '<instance>',
                            fixture.manifest['run_id']: '<run>', fixture.project: '<project>',
                            client.number: '<project-number>',
                            **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    snapshot.match('instance-operation', operation(fixture.finish(created)))
    snapshot.match('instance-read', observe.select(fixture.call('GET'), ('name', 'displayName', 'state', 'type', 'labels')))
    clusters = fixture.call('GET', '/clusters')
    snapshot.match('clusters', {'http_status': clusters.status, 'clusters': [
        {'fields': sorted(item), 'name': observe.text(item['name']), 'location': observe.text(item['location']),
         'state': item.get('state'), 'serveNodes': item.get('serveNodes'), 'defaultStorageType': item.get('defaultStorageType')}
        for item in clusters.json().get('clusters', [])]})
    snapshot.match('denied-read', observe.error(fixture.call('GET', actor='denied')))
    policy = fixture.call('POST', ':getIamPolicy', body={})
    snapshot.match('policy-read', observe.select(policy, ('bindings',)))
    granted = fixture.call('POST', ':setIamPolicy', body={'policy': {'etag': policy.json().get('etag', ''), 'bindings': [
        {'role': 'roles/bigtable.viewer', 'members': ['serviceAccount:' + client.emails['allowed']]}]}})
    snapshot.match('viewer-granted', observe.select(granted, ('bindings',)))
    allowed = fixture.wait_until(lambda: fixture.call('GET', actor='allowed'), {403}, 180)
    snapshot.match('viewer-read', {'http_status': allowed.status})
    snapshot.match('viewer-cannot-create-table', observe.error(fixture.call(
        'POST', '/tables', actor='allowed', body={'tableId': 'denied', 'table': {}})))
    table = fixture.call('POST', '/tables', body={'tableId': 't1', 'table': {'columnFamilies': {'cf1': {}}}})
    snapshot.match('table-created', observe.select(table, ('name', 'columnFamilies', 'granularity')))
    modified = fixture.call('POST', '/tables/t1:modifyColumnFamilies', body={'modifications': [{'id': 'cf2', 'create': {}}]})
    snapshot.match('families-modified', observe.select(modified, ('name', 'columnFamilies')))
    snapshot.match('table-read', observe.select(fixture.call('GET', '/tables/t1'), ('name', 'columnFamilies')))
    snapshot.match('duplicate-table', observe.error(fixture.call('POST', '/tables', body={'tableId': 't1', 'table': {}})))
    dropped = fixture.call('DELETE', '/tables/t1')
    snapshot.match('table-deleted', {'http_status': dropped.status, 'body': dropped.json()})
    snapshot.match('table-absent', observe.error(fixture.call('GET', '/tables/t1')))
    snapshot.match('duplicate-instance', observe.error(fixture.create_instance()))
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
    scope = load_scope(args.scope, sandbox, 'cleanup')
    target = Target.select('record', project=sandbox.project, credentials_file=sandbox.identities['allowed'].credentials_file,
                           live_scope=sandbox.live_scope, allow_live=args.allow_live)
    client = AdminClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    InstanceFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                    manifest=args.manifest).cleanup()
    print('Owned Bigtable cleanup passed.')


if __name__ == '__main__':
    main()
