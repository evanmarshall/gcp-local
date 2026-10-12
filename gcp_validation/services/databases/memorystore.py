"""Bounded Memorystore for Redis instance admin recording.

A Basic Tier instance bills while it exists, so the scenario creates one 1 GB REDIS_7_2
instance in us-central1, records the control plane (never the AUTH string or private
address), and deletes it.
"""

import argparse
import re
import sys
import time
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, SnapshotCase, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.compute.networking import COMPUTE, ComputeClient

SERVICE = 'redis'
LIMITS = {'max_instances': 1, 'max_memory_gb': 1, 'max_run_seconds': 2400}
LIVE_ENDPOINT = 'https://redis.googleapis.com'
REGION = 'us-central1'
POLL_SECONDS = 15
UUID = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
INSTANCE_FIELDS = ('name', 'tier', 'memorySizeGb', 'redisVersion', 'state', 'labels',
                   'displayName', 'authEnabled', 'transitEncryptionMode', 'connectMode',
                   'locationId', 'authorizedNetwork')


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the owner-approved Memorystore live scope."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class InstanceFixture(OwnedRun):
    """One labeled Basic Tier instance; cleanup deletes it and waits until it is gone."""

    def __init__(self, client: RestClient, state_dir: Path, *, manifest: str | None = None) -> None:
        """Bind the owned instance name and labels to this run's manifest."""
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('instance') == 'gl-' + v['run_id'][:12])
        self.manifest.setdefault('instance', 'gl-' + self.manifest['run_id'][:12])
        self.instance = self.manifest['instance']
        self.parent = f'/v1/projects/{self.project}/locations/{REGION}'
        self.base = self.parent + '/instances'
        self.labels = {'managed-by': 'gcp-local', 'gcp-local-run': self.manifest['run_id']}
        # The sandbox has no default VPC, so the run owns the authorized network.
        self.network = self.instance + '-net'
        self.compute = ComputeClient(client.target, COMPUTE, sandbox=client.sandbox, scope=client.scope)
        self.network_path = f'/compute/v1/projects/{self.project}/global/networks/{self.network}'

    def compute_wait(self, response: Response) -> Response:
        """Poll one compute operation until DONE."""
        value = response.json()
        while response.status == 200 and value.get('status') != 'DONE':
            self.pause()
            path = '/compute/v1/' + value['selfLink'].split('/compute/v1/', 1)[1]
            response = self.compute.request('fixture', 'GET', path, timeout=self.remaining())
            value = response.json()
        return response

    def call(self, method: str, suffix: str = '', *, actor: str = 'fixture',
             body: dict | None = None, query: dict | None = None) -> Response:
        """Call the owned instance as one scenario identity."""
        return self.client.request(actor, method, f'{self.base}/{self.instance}{suffix}',
                                   body=body, query=query, timeout=self.remaining())

    def create_instance(self) -> Response:
        """Request the bounded instance; the response is a long-running operation."""
        body = {'tier': 'BASIC', 'memorySizeGb': LIMITS['max_memory_gb'],
                'redisVersion': 'REDIS_7_2', 'authEnabled': True,
                'transitEncryptionMode': 'DISABLED', 'displayName': 'gcp-local conformance',
                'labels': self.labels,
                'authorizedNetwork': f'projects/{self.project}/global/networks/{self.network}',
                # GCP picks a zone when none is given, which made revalidation diverge.
                'locationId': 'us-central1-a'}
        return self.client.request('fixture', 'POST', self.base, body=body,
                                   query={'instanceId': self.instance}, timeout=self.remaining())

    def operation_path(self, name: str) -> str:
        """Address an operation by project ID whatever project form its name uses."""
        return f'/v1/projects/{self.project}/' + name.split('/', 2)[2]

    def pause(self) -> None:
        """Wait between live polls; local operations are already complete."""
        time.sleep(POLL_SECONDS if self.client.sandbox else 0)

    def finish(self, response: Response) -> Response:
        """Poll a long-running operation to completion; return the final operation."""
        operation = response.json()
        while response.status == 200 and not operation.get('done'):
            self.pause()
            response = self.client.request('fixture', 'GET', self.operation_path(operation['name']),
                                           timeout=self.remaining())
            operation = response.json()
        return response

    def wait_absent(self) -> Response:
        """Wait for the asynchronous deletion to make the read return 404."""
        while True:
            current = self.client.request('fixture', 'GET', f'{self.base}/{self.instance}',
                                          timeout=self.remaining())
            if current.status == 404:
                return current
            if current.status != 200:
                raise ValueError(f'Owned instance read returned HTTP {current.status}')
            self.pause()

    def setup(self) -> Response:
        """Refuse a region that already holds an instance, then create the owned one."""
        self.acquire()
        self.client.verify_project()
        listing = self.client.request('fixture', 'GET', self.base)
        if listing.status != 200:
            raise Inconclusive(f'Instance listing returned HTTP {listing.status}')
        if listing.json().get('instances'):
            raise Inconclusive('Memorystore sandbox region already holds an instance; '
                               'refusing to add billable capacity')
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        network = self.compute_wait(self.compute.request('fixture', 'POST', self.network_path.rsplit('/', 1)[0],
                                                         body={'name': self.network, 'autoCreateSubnetworks': False},
                                                         timeout=self.remaining()))
        if network.status != 200 or network.json().get('error'):
            raise Inconclusive(f'Authorized network create returned HTTP {network.status}')
        return self.create_instance()

    def cleanup(self) -> None:
        """Delete only the instance carrying this run's ownership labels."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            path = f'{self.base}/{self.instance}'
            current = self.client.request('fixture', 'GET', path)
            if current.status == 200:
                self.delete_owned(path, current.json())
            elif current.status != 404:
                raise ValueError(f'Owned instance read returned HTTP {current.status}')
            self.wait_absent()
            self.delete_network()
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()

    def delete_network(self) -> None:
        """Delete the run's network; Google's service peering can hold it briefly."""
        deadline = time.monotonic() + (600 if self.client.sandbox else 0)
        while True:
            deleted = self.compute.request('fixture', 'DELETE', self.network_path)
            if deleted.status == 404:
                return
            done = self.compute_wait(deleted) if deleted.status == 200 else deleted
            if done.status == 200 and not done.json().get('error'):
                return
            if time.monotonic() >= deadline:
                raise ValueError('Owned authorized network remains after cleanup')
            self.pause()

    def delete_owned(self, path: str, instance: dict) -> None:
        """Delete an existing instance after checking the ownership labels."""
        labels = instance.get('labels') or {}
        if any(labels.get(key) != value for key, value in self.labels.items()):
            raise ValueError('Refusing Memorystore cleanup: ownership does not match the manifest')
        if instance.get('state') == 'DELETING':
            return
        deleted = self.client.request('fixture', 'DELETE', path)
        if deleted.status not in {200, 404}:
            raise ValueError(f'Owned instance delete returned HTTP {deleted.status}')


def operation(response: Response) -> dict:
    """Select an operation's public shape."""
    value = response.json()
    result = value.get('response', {})
    metadata = value.get('metadata', {})
    return {'http_status': response.status, 'fields': sorted(value), 'done': value.get('done'),
            'metadata_type': metadata.get('@type'), 'verb': metadata.get('verb'),
            'response_type': result.get('@type'), 'response_fields': sorted(result),
            'error': value.get('error', {}).get('code')}


def observe_instance(fixture: InstanceFixture, observe: Observations,
                     snapshot: SnapshotCase) -> None:
    """Record the instance read, list membership, and AUTH string shape."""
    instance = fixture.call('GET')
    snapshot.match('instance-read', {**observe.select(instance, INSTANCE_FIELDS),
                                     'host_present': bool(instance.json().get('host')),
                                     'port_present': bool(instance.json().get('port'))})
    listing = fixture.client.request('fixture', 'GET', fixture.base)
    names = [item.get('name', '') for item in listing.json().get('instances', [])]
    snapshot.match('instance-list', {
        'http_status': listing.status,
        'owned': sum(name.endswith('/' + fixture.instance) for name in names)})
    auth = fixture.call('GET', '/authString')
    # The AUTH string is a credential: record only its presence and shape.
    snapshot.match('auth-string', {
        'http_status': auth.status, 'fields': sorted(auth.json()),
        'uuid_shaped': bool(UUID.fullmatch(auth.json().get('authString', '')))})


def run_scenario(fixture: InstanceFixture, snapshot: SnapshotCase, created: Response) -> None:
    """Record the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({
        fixture.instance: '<instance>', fixture.manifest['run_id']: '<run>',
        fixture.project: '<project>', client.number: '<project-number>',
        **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    created = fixture.finish(created)
    snapshot.match('instance-operation', operation(created))
    observe_instance(fixture, observe, snapshot)
    snapshot.match('denied-read', observe.error(fixture.call('GET', actor='denied')))
    snapshot.match('denied-auth-string',
                   observe.error(fixture.call('GET', '/authString', actor='denied')))
    snapshot.match('denied-delete', observe.error(fixture.call('DELETE', actor='denied')))
    update = {'displayName': 'gcp-local renamed', 'labels': {**fixture.labels, 'stage': 'patched'}}
    patched = fixture.finish(fixture.call('PATCH', query={'updateMask': 'displayName,labels'},
                                          body=update))
    snapshot.match('update-operation', operation(patched))
    snapshot.match('after-update', observe.select(fixture.call('GET'), ('displayName', 'labels')))
    snapshot.match('operation-read', operation(client.request(
        'fixture', 'GET', fixture.operation_path(created.json()['name']))))
    operations = client.request('fixture', 'GET', fixture.parent + '/operations')
    snapshot.match('operations-list', {'http_status': operations.status,
                                       'has_operations': bool(operations.json().get('operations'))})
    snapshot.match('missing-update-mask',
                   observe.error(fixture.call('PATCH', body={'displayName': 'x'})))
    snapshot.match('duplicate-instance', observe.error(fixture.create_instance()))
    snapshot.match('delete-operation', operation(fixture.finish(fixture.call('DELETE'))))
    snapshot.match('instance-absent', observe.error(fixture.wait_absent()))


def main(argv: list[str] | None = None) -> None:
    """Delete only the owned instance after an interrupted run."""
    parser = argparse.ArgumentParser(description=main.__doc__)
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
    sys.stdout.write('Owned Memorystore cleanup passed.\n')


if __name__ == '__main__':
    main()
