"""Bounded VPC control-plane and Connectivity Tests recording. Networks, firewalls,
routes, routers, and Connectivity Tests are free to hold briefly; no VMs are created."""

import argparse
import time
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, now
from gcp_validation.live_paths import PrefixClient
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = 'networking'
LIMITS = {'max_networks': 2, 'max_subnetworks': 2, 'max_firewalls': 2, 'max_routes': 1, 'max_routers': 1,
          'max_connectivity_tests': 4, 'max_peerings': 2, 'max_instances': 0, 'max_run_seconds': 1500}
COMPUTE = 'https://compute.googleapis.com'
NETWORK_MANAGEMENT = 'https://networkmanagement.googleapis.com'
REGION = 'us-central1'


def load_scope(filename, sandbox, mode: str):
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class ComputeClient(PrefixClient):
    prefix = '/compute/v1'


def clients(target, *, sandbox=None, scope=None):
    """Compute and Network Management share identities but not endpoints."""
    return (ComputeClient(target, COMPUTE, sandbox=sandbox, scope=scope),
            RestClient(target, NETWORK_MANAGEMENT, sandbox=sandbox, scope=scope))


class NetworkFixture(OwnedRun):
    """Two run-named custom-mode networks; cleanup deletes every owned resource in reverse."""

    def __init__(self, compute, management, state_dir, *, manifest=None):
        super().__init__(compute, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('prefix') == 'gl-' + v['run_id'][:10])
        self.management = management
        self.manifest.setdefault('prefix', 'gl-' + self.manifest['run_id'][:10])
        self.manifest.setdefault('created', [])
        self.prefix = self.manifest['prefix']
        self.global_path = f'/compute/v1/projects/{self.project}/global'
        self.region_path = f'/compute/v1/projects/{self.project}/regions/{REGION}'
        self.tests_path = f'/v1/projects/{self.project}/locations/global/connectivityTests'

    def name(self, suffix):
        return f'{self.prefix}-{suffix}'

    def network_url(self, suffix):
        return f'projects/{self.project}/global/networks/{self.name(suffix)}'

    def wait(self, response, *, client=None):
        """Poll a compute or network-management operation until it finishes."""
        client = client or self.client
        value = response.json()
        if response.status != 200:
            return response
        while not (value.get('status') == 'DONE' or value.get('done')):
            time.sleep(2 if client.sandbox else 0)
            if 'selfLink' in value:
                path = '/compute/v1/' + value['selfLink'].split('/compute/v1/', 1)[1]
            else:
                path = '/v1/' + value['name']
            response = client.request('fixture', 'GET', path, timeout=self.remaining())
            value = response.json()
        return response

    def insert(self, kind, collection, body):
        """Record ownership before creating, then wait for the operation."""
        self.manifest['created'].append([kind, body['name']])
        self.save()
        base = self.global_path if kind in {'networks', 'firewalls', 'routes'} else self.region_path
        return self.wait(self.client.request('fixture', 'POST', f'{base}/{collection}', body=body,
                                             timeout=self.remaining()))

    def peer(self, local, remote):
        """Peer network local to remote, recording the peering for cleanup first."""
        peering = self.name(f'{local}-to-{remote}')
        self.manifest['created'].append(['peering', f'{self.name(local)}/{peering}'])
        self.save()
        return self.wait(self.client.request('fixture', 'POST', f'{self.global_path}/networks/{self.name(local)}/addPeering',
                                             timeout=self.remaining(), body={'networkPeering': {
                                                 'name': peering, 'exchangeSubnetRoutes': True,
                                                 'network': f'https://www.googleapis.com/compute/v1/{self.network_url(remote)}'}}))

    def connectivity(self, suffix, body):
        self.manifest['created'].append(['connectivityTests', self.name(suffix)])
        self.save()
        operation = self.management.request('fixture', 'POST', self.tests_path, query={'testId': self.name(suffix)},
                                            body=body, timeout=self.remaining())
        self.wait(operation, client=self.management)
        return self.settle(suffix)

    def settle(self, suffix):
        """A new test is analyzed asynchronously; poll until a result exists."""
        deadline = time.monotonic() + (180 if self.client.sandbox else 0)
        while True:
            test = self.management.request('fixture', 'GET', f'{self.tests_path}/{self.name(suffix)}',
                                           timeout=self.remaining())
            if test.status != 200 or test.json().get('reachabilityDetails', {}).get('result') \
                    or time.monotonic() >= deadline:
                return test
            time.sleep(5)

    def setup(self):
        self.acquire()
        self.client.verify_project()
        self.manifest['attempted'] = True
        self.save()
        return self.insert('networks', 'networks', {
            'name': self.name('a'), 'autoCreateSubnetworks': False, 'routingConfig': {'routingMode': 'REGIONAL'},
            'description': 'gcp-local conformance ' + self.manifest['run_id']})

    def cleanup(self):
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            for kind, name in reversed(self.manifest['created']):
                if not name.startswith(self.prefix + '-'):
                    raise ValueError('Refusing networking cleanup outside the run prefix')
                if kind == 'peering':
                    network, peering = name.split('/', 1)
                    removed = self.client.request('fixture', 'POST', f'{self.global_path}/networks/{network}/removePeering',
                                                  body={'name': peering})
                    if removed.status == 200:
                        self.wait(removed)
                    elif removed.status not in {400, 404}:  # 400: the peering is already gone
                        raise ValueError(f'Owned peering removal returned HTTP {removed.status}')
                    continue
                if kind == 'connectivityTests':
                    client, path = self.management, f'{self.tests_path}/{name}'
                elif kind in {'networks', 'firewalls', 'routes'}:
                    client, path = self.client, f'{self.global_path}/{kind}/{name}'
                else:
                    client, path = self.client, f'{self.region_path}/{kind}/{name}'
                deleted = client.request('fixture', 'DELETE', path)
                if deleted.status == 200:
                    self.wait(deleted, client=client)
                elif deleted.status != 404:
                    raise ValueError(f'Owned {kind} delete returned HTTP {deleted.status}')
                if client.request('fixture', 'GET', path).status != 404:
                    raise ValueError(f'Owned {kind} remains after deletion')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()


def operation(response):
    value = response.json()
    return {'http_status': response.status, 'kind': value.get('kind'), 'operationType': value.get('operationType'),
            'status': value.get('status'), 'error': bool(value.get('error'))}


def refusal(fixture, response):
    """Compute refuses either synchronously or in the finished operation's error."""
    value = response.json()
    if response.status == 200 and value.get('kind') == 'compute#operation':
        done = fixture.wait(response).json()
        return {'http_status': 200, 'operation_status': done.get('status'),
                'operation_error_codes': [e.get('code') for e in done.get('error', {}).get('errors', [])]}
    error = value.get('error', {})
    return {'http_status': response.status, 'code': error.get('code'),
            'reasons': [e.get('reason') for e in error.get('errors', [])]}


def verdict(observe, response):
    """Connectivity Test results: verdict, final step, and drop cause, not the whole trace."""
    value = response.json()
    details = value.get('reachabilityDetails', {})
    traces = details.get('traces', [])
    steps = traces[0].get('steps', []) if traces else []
    last = steps[-1] if steps else {}
    return {'http_status': response.status, 'result': details.get('result'), 'trace_count': len(traces),
            'step_states': [step.get('state') for step in steps],
            'final_state': last.get('state'), 'drop_cause': (last.get('drop') or {}).get('cause'),
            'abort_cause': (last.get('abort') or {}).get('cause'),
            'firewall': observe.value((next((s['firewall'] for s in steps if 'firewall' in s), {}) or {}).get('displayName'))}


def endpoint(fixture, ip, suffix, port=None):
    value = {'ipAddress': ip, 'network': fixture.network_url(suffix), 'projectId': fixture.project}
    if port:
        value['port'] = port
    return value


def run_scenario(fixture, snapshot, created):
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.prefix: '<prefix>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>'})
    snapshot.match('network-operation', operation(created))
    network = client.request('fixture', 'GET', f'{fixture.global_path}/networks/{fixture.name("a")}')
    snapshot.match('network', observe.select(network, ('name', 'autoCreateSubnetworks', 'routingConfig',
                                                       'networkFirewallPolicyEnforcementOrder')))
    snapshot.match('denied-read', observe.error(client.request(
        'denied', 'GET', f'{fixture.global_path}/networks/{fixture.name("a")}')))
    subnet = fixture.insert('subnetworks', 'subnetworks', {
        'name': fixture.name('a1'), 'network': fixture.network_url('a'), 'ipCidrRange': '10.10.0.0/24', 'region': REGION})
    snapshot.match('subnetwork-operation', operation(subnet))
    snapshot.match('subnetwork', observe.select(client.request(
        'fixture', 'GET', f'{fixture.region_path}/subnetworks/{fixture.name("a1")}'),
        ('name', 'ipCidrRange', 'gatewayAddress', 'network', 'region', 'privateIpGoogleAccess', 'stackType')))
    fixture.manifest['created'].append(['subnetworks', fixture.name('bad')])
    fixture.save()
    snapshot.match('overlapping-subnetwork', refusal(fixture, client.request(
        'fixture', 'POST', f'{fixture.region_path}/subnetworks', body={
            'name': fixture.name('bad'), 'network': fixture.network_url('a'), 'ipCidrRange': '10.10.0.128/25'})))
    allow = fixture.insert('firewalls', 'firewalls', {
        'name': fixture.name('allow'), 'network': fixture.network_url('a'), 'direction': 'INGRESS', 'priority': 1000,
        'sourceRanges': ['10.10.0.0/24'], 'allowed': [{'IPProtocol': 'tcp', 'ports': ['8080']}]})
    snapshot.match('firewall-operation', operation(allow))
    snapshot.match('firewall', observe.select(client.request(
        'fixture', 'GET', f'{fixture.global_path}/firewalls/{fixture.name("allow")}'),
        ('name', 'direction', 'priority', 'sourceRanges', 'allowed', 'disabled', 'logConfig')))
    snapshot.match('network-in-use', refusal(fixture, client.request(
        'fixture', 'DELETE', f'{fixture.global_path}/networks/{fixture.name("a")}')))
    peer = fixture.insert('networks', 'networks', {'name': fixture.name('b'), 'autoCreateSubnetworks': False})
    snapshot.match('peer-network-operation', operation(peer))
    fixture.insert('subnetworks', 'subnetworks', {
        'name': fixture.name('b1'), 'network': fixture.network_url('b'), 'ipCidrRange': '10.20.0.0/24', 'region': REGION})
    reachable = fixture.connectivity('t-allow', {
        'source': endpoint(fixture, '10.10.0.5', 'a'), 'destination': endpoint(fixture, '10.10.0.6', 'a', 8080),
        'protocol': 'TCP'})
    snapshot.match('test-allowed-port', verdict(observe, reachable))
    blocked = fixture.connectivity('t-block', {
        'source': endpoint(fixture, '10.10.0.5', 'a'), 'destination': endpoint(fixture, '10.10.0.6', 'a', 9090),
        'protocol': 'TCP'})
    snapshot.match('test-blocked-port', verdict(observe, blocked))
    unpeered = fixture.connectivity('t-cross', {
        'source': endpoint(fixture, '10.10.0.5', 'a'), 'destination': endpoint(fixture, '10.20.0.6', 'b', 8080),
        'protocol': 'TCP'})
    snapshot.match('test-unpeered-network', verdict(observe, unpeered))
    snapshot.match('peering-added', operation(fixture.peer('a', 'b')))
    fixture.peer('b', 'a')
    peered = client.request('fixture', 'GET', f'{fixture.global_path}/networks/{fixture.name("a")}')
    snapshot.match('peering', [{'name': observe.text(item.get('name', '')), 'state': item.get('state'),
                                'exchangeSubnetRoutes': item.get('exchangeSubnetRoutes'), 'fields': sorted(item)}
                               for item in peered.json().get('peerings', [])])
    peered_test = fixture.connectivity('t-peered', {
        'source': endpoint(fixture, '10.10.0.5', 'a'), 'destination': endpoint(fixture, '10.20.0.6', 'b', 8080),
        'protocol': 'TCP'})
    snapshot.match('test-peered-network', verdict(observe, peered_test))
    snapshot.match('test-fields', {'fields': sorted(reachable.json()),
                                   'details_fields': sorted(reachable.json().get('reachabilityDetails', {}))})


def main(argv=None):
    parser = argparse.ArgumentParser(description='Delete only the owned networking resources after an interrupted run')
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
    compute, management = clients(target, sandbox=sandbox, scope=scope)
    NetworkFixture(compute, management, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                   manifest=args.manifest).cleanup()
    print('Owned networking cleanup passed.')


if __name__ == '__main__':
    main()
