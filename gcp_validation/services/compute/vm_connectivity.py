"""Connectivity Tests between real VM endpoints: firewall, peering, and Cloud NAT verdicts.
Three e2-micro instances without external addresses run for a few minutes per run."""

import argparse
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import now
from gcp_validation.live_rest import LiveScope, Observations
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.compute.networking import NetworkFixture, clients, operation, verdict

SERVICE = 'vmconnectivity'
LIMITS = {'max_networks': 2, 'max_subnetworks': 2, 'max_firewalls': 2, 'max_instances': 3, 'max_routers': 1,
          'max_connectivity_tests': 6, 'max_peerings': 2, 'max_run_seconds': 2400}
ZONE = 'us-central1-a'
REGION = 'us-central1'


def load_scope(filename, sandbox, mode: str):
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class VmFixture(NetworkFixture):
    """Adds zonal instances and a router to the owned networking resources."""

    def __init__(self, compute, management, state_dir, *, manifest=None):
        super().__init__(compute, management, state_dir, manifest=manifest)
        self.deadline += LIMITS['max_run_seconds'] - 1500
        self.zone_path = f'/compute/v1/projects/{self.project}/zones/{ZONE}'

    def instance(self, suffix, subnet):
        """One e2-micro VM with no external address and no service account."""
        self.manifest['created'].append(['instances', self.name(suffix)])
        self.save()
        body = {'name': self.name(suffix), 'machineType': f'zones/{ZONE}/machineTypes/e2-micro',
                'disks': [{'boot': True, 'autoDelete': True, 'initializeParams': {
                    'sourceImage': 'projects/debian-cloud/global/images/family/debian-12', 'diskSizeGb': '10'}}],
                'networkInterfaces': [{'subnetwork': f'projects/{self.project}/regions/{REGION}/subnetworks/{self.name(subnet)}'}],
                'serviceAccounts': [], 'labels': {'managed-by': 'gcp-local'}}
        return self.wait(self.client.request('fixture', 'POST', f'{self.zone_path}/instances', body=body,
                                             timeout=self.remaining()))

    def instance_url(self, suffix):
        return f'projects/{self.project}/zones/{ZONE}/instances/{self.name(suffix)}'

    def cleanup(self):
        """Delete instances first (they pin subnetworks), then the base resources."""
        if self.manifest['attempted']:
            self.acquire()
            try:
                for kind, name in reversed(self.manifest['created']):
                    if kind != 'instances':
                        continue
                    if not name.startswith(self.prefix + '-'):
                        raise ValueError('Refusing instance cleanup outside the run prefix')
                    path = f'{self.zone_path}/instances/{name}'
                    deleted = self.client.request('fixture', 'DELETE', path)
                    if deleted.status == 200:
                        self.wait(deleted)
                    elif deleted.status != 404:
                        raise ValueError(f'Owned instance delete returned HTTP {deleted.status}')
                self.manifest['created'] = [entry for entry in self.manifest['created'] if entry[0] != 'instances']
                self.save()
            finally:
                self.release()
        super().cleanup()


def test(fixture, suffix, source, destination, port=None, ip=None):
    """Instance-to-instance (or instance-to-IP) TCP test."""
    target = {'instance': fixture.instance_url(destination)} if destination else {'ipAddress': ip}
    if port:
        target['port'] = port
    return fixture.connectivity(suffix, {'source': {'instance': fixture.instance_url(source)},
                                         'destination': target, 'protocol': 'TCP'})


def run_scenario(fixture, snapshot, created):
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.prefix: '<prefix>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>'})
    snapshot.match('network-operation', operation(created))
    fixture.insert('networks', 'networks', {'name': fixture.name('b'), 'autoCreateSubnetworks': False})
    for net, cidr in (('a', '10.10.0.0/24'), ('b', '10.20.0.0/24')):
        fixture.insert('subnetworks', 'subnetworks', {
            'name': fixture.name(net + '1'), 'network': fixture.network_url(net), 'ipCidrRange': cidr, 'region': REGION})
        fixture.insert('firewalls', 'firewalls', {
            'name': fixture.name(net + '-allow'), 'network': fixture.network_url(net), 'direction': 'INGRESS',
            'priority': 1000, 'sourceRanges': ['10.10.0.0/24'], 'allowed': [{'IPProtocol': 'tcp', 'ports': ['8080']}]})
    for suffix, subnet in (('vm1', 'a1'), ('vm2', 'a1'), ('vm3', 'b1')):
        snapshot.match(f'instance-{suffix}', operation(fixture.instance(suffix, subnet)))
    snapshot.match('open-port', verdict(observe, test(fixture, 't-open', 'vm1', 'vm2', 8080)))
    snapshot.match('closed-port', verdict(observe, test(fixture, 't-closed', 'vm1', 'vm2', 9090)))
    snapshot.match('unpeered', verdict(observe, test(fixture, 't-unpeered', 'vm1', 'vm3', 8080)))
    fixture.peer('a', 'b')
    fixture.peer('b', 'a')
    snapshot.match('peered', verdict(observe, test(fixture, 't-peered', 'vm1', 'vm3', 8080)))
    snapshot.match('internet-without-nat', verdict(observe, test(fixture, 't-nonat', 'vm1', None, 443, ip='8.8.8.8')))
    router = fixture.insert('routers', 'routers', {
        'name': fixture.name('router'), 'network': fixture.network_url('a'), 'region': REGION,
        'nats': [{'name': fixture.name('nat'), 'natIpAllocateOption': 'AUTO_ONLY',
                  'sourceSubnetworkIpRangesToNat': 'ALL_SUBNETWORKS_ALL_IP_RANGES'}]})
    snapshot.match('router-operation', operation(router))
    snapshot.match('internet-with-nat', verdict(observe, test(fixture, 't-nat', 'vm1', None, 443, ip='8.8.8.8')))


def main(argv=None):
    parser = argparse.ArgumentParser(description='Delete only the owned VM connectivity resources')
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
    VmFixture(compute, management, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
              manifest=args.manifest).cleanup()
    print('Owned VM connectivity cleanup passed at', now())


if __name__ == '__main__':
    main()
