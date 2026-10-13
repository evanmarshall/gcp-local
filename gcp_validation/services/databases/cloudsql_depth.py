"""Bounded Cloud SQL depth recordings: backups and restore, availability, TLS, networks, flags.

Each scenario owns one or two labeled instances named from its run ID and deletes them
at the end; only the ``fixture`` identity calls the API. Requests that GCP should refuse
are sent as inserts that reuse an owned instance's name, so a request GCP accepts after
all answers 409 instead of creating a billed instance. The instance shapes, operation
types, and error messages are what the local provider renders.
"""

import argparse
import json
import re
import secrets
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, SnapshotCase, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.databases.cloudsql import (
    API_PREFIX,
    REGION,
    SERVICE,
    SqlAdminClient,
    client_for,
    operation,
    sql_error,
)

SMALL_TIER, HA_TIER = 'db-f1-micro', 'db-custom-1-3840'
POSTGRES, MYSQL = 'POSTGRES_16', 'MYSQL_8_0'
CLIENT_CERT = 'gl-client'
BACKUP_DESCRIPTION = 'gl on-demand'


def backup_start() -> str:
    """Return a daily backup start twelve hours away, so no automated run starts mid-run."""
    return (datetime.now(UTC) + timedelta(hours=12)).strftime('%H:00')


class DepthFixture(OwnedRun):
    """Owned instances named from the run ID; cleanup deletes every one it created."""

    def __init__(self, client: SqlAdminClient, state_dir: Path, limits: dict, *,
                 suffixes: tuple[str, ...] = ('',), manifest: str | None = None) -> None:
        """Bind the owned instance names to the run manifest."""
        def owned(value: dict) -> bool:
            stem = 'gl-' + value['run_id'][:12]
            return all(name.startswith(stem) for name in value.get('instances', ()))
        super().__init__(client, state_dir, SERVICE, limits['max_run_seconds'], manifest=manifest,
                         check=owned)
        stem = 'gl-' + self.manifest['run_id'][:12]
        self.manifest.setdefault('instances', [stem + suffix for suffix in suffixes])
        self.instances = list(self.manifest['instances'])
        self.instance = self.instances[0]
        self.base = f'{API_PREFIX}/projects/{self.project}/instances'
        self.labels = {'managed-by': 'gcp-local', 'gcp-local-run': self.manifest['run_id']}
        self.backup_start = self.manifest.setdefault('backup_start', backup_start())

    def call(self, method: str, suffix: str = '', *, instance: str | None = None,
             body: dict | None = None, query: dict | None = None) -> Response:
        """Send one request about an owned instance as the fixture identity."""
        name = instance or self.instance
        return self.client.request('fixture', method, f'{self.base}/{name}{suffix}', body=body,
                                   query=query, timeout=self.remaining())

    def insert(self, name: str, version: str, settings: dict) -> Response:
        """Request an instance; the root password is never stored."""
        body = {'name': name, 'region': REGION, 'databaseVersion': version,
                'rootPassword': secrets.token_urlsafe(24),
                'settings': {'userLabels': self.labels, 'deletionProtectionEnabled': False,
                             **settings}}
        return self.client.request('fixture', 'POST', self.base, body=body,
                                   timeout=self.remaining())

    def refused(self, version: str, settings: dict) -> Response:
        """Send an insert GCP should refuse, reusing the owned name so nothing is created."""
        return self.insert(self.instance, version, settings)

    def attempt(self, observe: Observations, settings: dict) -> dict:
        """Patch settings GCP should refuse; wait out a patch it accepts after all."""
        response = self.call('PATCH', body={'settings': settings})
        if response.status >= 400:
            return sql_error(observe, response)
        return {'accepted': operation(self.finish(response))}

    def finish(self, response: Response) -> Response:
        """Poll an sql#operation until DONE and return the final operation response."""
        value = response.json()
        while response.status == 200 and value.get('kind') == 'sql#operation' \
                and value.get('status') != 'DONE':
            time.sleep(10 if self.client.sandbox else 0)
            path = f"{API_PREFIX}/projects/{self.project}/operations/{value['name']}"
            response = self.client.request('fixture', 'GET', path, timeout=self.remaining())
            value = response.json()
        return response

    def setup(self, specs: list[tuple[str, dict]]) -> list[Response]:
        """Refuse a non-empty sandbox, record the attempt, and request every instance."""
        self.acquire()
        self.client.verify_project()
        listing = self.client.request('fixture', 'GET', self.base)
        if listing.status != 200:
            raise Inconclusive(f'Instance listing returned HTTP {listing.status}')
        if listing.json().get('items'):
            raise Inconclusive('Cloud SQL sandbox already holds an instance; refusing to add one')
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        return [self.insert(name, version, settings)
                for name, (version, settings) in zip(self.instances, specs, strict=True)]

    def cleanup(self) -> None:
        """Delete only the labeled instances this manifest owns."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            for name in self.instances:
                self.delete_owned(name)
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()

    def delete_owned(self, name: str) -> None:
        """Delete one instance after checking its ownership labels, waiting out busy states."""
        deadline = time.monotonic() + (1800 if self.client.sandbox else 0)
        while True:
            current = self.call('GET', instance=name)
            if current.status == 404:
                return
            if current.status != 200:
                raise ValueError(f'Owned instance read returned HTTP {current.status}')
            if current.json().get('settings', {}).get('userLabels') != self.labels:
                raise ValueError('Refusing Cloud SQL cleanup: ownership does not match')
            deleted = self.finish(self.call('DELETE', instance=name))
            if deleted.status in {200, 404}:
                if self.call('GET', instance=name).status != 404:
                    raise ValueError('Owned instance remains after deletion')
                return
            # An instance still restoring or creating refuses deletion until it settles.
            if time.monotonic() >= deadline:
                raise ValueError(f'Owned instance delete returned HTTP {deleted.status}')
            time.sleep(30)


def load_scope(filename: str, sandbox: Sandbox, mode: str, limits: dict) -> LiveScope:
    """Load the separately approved scope of one depth scenario."""
    return LiveScope.load(SERVICE, limits, filename, sandbox, mode)


def observer(fixture: DepthFixture) -> Observations:
    """Alias the run's identifiers, keeping their relationships."""
    client = fixture.client
    aliases = {fixture.project: '<project>', client.number: '<project-number>',
               fixture.manifest['run_id']: '<run>', fixture.backup_start: '<backup-start>',
               **{email: '<' + actor + '>' for actor, email in client.emails.items()}}
    for index, name in enumerate(fixture.instances):
        aliases[name] = '<instance>' if index == 0 else f'<instance-{index + 1}>'
    return Observations(aliases)


def fields(value: object) -> list[str]:
    """Return a mapping's sorted keys, or [] for anything else."""
    return sorted(value) if isinstance(value, dict) else []


def certificate(value: dict) -> dict:
    """Select an sql#sslCert's shape without its identifiers or key material."""
    return {'fields': fields(value), 'kind': value.get('kind'),
            'pem': str(value.get('cert', '')).startswith('-----BEGIN CERTIFICATE-----')}


def instance_view(observe: Observations, response: Response) -> dict:
    """Select the depth settings of one instance.

    A backup start time the run didn't choose is GCP's assignment: only its format is kept.
    """
    value = response.json()
    settings = value.get('settings', {})
    ip = settings.get('ipConfiguration', {})
    backup = dict(settings.get('backupConfiguration') or {})
    start = backup.get('startTime')
    if start and start not in observe.aliases and re.fullmatch(r'\d\d:\d\d', start):
        backup['startTime'] = '<assigned>'

    return {'http_status': response.status, 'fields': fields(value),
            'settings_fields': fields(settings), 'state': value.get('state'),
            'availabilityType': settings.get('availabilityType'), 'tier': settings.get('tier'),
            'edition': settings.get('edition'),
            'backupConfiguration': observe.value(backup or None),
            'ipConfiguration': observe.value(ip),
            'databaseFlags': observe.value(settings.get('databaseFlags')),
            'serverCaCert': certificate(value.get('serverCaCert', {})),
            'ip_types': sorted(item.get('type') for item in value.get('ipAddresses', []))}


def operation_view(observe: Observations, response: Response) -> dict:
    """Select a finished operation and the shapes of its contexts."""
    selected = operation(response)
    value = response.json()
    for key in ('backupContext', 'error'):
        if key in value:
            selected[key + '_fields'] = fields(value[key])
    if 'error' in value:
        selected['error_detail'] = observe.value(value['error'])
    return selected


def outcome(observe: Observations, response: Response) -> dict:
    """Select an error, or a success's status and field names."""
    if response.status >= 400:
        return sql_error(observe, response)
    return {'http_status': response.status, 'fields': fields(response.json())}


def backup_run(observe: Observations, value: dict) -> dict:
    """Select one sql#backupRun without its times and ID."""
    keys = ('kind', 'status', 'type', 'description', 'backupKind', 'databaseVersion', 'location',
            'instance')
    return {'fields': fields(value), **{key: observe.value(value.get(key)) for key in keys}}


def backup_config(start: str, **changes: object) -> dict:
    """Return the scenario's PostgreSQL backup configuration with changes applied."""
    value = {'enabled': True, 'startTime': start, 'location': 'us',
             'pointInTimeRecoveryEnabled': True, 'transactionLogRetentionDays': 3,
             'backupRetentionSettings': {'retainedBackups': 5, 'retentionUnit': 'COUNT'}}
    value.update(changes)
    return value


def run_backups(fixture: DepthFixture, snapshot: SnapshotCase, created: list[Response]) -> None:
    """Back up a PostgreSQL instance on demand and restore it in place and into a second one."""
    observe = observer(fixture)
    source, target = fixture.instances
    for label, response in zip(('source-operation', 'target-operation'), created, strict=True):
        snapshot.match(label, operation_view(observe, fixture.finish(response)))
    snapshot.match('source-read', instance_view(observe, fixture.call('GET')))
    snapshot.match('target-read', instance_view(observe, fixture.call('GET', instance=target)))
    start = fixture.backup_start
    for label, config in (('invalid-start-time', backup_config('25:00')),
                          ('invalid-binary-log', backup_config(start, binaryLogEnabled=True))):
        settings = {'tier': SMALL_TIER, 'edition': 'ENTERPRISE', 'backupConfiguration': config}
        snapshot.match(label, sql_error(observe, fixture.refused(POSTGRES, settings)))
    patch = {'settings': {'backupConfiguration': {'transactionLogRetentionDays': 5}}}
    snapshot.match('patch-operation', operation_view(observe, fixture.finish(
        fixture.call('PATCH', body=patch))))
    snapshot.match('patched-read', instance_view(observe, fixture.call('GET')))
    # GCP checks these only for an instance that exists, so they are patches.
    for label, config in (
            ('patch-retention-days', {'transactionLogRetentionDays': 8}),
            ('patch-retained-backups', {'backupRetentionSettings': {'retainedBackups': 0,
                                                                    'retentionUnit': 'COUNT'}}),
            ('patch-pitr-disabled', {'enabled': False, 'pointInTimeRecoveryEnabled': True})):
        snapshot.match(label, fixture.attempt(observe, {'backupConfiguration': config}))
    run_backup_runs(fixture, snapshot, observe, source, target)


def run_backup_runs(fixture: DepthFixture, snapshot: SnapshotCase, observe: Observations,
                    source: str, target: str) -> None:
    """Insert, list, read, restore from, and delete one on-demand backup run."""
    inserted = fixture.finish(fixture.call('POST', '/backupRuns',
                                           body={'description': BACKUP_DESCRIPTION}))
    snapshot.match('backup-operation', operation_view(observe, inserted))
    backup_id = inserted.json().get('backupContext', {}).get('backupId')
    if not backup_id:
        raise Inconclusive('The backup operation named no backup run')
    observe.aliases[str(backup_id)] = '<backup-id>'
    listing = fixture.call('GET', '/backupRuns')
    value = listing.json()
    on_demand = [item for item in value.get('items', []) if item.get('type') == 'ON_DEMAND']
    snapshot.match('backup-runs-list', {'http_status': listing.status, 'fields': fields(value),
                                        'kind': value.get('kind'),
                                        'items': [backup_run(observe, item) for item in on_demand]})
    read = fixture.call('GET', f'/backupRuns/{backup_id}')
    snapshot.match('backup-run-read', {'http_status': read.status,
                                       **backup_run(observe, read.json())})
    snapshot.match('backup-run-missing', outcome(observe, fixture.call('GET', '/backupRuns/1')))
    context = {'backupRunId': backup_id, 'instanceId': source, 'project': fixture.project}
    snapshot.match('restore-in-place', operation_view(observe, fixture.finish(fixture.call(
        'POST', '/restoreBackup', body={'restoreBackupContext': context}))))
    snapshot.match('restore-other', operation_view(observe, fixture.finish(fixture.call(
        'POST', '/restoreBackup', instance=target, body={'restoreBackupContext': context}))))
    snapshot.match('restore-missing-run', outcome(observe, fixture.call(
        'POST', '/restoreBackup', body={'restoreBackupContext': {**context, 'backupRunId': '1'}})))
    snapshot.match('restore-no-context', outcome(observe, fixture.call(
        'POST', '/restoreBackup', body={})))
    snapshot.match('target-restored', instance_view(observe, fixture.call('GET', instance=target)))
    snapshot.match('backup-deleted', operation_view(observe, fixture.finish(fixture.call(
        'DELETE', f'/backupRuns/{backup_id}'))))
    after = fixture.call('GET', f'/backupRuns/{backup_id}')
    snapshot.match('backup-run-after-delete', {'http_status': after.status,
                                               **backup_run(observe, after.json())})
    listing = fixture.call('GET', '/backupRuns').json().get('items', [])
    snapshot.match('backup-runs-after-delete', {'statuses': sorted(
        item.get('status') for item in listing if item.get('type') == 'ON_DEMAND')})
    for label, name in (('source-deleted', source), ('target-deleted', target)):
        snapshot.match(label, operation_view(observe, fixture.finish(fixture.call(
            'DELETE', instance=name))))


AUTHORIZED = [{'value': '203.0.113.0/24', 'name': 'gl-docs', 'kind': 'sql#aclEntry'}]
PG_FLAGS = [{'name': 'max_connections', 'value': '60'},
            {'name': 'log_min_duration_statement', 'value': '250'}]


def connectivity_settings(**changes: object) -> dict:
    """Return the connectivity scenario's instance settings with changes applied."""
    value = {'tier': HA_TIER, 'edition': 'ENTERPRISE', 'availabilityType': 'REGIONAL',
             'ipConfiguration': {'ipv4Enabled': True, 'sslMode': 'ENCRYPTED_ONLY',
                                 'authorizedNetworks': AUTHORIZED},
             'databaseFlags': PG_FLAGS}
    value.update(changes)
    return value


def ip_settings(**changes: object) -> dict:
    """Return the scenario's IP configuration with changes applied."""
    return {'ipv4Enabled': True, 'sslMode': 'ENCRYPTED_ONLY', 'authorizedNetworks': AUTHORIZED,
            **changes}


def run_connectivity(fixture: DepthFixture, snapshot: SnapshotCase,
                     created: list[Response]) -> None:
    """Round-trip REGIONAL availability, TLS modes, client certificates, networks, and flags."""
    observe = observer(fixture)
    snapshot.match('instance-operation', operation_view(observe, fixture.finish(created[0])))
    snapshot.match('instance-read', instance_view(observe, fixture.call('GET')))
    for label, changes in (
            ('invalid-edition-tier', {'tier': SMALL_TIER, 'edition': 'ENTERPRISE_PLUS',
                                      'availabilityType': 'ZONAL'}),
            ('invalid-availability', {'availabilityType': 'BOGUS'}),
            ('invalid-ssl-mode', {'ipConfiguration': ip_settings(sslMode='BOGUS')}),
            ('invalid-network', {'ipConfiguration': ip_settings(
                authorizedNetworks=[{'value': 'not-a-cidr'}])}),
            ('invalid-network-host-bits', {'ipConfiguration': ip_settings(
                authorizedNetworks=[{'value': '203.0.113.7/24'}])})):
        snapshot.match(label, sql_error(observe, fixture.refused(
            POSTGRES, connectivity_settings(**changes))))
    # GCP checks these only for an instance that exists, so they are patches.
    for label, settings in (
            ('patch-unknown-flag', {'databaseFlags': [{'name': 'gl_not_a_flag', 'value': '1'}]}),
            ('patch-flag-value', {'databaseFlags': [{'name': 'max_connections', 'value': '1'}]}),
            ('patch-flag-type', {'databaseFlags': [
                {'name': 'log_min_duration_statement', 'value': 'soon'}]}),
            ('patch-flag-duplicate', {'databaseFlags': [
                {'name': 'max_connections', 'value': '60'},
                {'name': 'max_connections', 'value': '70'}]}),
            ('patch-ssl-pair', {'ipConfiguration': {'sslMode': 'ENCRYPTED_ONLY',
                                                    'requireSsl': True}})):
        snapshot.match(label, fixture.attempt(observe, settings))
    connect = fixture.call('GET', '/connectSettings')
    value = connect.json()
    snapshot.match('connect-settings', {
        'http_status': connect.status, 'fields': fields(value), 'kind': value.get('kind'),
        'backendType': value.get('backendType'), 'databaseVersion': value.get('databaseVersion'),
        'region': value.get('region'), 'serverCaMode': value.get('serverCaMode'),
        'serverCaCert': certificate(value.get('serverCaCert', {})),
        'ip_types': sorted(item.get('type') for item in value.get('ipAddresses', []))})
    trusted = {'sslMode': 'TRUSTED_CLIENT_CERTIFICATE_REQUIRED', 'requireSsl': True}
    for label, ip in (('require-ssl', {'requireSsl': True}), ('allow-ssl', {'requireSsl': False}),
                      ('ssl', trusted)):
        snapshot.match(label + '-patch-operation', operation_view(observe, fixture.finish(
            fixture.call('PATCH', body={'settings': {'ipConfiguration': ip}}))))
        snapshot.match(label + '-patched-read', instance_view(observe, fixture.call('GET')))
    networks = [{'value': '198.51.100.17/32', 'name': 'gl-host'}, *AUTHORIZED,
                {'value': '192.0.2.0/28', 'name': 'gl-test'}]
    patch = {'settings': {'ipConfiguration': {'authorizedNetworks': networks}}}
    snapshot.match('network-patch-operation', operation_view(observe, fixture.finish(
        fixture.call('PATCH', body=patch))))
    snapshot.match('network-patched-read', instance_view(observe, fixture.call('GET')))
    run_ssl_certs(fixture, snapshot, observe)
    patch = {'settings': {'databaseFlags': [{'name': 'log_min_duration_statement',
                                             'value': '500'}]}}
    snapshot.match('flags-patch-operation', operation_view(observe, fixture.finish(
        fixture.call('PATCH', body=patch))))
    snapshot.match('flags-patched-read', instance_view(observe, fixture.call('GET')))
    snapshot.match('instance-deleted', operation_view(observe, fixture.finish(
        fixture.call('DELETE'))))


def run_ssl_certs(fixture: DepthFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Insert, list, read, refuse a duplicate of, and delete one client certificate."""
    inserted = fixture.call('POST', '/sslCerts', body={'commonName': CLIENT_CERT})
    value = inserted.json()
    client = value.get('clientCert', {})
    info = client.get('certInfo', {})
    key = str(client.get('certPrivateKey', ''))
    snapshot.match('ssl-cert-insert', {
        'http_status': inserted.status, 'fields': fields(value), 'kind': value.get('kind'),
        'client_fields': fields(client), 'certInfo': certificate(info),
        'commonName': info.get('commonName'), 'instance': observe.value(info.get('instance')),
        'key_label': key.split('-----')[1].removeprefix('BEGIN ') if '-----' in key else '',
        'serverCaCert': certificate(value.get('serverCaCert', {})),
        'operation': operation_view(observe, fixture.finish(Response(200, json.dumps(value.get(
            'operation', {})).encode())))})
    fingerprint = info.get('sha1Fingerprint')
    if not fingerprint:
        raise Inconclusive('The certificate insert returned no fingerprint')
    listing = fixture.call('GET', '/sslCerts')
    items = listing.json().get('items', [])
    snapshot.match('ssl-certs-list', {
        'http_status': listing.status, 'fields': fields(listing.json()),
        'kind': listing.json().get('kind'),
        'items': [{**certificate(item), 'commonName': item.get('commonName')} for item in items]})
    read = fixture.call('GET', f'/sslCerts/{fingerprint}')
    snapshot.match('ssl-cert-read', {'http_status': read.status, **certificate(read.json()),
                                     'commonName': read.json().get('commonName')})
    snapshot.match('ssl-cert-duplicate', outcome(observe, fixture.call(
        'POST', '/sslCerts', body={'commonName': CLIENT_CERT})))
    snapshot.match('ssl-cert-deleted', operation_view(observe, fixture.finish(fixture.call(
        'DELETE', f'/sslCerts/{fingerprint}'))))
    snapshot.match('ssl-cert-after-delete', outcome(observe, fixture.call(
        'GET', f'/sslCerts/{fingerprint}')))


MYSQL_FLAGS = [{'name': 'max_connections', 'value': '50'},
               {'name': 'slow_query_log', 'value': 'on'}, {'name': 'long_query_time', 'value': '2'}]


def mysql_settings(start: str, **changes: object) -> dict:
    """Return the MySQL scenario's instance settings with changes applied."""
    value = {'tier': SMALL_TIER, 'edition': 'ENTERPRISE', 'databaseFlags': MYSQL_FLAGS,
             'backupConfiguration': {'enabled': True, 'binaryLogEnabled': True,
                                     'startTime': start, 'transactionLogRetentionDays': 2}}
    value.update(changes)
    return value


def run_mysql(fixture: DepthFixture, snapshot: SnapshotCase, created: list[Response]) -> None:
    """Round-trip MySQL backups with binary logging and flags; back up and restore in place."""
    observe = observer(fixture)
    start = fixture.backup_start
    snapshot.match('instance-operation', operation_view(observe, fixture.finish(created[0])))
    snapshot.match('instance-read', instance_view(observe, fixture.call('GET')))
    snapshot.match('invalid-binary-log-disabled', sql_error(observe, fixture.refused(
        MYSQL, mysql_settings(start, backupConfiguration={'enabled': False,
                                                          'binaryLogEnabled': True}))))
    for label, settings in (
            ('patch-unknown-flag', {'databaseFlags': [{'name': 'gl_not_a_flag', 'value': '1'}]}),
            ('patch-flag-value', {'databaseFlags': [{'name': 'slow_query_log', 'value': 'maybe'}]}),
            ('patch-flag-range', {'databaseFlags': [{'name': 'long_query_time', 'value': '-1'}]})):
        snapshot.match(label, fixture.attempt(observe, settings))
    patch = {'settings': {'databaseFlags': [{'name': 'max_connections', 'value': '60'}]}}
    snapshot.match('flags-patch-operation', operation_view(observe, fixture.finish(
        fixture.call('PATCH', body=patch))))
    snapshot.match('flags-patched-read', instance_view(observe, fixture.call('GET')))
    inserted = fixture.finish(fixture.call('POST', '/backupRuns',
                                           body={'description': BACKUP_DESCRIPTION}))
    snapshot.match('backup-operation', operation_view(observe, inserted))
    backup_id = inserted.json().get('backupContext', {}).get('backupId')
    if not backup_id:
        raise Inconclusive('The backup operation named no backup run')
    observe.aliases[str(backup_id)] = '<backup-id>'
    read = fixture.call('GET', f'/backupRuns/{backup_id}')
    snapshot.match('backup-run-read', {'http_status': read.status,
                                       **backup_run(observe, read.json())})
    context = {'backupRunId': backup_id, 'instanceId': fixture.instance,
               'project': fixture.project}
    snapshot.match('restore-in-place', operation_view(observe, fixture.finish(fixture.call(
        'POST', '/restoreBackup', body={'restoreBackupContext': context}))))
    snapshot.match('backup-deleted', operation_view(observe, fixture.finish(fixture.call(
        'DELETE', f'/backupRuns/{backup_id}'))))
    snapshot.match('instance-deleted', operation_view(observe, fixture.finish(
        fixture.call('DELETE'))))


SCENARIOS = {
    'backups': (('', '-r'), lambda start: [
        (POSTGRES, {'tier': SMALL_TIER, 'edition': 'ENTERPRISE',
                    'backupConfiguration': backup_config(start)}),
        (POSTGRES, {'tier': SMALL_TIER, 'edition': 'ENTERPRISE'})]),
    'connectivity': (('',), lambda _start: [(POSTGRES, connectivity_settings())]),
    'mysql': (('',), lambda start: [(MYSQL, mysql_settings(start))]),
}


def specs(kind: str, fixture: DepthFixture) -> list[tuple[str, dict]]:
    """Return the instances one scenario creates."""
    return SCENARIOS[kind][1](fixture.backup_start)


def main(argv: list[str] | None = None) -> None:
    """Delete only the owned instances after an interrupted run."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument('command', choices=['cleanup'])
    parser.add_argument('--kind', choices=sorted(SCENARIOS), required=True)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--scope', required=True)
    parser.add_argument('--limits', required=True, help='The scenario LIMITS, as JSON')
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--allow-live', action='store_true', required=True)
    args = parser.parse_args(argv)
    limits = json.loads(args.limits)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox, 'cleanup', limits)
    target = Target.select('record', project=sandbox.project,
                           credentials_file=sandbox.identities['allowed'].credentials_file,
                           live_scope=sandbox.live_scope, allow_live=args.allow_live)
    client = client_for(target, sandbox=sandbox, scope=scope)
    DepthFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                 limits, suffixes=SCENARIOS[args.kind][0], manifest=args.manifest).cleanup()
    sys.stdout.write('Owned Cloud SQL cleanup passed.\n')


if __name__ == '__main__':
    main()
