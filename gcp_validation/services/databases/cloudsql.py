"""Bounded Cloud SQL for PostgreSQL admin recording on the smallest shared-core tier.

The instance bills while it exists, so the scenario deletes it at the end. Cloud SQL
has no instance-level IAM policy: every permission is evaluated on the project. The
scenario observes the sandbox's ``denied`` identity (no Cloud SQL role in the project),
then grants the ``allowed`` identity ``roles/cloudsql.viewer`` on the owned instance only,
through one conditional project binding that cleanup removes.
"""

import argparse
import json
import secrets
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, SnapshotCase, now
from gcp_validation.live_rest import ACTORS, LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.databases.projectgrant import ConditionalGrant

SERVICE = 'cloudsql'
LIMITS = {'max_instances': 1, 'max_databases': 1, 'max_users': 1, 'max_conditional_grants': 1,
          'max_run_seconds': 3300}
LIVE_ENDPOINT = 'https://sqladmin.googleapis.com'
API_PREFIX = '/sql/v1beta4'
REGION = 'us-central1'
VERSION = 'POSTGRES_16'
TIER = 'db-f1-micro'
DATABASE = 'gl_db'
USER = 'gl_user'
SETTINGS = ('tier', 'edition', 'userLabels', 'deletionProtectionEnabled')
INSTANCE_FIELDS = ('kind', 'name', 'databaseVersion', 'state', 'region', 'connectionName',
                   'instanceType', 'backendType')


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the separately approved Cloud SQL live scope."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class SqlAdminClient(RestClient):
    """The shared explicit-identity transport, admitting only this project's v1beta4 paths.

    The shared client admits ``/v1`` paths only; Cloud SQL v1 paths would collide with
    Spanner's on gcp-local's path-only gateway, so both targets use v1beta4.
    """

    def request(self, actor: str, method: str, path: str, *, query: dict | None = None,
                body: dict | None = None, timeout: float = 30) -> Response:
        """Send one request as a scenario identity, with no cloud fallback."""
        if actor not in ACTORS:
            raise ValueError('Unknown scenario identity')
        if not path.startswith(API_PREFIX + '/projects/' + self.target.project + '/'):
            raise ValueError('Select an explicit path in the selected project')
        token = self.token(actor)
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token}
        if self.target.mode != 'local':
            headers['x-goog-user-project'] = self.target.project
        empty = b'' if method == 'POST' else None
        data = json.dumps(body).encode() if body is not None else empty
        url = self.endpoint + path + ('?' + urlencode(query) if query else '')
        request = Request(url, data=data, headers=headers, method=method)  # noqa: S310 -- fixed endpoint
        try:
            response = self.opener.open(request, timeout=timeout)
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError):
            raise Inconclusive('Transport failed; no cloud fallback is available') from None
        with response:
            return Response(response.code, response.read(1 << 20))


def client_for(target: Target, *, sandbox: Sandbox | None = None,
               scope: LiveScope | None = None) -> SqlAdminClient:
    """Build the transport for the v1beta4 paths gcp-local also serves."""
    return SqlAdminClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)


class InstanceFixture(OwnedRun):
    """One labeled db-f1-micro PostgreSQL instance; cleanup deletes it with its contents."""

    def __init__(self, client: SqlAdminClient, state_dir: Path, *,
                 manifest: str | None = None) -> None:
        """Bind the owned instance name to the run manifest."""
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('instance') == 'gl-' + v['run_id'][:12])
        self.manifest.setdefault('instance', 'gl-' + self.manifest['run_id'][:12])
        self.instance = self.manifest['instance']
        self.base = f'{API_PREFIX}/projects/{self.project}/instances'
        self.labels = {'managed-by': 'gcp-local', 'gcp-local-run': self.manifest['run_id']}
        self.grant = ConditionalGrant(self, 'roles/cloudsql.viewer', (
            "resource.service == 'sqladmin.googleapis.com' && "
            "resource.type == 'sqladmin.googleapis.com/Instance' && "
            f"resource.name == 'projects/{self.project}/instances/{self.instance}'"))

    def call(self, method: str, suffix: str = '', *, actor: str = 'fixture',
             body: dict | None = None, query: dict | None = None) -> Response:
        """Send one request about the owned instance."""
        return self.client.request(actor, method, f'{self.base}/{self.instance}{suffix}',
                                   body=body, query=query, timeout=self.remaining())

    def create_instance(self) -> Response:
        """Request the owned instance; the root password is never stored."""
        settings = {'tier': TIER, 'edition': 'ENTERPRISE', 'userLabels': self.labels,
                    'deletionProtectionEnabled': False, 'ipConfiguration': {'ipv4Enabled': True}}
        return self.client.request('fixture', 'POST', self.base, timeout=self.remaining(), body={
            'name': self.instance, 'region': REGION, 'databaseVersion': VERSION,
            'rootPassword': secrets.token_urlsafe(24), 'settings': settings})

    def finish(self, response: Response) -> Response:
        """Poll an sql#operation until DONE and return the final operation response."""
        operation = response.json()
        while response.status == 200 and operation.get('status') != 'DONE':
            time.sleep(10 if self.client.sandbox else 0)
            path = f"{API_PREFIX}/projects/{self.project}/operations/{operation['name']}"
            response = self.client.request('fixture', 'GET', path, timeout=self.remaining())
            operation = response.json()
        return response

    def protect(self, *, enabled: bool) -> Response:
        """Set deletion protection and wait for the update."""
        body = {'settings': {'deletionProtectionEnabled': enabled}}
        return self.finish(self.call('PATCH', body=body))

    def setup(self) -> Response:
        """Refuse a non-empty sandbox, record the attempt, and create the instance."""
        self.acquire()
        self.client.verify_project()
        listing = self.client.request('fixture', 'GET', self.base)
        if listing.status != 200:
            raise Inconclusive(f'Instance listing returned HTTP {listing.status}')
        if listing.json().get('items'):
            raise Inconclusive('Cloud SQL sandbox already holds an instance; refusing to add one')
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.create_instance()

    def cleanup(self) -> None:
        """Delete only the labeled instance this manifest owns."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            self.grant.remove()
            current = self.call('GET')
            if current.status == 200:
                self.delete_owned(current.json())
            elif current.status != 404:
                raise ValueError(f'Owned instance read returned HTTP {current.status}')
            if self.call('GET').status != 404:
                raise ValueError('Owned instance remains after deletion')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()

    def delete_owned(self, value: dict) -> None:
        """Delete the instance after checking its ownership labels."""
        settings = value.get('settings', {})
        if settings.get('userLabels') != self.labels:
            raise ValueError('Refusing Cloud SQL cleanup: ownership does not match the manifest')
        if settings.get('deletionProtectionEnabled'):
            self.protect(enabled=False)
        deleted = self.finish(self.call('DELETE'))
        if deleted.status not in {200, 404}:
            raise ValueError(f'Owned instance delete returned HTTP {deleted.status}')


def operation(response: Response) -> dict:
    """Select the stable fields of a finished sql#operation."""
    value = response.json()
    return {'http_status': response.status, 'fields': sorted(value), 'kind': value.get('kind'),
            'operationType': value.get('operationType'), 'status': value.get('status'),
            'error': bool(value.get('error'))}


def instance_view(observe: Observations, response: Response) -> dict:
    """Select the instance fields and modeled settings."""
    selected = observe.select(response, INSTANCE_FIELDS)
    value = response.json()
    settings = value.get('settings', {})
    selected['settings'] = observe.value({key: settings.get(key) for key in SETTINGS})
    selected['ip_types'] = sorted(item.get('type') for item in value.get('ipAddresses', []))
    return selected


def names(response: Response) -> dict:
    """Select the sorted item names of a list response."""
    items = response.json().get('items', [])
    return {'http_status': response.status, 'names': sorted(item['name'] for item in items)}


def sql_error(observe: Observations, response: Response) -> dict:
    """Select a v1beta4 error: code, status, message, ``errors[]`` reasons, and detail types."""
    error = response.json().get('error', {})
    reasons = [[item.get('domain'), item.get('reason')] for item in error.get('errors') or ()]
    details = [detail.get('@type') for detail in error.get('details') or ()]
    return {**observe.error(response), 'message': observe.text(error.get('message', '')),
            'errors': reasons, 'details': details}


def outcome(observe: Observations, response: Response) -> dict:
    """Select an error, or only the status of a success."""
    if response.status >= 400:
        return sql_error(observe, response)
    return {'http_status': response.status}


def run_scenario(fixture: InstanceFixture, snapshot: SnapshotCase, created: Response) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({created.json().get('name', '<operation>'): '<operation>',
                            fixture.instance: '<instance>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>',
                            **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    snapshot.match('instance-insert-status', {'status': created.json().get('status')})
    snapshot.match('instance-operation', operation(fixture.finish(created)))
    snapshot.match('instance-read', instance_view(observe, fixture.call('GET')))
    snapshot.match('denied-read', sql_error(observe, fixture.call('GET', actor='denied')))
    snapshot.match('denied-create-database', sql_error(observe, fixture.call(
        'POST', '/databases', actor='denied', body={'name': DATABASE})))
    snapshot.match('protect-operation', operation(fixture.protect(enabled=True)))
    snapshot.match('instance-protected', instance_view(observe, fixture.call('GET')))
    snapshot.match('protected-delete', sql_error(observe, fixture.call('DELETE')))
    snapshot.match('unprotect-operation', operation(fixture.protect(enabled=False)))
    snapshot.match('duplicate-instance', sql_error(observe, fixture.create_instance()))
    snapshot.match('unknown-field', sql_error(observe, client.request(
        'fixture', 'POST', fixture.base, body={'name': fixture.instance, 'bogus': True},
        timeout=fixture.remaining())))
    run_lists(fixture, snapshot, observe)
    run_children(fixture, snapshot, observe)
    run_operations(fixture, snapshot, observe, created.json()['name'])
    snapshot.match('instance-deleted', operation(fixture.finish(fixture.call('DELETE'))))
    snapshot.match('instance-absent', sql_error(observe, fixture.call('GET')))


def run_lists(fixture: InstanceFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Page instances.list and refuse a page token the service didn't issue."""
    client = fixture.client
    for label, query in (('list-page', {'maxResults': '1'}),
                         ('list-invalid-token', {'pageToken': 'not-a-page-token'}),
                         ('list-negative-size', {'maxResults': '-1'})):
        listing = client.request('fixture', 'GET', fixture.base, query=query,
                                 timeout=fixture.remaining())
        if listing.status >= 400:
            snapshot.match(label, sql_error(observe, listing))
            continue
        value = listing.json()
        snapshot.match(label, {'http_status': listing.status, 'kind': value.get('kind'),
                               'items': len(value.get('items', [])),
                               'next': bool(value.get('nextPageToken'))})


def run_operations(fixture: InstanceFixture, snapshot: SnapshotCase, observe: Observations,
                   name: str) -> None:
    """Page operations, cancel a finished one, and read it through a conditional grant.

    The grant is roles/cloudsql.viewer on the owned instance alone.
    """
    client = fixture.client
    path = f'{API_PREFIX}/projects/{fixture.project}/operations'
    pages, token = [], None
    while len(pages) < 4:
        query = {'instance': fixture.instance, 'maxResults': '2'}
        if token:
            query['pageToken'] = token
        page = client.request('fixture', 'GET', path, query=query,
                              timeout=fixture.remaining()).json()
        pages.append([item.get('operationType') for item in page.get('items', [])])
        token = page.get('nextPageToken')
        if not token:
            break
    snapshot.match('operations-pages', {'pages': pages})
    cancel = client.request('fixture', 'POST', f'{path}/{name}/cancel',
                            timeout=fixture.remaining())
    snapshot.match('operation-cancel', outcome(observe, cancel))
    snapshot.match('operation-cancel-missing', outcome(observe, client.request(
        'fixture', 'POST', f'{path}/{fixture.instance}-missing/cancel',
        timeout=fixture.remaining())))
    fixture.grant.add()
    allowed = fixture.grant.wait(lambda: fixture.call('GET', actor='allowed'))
    snapshot.match('condition-read', {'http_status': allowed.status})
    snapshot.match('condition-operation-read', outcome(observe, client.request(
        'allowed', 'GET', f'{path}/{name}', timeout=fixture.remaining())))
    snapshot.match('condition-operation-cancel', outcome(observe, client.request(
        'allowed', 'POST', f'{path}/{name}/cancel', timeout=fixture.remaining())))
    other = client.request('allowed', 'GET', f'{fixture.base}/{fixture.instance}-other',
                           timeout=fixture.remaining())
    snapshot.match('condition-other-instance', outcome(observe, other))
    snapshot.match('condition-list', outcome(observe, client.request(
        'allowed', 'GET', fixture.base, timeout=fixture.remaining())))
    fixture.grant.remove()


def run_children(fixture: InstanceFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Create, list, and delete one database and one built-in user."""
    database = fixture.finish(fixture.call('POST', '/databases', body={'name': DATABASE}))
    snapshot.match('database-operation', operation(database))
    read = fixture.call('GET', '/databases/' + DATABASE)
    fields = ('kind', 'name', 'charset', 'instance', 'project')
    snapshot.match('database-read', observe.select(read, fields))
    snapshot.match('databases-list', names(fixture.call('GET', '/databases')))
    body = {'name': USER, 'password': secrets.token_urlsafe(24)}
    user = fixture.finish(fixture.call('POST', '/users', body=body))
    snapshot.match('user-operation', operation(user))
    snapshot.match('users-list', names(fixture.call('GET', '/users')))
    path = f'{API_PREFIX}/projects/{fixture.project}/operations'
    listing = fixture.client.request('fixture', 'GET', path, query={'instance': fixture.instance},
                                     timeout=fixture.remaining())
    value = listing.json()
    types = sorted({item.get('operationType') for item in value.get('items', [])})
    snapshot.match('operations-list',
                   {'http_status': listing.status, 'kind': value.get('kind'), 'types': types})
    deleted_user = fixture.finish(fixture.call('DELETE', '/users', query={'name': USER}))
    snapshot.match('user-deleted', operation(deleted_user))
    deleted = fixture.finish(fixture.call('DELETE', '/databases/' + DATABASE))
    snapshot.match('database-deleted', operation(deleted))
    absent = fixture.call('GET', '/databases/' + DATABASE)
    snapshot.match('database-absent', {'http_status': absent.status})


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
    client = client_for(target, sandbox=sandbox, scope=scope)
    InstanceFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                    manifest=args.manifest).cleanup()
    sys.stdout.write('Owned Cloud SQL cleanup passed.\n')


if __name__ == '__main__':
    main()
