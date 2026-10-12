"""Bounded Cloud SQL for MySQL admin recording on the smallest shared-core tier.

It reuses the PostgreSQL scenario's transport and owned-instance fixture (the same
label-checked cleanup and per-project lock) with a MySQL 8.0 instance, and observes
the MySQL-specific shapes: charset and collation on databases, the default and system
schemas, and users addressed by name and host. As in the PostgreSQL scenario, the
``denied`` identity has no Cloud SQL role in the project and project IAM is never edited.
"""

import argparse
import secrets
import sys
from pathlib import Path

from .cloudsql import (
    REGION,
    SERVICE,
    TIER,
    InstanceFixture,
    SqlAdminClient,
    client_for,
    instance_view,
    names,
    operation,
)
from .config import Target
from .evidence import SnapshotCase
from .live_rest import LiveScope, Observations, Response
from .sandbox import Sandbox

LIMITS = {'max_instances': 1, 'max_databases': 2, 'max_users': 1, 'max_run_seconds': 2700}
VERSION = 'MYSQL_8_0'
DATABASE = 'gl_db'
DEFAULT_DATABASE = 'gl_default'
CHARSET, COLLATION = 'utf8mb4', 'utf8mb4_unicode_ci'
USER, HOST = 'gl_user', '%'
DATABASE_FIELDS = ('kind', 'name', 'charset', 'collation', 'instance', 'project')


def load_scope(filename: str, sandbox: Sandbox) -> LiveScope:
    """Load the separately approved Cloud SQL for MySQL live scope."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox)


class MySQLInstanceFixture(InstanceFixture):
    """One labeled db-f1-micro MySQL instance; cleanup deletes it with its contents."""

    def create_instance(self) -> Response:
        """Request the owned instance; the root password is never stored."""
        settings = {'tier': TIER, 'edition': 'ENTERPRISE', 'userLabels': self.labels,
                    'deletionProtectionEnabled': False, 'ipConfiguration': {'ipv4Enabled': True}}
        return self.client.request('fixture', 'POST', self.base, timeout=self.remaining(), body={
            'name': self.instance, 'region': REGION, 'databaseVersion': VERSION,
            'rootPassword': secrets.token_urlsafe(24), 'settings': settings})


def users(observe: Observations, response: Response) -> dict:
    """Select each user's name and host, sorted."""
    items = response.json().get('items', [])
    fields = sorted({key for item in items for key in item})
    pairs = sorted([item.get('name'), observe.value(item.get('host', ''))] for item in items)
    return {'http_status': response.status, 'fields': fields, 'users': pairs}


def run_scenario(fixture: MySQLInstanceFixture, snapshot: SnapshotCase, created: Response) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.instance: '<instance>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>',
                            **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    snapshot.match('instance-operation', operation(fixture.finish(created)))
    snapshot.match('instance-read', instance_view(observe, fixture.call('GET')))
    snapshot.match('denied-read', observe.error(fixture.call('GET', actor='denied')))
    snapshot.match('denied-create-database', observe.error(fixture.call(
        'POST', '/databases', actor='denied', body={'name': DATABASE})))
    snapshot.match('initial-databases', names(fixture.call('GET', '/databases')))
    snapshot.match('initial-users', users(observe, fixture.call('GET', '/users')))
    run_databases(fixture, snapshot, observe)
    run_user(fixture, snapshot, observe)
    snapshot.match('instance-deleted', operation(fixture.finish(fixture.call('DELETE'))))
    snapshot.match('instance-absent', observe.error(fixture.call('GET')))


def run_databases(fixture: MySQLInstanceFixture, snapshot: SnapshotCase,
                  observe: Observations) -> None:
    """Create one database with an explicit charset and collation and one with defaults."""
    body = {'name': DATABASE, 'charset': CHARSET, 'collation': COLLATION}
    snapshot.match('database-operation', operation(fixture.finish(fixture.call(
        'POST', '/databases', body=body))))
    snapshot.match('database-read', observe.select(fixture.call('GET', '/databases/' + DATABASE),
                                                   DATABASE_FIELDS))
    duplicate = fixture.call('POST', '/databases', body=body)
    snapshot.match('duplicate-database', observe.error(duplicate))
    snapshot.match('default-database-operation', operation(fixture.finish(fixture.call(
        'POST', '/databases', body={'name': DEFAULT_DATABASE}))))
    snapshot.match('default-database-read', observe.select(
        fixture.call('GET', '/databases/' + DEFAULT_DATABASE), DATABASE_FIELDS))
    snapshot.match('databases-list', names(fixture.call('GET', '/databases')))
    deletes = (('database-deleted', DATABASE), ('default-database-deleted', DEFAULT_DATABASE))
    for label, name in deletes:
        deleted = fixture.finish(fixture.call('DELETE', '/databases/' + name))
        snapshot.match(label, operation(deleted))
    absent = fixture.call('GET', '/databases/' + DATABASE)
    snapshot.match('database-absent', {'http_status': absent.status})


def run_user(fixture: MySQLInstanceFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Create, list, and delete one built-in user addressed by name and host."""
    body = {'name': USER, 'host': HOST, 'password': secrets.token_urlsafe(24)}
    created = fixture.finish(fixture.call('POST', '/users', body=body))
    snapshot.match('user-operation', operation(created))
    snapshot.match('users-list', users(observe, fixture.call('GET', '/users')))
    deleted = fixture.finish(fixture.call('DELETE', '/users', query={'name': USER, 'host': HOST}))
    snapshot.match('user-deleted', operation(deleted))
    snapshot.match('users-after-delete', users(observe, fixture.call('GET', '/users')))


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
    scope = load_scope(args.scope, sandbox)
    target = Target.select('record', project=sandbox.project,
                           credentials_file=sandbox.identities['allowed'].credentials_file,
                           live_scope=sandbox.live_scope, allow_live=args.allow_live)
    client: SqlAdminClient = client_for(target, sandbox=sandbox, scope=scope)
    MySQLInstanceFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                         manifest=args.manifest).cleanup()
    sys.stdout.write('Owned Cloud SQL for MySQL cleanup passed.\n')


if __name__ == '__main__':
    main()
