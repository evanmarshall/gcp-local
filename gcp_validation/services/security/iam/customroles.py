"""Bounded IAM custom-role recording: one project role granted on one owned service account."""

import argparse
import sys
from pathlib import Path
from typing import Protocol

from gcp_validation.config import Target
from gcp_validation.evidence import now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = 'customroles'
LIMITS = {'max_custom_roles': 1, 'max_service_accounts': 1, 'max_run_seconds': 1800}
LIVE_ENDPOINT = 'https://iam.googleapis.com'
GET = 'iam.serviceAccounts.get'
LIST = 'iam.serviceAccounts.list'
ROLE_FIELDS = ('name', 'title', 'description', 'includedPermissions', 'stage', 'deleted')
PROPAGATION_SECONDS = 420


class Snapshot(Protocol):
    """The snapshot fixture's matching interface."""

    def match(self, name: str, value: object) -> None:
        """Record or compare one observation."""


def load_scope(filename: str | None, sandbox: Sandbox | None, mode: str) -> LiveScope:
    """Load the owner-approved custom-role scope."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class CustomRoleFixture(OwnedRun):
    """One custom role and one service account, both named after the run.

    A deleted role's ID stays reserved for weeks, so every run uses a new ID.
    """

    def __init__(self, client: RestClient, state_dir: Path, *, manifest: str | None = None) -> None:
        """Name the run's role and account; a manifest resumes an interrupted run."""
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('role_id') == 'gcl_' + v['run_id'][:12])
        run = self.manifest['run_id']
        self.manifest.setdefault('role_id', 'gcl_' + run[:12])
        self.manifest.setdefault('account_id', 'glr-' + run[:12])
        self.role_id = self.manifest['role_id']
        self.roles = f'/v1/projects/{self.project}/roles'
        self.role = f'{self.roles}/{self.role_id}'
        self.role_name = f'projects/{self.project}/roles/{self.role_id}'
        self.email = f"{self.manifest['account_id']}@{self.project}.iam.gserviceaccount.com"
        self.account = f'/v1/projects/{self.project}/serviceAccounts/{self.email}'
        self.description = 'gcp-local conformance run ' + run
        self.etag = ''

    def call(self, method: str, path: str, *, actor: str = 'fixture', body: object = None,
             query: dict | None = None) -> Response:
        """Send one request within the run deadline."""
        return self.client.request(actor, method, path, body=body, query=query,
                                   timeout=self.remaining())

    def create_role(self, permissions: list[str], *, role_id: str | None = None,
                    actor: str = 'fixture', stage: str = 'GA') -> Response:
        """Create the run's role, or another ID that GCP is expected to refuse."""
        return self.call('POST', self.roles, actor=actor, body={
            'roleId': role_id or self.role_id, 'role': {
                'title': 'gcp-local conformance', 'description': self.description,
                'includedPermissions': permissions, 'stage': stage}})

    def set_policy(self, bindings: list[dict], etag: str | None = None) -> Response:
        """Replace the account's allow policy."""
        policy = {'bindings': bindings}
        if etag:
            policy['etag'] = etag
        return self.call('POST', self.account + ':setIamPolicy', body={'policy': policy})

    def setup(self) -> Response:
        """Take the lock, record the attempt, and create the account."""
        self.acquire()
        self.client.verify_project()
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.call('POST', f'/v1/projects/{self.project}/serviceAccounts', body={
            'accountId': self.manifest['account_id'],
            'serviceAccount': {'displayName': 'gcp-local conformance',
                               'description': self.description}})

    def cleanup(self) -> None:
        """Delete the owned role (its ID stays reserved) and the owned account."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            self.remove_role()
            self.remove_account()
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()

    def remove_role(self) -> None:
        """Soft-delete the role unless it is already deleted or absent."""
        current = self.client.request('fixture', 'GET', self.role)
        if current.status == 200:
            if current.json().get('description') != self.description:
                raise ValueError('Refusing custom-role cleanup: ownership does not match')
            if not current.json().get('deleted'):
                deleted = self.client.request('fixture', 'DELETE', self.role)
                if deleted.status != 200:
                    raise ValueError(f'Owned custom-role delete returned HTTP {deleted.status}')
        elif current.status != 404:
            raise ValueError(f'Owned custom-role read returned HTTP {current.status}')
        after = self.client.request('fixture', 'GET', self.role)
        if after.status == 200 and not after.json().get('deleted'):
            raise ValueError('Owned custom role is still active after deletion')

    def remove_account(self) -> None:
        """Delete the account and confirm it is gone."""
        current = self.client.request('fixture', 'GET', self.account)
        if current.status == 200:
            if current.json().get('description') != self.description:
                raise ValueError('Refusing service-account cleanup: ownership does not match')
            deleted = self.client.request('fixture', 'DELETE', self.account)
            if deleted.status not in {200, 404}:
                raise ValueError(f'Owned service-account delete returned HTTP {deleted.status}')
        elif current.status != 404:
            raise ValueError(f'Owned service-account read returned HTTP {current.status}')
        if self.client.request('fixture', 'GET', self.account).status != 404:
            raise ValueError('Owned service account remains after deletion')


def listed(fixture: CustomRoleFixture, query: dict | None) -> dict:
    """Select the scenario role from a project role list, ignoring other roles."""
    response = fixture.call('GET', fixture.roles, query=query)
    roles = [role for role in response.json().get('roles', [])
             if role.get('name') == fixture.role_name]
    selected = {'http_status': response.status, 'present': bool(roles)}
    if roles:
        selected.update(fields=sorted(roles[0]), deleted=roles[0].get('deleted'))
    return selected


def reads(fixture: CustomRoleFixture, retry: set[int], expected: int) -> Response:
    """Wait until the allowed caller's account read stops returning a retry status."""
    response = fixture.wait_until(lambda: fixture.call('GET', fixture.account, actor='allowed'),
                                  retry, PROPAGATION_SECONDS)
    if response.status != expected:
        raise AssertionError(f'Allowed read returned HTTP {response.status}, not {expected}')
    return response


def run_scenario(fixture: CustomRoleFixture, snapshot: Snapshot, created: Response) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({
        fixture.email: '<account>', fixture.role_id: '<role>',
        fixture.manifest['run_id']: '<run>', fixture.project: '<project>',
        client.number: '<project-number>',
        **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    if created.status != 200:
        raise AssertionError(f'Service-account create returned HTTP {created.status}')
    member = 'serviceAccount:' + client.emails['allowed']
    lifecycle(fixture, snapshot, observe)
    refusals(fixture, snapshot, observe)
    grants(fixture, snapshot, observe, member)
    bindings(fixture, snapshot, observe, member)


def lifecycle(fixture: CustomRoleFixture, snapshot: Snapshot, observe: Observations) -> None:
    """Create, read, and list the role; refuse a duplicate and an unauthorized caller."""
    role = fixture.create_role([GET])
    snapshot.match('role-created', observe.select(role, ROLE_FIELDS))
    fixture.etag = role.json().get('etag', '')
    read = fixture.wait_until(lambda: fixture.call('GET', fixture.role), {404}, 60)
    snapshot.match('role-read', observe.select(read, ROLE_FIELDS))
    snapshot.match('role-listed', listed(fixture, None))
    snapshot.match('role-listed-full', listed(fixture, {'view': 'FULL'}))
    snapshot.match('duplicate-create', observe.error(fixture.create_role([GET])))
    denied = fixture.create_role([GET], role_id=fixture.role_id + 'x', actor='denied')
    snapshot.match('denied-create', observe.error(denied))
    snapshot.match('denied-read', observe.error(fixture.call('GET', fixture.role, actor='denied')))
    with_view = fixture.call('GET', fixture.role, query={'view': 'FULL'})
    snapshot.match('read-with-view', observe.error(with_view))
    undelete = fixture.call('POST', fixture.role + ':undelete', body={})
    snapshot.match('not-deleted-undelete', observe.error(undelete))


def refusals(fixture: CustomRoleFixture, snapshot: Snapshot, observe: Observations) -> None:
    """Check the role IDs, permissions, and stages GCP refuses at create."""
    other = fixture.role_id + 'x'
    for name, permission in {
        'unknown-permission': 'storage.bogus.get',
        'unsupported-permission': 'stackdriver.projects.edit',
        'above-project-permission': 'resourcemanager.projects.list',
    }.items():
        snapshot.match(name, observe.error(fixture.create_role([permission], role_id=other)))
    snapshot.match('invalid-role-id', observe.error(fixture.create_role([GET], role_id='a')))
    staged = fixture.create_role([GET], role_id=other, stage='WEIRD')
    snapshot.match('invalid-stage', observe.error(staged))


def grants(fixture: CustomRoleFixture, snapshot: Snapshot, observe: Observations,
           member: str) -> None:
    """Bind the role, then follow the grant through delete, undelete, and patch."""
    before = fixture.call('GET', fixture.account, actor='allowed')
    snapshot.match('allowed-before-grant', observe.error(before))
    granted = fixture.set_policy([{'role': fixture.role_name, 'members': [member]}])
    snapshot.match('role-granted', observe.select(granted, ('bindings',)))
    snapshot.match('allowed-read', observe.select(reads(fixture, {403}, 200), ('email',)))
    deleted = fixture.call('DELETE', fixture.role)
    snapshot.match('role-deleted', observe.select(deleted, ROLE_FIELDS))
    if deleted.json().get('etag') == fixture.etag:
        raise AssertionError('Deleting the role did not change its etag')
    snapshot.match('allowed-after-delete', observe.error(reads(fixture, {200}, 403)))
    snapshot.match('deleted-read', observe.select(fixture.call('GET', fixture.role), ROLE_FIELDS))
    snapshot.match('deleted-listed', listed(fixture, None))
    snapshot.match('deleted-listed-shown', listed(fixture, {'showDeleted': 'true'}))
    snapshot.match('deleted-delete', observe.error(fixture.call('DELETE', fixture.role)))
    renamed = fixture.call('PATCH', fixture.role, query={'updateMask': 'title'},
                           body={'title': 'renamed'})
    snapshot.match('deleted-patch', observe.error(renamed))
    snapshot.match('deleted-id-create', observe.error(fixture.create_role([GET])))
    restored = fixture.call('POST', fixture.role + ':undelete', body={})
    snapshot.match('role-undeleted', observe.select(restored, ROLE_FIELDS))
    after = reads(fixture, {403}, 200)
    snapshot.match('allowed-after-undelete', observe.select(after, ('email',)))
    stale = fixture.call('PATCH', fixture.role, query={'updateMask': 'title'},
                         body={'title': 'stale', 'etag': fixture.etag})
    snapshot.match('stale-etag-patch', observe.error(stale))
    patched = fixture.call('PATCH', fixture.role, query={'updateMask': 'includedPermissions'},
                           body={'includedPermissions': [LIST],
                                 'etag': restored.json().get('etag')})
    snapshot.match('role-patched', observe.select(patched, ROLE_FIELDS))
    snapshot.match('allowed-after-patch', observe.error(reads(fixture, {200}, 403)))


def bindings(fixture: CustomRoleFixture, snapshot: Snapshot, observe: Observations,
             member: str) -> None:
    """Write policies naming a deleted, a missing, and a malformed role."""
    fixture.call('DELETE', fixture.role)
    policy = fixture.call('POST', fixture.account + ':getIamPolicy', body={})
    snapshot.match('policy-with-deleted-role', observe.select(policy, ('bindings',)))
    unchanged = fixture.set_policy(policy.json().get('bindings', []), policy.json().get('etag'))
    snapshot.match('deleted-role-rewritten', observe.select(unchanged, ('bindings',)))
    members = [member, 'serviceAccount:' + fixture.email]
    added = fixture.set_policy([{'role': fixture.role_name, 'members': members}])
    snapshot.match('deleted-role-new-member', observe.error(added))
    for name, role in {
        'missing-role': f'projects/{fixture.project}/roles/gcl_missing_role',
        'other-project-role': 'projects/gcp-local-other-project/roles/gcl_missing_role',
        'malformed-role': f'projects/{fixture.project}/roles/a',
    }.items():
        response = fixture.set_policy([{'role': role, 'members': [member]}])
        snapshot.match(name, observe.error(response))


def main(argv: list[str] | None = None) -> None:
    """Clean only the owned role and account from an interrupted run."""
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
    state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
    CustomRoleFixture(client, state, manifest=args.manifest).cleanup()
    sys.stdout.write('Owned custom-role cleanup passed.\n')


if __name__ == '__main__':
    main()
