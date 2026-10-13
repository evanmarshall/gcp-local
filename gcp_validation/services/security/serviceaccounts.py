"""Bounded IAM service-account recording with explicit identities and owned cleanup."""

import argparse
import re
import time
from collections.abc import Callable
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.security.refusals import outcome

SERVICE = 'serviceaccounts'
LIMITS = {'max_service_accounts': 2, 'max_keys': 0, 'max_run_seconds': 900}
LIVE_ENDPOINT = 'https://iam.googleapis.com'


def load_scope(filename, sandbox, mode: str):
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class ServiceAccountFixture(OwnedRun):
    """One service account whose description names the run; no keys are created."""

    def __init__(self, client, state_dir, *, manifest=None):
        super().__init__(
            client,
            state_dir,
            SERVICE,
            LIMITS['max_run_seconds'],
            manifest=manifest,
            check=lambda v: (
                v.get('account_id') == 'gl-' + v['run_id'][:12]
                and v.get('second_id', 'gl--' + v['run_id'][:12]) == 'gl--' + v['run_id'][:12]
            ),
        )
        self.manifest.setdefault('account_id', 'gl-' + self.manifest['run_id'][:12])
        # Sorts before the first account ('-' < any hex digit) and is created after it.
        self.manifest.setdefault('second_id', 'gl--' + self.manifest['run_id'][:12])
        self.account_id = self.manifest['account_id']
        self.email = f'{self.account_id}@{self.project}.iam.gserviceaccount.com'
        self.second = f'{self.manifest["second_id"]}@{self.project}.iam.gserviceaccount.com'
        self.path_prefix = f'/v1/projects/{self.project}/serviceAccounts'
        self.description = 'gcp-local conformance run ' + self.manifest['run_id']

    def call(
        self,
        method: str,
        suffix: str = '',
        *,
        actor: str = 'fixture',
        body: object = None,
        query: dict | None = None,
        email: str | None = None,
    ) -> Response:
        return self.client.request(
            actor,
            method,
            f'{self.path_prefix}/{email or self.email}{suffix}',
            body=body,
            query=query,
            timeout=self.remaining(),
        )

    def create(self, account_id: str | None = None, **extra: object) -> Response:
        """Create an owned account; extra adds top-level body fields."""
        return self.client.request(
            'fixture',
            'POST',
            self.path_prefix,
            timeout=self.remaining(),
            body={
                'accountId': account_id or self.account_id,
                'serviceAccount': {
                    'displayName': 'gcp-local conformance',
                    'description': self.description,
                },
                **extra,
            },
        )

    def list(self, **query: object) -> Response:
        """List the project's accounts."""
        return self.client.request(
            'fixture', 'GET', self.path_prefix, query=query or None, timeout=self.remaining()
        )

    def setup(self):
        self.acquire()
        self.client.verify_project()
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.create()

    def owned(self, response: Response, email: str | None = None) -> None:
        """Refuse cleanup of an account the manifest doesn't own."""
        value = response.json()
        if (
            value.get('email') != (email or self.email)
            or value.get('description') != self.description
        ):
            raise ValueError(
                'Refusing service-account cleanup: ownership does not match the manifest'
            )

    def remove(self, email: str) -> None:
        """Delete one owned account and wait until reads miss it."""
        path = f'{self.path_prefix}/{email}'
        current = self.client.request('fixture', 'GET', path)
        if current.status == 200:
            self.owned(current, email)
            deleted = self.client.request('fixture', 'DELETE', path)
            if deleted.status not in {200, 404}:
                raise ValueError(f'Owned service-account delete returned HTTP {deleted.status}')
        elif current.status != 404:
            raise ValueError(f'Owned service-account read returned HTTP {current.status}')
        # Deletion propagates; a read may still find the account for a few seconds.
        deadline = time.monotonic() + (60 if self.client.sandbox else 0)
        while self.client.request('fixture', 'GET', path).status != 404:
            if time.monotonic() >= deadline:
                raise ValueError('Owned service account remains after deletion')
            time.sleep(5)

    def cleanup(self):
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            for email in (self.email, self.second):
                self.remove(email)
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()


def poll(
    fixture: ServiceAccountFixture,
    operation: Callable[[], Response],
    retry: frozenset = frozenset({403}),
    seconds: int = 180,
) -> Response:
    """Retry while a fresh grant propagates (live only); return the last response, allowed or not."""
    deadline = min(fixture.deadline, time.monotonic() + (seconds if fixture.client.sandbox else 0))
    while True:
        response = operation()
        if response.status not in retry or time.monotonic() >= deadline:
            return response
        time.sleep(10)


def observe_conditions(
    fixture: ServiceAccountFixture,
    snapshot: object,
    observe: Observations,
    policies: list[Response],
    unique_ids: tuple[str, str],
) -> None:
    """Grant a conditional viewer role on each account and observe whether reads are allowed.

    The first condition names the account's type and IAM resource name; the
    second names no resource attribute, so it separates conditions on account
    policies from the resource attributes they see.
    """
    member = 'serviceAccount:' + fixture.client.emails['allowed']
    expressions = (
        'resource.type == "iam.googleapis.com/ServiceAccount" && '
        f'resource.name == "projects/-/serviceAccounts/{unique_ids[0]}"',
        'request.time < timestamp("2100-01-01T00:00:00Z")',
    )
    emails = (fixture.email, fixture.second)
    for label, email, policy, expression in zip(
        ('first', 'second'), emails, policies, expressions, strict=True
    ):
        granted = fixture.call(
            'POST',
            ':setIamPolicy',
            email=email,
            body={
                'policy': {
                    'version': 3,
                    'etag': policy.json().get('etag', ''),
                    'bindings': [
                        {
                            'role': 'roles/iam.serviceAccountViewer',
                            'members': [member],
                            'condition': {'title': 'account-' + label, 'expression': expression},
                        }
                    ],
                }
            },
        )
        snapshot.match(
            'conditions-granted-' + label,
            {
                'http_status': granted.status,
                'version': granted.json().get('version'),
                'bindings': observe.value(granted.json().get('bindings')),
            },
        )
    snapshot.match(
        'conditional-access',
        {
            label: poll(
                fixture, lambda email=email: fixture.call('GET', actor='allowed', email=email)
            ).status
            for label, email in zip(('first', 'second'), emails, strict=True)
        },
    )


def observe_keys(fixture: ServiceAccountFixture, snapshot: object, observe: Observations) -> None:
    """Observe key types listed under a key-admin grant on the account, and a refused body.

    The sandbox organization forbids user-managed keys
    (iam.managed.disableServiceAccountKeyCreation), so none is created.
    """

    def types(**query: str) -> dict:
        listed = fixture.call('GET', '/keys', query=query or None)
        return {
            'http_status': listed.status,
            'types': [key.get('keyType') for key in listed.json().get('keys', [])],
        }

    poll(fixture, lambda: fixture.call('GET', '/keys'))
    snapshot.match(
        'keys-listed',
        {
            'all': types(),
            'user': types(keyTypes='USER_MANAGED'),
            'system': types(keyTypes='SYSTEM_MANAGED'),
        },
    )
    snapshot.match(
        'key-unknown-field',
        outcome(observe, fixture.call('POST', '/keys', body={'unknownField': 'x'})),
    )


def observe_decoding(
    fixture: ServiceAccountFixture, snapshot: object, observe: Observations
) -> None:
    """Observe refused create bodies, each naming the second account, so a refusal leaves nothing behind."""
    second = fixture.manifest['second_id']
    snapshot.match('unknown-field', outcome(observe, fixture.create(second, unknownField='x')))
    snapshot.match(
        'top-level-display-name', outcome(observe, fixture.create(second, displayName='x'))
    )
    mistyped = fixture.client.request(
        'fixture',
        'POST',
        fixture.path_prefix,
        timeout=fixture.remaining(),
        body={'accountId': second, 'serviceAccount': {'displayName': ['x']}},
    )
    snapshot.match('mistyped-field', outcome(observe, mistyped))


def observe_lists(fixture: ServiceAccountFixture, snapshot: object, observe: Observations) -> None:
    """Observe the two owned accounts' order, one-account pages, and refused page parameters.

    Unique ids are random in GCP, so the order is recorded as whether the
    accounts list by unique id.
    """
    owned = {fixture.email, fixture.second}
    unique_ids, token = [], ''
    for _ in range(20):
        page = fixture.list(pageSize=100, **({'pageToken': token} if token else {}))
        unique_ids += [
            item['uniqueId']
            for item in page.json().get('accounts', [])
            if item.get('email') in owned
        ]
        token = page.json().get('nextPageToken', '')
        if not token:
            break
    first = fixture.list(pageSize=1)
    token = first.json().get('nextPageToken', '')
    second = fixture.list(pageSize=1, pageToken=token)
    snapshot.match(
        'list-paged',
        {
            'owned_count': len(unique_ids),
            'owned_in_unique_id_order': unique_ids == sorted(unique_ids),
            'first_status': first.status,
            'first_count': len(first.json().get('accounts', [])),
            'first_has_next': bool(token),
            'second_status': second.status,
            'second_count': len(second.json().get('accounts', [])),
        },
    )
    snapshot.match(
        'list-invalid-token', outcome(observe, fixture.list(pageToken='not-a-page-token'))
    )
    snapshot.match('list-negative-size', outcome(observe, fixture.list(pageSize=-1)))
    snapshot.match('list-oversize', outcome(observe, fixture.list(pageSize=101)))


def run_scenario(fixture, snapshot, created):
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations(
        {
            fixture.email: '<account>',
            fixture.second: '<second>',
            fixture.manifest['run_id']: '<run>',
            fixture.manifest['run_id'][:12]: '<run12>',
            fixture.project: '<project>',
            client.number: '<project-number>',
            **{email: '<' + actor + '>' for actor, email in client.emails.items()},
        }
    )
    if created.status != 200:
        raise AssertionError(f'Service-account create returned HTTP {created.status}')
    fixture.owned(created)
    value = created.json()
    observe.aliases[value.get('uniqueId', '<none>')] = '<unique-id>'
    snapshot.match(
        'created',
        {
            **observe.select(created, ('name', 'email', 'displayName', 'description', 'projectId')),
            'unique_id_numeric': bool(re.fullmatch('[0-9]{21}', value.get('uniqueId', ''))),
            'client_id_equals_unique_id': value.get('oauth2ClientId') == value.get('uniqueId'),
        },
    )
    read = fixture.wait_until(lambda: fixture.call('GET'), {404}, 60)
    snapshot.match('fixture-read', observe.select(read, ('name', 'email', 'displayName')))
    snapshot.match('denied-read', outcome(observe, fixture.call('GET', actor='denied')))
    snapshot.match(
        'denied-missing',
        outcome(observe, fixture.call('GET', actor='denied', email=fixture.second)),
    )
    snapshot.match('fixture-missing', outcome(observe, fixture.call('GET', email=fixture.second)))
    observe_decoding(fixture, snapshot, observe)
    second = fixture.create(fixture.manifest['second_id'])
    observe.aliases[second.json().get('uniqueId', '<none>')] = '<second-unique-id>'
    snapshot.match('second-created', observe.select(second, ('email',)))
    policies = [
        fixture.wait_until(
            lambda email=email: fixture.call('POST', ':getIamPolicy', body={}, email=email),
            {404},
            60,
        )
        for email in (fixture.email, fixture.second)
    ]
    snapshot.match('policy-read', observe.select(policies[0], ('bindings',)))
    observe_conditions(
        fixture,
        snapshot,
        observe,
        policies,
        (value.get('uniqueId', ''), second.json().get('uniqueId', '')),
    )
    current = fixture.call('POST', ':getIamPolicy', body={'options': {'requestedPolicyVersion': 3}})
    granted = fixture.call(
        'POST',
        ':setIamPolicy',
        body={
            'policy': {
                'version': 3,
                'etag': current.json().get('etag', ''),
                'bindings': [
                    {
                        'role': 'roles/iam.serviceAccountKeyAdmin',
                        'members': ['serviceAccount:' + client.emails['fixture']],
                    },
                    {
                        'role': 'roles/iam.serviceAccountUser',
                        'members': ['serviceAccount:' + client.emails['allowed']],
                    },
                ],
            }
        },
    )
    snapshot.match('user-granted', observe.select(granted, ('bindings',)))
    allowed = fixture.wait_until(lambda: fixture.call('GET', actor='allowed'), {403}, 180)
    snapshot.match('allowed-read', observe.select(allowed, ('email',)))
    snapshot.match(
        'allowed-cannot-update',
        outcome(
            observe,
            fixture.call(
                'PATCH',
                actor='allowed',
                body={
                    'serviceAccount': {'displayName': 'not permitted'},
                    'updateMask': 'displayName',
                },
            ),
        ),
    )
    patched = fixture.call(
        'PATCH',
        body={'serviceAccount': {'displayName': 'gcp-local renamed'}, 'updateMask': 'displayName'},
    )
    snapshot.match('patched', observe.select(patched, ('displayName', 'description')))
    snapshot.match('duplicate-create', observe.error(fixture.create()))
    snapshot.match('invalid-id', observe.error(fixture.create('ab')))
    observe_keys(fixture, snapshot, observe)
    observe_lists(fixture, snapshot, observe)
    deleted = fixture.call('DELETE')
    snapshot.match('deleted', {'http_status': deleted.status, 'body': deleted.json()})
    absent = fixture.wait_until(lambda: fixture.call('GET'), {200}, 60)
    snapshot.match('absent', observe.error(absent))


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Clean only the owned service account from an interrupted run'
    )
    parser.add_argument('command', choices=['cleanup'])
    parser.add_argument('--profile', required=True)
    parser.add_argument('--scope', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--allow-live', action='store_true', required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox, 'cleanup')
    target = Target.select(
        'record',
        project=sandbox.project,
        credentials_file=sandbox.identities['allowed'].credentials_file,
        live_scope=sandbox.live_scope,
        allow_live=args.allow_live,
    )
    client = RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    ServiceAccountFixture(
        client,
        Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
        manifest=args.manifest,
    ).cleanup()
    print('Owned service-account cleanup passed.')


if __name__ == '__main__':
    main()
