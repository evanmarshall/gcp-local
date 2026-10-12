"""Bounded IAM service-account recording with explicit identities and owned cleanup."""

import argparse
import re
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = 'serviceaccounts'
LIMITS = {'max_service_accounts': 1, 'max_keys': 0, 'max_run_seconds': 600}
LIVE_ENDPOINT = 'https://iam.googleapis.com'


def load_scope(filename, sandbox, mode: str):
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class ServiceAccountFixture(OwnedRun):
    """One service account whose description names the run; no keys are created."""

    def __init__(self, client, state_dir, *, manifest=None):
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('account_id') == 'gl-' + v['run_id'][:12])
        self.manifest.setdefault('account_id', 'gl-' + self.manifest['run_id'][:12])
        self.account_id = self.manifest['account_id']
        self.email = f'{self.account_id}@{self.project}.iam.gserviceaccount.com'
        self.path_prefix = f'/v1/projects/{self.project}/serviceAccounts'
        self.description = 'gcp-local conformance run ' + self.manifest['run_id']

    def call(self, method, suffix='', *, actor='fixture', body=None, query=None):
        return self.client.request(actor, method, f'{self.path_prefix}/{self.email}{suffix}',
                                   body=body, query=query, timeout=self.remaining())

    def create(self, account_id=None):
        return self.client.request('fixture', 'POST', self.path_prefix, timeout=self.remaining(), body={
            'accountId': account_id or self.account_id,
            'serviceAccount': {'displayName': 'gcp-local conformance', 'description': self.description}})

    def setup(self):
        self.acquire()
        self.client.verify_project()
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.create()

    def owned(self, response):
        value = response.json()
        if value.get('email') != self.email or value.get('description') != self.description:
            raise ValueError('Refusing service-account cleanup: ownership does not match the manifest')

    def cleanup(self):
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            path = f'{self.path_prefix}/{self.email}'
            current = self.client.request('fixture', 'GET', path)
            if current.status == 200:
                self.owned(current)
                deleted = self.client.request('fixture', 'DELETE', path)
                if deleted.status not in {200, 404}:
                    raise ValueError(f'Owned service-account delete returned HTTP {deleted.status}')
            elif current.status != 404:
                raise ValueError(f'Owned service-account read returned HTTP {current.status}')
            if self.client.request('fixture', 'GET', path).status != 404:
                raise ValueError('Owned service account remains after deletion')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()


def run_scenario(fixture, snapshot, created):
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.email: '<account>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>',
                            **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    if created.status != 200:
        raise AssertionError(f'Service-account create returned HTTP {created.status}')
    fixture.owned(created)
    value = created.json()
    snapshot.match('created', {**observe.select(created, ('name', 'email', 'displayName', 'description',
                                                          'projectId')),
                               'unique_id_numeric': bool(re.fullmatch('[0-9]{21}', value.get('uniqueId', ''))),
                               'client_id_equals_unique_id': value.get('oauth2ClientId') == value.get('uniqueId')})
    read = fixture.wait_until(lambda: fixture.call('GET'), {404}, 60)
    snapshot.match('fixture-read', observe.select(read, ('name', 'email', 'displayName')))
    snapshot.match('denied-read', observe.error(fixture.call('GET', actor='denied')))
    policy = fixture.wait_until(lambda: fixture.call('POST', ':getIamPolicy', body={}), {404}, 60)
    snapshot.match('policy-read', observe.select(policy, ('bindings',)))
    granted = fixture.call('POST', ':setIamPolicy', body={'policy': {
        'etag': policy.json().get('etag', ''),
        'bindings': [{'role': 'roles/iam.serviceAccountUser', 'members': ['serviceAccount:' + client.emails['allowed']]}]}})
    snapshot.match('user-granted', observe.select(granted, ('bindings',)))
    allowed = fixture.wait_until(lambda: fixture.call('GET', actor='allowed'), {403}, 180)
    snapshot.match('allowed-read', observe.select(allowed, ('email',)))
    snapshot.match('allowed-cannot-update', observe.error(fixture.call(
        'PATCH', actor='allowed', body={'serviceAccount': {'displayName': 'not permitted'}, 'updateMask': 'displayName'})))
    patched = fixture.call('PATCH', body={'serviceAccount': {'displayName': 'gcp-local renamed'},
                                          'updateMask': 'displayName'})
    snapshot.match('patched', observe.select(patched, ('displayName', 'description')))
    snapshot.match('duplicate-create', observe.error(fixture.create()))
    snapshot.match('invalid-id', observe.error(fixture.create('ab')))
    deleted = fixture.call('DELETE')
    snapshot.match('deleted', {'http_status': deleted.status, 'body': deleted.json()})
    absent = fixture.wait_until(lambda: fixture.call('GET'), {200}, 60)
    snapshot.match('absent', observe.error(absent))


def main(argv=None):
    parser = argparse.ArgumentParser(description='Clean only the owned service account from an interrupted run')
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
    client = RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    ServiceAccountFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                          manifest=args.manifest).cleanup()
    print('Owned service-account cleanup passed.')


if __name__ == '__main__':
    main()
