"""Bounded Artifact Registry repository recording with explicit identities and owned cleanup."""

import argparse
import time
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = 'artifactregistry'
LIMITS = {'max_repositories': 1, 'max_artifacts': 0, 'max_run_seconds': 600}
LIVE_ENDPOINT = 'https://artifactregistry.googleapis.com'
LOCATION = 'us-central1'


def load_scope(filename, sandbox):
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox)


class RepositoryFixture(OwnedRun):
    """One empty labeled DOCKER repository; nothing is pushed."""

    def __init__(self, client, state_dir, *, manifest=None):
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('repository') == 'gl-' + v['run_id'][:12])
        self.manifest.setdefault('repository', 'gl-' + self.manifest['run_id'][:12])
        self.repository = self.manifest['repository']
        self.parent = f'/v1/projects/{self.project}/locations/{LOCATION}'
        self.labels = {'managed-by': 'gcp-local', 'gcp-local-run': self.manifest['run_id']}

    def call(self, method, suffix='', *, actor='fixture', body=None, query=None):
        return self.client.request(actor, method, f'{self.parent}/repositories/{self.repository}{suffix}',
                                   body=body, query=query, timeout=self.remaining())

    def create(self, fmt='DOCKER'):
        return self.client.request('fixture', 'POST', f'{self.parent}/repositories', timeout=self.remaining(),
                                   query={'repositoryId': self.repository},
                                   body={'format': fmt, 'description': 'gcp-local conformance', 'labels': self.labels})

    def finish(self, response):
        """Poll a long-running operation to completion; return the final operation."""
        operation = response.json()
        while response.status == 200 and not operation.get('done'):
            time.sleep(2 if self.client.sandbox else 0)
            name = operation['name'].split('/', 2)[2]  # drop "projects/<id-or-number>"
            response = self.client.request('fixture', 'GET', f'/v1/projects/{self.project}/{name}',
                                           timeout=self.remaining())
            operation = response.json()
        return response

    def setup(self):
        self.acquire()
        self.client.verify_project()
        listing = self.client.request('fixture', 'GET', f'{self.parent}/repositories')
        if listing.status != 200:
            raise Inconclusive(f'Repository listing returned HTTP {listing.status}')
        if listing.json().get('repositories'):
            raise Inconclusive('Artifact Registry sandbox location is occupied')
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.create()

    def owned(self, response):
        value = response.json()
        if not value.get('name', '').endswith('/repositories/' + self.repository) or value.get('labels') != self.labels:
            raise ValueError('Refusing repository cleanup: ownership does not match the manifest')

    def cleanup(self):
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            path = f'{self.parent}/repositories/{self.repository}'
            current = self.client.request('fixture', 'GET', path)
            if current.status == 200:
                self.owned(current)
                deleted = self.finish(self.client.request('fixture', 'DELETE', path))
                if deleted.status not in {200, 404} or 'error' in deleted.json():
                    raise ValueError(f'Owned repository delete returned HTTP {deleted.status}')
            elif current.status != 404:
                raise ValueError(f'Owned repository read returned HTTP {current.status}')
            if self.client.request('fixture', 'GET', path).status != 404:
                raise ValueError('Owned repository remains after deletion')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()


def operation(observe, response):
    value = response.json()
    result = value.get('response', {})
    return {'http_status': response.status, 'fields': sorted(value), 'done': value.get('done'),
            'response_type': result.get('@type'), 'response_fields': sorted(result),
            'error': value.get('error', {}).get('code')}


def run_scenario(fixture, snapshot, created):
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.repository: '<repository>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>',
                            **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    snapshot.match('create-operation', operation(observe, fixture.finish(created)))
    read = fixture.call('GET')
    fixture.owned(read)
    snapshot.match('fixture-read', observe.select(read, ('name', 'format', 'mode', 'labels', 'description')))
    snapshot.match('denied-read', observe.error(fixture.call('GET', actor='denied')))
    policy = fixture.call('GET', ':getIamPolicy')
    snapshot.match('policy-read', observe.select(policy, ('bindings',)))
    granted = fixture.call('POST', ':setIamPolicy', body={'policy': {
        'etag': policy.json().get('etag', ''),
        'bindings': [{'role': 'roles/artifactregistry.reader', 'members': ['serviceAccount:' + client.emails['allowed']]}]}})
    snapshot.match('reader-granted', observe.select(granted, ('bindings',)))
    allowed = fixture.wait_until(lambda: fixture.call('GET', actor='allowed'), {403}, 180)
    snapshot.match('allowed-read', observe.select(allowed, ('name',)))
    snapshot.match('allowed-cannot-update', observe.error(fixture.call(
        'PATCH', actor='allowed', query={'updateMask': 'description'}, body={'description': 'not permitted'})))
    patched = fixture.call('PATCH', query={'updateMask': 'description'}, body={'description': 'gcp-local renamed'})
    snapshot.match('patched', observe.select(patched, ('description', 'labels')))
    snapshot.match('duplicate-create', observe.error(fixture.create()))
    snapshot.match('invalid-format', observe.error(fixture.create('NOT_A_FORMAT')))
    snapshot.match('delete-operation', operation(observe, fixture.finish(fixture.call('DELETE'))))
    snapshot.match('absent', observe.error(fixture.call('GET')))


def main(argv=None):
    parser = argparse.ArgumentParser(description='Clean only the owned repository from an interrupted run')
    parser.add_argument('command', choices=['cleanup'])
    parser.add_argument('--profile', required=True)
    parser.add_argument('--scope', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--allow-live', action='store_true', required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox)
    target = Target.select('record', project=sandbox.project, credentials_file=sandbox.identities['allowed'].credentials_file,
                           live_scope=sandbox.live_scope, allow_live=args.allow_live)
    client = RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    RepositoryFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                      manifest=args.manifest).cleanup()
    print('Owned repository cleanup passed.')


if __name__ == '__main__':
    main()
