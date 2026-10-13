"""Bounded Artifact Registry repository recording with explicit identities and owned cleanup.

Owns three empty labeled DOCKER repositories: the run's ID and a sibling in us-central1,
and the same ID in us-east1, so the recording shows repositories keyed by location.
"""

import argparse
import time
from collections.abc import Callable
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.errors import detailed
from gcp_validation.evidence import Inconclusive, SnapshotCase, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.denials import iam_denial

SERVICE = 'artifactregistry'
LIMITS = {'max_repositories': 3, 'max_artifacts': 0, 'max_run_seconds': 600}
LIVE_ENDPOINT = 'https://artifactregistry.googleapis.com'
LOCATION = 'us-central1'
SECOND_LOCATION = 'us-east1'
ALWAYS = 'request.time < timestamp("2100-01-01T00:00:00Z")'


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the owner-approved Artifact Registry live scope."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class RepositoryFixture(OwnedRun):
    """Three empty labeled DOCKER repositories; nothing is pushed."""

    def __init__(self, client: RestClient, state_dir: Path, *, manifest: str | None = None) -> None:
        """Bind the owned repository names to this run's manifest."""
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('repository') == 'gl-' + v['run_id'][:12])
        self.manifest.setdefault('repository', 'gl-' + self.manifest['run_id'][:12])
        self.repository = self.manifest['repository']
        self.sibling = self.repository + '-b'
        self.refused = self.repository + '-c'  # every create is refused; cleanup still checks
        self.owned_repositories = ((LOCATION, self.repository), (LOCATION, self.sibling),
                                   (SECOND_LOCATION, self.repository), (LOCATION, self.refused))
        self.parent = f'/v1/projects/{self.project}/locations/{LOCATION}'
        self.labels = {'managed-by': 'gcp-local', 'gcp-local-run': self.manifest['run_id']}

    def call(self, method: str, suffix: str = '', *, actor: str = 'fixture',
             body: dict | None = None, query: dict | None = None) -> Response:
        """Call the run's us-central1 repository, or a method on it."""
        path = f'{self.parent}/repositories/{self.repository}{suffix}'
        return self.client.request(actor, method, path, body=body, query=query,
                                   timeout=self.remaining())

    def location(self, location: str, method: str = 'GET', suffix: str = '', *,
                 actor: str = 'fixture', body: dict | None = None,
                 query: dict | None = None) -> Response:
        """Call a location, or a path below it."""
        path = f'/v1/projects/{self.project}/locations/{location}{suffix}'
        return self.client.request(actor, method, path, body=body, query=query,
                                   timeout=self.remaining())

    def create(self, fmt: str = 'DOCKER', *, location: str = LOCATION,
               repository: str | None = None, extra: dict | None = None) -> Response:
        """Create one labeled repository; extra adds body fields."""
        body = {'format': fmt, 'description': 'gcp-local conformance', 'labels': self.labels,
                **(extra or {})}
        return self.location(location, 'POST', '/repositories', body=body,
                             query={'repositoryId': repository or self.repository})

    def operation_path(self, name: str) -> str:
        """Return an operation's path with the project ID in place of a project number."""
        return f'/v1/projects/{self.project}/' + name.split('/', 2)[2]

    def finish(self, response: Response) -> Response:
        """Poll a long-running operation to completion; return the final operation."""
        operation = response.json()
        while response.status == 200 and not operation.get('done'):
            time.sleep(2 if self.client.sandbox else 0.2)
            response = self.client.request('fixture', 'GET', self.operation_path(operation['name']),
                                           timeout=self.remaining())
            operation = response.json()
        return response

    def setup(self) -> Response:
        """Confirm the first location is empty, record the attempt, and create the repository."""
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

    def owned(self, response: Response, repository: str | None = None) -> None:
        """Refuse a repository whose name or labels aren't this run's."""
        value = response.json()
        suffix = '/repositories/' + (repository or self.repository)
        if not value.get('name', '').endswith(suffix) or value.get('labels') != self.labels:
            raise ValueError('Refusing repository cleanup: ownership does not match the manifest')

    def remove(self, location: str, repository: str) -> None:
        """Delete one owned repository if it exists and confirm it is gone."""
        path = f'/v1/projects/{self.project}/locations/{location}/repositories/{repository}'
        current = self.client.request('fixture', 'GET', path)
        if current.status == 200:
            self.owned(current, repository)
            deleted = self.finish(self.client.request('fixture', 'DELETE', path))
            if deleted.status not in {200, 404} or 'error' in deleted.json():
                raise ValueError(f'Owned repository delete returned HTTP {deleted.status}')
        elif current.status != 404:
            raise ValueError(f'Owned repository read returned HTTP {current.status}')
        if self.client.request('fixture', 'GET', path).status != 404:
            raise ValueError('Owned repository remains after deletion')

    def cleanup(self) -> None:
        """Delete every owned repository."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            for location, repository in self.owned_repositories:
                self.remove(location, repository)
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()


def operation(response: Response) -> dict:
    """Select a finished operation's shape."""
    value = response.json()
    result = value.get('response', {})
    return {'http_status': response.status, 'fields': sorted(value), 'done': value.get('done'),
            'response_type': result.get('@type'), 'response_fields': sorted(result),
            'error': value.get('error', {}).get('code')}


def names(response: Response, observe: Observations,
          keep: Callable[[str], bool] | None = None) -> list[str]:
    """Select a listing's repository names in order, optionally only those keep accepts."""
    return [observe.text(item['name']) for item in response.json().get('repositories', [])
            if keep is None or keep(item['name'])]


def operations(fixture: RepositoryFixture, snapshot: SnapshotCase, observe: Observations,
               created: Response) -> None:
    """Record the create operation before and after it completes."""
    started = created.json()
    snapshot.match('create-started', {
        'http_status': created.status, 'fields': sorted(started),
        'done': started.get('done', False),
        'metadata_type': started.get('metadata', {}).get('@type')})
    polled = fixture.client.request('fixture', 'GET', fixture.operation_path(started['name']),
                                    timeout=fixture.remaining())
    snapshot.match('operation-get', {'http_status': polled.status,
                                     'same_name': polled.json().get('name') == started['name']})
    # Another ID of the same shape: GCP's are UUIDs; gcp-local's carry a prefix that
    # routes them past Memorystore's operations on the same path.
    missing = started['name'][:-12] + '000000000000'
    named = Observations({**observe.aliases, started['name'][:-12]: '<operation-prefix>'})
    snapshot.match('operation-missing', detailed(named, fixture.client.request(
        'fixture', 'GET', fixture.operation_path(missing), timeout=fixture.remaining())))
    snapshot.match('create-operation', operation(fixture.finish(created)))


def listings(fixture: RepositoryFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Record repositories with one ID in two locations, list order, and paging."""
    fixture.finish(fixture.create(repository=fixture.sibling))
    fixture.finish(fixture.create(location=SECOND_LOCATION))
    other = fixture.location(SECOND_LOCATION, suffix=f'/repositories/{fixture.repository}')
    snapshot.match('second-location-read', observe.select(other, ('name', 'registryUri')))
    listing = fixture.location(LOCATION, suffix='/repositories')
    snapshot.match('list-first-location', {'http_status': listing.status,
                                           'names': names(listing, observe)})
    owned = f'/repositories/{fixture.repository}'
    listing = fixture.location(SECOND_LOCATION, suffix='/repositories')
    snapshot.match('list-second-location', {
        'http_status': listing.status,
        'names': names(listing, observe, lambda name: name.endswith(owned))})
    first = fixture.location(LOCATION, suffix='/repositories', query={'pageSize': 1})
    token = first.json().get('nextPageToken', '')
    second = fixture.location(LOCATION, suffix='/repositories',
                              query={'pageSize': 1, 'pageToken': token})
    snapshot.match('pages', {'first': names(first, observe), 'first_has_next': bool(token),
                             'second': names(second, observe),
                             'second_has_next': bool(second.json().get('nextPageToken'))})
    descending = fixture.location(LOCATION, suffix='/repositories', query={'orderBy': 'name desc'})
    snapshot.match('order-name-desc', {'http_status': descending.status,
                                       'names': names(descending, observe)})
    snapshot.match('bad-page-token', detailed(observe, fixture.location(
        LOCATION, suffix='/repositories', query={'pageToken': 'not-a-token'})))
    snapshot.match('negative-page-size', detailed(observe, fixture.location(
        LOCATION, suffix='/repositories', query={'pageSize': -1})))


def policies(fixture: RepositoryFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Record the empty policy, a refused conditional binding, and a reader grant."""
    policy = fixture.call('GET', ':getIamPolicy')
    snapshot.match('policy-read', observe.select(policy, ('bindings',)))
    member = 'serviceAccount:' + fixture.client.emails['allowed']
    conditional = {'version': 3, 'etag': policy.json().get('etag', ''), 'bindings': [{
        'role': 'roles/artifactregistry.reader', 'members': [member],
        'condition': {'title': 'always', 'expression': ALWAYS}}]}
    snapshot.match('conditional-policy', detailed(observe, fixture.call(
        'POST', ':setIamPolicy', body={'policy': conditional})))
    granted = fixture.call('POST', ':setIamPolicy', body={'policy': {
        'etag': policy.json().get('etag', ''),
        'bindings': [{'role': 'roles/artifactregistry.reader', 'members': [member]}]}})
    snapshot.match('reader-granted', observe.select(granted, ('bindings',)))
    allowed = fixture.wait_until(lambda: fixture.call('GET', actor='allowed'), {403}, 180)
    snapshot.match('allowed-read', observe.select(allowed, ('name',)))
    snapshot.match('allowed-cannot-update', iam_denial(observe, fixture.call(
        'PATCH', actor='allowed', query={'updateMask': 'description'},
        body={'description': 'not permitted'})))


def updates(fixture: RepositoryFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Record masked, unknown-mask, and maskless patches and refused creates."""
    patched = fixture.call('PATCH', query={'updateMask': 'description'}, body={
        'description': 'gcp-local renamed', 'createTime': '2020-01-01T00:00:00Z'})
    snapshot.match('patched', observe.select(patched, ('description', 'labels')))
    ignored = fixture.call('PATCH', query={'updateMask': 'notAField'},
                           body={'description': 'ignored'})
    snapshot.match('patch-unknown-mask', observe.select(ignored, ('description',)))
    snapshot.match('patch-without-mask', detailed(observe, fixture.call(
        'PATCH', body={'description': 'no mask'})))
    snapshot.match('duplicate-create', observe.error(fixture.create()))
    snapshot.match('invalid-format', observe.error(fixture.create('NOT_A_FORMAT')))
    snapshot.match('unknown-field', detailed(observe, fixture.create(
        repository=fixture.refused, extra={'notAField': 1})))
    snapshot.match('mistyped-field', observe.error(fixture.create(
        repository=fixture.refused, extra={'labels': 'x'})))


def run_scenario(fixture: RepositoryFixture, snapshot: SnapshotCase, created: Response) -> None:
    """Record the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.repository: '<repository>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>',
                            **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    operations(fixture, snapshot, observe, created)
    read = fixture.call('GET')
    fixture.owned(read)
    snapshot.match('fixture-read', observe.select(read, ('name', 'format', 'mode', 'labels',
                                                         'description')))
    snapshot.match('denied-read', iam_denial(observe, fixture.call('GET', actor='denied')))
    snapshot.match('denied-list', iam_denial(observe, fixture.location(
        LOCATION, suffix='/repositories', actor='denied')))
    listings(fixture, snapshot, observe)
    policies(fixture, snapshot, observe)
    updates(fixture, snapshot, observe)
    snapshot.match('delete-operation', operation(fixture.finish(fixture.call('DELETE'))))
    snapshot.match('absent', detailed(observe, fixture.call('GET')))


def main(argv: list[str] | None = None) -> None:
    """Clean only the owned repositories of an interrupted run."""
    parser = argparse.ArgumentParser(
        description='Clean only the owned repositories of an interrupted run')
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
    RepositoryFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                      manifest=args.manifest).cleanup()
    print('Owned repository cleanup passed.')


if __name__ == '__main__':
    main()
