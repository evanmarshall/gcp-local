"""Bounded Secret Manager REST recording with explicit identities and owned cleanup."""

import argparse
import base64
from dataclasses import dataclass
import fcntl
import json
from pathlib import Path
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, build_opener, ProxyHandler
from uuid import uuid4

from .config import Target
from .evidence import Inconclusive, check_observation, now
from .pubsub_scope import private_json
from .sandbox import Sandbox, inspect_credentials, load_credentials, refresh_credentials
from .storage import NoRedirects

ACTORS = ('fixture', 'allowed', 'denied')
LIMITS = {'max_secrets': 1, 'max_versions': 3, 'max_payload_bytes': 65537, 'max_run_seconds': 600}
LIVE_ENDPOINT = 'https://secretmanager.googleapis.com'
PAYLOAD = b'gcp-local synthetic secret payload'
# One byte past the documented 64 KiB payload limit.
OVERSIZE = 65537


@dataclass(frozen=True)
class LiveScope:
    project: str
    project_number: str

    @classmethod
    def load(cls, filename, sandbox):
        if not filename or sandbox is None:
            raise ValueError('Secret Manager requires a separate explicitly approved --gcp-secretmanager-scope')
        value = json.loads(Path(filename).read_text())
        expected = {'schema': 1, 'approved': True, 'services': ['secretmanager'],
                    'project': sandbox.project, 'project_number': sandbox.project_number,
                    'location': 'global', 'limits': LIMITS, 'cleanup_required': True,
                    'scheduled_runs': False}
        if value != expected or any(type(item) is not int for item in value['limits'].values()):
            raise ValueError('Secret Manager scope must match the approved project, fixed limits, and cleanup contract')
        return cls(sandbox.project, sandbox.project_number)

    def evidence(self):
        return {'services': ['secretmanager'], 'location': 'global', 'limits': dict(LIMITS),
                'cleanup_required': True, 'identities': list(ACTORS)}


@dataclass
class Response:
    status: int
    body: bytes

    def json(self):
        return json.loads(self.body) if self.body else {}


class SecretManagerClient:
    def __init__(self, target, *, sandbox=None, scope=None):
        self.target, self.sandbox, self.scope = target, sandbox, scope
        if target.mode == 'local':
            if sandbox is not None or scope is not None:
                raise ValueError('Local transport cannot receive real credentials')
            self.endpoint = target.endpoint
            self.emails = {actor: f'{actor}@{target.project}.iam.gserviceaccount.com' for actor in ACTORS}
        elif target.mode in {'record', 'revalidate'}:
            if (sandbox is None or scope is None or scope.project != target.project
                    or sandbox.project != target.project
                    or sandbox.identities['allowed'].credentials_file != target.credentials_file):
                raise ValueError('Live Secret Manager requires matching explicit profile and separate scope')
            for actor in ACTORS:
                inspect_credentials(sandbox.identities[actor], sandbox.project)
            self.endpoint = LIVE_ENDPOINT
            self.emails = {actor: sandbox.identities[actor].email for actor in ACTORS}
        else:
            raise ValueError('Secret Manager requires an explicit local or live target')
        if len(set(self.emails.values())) != len(ACTORS):
            raise ValueError('Scenario identities must be distinct')
        self.opener = build_opener(ProxyHandler({}), NoRedirects())
        self.credentials = {}

    def token(self, actor):
        if self.target.mode == 'local':
            return 'gcp-local-test:' + actor
        if actor not in self.credentials:
            self.credentials[actor] = load_credentials(self.sandbox.identities[actor], self.sandbox.project)
        credential = self.credentials[actor]
        if not credential.valid:
            refresh_credentials(credential)
        return credential.token

    def request(self, actor, method, path, *, query=None, body=None, timeout=30):
        if actor not in ACTORS:
            raise ValueError('Unknown scenario identity')
        if not path.startswith('/v1/projects/' + self.target.project + '/'):
            raise ValueError('Select an explicit Secret Manager path in the selected project')
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.token(actor)}
        if self.target.mode != 'local':
            headers['x-goog-user-project'] = self.target.project
        data = json.dumps(body).encode() if body is not None else (b'' if method == 'POST' else None)
        url = self.endpoint + path + ('?' + urlencode(query) if query else '')
        try:
            response = self.opener.open(Request(url, data=data, headers=headers, method=method), timeout=timeout)
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError):
            raise Inconclusive('Secret Manager transport failed; no cloud fallback is available') from None
        with response:
            payload = response.read(1 << 20)
            return Response(response.code, payload)

    def verify_project(self):
        if self.target.mode == 'local':
            return
        token = self.token('fixture')
        request = Request('https://cloudresourcemanager.googleapis.com/v3/projects/' + self.target.project,
                          headers={'Authorization': 'Bearer ' + token, 'x-goog-user-project': self.target.project})
        try:
            with self.opener.open(request, timeout=15) as response:
                name = json.loads(response.read(1 << 16)).get('name')
        except (HTTPError, URLError, TimeoutError, OSError):
            raise Inconclusive('Could not confirm the sandbox project number') from None
        if name != 'projects/' + self.scope.project_number:
            raise ValueError('Live project number does not match the Secret Manager scope')


class SecretManagerFixture:
    """One labeled secret; the fixture identity administers it, callers only read."""

    def __init__(self, client, state_dir, *, manifest=None):
        self.client, self.project = client, client.target.project
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.lock = None
        self.deadline = time.monotonic() + LIMITS['max_run_seconds']
        self.number = client.sandbox.project_number if client.sandbox else '000000000001'
        if manifest is None:
            run = uuid4().hex
            self.manifest = {'schema': 1, 'project': self.project, 'project_number': self.number,
                             'run_id': run, 'secret': f'projects/{self.project}/secrets/gcp-local-{run}',
                             'attempted': False, 'versions_added': 0, 'cleanup': 'pending',
                             'started_at': now()}
            self.path = self.state_dir / (run + '.json')
        else:
            self.path = Path(manifest)
            if self.path.is_symlink() or not self.path.is_file() or self.path.stat().st_mode & 0o077:
                raise ValueError('Cleanup requires a private regular ownership manifest')
            self.manifest = json.loads(self.path.read_text())
            v = self.manifest
            if (v.get('schema') != 1 or v.get('project') != self.project or v.get('project_number') != self.number
                    or not re.fullmatch('[0-9a-f]{32}', v.get('run_id', ''))
                    or v.get('secret') != f'projects/{self.project}/secrets/gcp-local-{v["run_id"]}'
                    or type(v.get('attempted')) is not bool
                    or type(v.get('versions_added')) is not int
                    or not 0 <= v['versions_added'] <= LIMITS['max_versions']):
                raise ValueError('Cleanup manifest is outside the selected Secret Manager ownership scope')
        self.secret = self.manifest['secret']
        self.labels = {'managed-by': 'gcp-local', 'gcp-local-run': self.manifest['run_id']}

    def save(self):
        private_json(self.path, self.manifest)

    def acquire(self):
        if self.lock is not None:
            return
        self.lock = (self.state_dir / 'secretmanager.lock').open('a')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            self.lock = None
            raise Inconclusive('Another Secret Manager run holds this project lock') from None

    def call(self, method, suffix='', *, actor='fixture', query=None, body=None):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise Inconclusive('Secret Manager scenario exceeded its run deadline; cleanup is still required')
        return self.client.request(actor, method, '/v1/' + self.secret + suffix, query=query, body=body,
                                   timeout=min(30, remaining))

    def setup(self):
        self.acquire()
        self.client.verify_project()
        listing = self.client.request('fixture', 'GET', f'/v1/projects/{self.project}/secrets',
                                      query={'pageSize': 1})
        if listing.status != 200:
            raise Inconclusive(f'Secret listing returned HTTP {listing.status}')
        if listing.json().get('secrets'):
            raise Inconclusive('Secret Manager sandbox is occupied; clean owned secrets before recording')
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.client.request('fixture', 'POST', f'/v1/projects/{self.project}/secrets',
                                   query={'secretId': self.secret.rsplit('/', 1)[1]},
                                   body={'replication': {'automatic': {}}, 'labels': self.labels})

    def add_version(self, payload, *, actor='fixture'):
        if self.manifest['versions_added'] >= LIMITS['max_versions']:
            raise Inconclusive('Secret version limit reached')
        if len(payload) > LIMITS['max_payload_bytes']:
            raise ValueError('Payload exceeds the bounded recording scope')
        if actor == 'fixture':
            self.manifest['versions_added'] += 1
            self.save()
        return self.call('POST', ':addVersion', actor=actor,
                         body={'payload': {'data': base64.b64encode(payload).decode()}})

    def grant_accessor(self):
        policy = self.call('GET', ':getIamPolicy')
        if policy.status != 200:
            raise Inconclusive(f'Secret getIamPolicy returned HTTP {policy.status}')
        current = policy.json()
        if current.get('bindings'):
            raise Inconclusive('New secret unexpectedly has a direct IAM policy')
        return self.call('POST', ':setIamPolicy', body={'policy': {
            'etag': current.get('etag', ''),
            'bindings': [{'role': 'roles/secretmanager.secretAccessor',
                          'members': ['serviceAccount:' + self.client.emails['allowed']]}]}})

    def wait_allowed(self, operation):
        deadline = min(self.deadline, time.monotonic() + (180 if self.client.sandbox else 0))
        while True:
            response = operation()
            if response.status != 403:
                return response
            if time.monotonic() >= deadline:
                raise Inconclusive('Secret accessor grant propagation did not complete')
            time.sleep(10)

    def owned(self, response):
        value = response.json()
        # GCP answers with the project number in resource names.
        names = {self.secret, self.secret.replace('projects/' + self.project + '/', 'projects/' + self.number + '/', 1)}
        if value.get('name') not in names or value.get('labels') != self.labels:
            raise ValueError('Refusing Secret Manager cleanup: secret ownership does not match the manifest')

    def cleanup(self):
        if not self.manifest['attempted']:
            if self.lock:
                self.lock.close()
                self.lock = None
            return
        self.acquire()
        try:
            self.client.verify_project()
            path = '/v1/' + self.secret
            current = self.client.request('fixture', 'GET', path)
            if current.status == 200:
                self.owned(current)
                deleted = self.client.request('fixture', 'DELETE', path)
                if deleted.status not in {200, 404}:
                    raise ValueError(f'Owned secret delete returned HTTP {deleted.status}')
            elif current.status != 404:
                raise ValueError(f'Owned secret read returned HTTP {current.status}')
            if self.client.request('fixture', 'GET', path).status != 404:
                raise ValueError('Owned secret remains after deletion')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.lock.close()
            self.lock = None


class Observations:
    """Select public fields; aliases keep relationships without identifiers."""

    def __init__(self, fixture):
        numbered = fixture.secret.replace('projects/' + fixture.project + '/', 'projects/' + fixture.number + '/', 1)
        self.aliases = {fixture.secret: '<secret>', numbered: '<secret-by-number>', fixture.project: '<project>',
                        fixture.number: '<project-number>'}
        self.aliases.update({email: '<' + actor + '>' for actor, email in fixture.client.emails.items()})
        self.aliases[fixture.manifest['run_id']] = '<run>'

    def text(self, value):
        for original, alias in sorted(self.aliases.items(), key=lambda pair: -len(pair[0])):
            value = value.replace(original, alias)
        return value

    def select(self, response, fields=()):
        value = response.json()
        selected = {'http_status': response.status, 'fields': sorted(value)}
        for field in fields:
            item = value.get(field)
            if isinstance(item, str):
                item = self.text(item)
            elif field == 'labels' and isinstance(item, dict):
                item = {key: self.text(label) for key, label in item.items()}
            selected[field] = item
        check_observation(selected)
        return selected

    def error(self, response):
        value = response.json()
        error = value.get('error') if isinstance(value, dict) else None
        if not isinstance(error, dict):
            raise ValueError('Unexpected Secret Manager error structure')
        # Message text can carry identifiers and changes without notice.
        selected = {'http_status': response.status, 'code': error.get('code'), 'status': error.get('status')}
        check_observation(selected)
        return selected


def crc32c(data):
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def run_scenario(fixture, snapshot, created):
    """Same selected observations in local, recording, and revalidation runs."""
    observe = Observations(fixture)
    if created.status != 200:
        raise AssertionError(f'Secret create returned HTTP {created.status}')
    fixture.owned(created)
    snapshot.match('created', observe.select(created, ('name', 'replication', 'labels')))
    added = fixture.add_version(PAYLOAD)
    snapshot.match('version-added', observe.select(added, ('name', 'state')))
    version = '/versions/1'
    snapshot.match('denied-access', observe.error(fixture.call('GET', version + ':access', actor='denied')))
    granted = fixture.grant_accessor()
    snapshot.match('accessor-granted', {'http_status': granted.status, 'bindings': [
        {'role': item['role'], 'members': [observe.text(member) for member in item['members']]}
        for item in granted.json().get('bindings', [])]})
    accessed = fixture.wait_allowed(lambda: fixture.call('GET', version + ':access', actor='allowed'))
    value = accessed.json()
    assert accessed.status == 200 and base64.b64decode(value['payload']['data']) == PAYLOAD
    checksum = value['payload'].get('dataCrc32c')
    snapshot.match('accessed', {'http_status': accessed.status, 'fields': sorted(value),
                                'name': observe.text(value['name']), 'payload_fields': sorted(value['payload']),
                                'payload_matches': True,
                                'crc32c_matches': checksum is None or int(checksum) == crc32c(PAYLOAD)})
    latest = fixture.call('GET', '/versions/latest:access', actor='allowed')
    snapshot.match('latest-alias', {'http_status': latest.status, 'name': observe.text(latest.json().get('name', ''))})
    snapshot.match('accessor-cannot-read-metadata', observe.error(fixture.call('GET', version, actor='allowed')))
    snapshot.match('accessor-cannot-add-version',
                   observe.error(fixture.add_version(b'not-permitted', actor='allowed')))
    snapshot.match('oversize-payload', observe.error(fixture.add_version(b'x' * OVERSIZE)))
    disabled = fixture.call('POST', version + ':disable', body={})
    snapshot.match('disabled', observe.select(disabled, ('state',)))
    snapshot.match('access-disabled', observe.error(fixture.call('GET', version + ':access', actor='allowed')))
    enabled = fixture.call('POST', version + ':enable', body={})
    snapshot.match('enabled', observe.select(enabled, ('state',)))
    destroyed = fixture.call('POST', version + ':destroy', body={})
    snapshot.match('destroyed', observe.select(destroyed, ('state',)))
    snapshot.match('access-destroyed', observe.error(fixture.call('GET', version + ':access', actor='allowed')))
    snapshot.match('destroy-again', observe.error(fixture.call('POST', version + ':destroy', body={})))
    listed = fixture.call('GET', '/versions')
    snapshot.match('versions-listed', {'http_status': listed.status, 'states': sorted(
        item['state'] for item in listed.json().get('versions', []))})
    deleted = fixture.call('DELETE')
    snapshot.match('deleted', {'http_status': deleted.status})
    snapshot.match('absent', observe.error(fixture.call('GET')))


def main(argv=None):
    parser = argparse.ArgumentParser(description='Clean only owned secrets from an interrupted run')
    parser.add_argument('command', choices=['cleanup'])
    parser.add_argument('--profile', required=True)
    parser.add_argument('--scope', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--allow-live', action='store_true', required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = LiveScope.load(args.scope, sandbox)
    target = Target.select('record', project=sandbox.project, credentials_file=sandbox.identities['allowed'].credentials_file,
                           live_scope=sandbox.live_scope, allow_live=args.allow_live)
    client = SecretManagerClient(target, sandbox=sandbox, scope=scope)
    SecretManagerFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / 'secretmanager',
                         manifest=args.manifest).cleanup()
    print('Owned Secret Manager cleanup passed.')


if __name__ == '__main__':
    main()
