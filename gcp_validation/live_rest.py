"""Shared pieces for bounded live REST recorders: scope, transport, and owned runs."""

import fcntl
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, Request, build_opener
from uuid import uuid4

from gcp_validation.evidence import Inconclusive, check_observation, now
from gcp_validation.sandbox import inspect_credentials, load_credentials, refresh_credentials
from gcp_validation.scope import load_scope_file
from gcp_validation.services.messaging.pubsub_scope import private_json
from gcp_validation.services.storage.client import NoRedirects

ACTORS = ('fixture', 'allowed', 'denied')


@dataclass(frozen=True)
class LiveScope:
    """An owner-approved scope file naming one service and its fixed limits."""

    service: str
    project: str
    project_number: str
    limits: dict

    @classmethod
    def load(cls, service, limits, filename, sandbox, mode: str):
        load_scope_file(filename, sandbox, service=service, limits=limits, mode=mode,
                        label=service, option='--gcp-live-scope-file')
        return cls(service, sandbox.project, sandbox.project_number, dict(limits))

    def evidence(self):
        return {'services': [self.service], 'location': 'global', 'limits': dict(self.limits),
                'cleanup_required': True, 'identities': list(ACTORS)}


@dataclass
class Response:
    status: int
    body: bytes

    def json(self):
        return json.loads(self.body) if self.body else {}


class RestClient:
    """Explicit-identity JSON transport: synthetic tokens locally, sandbox ADC live."""

    def __init__(self, target, live_endpoint, *, sandbox=None, scope=None):
        self.target, self.sandbox, self.scope = target, sandbox, scope
        if target.mode == 'local':
            if sandbox is not None or scope is not None:
                raise ValueError('Local transport cannot receive real credentials')
            self.endpoint = target.endpoint
            self.emails = {actor: f'{actor}@{target.project}.iam.gserviceaccount.com' for actor in ACTORS}
            self.number = '000000000001'
        elif target.mode in {'record', 'revalidate'}:
            if (sandbox is None or scope is None or scope.project != target.project
                    or sandbox.project != target.project
                    or sandbox.identities['allowed'].credentials_file != target.credentials_file):
                raise ValueError('Live recording requires a matching explicit profile and separate scope')
            for actor in ACTORS:
                inspect_credentials(sandbox.identities[actor], sandbox.project)
            self.endpoint = live_endpoint
            self.emails = {actor: sandbox.identities[actor].email for actor in ACTORS}
            self.number = sandbox.project_number
        else:
            raise ValueError('Select an explicit local or live target')
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
        prefix = '/v1/projects/' + self.target.project
        if not (path.startswith(prefix + '/') or path.startswith(prefix + ':')):
            raise ValueError('Select an explicit path in the selected project')
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
            raise Inconclusive('Transport failed; no cloud fallback is available') from None
        with response:
            return Response(response.code, response.read(1 << 20))

    def verify_project(self):
        if self.target.mode == 'local':
            return
        request = Request('https://cloudresourcemanager.googleapis.com/v3/projects/' + self.target.project,
                          headers={'Authorization': 'Bearer ' + self.token('fixture'),
                                   'x-goog-user-project': self.target.project})
        try:
            with self.opener.open(request, timeout=15) as response:
                name = json.loads(response.read(1 << 16)).get('name')
        except (HTTPError, URLError, TimeoutError, OSError):
            raise Inconclusive('Could not confirm the sandbox project number') from None
        if name != 'projects/' + self.scope.project_number:
            raise ValueError('Live project number does not match the approved scope')


class OwnedRun:
    """Owner-only manifest written before creation, a project lock, and a run deadline."""

    def __init__(self, client, state_dir, service, max_seconds, *, manifest=None, check=None):
        self.client, self.project = client, client.target.project
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.lock_name, self.lock = service + '.lock', None
        self.deadline = time.monotonic() + max_seconds
        if manifest is None:
            self.manifest = {'schema': 1, 'project': self.project, 'project_number': client.number,
                             'run_id': uuid4().hex, 'attempted': False, 'cleanup': 'pending',
                             'started_at': now()}
            self.path = self.state_dir / (self.manifest['run_id'] + '.json')
        else:
            self.path = Path(manifest)
            if self.path.is_symlink() or not self.path.is_file() or self.path.stat().st_mode & 0o077:
                raise ValueError('Cleanup requires a private regular ownership manifest')
            self.manifest = json.loads(self.path.read_text())
            v = self.manifest
            if (v.get('schema') != 1 or v.get('project') != self.project
                    or v.get('project_number') != client.number
                    or not re.fullmatch('[0-9a-f]{32}', v.get('run_id', ''))
                    or type(v.get('attempted')) is not bool or (check and not check(v))):
                raise ValueError('Cleanup manifest is outside the selected ownership scope')

    def save(self):
        private_json(self.path, self.manifest)

    def acquire(self):
        if self.lock is not None:
            return
        self.lock = (self.state_dir / self.lock_name).open('a')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            self.lock = None
            raise Inconclusive('Another run holds this project lock') from None

    def release(self):
        if self.lock:
            self.lock.close()
            self.lock = None

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise Inconclusive('Scenario exceeded its run deadline; cleanup is still required')
        return min(30, remaining)

    def wait_until(self, operation, retry_statuses, seconds):
        """Retry an operation while it answers with a propagation status, live only."""
        deadline = min(self.deadline, time.monotonic() + (seconds if self.client.sandbox else 0))
        while True:
            response = operation()
            if response.status not in retry_statuses:
                return response
            if time.monotonic() >= deadline:
                raise Inconclusive('Grant or resource propagation did not complete')
            time.sleep(10)


class Observations:
    """Select public fields; aliases keep relationships without identifiers."""

    def __init__(self, aliases):
        self.aliases = dict(aliases)

    def text(self, value):
        for original, alias in sorted(self.aliases.items(), key=lambda pair: -len(pair[0])):
            value = value.replace(original, alias)
        return value

    def value(self, item):
        if isinstance(item, str):
            return self.text(item)
        if isinstance(item, list):
            return [self.value(child) for child in item]
        if isinstance(item, dict):
            return {key: self.value(child) for key, child in item.items()}
        return item

    def select(self, response, fields=()):
        value = response.json()
        selected = {'http_status': response.status, 'fields': sorted(value)}
        for field in fields:
            selected[field] = self.value(value.get(field))
        check_observation(selected)
        return selected

    def error(self, response):
        value = response.json()
        error = value.get('error') if isinstance(value, dict) else None
        if not isinstance(error, dict):
            raise ValueError('Unexpected error structure')
        # Message text can carry identifiers and changes without notice.
        selected = {'http_status': response.status, 'code': error.get('code'), 'status': error.get('status')}
        check_observation(selected)
        return selected
