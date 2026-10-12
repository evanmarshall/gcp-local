"""Bounded Cloud Logging entries recording.

Writes three entries to one run-owned log in the sandbox project, reads them back
through filters, ordering, and paging, records the denied caller's errors, and deletes
the log. Ingestion is eventually consistent, so live reads poll briefly.
"""

import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

from .evidence import Inconclusive, SnapshotCase, now
from .live_rest import ACTORS, LiveScope, Observations, OwnedRun, Response, RestClient
from .sandbox import Sandbox

SERVICE = 'logging'
LIMITS = {'max_entries': 10, 'max_run_seconds': 600}
LIVE_ENDPOINT = 'https://logging.googleapis.com'
ENTRY_FIELDS = ('severity', 'labels', 'resource')
ENTRIES_PATHS = frozenset({'/v2/entries:write', '/v2/entries:list'})


def load_scope(filename: str, sandbox: Sandbox) -> LiveScope:
    """Load the owner-approved Logging live scope."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox)


class LoggingClient(RestClient):
    """The shared explicit-identity transport, admitting Logging's v2 paths.

    The shared client admits ``/v1/projects`` paths only; Logging writes and lists
    entries on project-free ``/v2/entries`` methods.
    """

    def request(self, actor: str, method: str, path: str, *, query: dict | None = None,
                body: dict | None = None, timeout: float = 30) -> Response:
        """Send one request as a scenario identity, with no cloud fallback."""
        if actor not in ACTORS:
            raise ValueError('Unknown scenario identity')
        project = f'/v2/projects/{self.target.project}/'
        if path not in ENTRIES_PATHS and not path.startswith(project):
            raise ValueError('Select an explicit path in the selected project')
        token = self.token(actor)
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token}
        if self.target.mode != 'local':
            headers['x-goog-user-project'] = self.target.project
        data = json.dumps(body).encode() if body is not None else None
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


class LogFixture(OwnedRun):
    """One run-owned log; cleanup deletes it."""

    def __init__(self, client: LoggingClient, state_dir: Path, *,
                 manifest: str | None = None) -> None:
        """Bind the owned log name to this run's manifest."""
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'],
                         manifest=manifest,
                         check=lambda v: v.get('log') == 'gcp-local-' + v['run_id'][:12])
        self.manifest.setdefault('log', 'gcp-local-' + self.manifest['run_id'][:12])
        self.log = self.manifest['log']
        self.log_name = f'projects/{self.project}/logs/{self.log}'
        self.resource = f'projects/{self.project}'

    def setup(self) -> None:
        """Record the attempt before the first write."""
        self.acquire()
        self.client.verify_project()
        self.manifest['attempted'] = True
        self.save()

    def cleanup(self) -> None:
        """Delete only the run-owned log."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            deleted = self.client.request('fixture', 'DELETE', f'/v2/{self.log_name}')
            if deleted.status not in {200, 404}:
                raise ValueError(f'Owned log delete returned HTTP {deleted.status}')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()

    def list(self, filter_text: str, *, actor: str = 'fixture', **extra: object) -> Response:
        """List entries of the owned log matching one extra filter."""
        query = f'logName="{self.log_name}" {filter_text}'.strip()
        body = {'resourceNames': [self.resource], 'filter': query, **extra}
        return self.client.request(actor, 'POST', '/v2/entries:list', body=body,
                                   timeout=self.remaining())

    def poll(self, filter_text: str, count: int) -> Response:
        """Wait, live only, until ingestion shows the expected number of entries."""
        deadline = time.monotonic() + (120 if self.client.sandbox else 0)
        while True:
            response = self.list(filter_text)
            if response.status != 200 or len(response.json().get('entries', [])) >= count:
                return response
            if time.monotonic() >= deadline:
                raise Inconclusive('Log entries did not become visible in time')
            time.sleep(5)


def payloads(response: Response) -> list:
    """Select the payloads of a listing in order."""
    entries = response.json().get('entries', [])
    return [entry.get('textPayload', entry.get('jsonPayload')) for entry in entries]


def write_entries(fixture: LogFixture, labels: dict) -> Response:
    """Write three entries with recent, distinct timestamps.

    Logging drops entries outside its retention window, so the times are relative.
    """
    start = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=1)
    stamps = [(start + timedelta(seconds=step)).strftime('%Y-%m-%dT%H:%M:%SZ') for step in range(3)]
    entries = [
        {'severity': 'INFO', 'textPayload': 'first', 'timestamp': stamps[0]},
        {'severity': 'ERROR', 'textPayload': 'second', 'timestamp': stamps[1]},
        {'severity': 'NOTICE', 'jsonPayload': {'step': 'third'}, 'timestamp': stamps[2]},
    ]
    body = {'logName': fixture.log_name, 'resource': {'type': 'global'}, 'labels': labels,
            'entries': entries}
    return fixture.client.request('fixture', 'POST', '/v2/entries:write', body=body,
                                  timeout=fixture.remaining())


def run_scenario(fixture: LogFixture, snapshot: SnapshotCase) -> None:
    """Record the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({
        fixture.log: '<log>', fixture.manifest['run_id']: '<run>',
        fixture.project: '<project>', client.number: '<project-number>',
        **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    written = write_entries(fixture, {'gcp-local-run': fixture.manifest['run_id']})
    snapshot.match('write', {'http_status': written.status, 'fields': sorted(written.json())})
    everything = fixture.poll('', 3)
    entry = everything.json().get('entries', [{}])[0]
    snapshot.match('list-all', {
        'http_status': everything.status, 'payloads': payloads(everything),
        'entry_fields': sorted(entry),
        **{field: observe.value(entry.get(field)) for field in ENTRY_FIELDS}})
    snapshot.match('filter-severity', {'payloads': payloads(fixture.list('severity>=NOTICE'))})
    snapshot.match('filter-text', {'payloads': payloads(fixture.list('"second"'))})
    snapshot.match('filter-json', {'payloads': payloads(fixture.list('jsonPayload.step="third"'))})
    page = fixture.list('', orderBy='timestamp desc', pageSize=2)
    snapshot.match('page-desc', {'payloads': payloads(page),
                                 'has_next': bool(page.json().get('nextPageToken'))})
    logs = client.request('fixture', 'GET', f'/v2/projects/{fixture.project}/logs',
                          timeout=fixture.remaining())
    snapshot.match('logs-list', {'http_status': logs.status,
                                 'owned': fixture.log_name in logs.json().get('logNames', [])})
    denied = {'logName': fixture.log_name, 'resource': {'type': 'global'},
              'entries': [{'textPayload': 'denied'}]}
    snapshot.match('denied-write', observe.error(
        client.request('denied', 'POST', '/v2/entries:write', body=denied)))
    snapshot.match('denied-list', observe.error(fixture.list('', actor='denied')))
    snapshot.match('invalid-filter', observe.error(fixture.list('severity>=LOUD')))
    deleted = client.request('fixture', 'DELETE', f'/v2/{fixture.log_name}',
                             timeout=fixture.remaining())
    snapshot.match('delete', {'http_status': deleted.status, 'fields': sorted(deleted.json())})
