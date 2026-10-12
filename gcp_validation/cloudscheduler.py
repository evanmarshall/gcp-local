"""Bounded Cloud Scheduler job lifecycle recording with a Pub/Sub target run."""

import argparse
import base64
import re
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .config import Target
from .evidence import Inconclusive, now
from .live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from .sandbox import Sandbox

SERVICE = 'cloudscheduler'
LIMITS = {'max_jobs': 8, 'max_topics': 1, 'max_runs': 2, 'max_run_seconds': 900}
LIVE_ENDPOINT = 'https://cloudscheduler.googleapis.com'
PUBSUB_ENDPOINT = 'https://pubsub.googleapis.com'
LOCATION = 'us-central1'
# Yearly schedules: no scheduled execution falls inside a run.
SCHEDULE, PATCHED_SCHEDULE, ZONE = '0 0 1 1 *', '30 2 1 1 *', 'America/New_York'
# A private, non-routable address. HTTP jobs are created but never run, so no
# request is sent anywhere.
TARGET = 'https://10.255.255.1/gcp-local'
JOB_FIELDS = ('name', 'description', 'schedule', 'timeZone', 'state', 'pubsubTarget',
              'retryConfig', 'attemptDeadline', 'status')
HTTP_FIELDS = ('httpTarget', 'attemptDeadline', 'retryConfig', 'timeZone', 'state')


def load_scope(filename: str, sandbox: Sandbox) -> LiveScope:
    """Load the owner-approved Cloud Scheduler live scope."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox)


def next_new_year(hour: int, minute: int, zone: str = ZONE) -> datetime:
    """Return the next 1 January at hour:minute in zone, in UTC."""
    tz, current = ZoneInfo(zone), datetime.now(UTC)
    for year in (current.year, current.year + 1):
        moment = datetime(year, 1, 1, hour, minute, tzinfo=tz).astimezone(UTC)
        if moment > current:
            return moment
    raise AssertionError('No next new year')


def parse_time(value: str | None) -> datetime | None:
    """Parse an RFC 3339 timestamp with up to nanosecond precision."""
    trimmed = re.sub(r'(\.\d{6})\d+', r'\1', value or '')
    return datetime.fromisoformat(trimmed) if trimmed else None


def is_next(value: dict, hour: int, minute: int) -> bool:
    """Report whether a job's scheduleTime is the next 1 January at hour:minute.

    GCP sometimes adds a sub-second offset after an update, so seconds compare.
    """
    moment = parse_time(value.get('scheduleTime'))
    return moment is not None and moment.replace(microsecond=0) == next_new_year(hour, minute)


def serialized(send: Callable[[], Response], client: RestClient) -> Response:
    """Send one mutation, retrying GCP's ABORTED answer to overlapping job mutations.

    GCP refuses a job mutation that arrives while another is applied ("sync
    mutate calls cannot be queued"); the local provider never does.
    """
    deadline = time.monotonic() + (120 if client.sandbox is not None else 0)
    while True:
        response = send()
        error = response.json().get('error', {}) if response.status == 409 else {}
        aborted = error.get('status') == 'ABORTED'
        if not aborted or time.monotonic() >= deadline:
            return response
        time.sleep(5)


class JobsFixture(OwnedRun):
    """One topic and a few jobs named for the run; each job is recorded before creation."""

    def __init__(self, client: RestClient, pubsub: RestClient, state_dir: Path, *,
                 manifest: str | None = None) -> None:
        """Bind the Scheduler and Pub/Sub transports to one owned run."""
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('topic') == 'gl-' + v['run_id'][:12])
        self.pubsub = pubsub
        self.manifest.setdefault('topic', 'gl-' + self.manifest['run_id'][:12])
        self.manifest.setdefault('jobs', [])
        self.manifest.setdefault('runs', 0)
        self.topic = self.manifest['topic']
        self.parent = f'/v1/projects/{self.project}/locations/{LOCATION}'
        self.topic_name = f'projects/{self.project}/topics/{self.topic}'
        self.job = self.topic + '-pubsub'

    def job_path(self, job: str) -> str:
        """Return the REST path of one owned job."""
        return f'{self.parent}/jobs/{job}'

    def call(self, method: str, job: str, suffix: str = '', *, actor: str = 'fixture',
             body: dict | None = None, query: dict | None = None) -> Response:
        """Send one request to an owned job."""
        return serialized(lambda: self.client.request(
            actor, method, self.job_path(job) + suffix, body=body, query=query,
            timeout=self.remaining()), self.client)

    def create(self, job: str, body: dict, *, actor: str = 'fixture') -> Response:
        """Record the job name, then create it; a refused creation leaves nothing."""
        if job not in self.manifest['jobs']:
            if len(self.manifest['jobs']) >= LIMITS['max_jobs']:
                raise ValueError('Job limit reached')
            self.manifest['jobs'].append(job)
            self.save()
        name = f'projects/{self.project}/locations/{LOCATION}/jobs/{job}'
        return serialized(lambda: self.client.request(
            actor, 'POST', f'{self.parent}/jobs', timeout=self.remaining(),
            body={'name': name, **body}), self.client)

    def run(self, job: str, *, actor: str = 'fixture') -> Response:
        """Run one job now; the fixture's runs count against the scope."""
        if actor == 'fixture':
            if self.manifest['runs'] >= LIMITS['max_runs']:
                raise ValueError('Run limit reached')
            self.manifest['runs'] += 1
            self.save()
        return self.call('POST', job, ':run', actor=actor, body={})

    def pubsub_body(self) -> dict:
        """Return the Pub/Sub target job body."""
        data = base64.b64encode(b'gcp-local').decode()
        return {'description': 'gcp-local recording', 'schedule': SCHEDULE, 'timeZone': ZONE,
                'pubsubTarget': {'topicName': self.topic_name, 'data': data,
                                 'attributes': {'origin': 'gcp-local'}},
                'retryConfig': {'retryCount': 1, 'minBackoffDuration': '5s',
                                'maxBackoffDuration': '10s', 'maxDoublings': 2}}

    def setup(self) -> Response:
        """Create the owned topic and the Pub/Sub target job."""
        self.acquire()
        self.client.verify_project()
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        topic = self.pubsub.request('fixture', 'PUT', '/v1/' + self.topic_name, body={},
                                    timeout=self.remaining())
        if topic.status != 200:
            raise Inconclusive(f'Topic creation returned HTTP {topic.status}')
        # A fresh grant can take minutes to reach the Scheduler API.
        return self.wait_until(lambda: self.create(self.job, self.pubsub_body()), {403}, 300)

    def cleanup(self) -> None:
        """Delete every owned job and the topic, then confirm they are gone."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            for job in self.manifest['jobs']:
                delete_owned(self.client, self.job_path(job), '/jobs/' + job)
            delete_owned(self.pubsub, '/v1/' + self.topic_name, '/topics/' + self.topic)
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()


def delete_owned(client: RestClient, path: str, suffix: str) -> None:
    """Delete one owned resource and confirm it reads 404."""
    current = client.request('fixture', 'GET', path)
    if current.status == 200:
        if not current.json().get('name', '').endswith(suffix):
            raise ValueError('Refusing cleanup: ownership does not match the manifest')
        deleted = serialized(lambda: client.request('fixture', 'DELETE', path), client)
        if deleted.status not in {200, 404}:
            raise ValueError(f'Owned resource delete returned HTTP {deleted.status}')
    elif current.status != 404:
        raise ValueError(f'Owned resource read returned HTTP {current.status}')
    if client.request('fixture', 'GET', path).status != 404:
        raise ValueError('Owned resource remains after deletion')


def outcome(observe: Observations, response: Response, fields: tuple) -> dict:
    """Select a job response, or the error the call returned."""
    if response.status == 200:
        return observe.select(response, fields)
    return observe.error(response)


def watch_attempt(fixture: JobsFixture) -> dict:
    """Poll the job, which has never run before, until its attempt reports a status."""
    seen, deadline = None, time.monotonic() + (180 if fixture.client.sandbox else 30)
    while time.monotonic() < deadline:
        value = fixture.call('GET', fixture.job).json()
        if parse_time(value.get('lastAttemptTime')) is not None:
            seen = value
            if 'status' in value:
                return value
        time.sleep(3)
    if seen is None:
        raise Inconclusive('The run did not attempt the job')
    return seen


def observe_lifecycle(fixture: JobsFixture, snapshot: object, created: Response,
                      observe: Observations) -> None:
    """Observe creation, refusals, reads, pause, resume, and patch of the Pub/Sub job."""
    client = fixture.client
    snapshot.match('job-created', {**observe.select(created, JOB_FIELDS),
                                   'schedule_time_is_next': is_next(created.json(), 0, 0),
                                   'user_update_time_set': parse_time(
                                       created.json().get('userUpdateTime')) is not None})
    snapshot.match('denied-get', observe.error(fixture.call('GET', fixture.job, actor='denied')))
    denied = fixture.create(fixture.topic + '-denied', fixture.pubsub_body(), actor='denied')
    snapshot.match('denied-create', observe.error(denied))
    snapshot.match('denied-run', observe.error(fixture.run(fixture.job, actor='denied')))
    snapshot.match('fetched', observe.select(fixture.call('GET', fixture.job), JOB_FIELDS))
    listed = client.request('fixture', 'GET', f'{fixture.parent}/jobs', timeout=fixture.remaining())
    names = [job.get('name', '') for job in listed.json().get('jobs', [])]
    snapshot.match('listed', {'http_status': listed.status, 'fields': sorted(listed.json()),
                              'contains_job': any(name.endswith('/jobs/' + fixture.job)
                                                  for name in names)})
    paused = fixture.call('POST', fixture.job, ':pause', body={})
    snapshot.match('paused', {**observe.select(paused, ('state',)),
                              'has_schedule_time': 'scheduleTime' in paused.json()})
    again = fixture.call('POST', fixture.job, ':pause', body={})
    snapshot.match('pause-twice', outcome(observe, again, ('state',)))
    resumed = fixture.call('POST', fixture.job, ':resume', body={})
    snapshot.match('resumed', {**observe.select(resumed, ('state',)),
                               'schedule_time_is_next': is_next(resumed.json(), 0, 0)})
    patched = fixture.call('PATCH', fixture.job, body={'description': 'updated',
                                                       'schedule': PATCHED_SCHEDULE},
                           query={'updateMask': 'description,schedule'})
    snapshot.match('patched', {**observe.select(patched, JOB_FIELDS),
                               'schedule_time_is_next': is_next(patched.json(), 2, 30)})


def observe_http_targets(fixture: JobsFixture, snapshot: object, observe: Observations) -> str:
    """Observe an OIDC HTTP job, actAs refusals, and invalid schedules; return its ID."""
    emails = fixture.client.emails
    http_job = fixture.topic + '-http'
    target = {'uri': TARGET, 'httpMethod': 'POST', 'body': base64.b64encode(b'gcp-local').decode(),
              'headers': {'X-Gcp-Local': 'recording'}}
    body = {'schedule': SCHEDULE, 'timeZone': 'UTC', 'attemptDeadline': '30s',
            'httpTarget': {**target, 'oidcToken': {'serviceAccountEmail': emails['allowed']}}}
    snapshot.match('http-oidc-created', observe.select(fixture.create(http_job, body), HTTP_FIELDS))
    denied = {'serviceAccountEmail': emails['denied']}
    oidc = {**body, 'httpTarget': {**target, 'oidcToken': denied}}
    snapshot.match('oidc-without-act-as',
                   observe.error(fixture.create(fixture.topic + '-oidc', oidc)))
    oauth = {**body, 'httpTarget': {**target, 'oauthToken': denied}}
    snapshot.match('oauth-without-act-as',
                   observe.error(fixture.create(fixture.topic + '-oauth', oauth)))
    cron = {**fixture.pubsub_body(), 'schedule': 'not a schedule'}
    snapshot.match('invalid-schedule', observe.error(fixture.create(fixture.topic + '-cron', cron)))
    zone = {**fixture.pubsub_body(), 'timeZone': 'Mars/Olympus_Mons'}
    snapshot.match('invalid-time-zone',
                   observe.error(fixture.create(fixture.topic + '-zone', zone)))
    return http_job


def run_scenario(fixture: JobsFixture, snapshot: object, created: Response) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.topic: '<topic>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>',
                            **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    observe_lifecycle(fixture, snapshot, created, observe)
    snapshot.match('run', observe.select(fixture.run(fixture.job), ('state', 'status')))
    attempted = watch_attempt(fixture)
    snapshot.match('attempted', {'fields': sorted(attempted), 'state': attempted.get('state'),
                                 'status': observe.value(attempted.get('status'))})
    http_job = observe_http_targets(fixture, snapshot, observe)
    for label, job in (('deleted-pubsub', fixture.job), ('deleted-http', http_job)):
        deleted = fixture.call('DELETE', job)
        snapshot.match(label, {'http_status': deleted.status, 'body': deleted.json()})
    snapshot.match('absent', observe.error(fixture.call('GET', fixture.job)))


def clients(target: Target, sandbox: Sandbox | None = None,
            scope: LiveScope | None = None) -> tuple[RestClient, RestClient]:
    """Return the Scheduler and Pub/Sub transports for one target."""
    return (RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope),
            RestClient(target, PUBSUB_ENDPOINT, sandbox=sandbox, scope=scope))


def main(argv: list[str] | None = None) -> None:
    """Delete only the owned jobs and topic after an interrupted run."""
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
    scheduler, pubsub = clients(target, sandbox, scope)
    state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
    JobsFixture(scheduler, pubsub, state, manifest=args.manifest).cleanup()
    sys.stdout.write('Owned job and topic cleanup passed.\n')


if __name__ == '__main__':
    main()
