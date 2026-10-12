"""Bounded Cloud Tasks queue, dispatch, and retry recording against an unreachable target."""

import argparse
import base64
import time
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, now
from gcp_validation.live_paths import PrefixClient
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun
from gcp_validation.sandbox import Sandbox

SERVICE = 'cloudtasks'
LIMITS = {'max_queues': 1, 'max_tasks': 4, 'max_attempts': 2, 'max_run_seconds': 600}
LIVE_ENDPOINT = 'https://cloudtasks.googleapis.com'
LOCATION = 'us-central1'
# A private, non-routable address: GCP attempts delivery and fails without
# sending traffic to any third party.
TARGET = 'https://10.255.255.1/gcp-local'


def load_scope(filename, sandbox, mode: str):
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class TasksClient(PrefixClient):
    prefix = '/v2beta3'


class QueueFixture(OwnedRun):
    """One queue named for the run; at most two delivery attempts per task."""

    def __init__(self, client, state_dir, *, manifest=None):
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('queue') == 'gl-' + v['run_id'][:12])
        self.manifest.setdefault('queue', 'gl-' + self.manifest['run_id'][:12])
        self.manifest.setdefault('tasks', 0)
        self.queue = self.manifest['queue']
        self.parent = f'/v2beta3/projects/{self.project}/locations/{LOCATION}'

    def call(self, method, suffix='', *, actor='fixture', body=None, query=None):
        return self.client.request(actor, method, f'{self.parent}/queues/{self.queue}{suffix}',
                                   body=body, query=query, timeout=self.remaining())

    def task(self, *, actor='fixture', url=TARGET, oidc=None):
        if self.manifest['tasks'] >= LIMITS['max_tasks']:
            raise ValueError('Task limit reached')
        request = {'url': url, 'httpMethod': 'POST', 'body': base64.b64encode(b'gcp-local').decode()}
        if oidc:
            request['oidcToken'] = {'serviceAccountEmail': oidc}
        response = self.call('POST', '/tasks', actor=actor, body={'task': {'httpRequest': request}})
        if response.status == 200:  # Refused creations cost nothing; queue deletion removes tasks.
            self.manifest['tasks'] += 1
            self.save()
        return response

    def setup(self):
        self.acquire()
        self.client.verify_project()
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.client.request('fixture', 'POST', f'{self.parent}/queues', timeout=self.remaining(), body={
            'name': f'projects/{self.project}/locations/{LOCATION}/queues/{self.queue}',
            'retryConfig': {'maxAttempts': LIMITS['max_attempts'], 'minBackoff': '1s', 'maxBackoff': '1s'}})

    def cleanup(self):
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            path = f'{self.parent}/queues/{self.queue}'
            current = self.client.request('fixture', 'GET', path)
            if current.status == 200:
                if not current.json().get('name', '').endswith('/queues/' + self.queue):
                    raise ValueError('Refusing queue cleanup: ownership does not match the manifest')
                deleted = self.client.request('fixture', 'DELETE', path)
                if deleted.status not in {200, 404}:
                    raise ValueError(f'Owned queue delete returned HTTP {deleted.status}')
            elif current.status != 404:
                raise ValueError(f'Owned queue read returned HTTP {current.status}')
            if self.client.request('fixture', 'GET', path).status != 404:
                raise ValueError('Owned queue remains after deletion')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()


def watch_dispatch(fixture, task_path):
    """Poll one task through its attempts until GCP removes it."""
    first, deadline = None, time.monotonic() + (240 if fixture.client.sandbox else 30)
    while time.monotonic() < deadline:
        response = fixture.call('GET', task_path, query={'responseView': 'BASIC'})
        if response.status == 404:
            return first, True
        value = response.json()
        if first is None and 'responseStatus' in value.get('lastAttempt', {}):
            first = value
        time.sleep(2)
    raise Inconclusive('Task was not removed after its retry budget')


def run_scenario(fixture, snapshot, created):
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.queue: '<queue>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>',
                            **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    snapshot.match('queue-created', observe.select(created, ('name', 'retryConfig', 'state')))
    snapshot.match('denied-enqueue', observe.error(fixture.task(actor='denied')))
    policy = fixture.call('POST', ':getIamPolicy', body={})
    snapshot.match('policy-read', observe.select(policy, ('bindings',)))
    granted = fixture.call('POST', ':setIamPolicy', body={'policy': {'etag': policy.json().get('etag', ''), 'bindings': [
        {'role': 'roles/cloudtasks.enqueuer', 'members': ['serviceAccount:' + client.emails['allowed']]}]}})
    snapshot.match('enqueuer-granted', observe.select(granted, ('bindings',)))
    task = fixture.wait_until(lambda: fixture.task(actor='allowed'), {403}, 180)
    value = task.json()
    snapshot.match('task-created', {**observe.select(task, ('httpRequest', 'view')),
                                    'name_in_queue': value.get('name', '').startswith(
                                        f'projects/{fixture.project}/locations/{LOCATION}/queues/{fixture.queue}/tasks/')})
    snapshot.match('enqueuer-cannot-list', observe.error(fixture.call('GET', '/tasks', actor='allowed')))
    first, removed = watch_dispatch(fixture, '/tasks/' + value['name'].rsplit('/', 1)[1])
    snapshot.match('first-attempt', {'seen': first is not None,
                                     'fields': sorted(first or {}),
                                     'last_attempt_fields': sorted((first or {}).get('lastAttempt', {})),
                                     'response_status_code': (first or {}).get('lastAttempt', {}).get(
                                         'responseStatus', {}).get('code')})
    snapshot.match('removed-after-retries', {'removed': removed})
    snapshot.match('oidc-without-act-as', observe.error(fixture.task(oidc=client.emails['denied'])))
    snapshot.match('invalid-url', observe.error(fixture.task(url='ftp://10.255.255.1/x')))
    snapshot.match('paused', observe.select(fixture.call('POST', ':pause', body={}), ('state',)))
    snapshot.match('resumed', observe.select(fixture.call('POST', ':resume', body={}), ('state',)))
    deleted = fixture.call('DELETE')
    snapshot.match('deleted', {'http_status': deleted.status, 'body': deleted.json()})
    snapshot.match('absent', observe.error(fixture.call('GET')))


def main(argv=None):
    parser = argparse.ArgumentParser(description='Delete only the owned queue after an interrupted run')
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
    client = TasksClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    QueueFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                 manifest=args.manifest).cleanup()
    print('Owned queue cleanup passed.')


if __name__ == '__main__':
    main()
