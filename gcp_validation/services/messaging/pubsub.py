"""Bounded Pub/Sub gRPC recording with explicit identities and owned cleanup."""

import argparse
import fcntl
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, now
from gcp_validation.sandbox import Sandbox, inspect_credentials, load_credentials
from gcp_validation.services.messaging.pubsub_scope import ACTORS, LIMITS, LiveScope, private_json
from gcp_validation.services.storage.sdk_slice import require_local_isolation


class RPCFailure(Exception):
    def __init__(self, status):
        self.status = status
        super().__init__('Pub/Sub RPC returned ' + status)


def local_credentials(filename, target):
    """Only the local ADC shape; never load arbitrary ADC into a local run."""
    path = Path(filename)
    if not path.is_file() or path.is_symlink() or path.stat().st_mode & 0o077:
        raise ValueError('Local ADC must be a regular owner-only file')
    value = json.loads(path.read_text())
    if (set(value) != {'type', 'audience', 'subject_token_type', 'token_url', 'credential_source'}
            or value['type'] != 'external_account' or value['audience'] != '//gcp-local/identity'
            or value['subject_token_type'] != 'urn:ietf:params:oauth:token-type:access_token'):
        raise ValueError('Only synthetic local ADC is accepted by the Pub/Sub runner')
    url = urlsplit(value['token_url'])
    Target.select('local', project=target.project, endpoint=f'{url.scheme}://{url.netloc}')
    if url.scheme != 'http' or url.path != '/v1/token' or url.query or url.fragment:
        raise ValueError('Local ADC must select the explicit loopback STS endpoint')
    source = value['credential_source']
    if not isinstance(source, dict) or set(source) != {'file'}:
        raise ValueError('Local ADC requires a private file subject')
    subject = Path(source['file'])
    if (not subject.is_absolute() or subject.is_symlink() or subject.parent != path.resolve().parent
            or not subject.is_file() or subject.stat().st_mode & 0o077):
        raise ValueError('Local subject must be an owner-only file next to the ADC profile')
    if not subject.read_text().startswith('gls.'):
        raise ValueError('Expected a synthetic local subject')
    from google.auth.identity_pool import Credentials
    return Credentials.from_info(value, scopes=['https://www.googleapis.com/auth/cloud-platform']), value['token_url']


class PubSubClient:
    def __init__(self, target, *, sandbox=None, scope=None, identities=None):
        self.target, self.sandbox, self.scope = target, sandbox, scope
        if target.mode == 'local':
            require_local_isolation()
            if sandbox or scope or not identities:
                raise ValueError('Local Pub/Sub requires explicit local identities, never a cloud profile')
            if urlsplit(target.endpoint).scheme != 'http':
                raise ValueError('The verified local Pub/Sub transport uses loopback TCP')
            values = json.loads(Path(identities).read_text())
            if set(values) != {'project', 'identities'} or values['project'] != target.project:
                raise ValueError('Local identity bundle must match the selected project')
            if set(values['identities']) != set(ACTORS):
                raise ValueError('Select distinct fixture, allowed, and denied local identities')
            self.members, credentials, token_urls = {}, {}, set()
            for actor, entry in values['identities'].items():
                if (set(entry) != {'email', 'adc'} or not re.fullmatch(
                        r'[a-z][a-z0-9-]{4,28}[a-z0-9]@' + re.escape(target.project) + r'\.iam\.gserviceaccount\.com', entry['email'])):
                    raise ValueError('Local identity must belong to the selected synthetic project')
                self.members[actor] = 'serviceAccount:' + entry['email']
                credentials[actor], token_url = local_credentials(entry['adc'], target)
                token_urls.add(token_url)
            if len(set(self.members.values())) != len(ACTORS):
                raise ValueError('Scenario identities must be distinct')
            endpoint = urlsplit(target.endpoint).netloc
        elif target.mode in {'record', 'revalidate'}:
            if (sandbox is None or scope is None or identities or scope.project != target.project
                    or scope.project_number != sandbox.project_number or sandbox.project != target.project
                    or sandbox.identities['allowed'].credentials_file != target.credentials_file):
                raise ValueError('Live Pub/Sub requires matching explicit profile and separate scope')
            for actor in ACTORS:
                inspect_credentials(sandbox.identities[actor], sandbox.project)
            credentials = {actor: load_credentials(sandbox.identities[actor], sandbox.project) for actor in ACTORS}
            self.members = {actor: 'serviceAccount:' + sandbox.identities[actor].email for actor in ACTORS}
            endpoint = 'pubsub.googleapis.com:443'
            token_urls = set()
        else:
            raise ValueError('Select a local or live Pub/Sub target')
        if os.environ.get('PUBSUB_EMULATOR_HOST'):
            raise ValueError('Anonymous Pub/Sub emulator mode cannot validate IAM')
        from importlib.metadata import version
        if any(version(package) != pin for package, pin in (
                ('google-cloud-pubsub', '2.42.0'), ('grpcio', '1.84.0'), ('google-auth', '2.56.2'))):
            raise ValueError('Install the pinned requirements/pubsub.txt')
        import grpc
        import requests
        from google.auth.transport.grpc import secure_authorized_channel
        from google.auth.transport.requests import Request
        from google.pubsub_v1.services.publisher.client import PublisherClient
        from google.pubsub_v1.services.publisher.transports.grpc import PublisherGrpcTransport
        from google.pubsub_v1.services.subscriber.client import SubscriberClient
        from google.pubsub_v1.services.subscriber.transports.grpc import SubscriberGrpcTransport
        self.session = requests.Session()
        self.session.trust_env = False
        local = target.mode == 'local'

        class BoundedRequest(Request):
            def __call__(self, url, *args, **kwargs):
                if local and url not in token_urls:
                    raise ValueError('Local credential refresh left the selected STS endpoint')
                kwargs.update(timeout=15, allow_redirects=False)
                return super().__call__(url, *args, **kwargs)

        self.request = BoundedRequest(session=self.session)
        self.credentials = credentials
        self.channels, self.clients = [], {}
        try:
            for actor, credential in credentials.items():
                channel = secure_authorized_channel(credential, self.request, endpoint,
                    ssl_credentials=grpc.local_channel_credentials() if local else grpc.ssl_channel_credentials(),
                    options=[('grpc.enable_http_proxy', 0), ('grpc.max_receive_message_length', 1 << 20)])
                self.channels.append(channel)
                self.clients[actor] = {'topic': PublisherClient(transport=PublisherGrpcTransport(channel=channel)),
                                      'subscription': SubscriberClient(transport=SubscriberGrpcTransport(channel=channel))}
        except BaseException:
            self.close()
            raise

    def call(self, actor, kind, method, request, timeout=10):
        from google.api_core.exceptions import GoogleAPICallError
        try:
            return getattr(self.clients[actor][kind], method)(request=request, retry=None, timeout=timeout)
        except GoogleAPICallError as error:
            code = error.grpc_status_code
            raise RPCFailure(code.name if code is not None else 'UNKNOWN') from None

    def verify_project(self):
        if self.target.mode == 'local':
            return
        credential = self.credentials['fixture']
        credential.refresh(self.request)
        response = self.session.get('https://cloudresourcemanager.googleapis.com/v3/projects/' + self.target.project,
            headers={'Authorization': 'Bearer ' + credential.token, 'x-goog-user-project': self.target.project},
            timeout=15, allow_redirects=False)
        try:
            if response.status_code != 200 or response.json().get('name') != 'projects/' + self.scope.project_number:
                raise ValueError('Live project number does not match the Pub/Sub scope')
        finally:
            response.close()

    def close(self):
        for channel in self.channels:
            channel.close()
        self.session.close()


class PubSubFixture:
    def __init__(self, client, state_dir, *, manifest=None):
        self.client, self.project = client, client.target.project
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.lock = None
        self.deadline = time.monotonic() + LIMITS['max_run_seconds']
        self.number = client.sandbox.project_number if client.sandbox else '000000000001'
        if manifest is None:
            run = uuid4().hex
            name = 'gcp-local-' + run
            self.manifest = {'schema': 1, 'project': self.project, 'project_number': self.number,
                'run_id': run, 'topic': f'projects/{self.project}/topics/{name}',
                'subscription': f'projects/{self.project}/subscriptions/{name}', 'attempted': [],
                'publish_attempts': 0, 'cleanup': 'pending', 'started_at': now()}
            self.path = self.state_dir / (run + '.json')
        else:
            self.path = Path(manifest)
            if self.path.is_symlink() or not self.path.is_file() or self.path.stat().st_mode & 0o077:
                raise ValueError('Cleanup requires a private regular ownership manifest')
            self.manifest = json.loads(self.path.read_text())
            v = self.manifest
            if (v.get('schema') != 1 or v.get('project') != self.project or v.get('project_number') != self.number
                    or not re.fullmatch('[0-9a-f]{32}', v.get('run_id', ''))
                    or v.get('attempted') not in [[], ['topic'], ['topic', 'subscription']]
                    or type(v.get('publish_attempts')) is not int or not 0 <= v['publish_attempts'] <= 10
                    or any(v.get(kind) != f'projects/{self.project}/{kind}s/gcp-local-{v["run_id"]}'
                           for kind in ('topic', 'subscription'))):
                raise ValueError('Cleanup manifest is outside the selected Pub/Sub ownership scope')
        self.topic, self.subscription = self.manifest['topic'], self.manifest['subscription']
        self.labels = {'managed-by': 'gcp-local', 'gcp-local-run': self.manifest['run_id']}

    def save(self):
        private_json(self.path, self.manifest)

    def acquire(self):
        if self.lock is not None:
            return
        self.lock = (self.state_dir / 'pubsub.lock').open('a')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            self.lock = None
            raise Inconclusive('Another Pub/Sub run holds this project lock') from None

    def call(self, kind, method, request, *, actor='fixture', timeout=10):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise Inconclusive('Pub/Sub scenario exceeded its run deadline; cleanup is still required')
        return self.client.call(actor, kind, method, request, timeout=min(timeout, remaining))

    def owned(self, kind, resource):
        if resource.name != self.manifest[kind] or dict(resource.labels) != self.labels:
            raise ValueError('Refusing Pub/Sub cleanup: resource ownership does not match the manifest')
        if kind == 'subscription' and resource.topic != self.topic:
            raise ValueError('Refusing Pub/Sub cleanup: subscription topic ownership changed')

    def setup(self):
        self.acquire()
        self.client.verify_project()
        for kind in ('topic', 'subscription'):
            listing = self.call(kind, 'list_' + kind + 's', {'project': 'projects/' + self.project, 'page_size': 2})
            if getattr(listing, kind + 's') or listing.next_page_token:
                raise Inconclusive('Pub/Sub sandbox is occupied; clean owned resources before recording')
        for kind in ('topic', 'subscription'):
            self.manifest['attempted'].append(kind)
            self.save()  # Creation may succeed even if the response is lost.
            request = {'name': self.manifest[kind], 'labels': self.labels}
            if kind == 'subscription':
                request.update(topic=self.topic, ack_deadline_seconds=10)
            response = self.call(kind, 'create_' + kind, request)
            self.owned(kind, response)
            if kind == 'subscription' and response.ack_deadline_seconds != 10:
                raise AssertionError('Subscription did not retain its configured ack deadline')
        self.grant('topic', 'roles/pubsub.publisher')
        self.grant('subscription', 'roles/pubsub.subscriber')

    def grant(self, kind, role):
        resource = self.manifest[kind]
        policy = self.call(kind, 'get_iam_policy', {'resource': resource})
        if policy.bindings:
            raise Inconclusive('New Pub/Sub resource unexpectedly has a direct IAM policy')
        self.call(kind, 'set_iam_policy', {'resource': resource, 'policy': {
            'version': 1, 'etag': policy.etag,
            'bindings': [{'role': role, 'members': [self.client.members['allowed']]}]}})

    def publish(self, payload, *, actor='allowed'):
        if not isinstance(payload, bytes) or len(payload) > LIMITS['max_message_bytes']:
            raise ValueError('Pub/Sub payload exceeds the bounded recording scope')
        if self.manifest['publish_attempts'] >= LIMITS['max_messages']:
            raise Inconclusive('Pub/Sub publish attempt limit reached; propagation may be inconclusive')
        self.manifest['publish_attempts'] += 1
        self.save()
        return self.call('topic', 'publish', {'topic': self.topic,
            'messages': [{'data': payload, 'attributes': {'source': 'gcp-local-conformance'}}]}, actor=actor)

    def wait_allowed(self, operation):
        deadline = min(self.deadline, time.monotonic() + (120 if self.client.sandbox else 0))
        while True:
            try:
                return operation()
            except RPCFailure as error:
                if error.status != 'PERMISSION_DENIED':
                    raise
                if time.monotonic() >= deadline:
                    raise Inconclusive('Pub/Sub grant propagation did not complete') from None
                time.sleep(15)

    def pull(self, *, actor='allowed', timeout=10):
        return self.call('subscription', 'pull', {'subscription': self.subscription, 'max_messages': 10},
                         actor=actor, timeout=timeout)

    def cleanup(self):
        if not self.manifest['attempted']:
            if self.lock:
                self.lock.close()
                self.lock = None
            return
        self.acquire()
        try:
            self.client.verify_project()
            # Cleanup gets its own bounded calls even when the scenario deadline expired.
            for kind in reversed(self.manifest['attempted']):
                request = {kind: self.manifest[kind]}
                try:
                    resource = self.client.call('fixture', kind, 'get_' + kind, request)
                except RPCFailure as error:
                    if error.status == 'NOT_FOUND':
                        continue
                    raise
                self.owned(kind, resource)
                try:
                    self.client.call('fixture', kind, 'delete_' + kind, request)
                except RPCFailure as error:
                    if error.status != 'NOT_FOUND':
                        raise
                try:
                    self.client.call('fixture', kind, 'get_' + kind, request)
                except RPCFailure as error:
                    if error.status == 'NOT_FOUND':
                        continue
                    raise
                raise ValueError('Owned Pub/Sub resource remains after deletion')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.lock.close()
            self.lock = None


def run_scenario(fixture, snapshot):
    """Same selected observations in local, recording, and revalidation runs."""
    def denied(operation):
        try:
            operation()
        except RPCFailure as error:
            if error.status == 'PERMISSION_DENIED':
                return {'grpc_status': error.status}
            raise
        raise AssertionError('Denied Pub/Sub caller succeeded')

    snapshot.match('denied-publication', denied(lambda: fixture.publish(b'denied', actor='denied')))
    payload = b'gcp-local bounded Pub/Sub conformance'
    published = fixture.wait_allowed(lambda: fixture.publish(payload))
    assert len(published.message_ids) == 1 and published.message_ids[0]
    message_id = published.message_ids[0]
    snapshot.match('published', {'message_ids': ['<message:1>']})
    snapshot.match('denied-consumption', denied(lambda: fixture.pull(actor='denied')))
    deadline = time.monotonic() + 45
    while True:
        try:
            pulled = fixture.wait_allowed(fixture.pull)
        except RPCFailure as error:
            if error.status != 'DEADLINE_EXCEEDED':
                raise
            pulled = None
        if pulled and pulled.received_messages:
            break
        if time.monotonic() >= deadline:
            raise Inconclusive('Published Pub/Sub message was not delivered before the deadline')
    assert len(pulled.received_messages) == 1
    received = pulled.received_messages[0]
    assert received.ack_id and received.message.message_id == message_id
    assert received.message.data == payload
    assert dict(received.message.attributes) == {'source': 'gcp-local-conformance'}
    snapshot.match('pulled', {'message_id': '<message:1>', 'data': payload.decode(),
                             'attributes': dict(received.message.attributes)})
    fixture.call('subscription', 'acknowledge', {'subscription': fixture.subscription, 'ack_ids': [received.ack_id]}, actor='allowed')
    snapshot.match('acknowledged', {'grpc_status': 'OK'})
    # No exactly-once claim: a duplicate of the acknowledged message is permitted.
    # An unexpected ID/payload would reveal a denied publication side effect.
    try:
        tail = fixture.pull(timeout=2)
        for item in tail.received_messages:
            assert item.message.message_id == message_id and item.message.data == payload
    except RPCFailure as error:
        if error.status != 'DEADLINE_EXCEEDED':
            raise
    snapshot.match('no-unexpected-delivery-observed', {'bounded_observation_seconds': 2})


def main(argv=None):
    parser = argparse.ArgumentParser(description='Clean only owned Pub/Sub resources from an interrupted run')
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
    client = PubSubClient(target, sandbox=sandbox, scope=scope)
    try:
        fixture = PubSubFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / 'pubsub', manifest=args.manifest)
        fixture.cleanup()
    finally:
        client.close()
    print('Owned Pub/Sub cleanup passed.')


if __name__ == '__main__':
    main()
