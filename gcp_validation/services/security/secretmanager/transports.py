"""Secret Manager over REST and gRPC: the same calls on both transports, diffed.

``secretmanager-transport-parity`` creates one secret over REST and one over
gRPC, then sends each call to the REST secret over REST and to the gRPC secret
over gRPC: get, add, access, list, and disable versions, and the refusals
(duplicate, missing, malformed, denied, disabled). Both secrets alias to
``<secret>``, so each step's two views compare directly.

The gRPC messages are the published ``google.cloud.secretmanager.v1`` types,
imported on first use (``requirements/services.txt``).
"""

import argparse
import base64
import importlib
import sys
from pathlib import Path

from google.protobuf import empty_pb2, json_format

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, SnapshotCase, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.runner.parity import parity
from gcp_validation.runner.rpc import GrpcClient, Reply
from gcp_validation.sandbox import Sandbox

SERVICE = 'secretmanager'
LIMITS = {'max_secrets': 2, 'max_versions': 2, 'max_run_seconds': 600}
LIVE_ENDPOINT = 'https://secretmanager.googleapis.com'
LIVE_HOST = 'secretmanager.googleapis.com'
RPC = '/google.cloud.secretmanager.v1.SecretManagerService/'
PAYLOAD = b'gcp-local synthetic transport payload'
OK = 200
# Each method's response message in google.cloud.secretmanager_v1.types.
RESPONSES = {
    'CreateSecret': 'Secret',
    'GetSecret': 'Secret',
    'AddSecretVersion': 'SecretVersion',
    'AccessSecretVersion': 'AccessSecretVersionResponse',
    'ListSecretVersions': 'ListSecretVersionsResponse',
    'DisableSecretVersion': 'SecretVersion',
}


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the separately approved live scope for this scenario."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class TransportsFixture(OwnedRun):
    """Two secrets named for the run: ``-rest`` and ``-grpc``."""

    def __init__(self, client: RestClient, grpc: GrpcClient, state_dir: Path, *,
                 manifest: str | None = None) -> None:
        """Bind the run manifest and the owned secret names."""
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'],
                         manifest=manifest,
                         check=lambda v: v.get('name') == 'gl-smt-' + v['run_id'][:12])
        self.grpc = grpc
        self.manifest.setdefault('name', 'gl-smt-' + self.manifest['run_id'][:12])
        self.manifest.setdefault('versions_added', 0)
        base = f"projects/{self.project}/secrets/{self.manifest['name']}"
        self.secrets = {'rest': base + '-rest', 'grpc': base + '-grpc'}
        self.labels = {'managed-by': 'gcp-local', 'gcp-local-run': self.manifest['run_id']}

    def rest(self, method: str, path: str, *, actor: str = 'fixture', query: dict | None = None,
             body: dict | None = None) -> Response:
        """Call the REST API under ``/v1/``."""
        return self.client.request(actor, method, '/v1/' + path, query=query, body=body,
                                   timeout=self.remaining())

    def rpc(self, name: str, request: dict, *, actor: str = 'fixture') -> Reply:
        """Call one SecretManagerService method with a proto3 JSON request."""
        types = importlib.import_module('google.cloud.secretmanager_v1.types')
        message = json_format.ParseDict(request, getattr(types, name + 'Request').pb()())
        response = (getattr(types, RESPONSES[name]).pb() if name in RESPONSES
                    else empty_pb2.Empty)
        routing = 'parent' if 'parent' in request else 'name'
        return self.grpc.call(actor, RPC + name, message, response,
                              params={routing: request[routing]}, timeout=self.remaining())

    def count_version(self) -> None:
        """Count one version before adding it."""
        if self.manifest['versions_added'] >= LIMITS['max_versions']:
            raise Inconclusive('Secret version limit reached')
        self.manifest['versions_added'] += 1
        self.save()

    def setup(self) -> None:
        """Take the lock and confirm the project before anything is created."""
        self.acquire()
        self.client.verify_project()
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.

    def cleanup(self) -> None:
        """Delete both owned secrets over REST and confirm they are gone."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            for secret in self.secrets.values():
                current = self.rest('GET', secret)
                if current.status == OK:
                    if current.json().get('labels') != self.labels:
                        raise ValueError('Refusing cleanup: secret ownership does not match')
                    self.rest('DELETE', secret)
                if self.rest('GET', secret).status != 404:
                    raise ValueError('Owned secret remains after deletion')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.grpc.close()
            self.release()


def observer(fixture: TransportsFixture) -> Observations:
    """Alias both secrets to ``<secret>``, by project ID and by number."""
    client = fixture.client
    aliases = {}
    for secret in fixture.secrets.values():
        aliases[secret] = '<secret>'
        aliases[secret.replace(fixture.project, client.number, 1)] = '<secret>'
    aliases.update({fixture.manifest['run_id']: '<run>', fixture.project: '<project>',
                    client.number: '<project-number>'})
    aliases.update({email: '<' + actor + '>' for actor, email in client.emails.items()})
    return Observations(aliases)


def payload(response: Response | Reply) -> bytes:
    """Decode an access response's payload data."""
    value = response.json() if isinstance(response, Response) else response.message
    return base64.b64decode(value.get('payload', {}).get('data', ''))


def run(fixture: TransportsFixture, snapshot: SnapshotCase) -> None:
    """Send each step over both transports and record both views and their differences."""
    observe = observer(fixture)
    project = 'projects/' + fixture.project
    rest, grpc = fixture.secrets['rest'], fixture.secrets['grpc']
    secret_id = {key: name.rsplit('/', 1)[1] for key, name in fixture.secrets.items()}
    body = {'replication': {'automatic': {}}, 'labels': fixture.labels}

    def create() -> tuple[Response, Reply]:
        return (
            fixture.rest('POST', project + '/secrets', query={'secretId': secret_id['rest']},
                         body=body),
            fixture.rpc('CreateSecret', {'parent': project, 'secretId': secret_id['grpc'],
                                         'secret': body}),
        )

    created = parity(observe, *create(), ('name', 'replication', 'labels'))
    snapshot.match('create', created)
    if created['rest']['code'] != 'OK' or created['grpc']['code'] != 'OK':
        raise AssertionError('A secret was not created')
    snapshot.match('create-duplicate', parity(observe, *create()))
    snapshot.match('get', parity(observe, fixture.rest('GET', rest),
                                 fixture.rpc('GetSecret', {'name': grpc}),
                                 ('name', 'replication', 'labels')))
    data = base64.b64encode(PAYLOAD).decode()
    fixture.count_version()
    added_rest = fixture.rest('POST', rest + ':addVersion', body={'payload': {'data': data}})
    fixture.count_version()
    added_grpc = fixture.rpc('AddSecretVersion', {'parent': grpc, 'payload': {'data': data}})
    snapshot.match('add-version', parity(observe, added_rest, added_grpc, ('name', 'state')))
    accessed_rest = fixture.rest('GET', rest + '/versions/1:access')
    accessed_grpc = fixture.rpc('AccessSecretVersion', {'name': grpc + '/versions/1'})
    if payload(accessed_rest) != PAYLOAD or payload(accessed_grpc) != PAYLOAD:
        raise AssertionError('An accessed payload differs from the one added')
    snapshot.match('access', parity(observe, accessed_rest, accessed_grpc, ('name',)))
    snapshot.match('list-versions', parity(
        observe, fixture.rest('GET', rest + '/versions'),
        fixture.rpc('ListSecretVersions', {'parent': grpc}), ('totalSize',)))
    refusals(fixture, snapshot, observe)
    snapshot.match('disable', parity(
        observe, fixture.rest('POST', rest + '/versions/1:disable', body={}),
        fixture.rpc('DisableSecretVersion', {'name': grpc + '/versions/1'}), ('state',)))
    snapshot.match('access-disabled', parity(
        observe, fixture.rest('GET', rest + '/versions/1:access'),
        fixture.rpc('AccessSecretVersion', {'name': grpc + '/versions/1'})))
    snapshot.match('delete', parity(observe, fixture.rest('DELETE', rest),
                                    fixture.rpc('DeleteSecret', {'name': grpc})))


def refusals(fixture: TransportsFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Missing, malformed, and denied calls on both transports."""
    project = 'projects/' + fixture.project
    rest, grpc = fixture.secrets['rest'], fixture.secrets['grpc']
    snapshot.match('get-missing', parity(
        observe, fixture.rest('GET', rest + '-missing'),
        fixture.rpc('GetSecret', {'name': grpc + '-missing'})))
    snapshot.match('create-malformed-id', parity(
        observe,
        fixture.rest('POST', project + '/secrets', query={'secretId': 'bad id!'},
                     body={'replication': {'automatic': {}}}),
        fixture.rpc('CreateSecret', {'parent': project, 'secretId': 'bad id!',
                                     'secret': {'replication': {'automatic': {}}}})))
    snapshot.match('get-denied', parity(
        observe, fixture.rest('GET', rest, actor='denied'),
        fixture.rpc('GetSecret', {'name': grpc}, actor='denied')))
    snapshot.match('access-denied', parity(
        observe, fixture.rest('GET', rest + '/versions/1:access', actor='denied'),
        fixture.rpc('AccessSecretVersion', {'name': grpc + '/versions/1'}, actor='denied')))


def main(argv: list[str] | None = None) -> None:
    """Delete the owned secrets after an interrupted live run."""
    parser = argparse.ArgumentParser(description=main.__doc__)
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
    state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
    TransportsFixture(client, GrpcClient(client, LIVE_HOST), state,
                      manifest=args.manifest).cleanup()
    sys.stdout.write('Owned Secret Manager transport cleanup passed.\n')


if __name__ == '__main__':
    main()
