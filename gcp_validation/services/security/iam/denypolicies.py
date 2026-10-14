"""IAM v2 deny policy recordings on the sandbox project.

Deny policies attach to the sandbox project only, never its organization.
They affect every caller in the project, so these scenarios run alone
(scripts/live/batch.py EXCLUSIVE). The fixture needs roles/iam.denyAdmin,
which GCP grants only at the organization. IAM allows 5 policy reads and 5
policy writes per minute per project; a live run waits out a rate-limit 429.

The lifecycle scenario denies only the denied identity, which holds no
grants, so its policies change nobody's access.
"""

import argparse
import json
import re
import sys
import time
from collections.abc import Callable
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, check_observation, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.security.hierarchy import Snapshot
from gcp_validation.services.security.refusals import outcome

SERVICE = 'denypolicies'
LIMITS = {'max_deny_policies': 1, 'max_run_seconds': 1800}
LIVE_ENDPOINT = 'https://iam.googleapis.com'
ATTACHMENT = 'cloudresourcemanager.googleapis.com/projects/'
# The permission the lifecycle policies deny: key creation, which the
# sandbox organization already refuses (orgpolicy-constraint-enforcement).
KEYS_CREATE = 'iam.googleapis.com/serviceAccountKeys.create'
# The standing group holding the allowed identity (docs/contributing/gcp-sandbox.md).
ALLOWED_GROUP = 'principalSet://goog/group/gcp-local-allowed@stitchwink.com'
SERVICE_ACCOUNT = 'principal://iam.googleapis.com/projects/-/serviceAccounts/'
RATE_LIMIT_WAIT = 61
# The policy IDs GCP accepts; only these can be created, so only these are owned.
POLICY_ID = re.compile(r'[a-z][a-z0-9.-]{2,62}')


def load_scope(filename: str | Path | None, sandbox: Sandbox | None, mode: str) -> LiveScope:
    """Load the owner-approved lifecycle scope."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


def collection(project: str) -> str:
    """Return the deny policy collection path of a project (by ID or number)."""
    return '/v2/policies/' + quote(ATTACHMENT + project, safe='') + '/denypolicies'


class DenyClient(RestClient):
    """RestClient admitting the sandbox project's deny policies and IAM v1 paths.

    Live, a request IAM refuses for its per-minute policy rate limit is
    retried after a minute, within the caller's deadline.
    """

    def allowed_paths(self) -> tuple[str, ...]:
        """Return the path prefixes these scenarios send."""
        project = self.target.project
        return (collection(project), collection(self.number),
                '/v2/policies/' + quote(ATTACHMENT + project, safe='') + '/allowpolicies',
                '/v1/projects/' + project + '/')

    def request(self, actor: str, method: str, path: str, *, query: dict | None = None,
                body: object = None, timeout: float = 30) -> Response:
        """Send one request as a scenario identity to an allowed path."""
        if actor not in self.emails:
            raise ValueError('Unknown scenario identity')
        if not path.startswith(self.allowed_paths()):
            raise ValueError('Select a path this scenario sends')
        while True:
            response = self.send(actor, method, path, query, body, timeout)
            if self.target.mode == 'local' or not rate_limited(response):
                return response
            time.sleep(RATE_LIMIT_WAIT)

    def send(self, actor: str, method: str, path: str, query: dict | None, body: object,
             timeout: float) -> Response:
        """Send one request without retrying."""
        headers = {'Content-Type': 'application/json',
                   'Authorization': 'Bearer ' + self.token(actor)}
        if self.target.mode != 'local':
            headers['x-goog-user-project'] = self.target.project
        empty = b'' if method == 'POST' else None
        data = json.dumps(body).encode() if body is not None else empty
        url = self.endpoint + path + ('?' + urlencode(query) if query else '')
        # The endpoint is the fixed https live host or the loopback target.
        request = Request(url, data=data, headers=headers, method=method)  # noqa: S310
        try:
            response = self.opener.open(request, timeout=timeout)
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError):
            raise Inconclusive('Transport failed; no cloud fallback is available') from None
        with response:
            return Response(response.code, response.read(4 << 20))


def rate_limited(response: Response) -> bool:
    """Report IAM's per-minute policy rate limit, not a deny policy limit."""
    if response.status != 429:
        return False
    details = response.json().get('error', {}).get('details') or ()
    return any(detail.get('reason') == 'RATE_LIMIT_EXCEEDED' for detail in details)


def deny_rule(principals: list[str], permissions: list[str], **extra: object) -> dict:
    """Return one deny policy rule."""
    return {'denyRule': {'deniedPrincipals': principals, 'deniedPermissions': permissions,
                         **extra}}


class DenyRun(OwnedRun):
    """Deny policies named by the run, recorded in the manifest before each create.

    Cleanup deletes each recorded policy that still exists and carries the
    run's display name, then requires all of them gone.
    """

    def __init__(self, client: DenyClient, state_dir: Path, *, service: str = SERVICE,
                 max_seconds: int = LIMITS['max_run_seconds'], manifest: str | None = None,
                 check: Callable[[dict], bool] | None = None) -> None:
        """Name the run; a manifest resumes an interrupted run."""
        super().__init__(client, state_dir, service, max_seconds, manifest=manifest, check=check)
        self.manifest.setdefault('policies', [])
        self.marker = 'gcp-local conformance run ' + self.manifest['run_id']
        self.collection = collection(self.project)

    def policy_id(self, label: str) -> str:
        """Return the run's policy ID for a label."""
        return f"gcl-{label}-{self.manifest['run_id'][:10]}"

    def call(self, actor: str, method: str, path: str, body: object = None,
             query: dict | None = None) -> Response:
        """Send one request within the run's deadline."""
        return self.client.request(actor, method, path, body=body, query=query,
                                   timeout=self.remaining())

    def create(self, policy_id: str, rules: list, *, actor: str = 'fixture') -> Response:
        """Create a policy, recording its ID first in case it is created."""
        if POLICY_ID.fullmatch(policy_id) and policy_id not in self.manifest['policies']:
            self.manifest['policies'].append(policy_id)
            self.save()
        query = {'policyId': policy_id} if policy_id else None
        body = {'displayName': self.marker, 'rules': rules}
        return self.call(actor, 'POST', self.collection, body, query)

    def setup(self) -> None:
        """Take the lock, confirm the project, and require no foreign deny policy."""
        self.acquire()
        self.client.verify_project()
        listed = self.call('fixture', 'GET', self.collection)
        if listed.status != 200:
            raise Inconclusive(f'Deny policy list returned HTTP {listed.status}')
        if listed.json().get('policies'):
            raise Inconclusive('The sandbox project already has deny policies; clean them first')
        self.manifest['attempted'] = True
        self.save()

    def cleanup(self) -> None:
        """Delete the run's policies, then require every one gone."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            self.cleanup_resources()
            for policy_id in self.manifest['policies']:
                if self.call('fixture', 'GET', self.collection + '/' + policy_id).status != 404:
                    raise ValueError('An owned deny policy remains after cleanup')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()

    def cleanup_resources(self) -> None:
        """Delete the run's deny policies; subclasses delete more."""
        for policy_id in self.manifest['policies']:
            path = self.collection + '/' + policy_id
            current = self.call('fixture', 'GET', path)
            if current.status == 404:
                continue
            if current.status != 200 or current.json().get('displayName') != self.marker:
                raise ValueError(f'Refusing cleanup of deny policy {policy_id}: not owned')
            deleted = self.call('fixture', 'DELETE', path)
            if deleted.status not in {200, 404}:
                raise ValueError(f'Owned deny policy delete returned HTTP {deleted.status}')


def aliases(run: DenyRun, extra: dict | None = None) -> Observations:
    """Alias the identities, the project, the run, and its policy IDs."""
    client = run.client
    names = {client.emails[actor]: '<' + actor + '>' for actor in client.emails}
    names |= {client.target.project: '<project>', client.number: '<project-number>',
              run.manifest['run_id'][:10]: '<run>', run.marker: '<marker>', **(extra or {})}
    return Observations({original: alias for original, alias in names.items() if original})


OPERATION_ID = re.compile(r'/operations/[^/"]+$')


def policy_view(observe: Observations, policy: dict) -> dict:
    """Select a policy's field names, name, kind, display name, and rules."""
    return {'fields': sorted(policy), 'name': observe.text(policy.get('name', '')),
            'kind': policy.get('kind'), 'displayName': observe.text(policy.get('displayName', '')),
            'rules': observe.value(policy.get('rules'))}


def operation_view(observe: Observations, response: Response) -> dict:
    """Select an operation: its fields, policy-scoped name, done, metadata, and response."""
    if response.status != 200:
        return outcome(observe, response)
    body = response.json()
    metadata = body.get('metadata') or {}
    result = body.get('response') or {}
    name = OPERATION_ID.sub('/operations/<operation>', observe.text(body.get('name', '')))
    selected = {'http_status': response.status, 'fields': sorted(body), 'name': name,
                'done': body.get('done'),
                'metadata': {'type': metadata.get('@type'), 'fields': sorted(metadata)},
                'response': {'type': result.get('@type'), **policy_view(observe, result)}}
    check_observation(selected)
    return selected


def read_view(observe: Observations, response: Response) -> dict:
    """Select a policy read, or its refusal."""
    if response.status != 200:
        return outcome(observe, response)
    selected = {'http_status': response.status, **policy_view(observe, response.json())}
    check_observation(selected)
    return selected


def listing(observe: Observations, response: Response) -> dict:
    """Select a list: its fields and each policy's view (lists carry no rules)."""
    if response.status != 200:
        return outcome(observe, response)
    body = response.json()
    selected = {'http_status': response.status, 'fields': sorted(body),
                'policies': [policy_view(observe, item) for item in body.get('policies', [])]}
    check_observation(selected)
    return selected


def condition_refusal(observe: Observations, response: Response) -> dict:
    """Select a condition refusal without its message, which embeds the CEL parser's text."""
    selected = outcome(observe, response)
    selected.pop('message', None)
    return selected


def run_lifecycle(run: DenyRun, snapshot: Snapshot) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    observe = aliases(run, {run.policy_id('main'): '<policy>'})
    denied = SERVICE_ACCOUNT + run.client.emails['denied']
    main = run.collection + '/' + run.policy_id('main')
    snapshot.match('denied-list', outcome(observe, run.call('denied', 'GET', run.collection)))
    snapshot.match('missing-get', outcome(observe, run.call('fixture', 'GET', main)))
    snapshot.match('empty-list', listing(observe, run.call('fixture', 'GET', run.collection)))
    created = run.create(run.policy_id('main'), [deny_rule([denied], [KEYS_CREATE])])
    snapshot.match('created', operation_view(observe, created))
    operation = created.json().get('name', '')
    polled = run.call('fixture', 'GET', '/v2/' + operation) if created.status == 200 else created
    snapshot.match('operation', operation_view(observe, polled))
    stored = run.call('fixture', 'GET', main)
    snapshot.match('get', read_view(observe, stored))
    by_number = collection(run.client.number) + '/' + run.policy_id('main')
    snapshot.match('get-by-number', read_view(observe, run.call('fixture', 'GET', by_number)))
    snapshot.match('list', listing(observe, run.call('fixture', 'GET', run.collection)))
    duplicate = run.create(run.policy_id('main'), [deny_rule([denied], [KEYS_CREATE])])
    snapshot.match('duplicate-create', outcome(observe, duplicate))
    etag = stored.json().get('etag', '')
    update(run, snapshot, observe, main, etag, denied)
    refusals(run, snapshot, observe, denied)
    remove(run, snapshot, observe, main)


def update(run: DenyRun, snapshot: Snapshot, observe: Observations, main: str, etag: str,
           denied: str) -> None:
    """Refuse a stale etag, then replace the rules with the other accepted shapes."""
    stale = run.call('fixture', 'PUT', main, {'displayName': run.marker, 'etag': 'BwXstale',
                                              'rules': [deny_rule([denied], [KEYS_CREATE])]})
    snapshot.match('update-stale-etag', outcome(observe, stale))
    rules = [
        deny_rule([denied], ['iam.googleapis.com/serviceAccountKeys.*']),
        deny_rule([], [KEYS_CREATE]),
        deny_rule([denied], []),
        deny_rule([denied, ALLOWED_GROUP], [KEYS_CREATE],
                  denialCondition={'expression': "resource.matchTag('1/env', 'prod')"}),
    ]
    updated = run.call('fixture', 'PUT', main, {'displayName': run.marker, 'etag': etag,
                                                'rules': rules})
    snapshot.match('updated', operation_view(observe, updated))
    after = updated.json().get('response', {})
    if updated.status == 200 and after.get('etag') == etag:
        raise AssertionError('An update kept the etag')
    snapshot.match('get-updated', read_view(observe, run.call('fixture', 'GET', main)))


def refusals(run: DenyRun, snapshot: Snapshot, observe: Observations, denied: str) -> None:
    """Send the policies GCP refuses at create; none may be created."""
    cases = {
        'v1-permission': [deny_rule([denied], ['iam.serviceAccountKeys.create'])],
        'unknown-permission': [deny_rule([denied], [KEYS_CREATE.removesuffix('create') + 'bogus'])],
        'service-wildcard': [deny_rule([denied], ['iam.googleapis.com/*'])],
        'v1-principal': [deny_rule(['serviceAccount:' + run.client.emails['denied']],
                                   [KEYS_CREATE])],
        'domain-principal': [deny_rule(['domain:stitchwink.com'], [KEYS_CREATE])],
        'unknown-principal-form': [deny_rule(['principal://goog/bogus/x'], [KEYS_CREATE])],
        'missing-group': [deny_rule(['principalSet://goog/group/gcl-missing-group@stitchwink.com'],
                                    [KEYS_CREATE])],
        'exempted-public': [deny_rule(['principalSet://goog/public:all'], [KEYS_CREATE],
                                      exceptionPrincipals=['principalSet://goog/public:all'])],
        'too-many-rules': [deny_rule([denied], [KEYS_CREATE]) for _ in range(501)],
    }
    refused = run.policy_id('refused')
    for name, rules in cases.items():
        response = run.create(refused, rules)
        snapshot.match(name, outcome(observe, response))
    condition = deny_rule([denied], [KEYS_CREATE],
                          denialCondition={'expression': 'resource.name.startsWith("projects/")'})
    refusal = condition_refusal(observe, run.create(refused, [condition]))
    snapshot.match('non-tag-condition', refusal)
    one = [deny_rule([denied], [KEYS_CREATE])]
    snapshot.match('invalid-policy-id', outcome(observe, run.create('Bad_ID', one)))
    snapshot.match('missing-policy-id', outcome(observe, run.create('', one)))
    other = run.collection.removesuffix('/denypolicies') + '/allowpolicies'
    snapshot.match('other-kind', outcome(observe, run.call('fixture', 'GET', other)))


def remove(run: DenyRun, snapshot: Snapshot, observe: Observations, main: str) -> None:
    """Refuse a stale delete, delete, and read the policy as gone."""
    current = run.call('fixture', 'GET', main)
    stale = run.call('fixture', 'DELETE', main, query={'etag': 'BwXstale'})
    snapshot.match('delete-stale-etag', outcome(observe, stale))
    deleted = run.call('fixture', 'DELETE', main, query={'etag': current.json().get('etag', '')})
    snapshot.match('deleted', operation_view(observe, deleted))
    snapshot.match('get-deleted', outcome(observe, run.call('fixture', 'GET', main)))
    after = run.call('fixture', 'GET', run.collection)
    snapshot.match('list-after-delete', listing(observe, after))


def main(argv: list[str] | None = None) -> None:
    """Clean only the owned deny policies of an interrupted lifecycle run."""
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
    client = DenyClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
    DenyRun(client, state, manifest=args.manifest).cleanup()
    sys.stdout.write('Owned deny policy cleanup passed.\n')


if __name__ == '__main__':
    main()
