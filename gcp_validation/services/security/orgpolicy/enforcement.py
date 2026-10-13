"""Organization policy enforcement recording: what the sandbox organization's constraints refuse.

The sandbox organization enforces iam.allowedPolicyMemberDomains,
storage.uniformBucketLevelAccess, and iam.managed.disableServiceAccountKeyCreation
(orgpolicy-effective-visibility). This scenario sends the mutations they block
and records the refusals, after IAM has authorized them, and what the denied
identity gets for the same requests. It owns one bucket (without soft delete)
and one service account, both deleted before it ends; no key is created, and
one that is created anyway is deleted and fails the run.
"""

import argparse
import re
import sys
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, check_observation, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.security.hierarchy import Snapshot
from gcp_validation.services.security.orgpolicy.visibility import OrgPolicyClient

SERVICE = 'orgpolicy-enforcement'
LIMITS = {'max_buckets': 1, 'max_service_accounts': 1, 'max_keys': 0, 'max_run_seconds': 900}
LIVE_ENDPOINT = 'https://orgpolicy.googleapis.com'
TROUBLESHOOTER = ' Remediate access with this Troubleshooter URL'
PUBLIC = {'allUsers', 'allAuthenticatedUsers'}


def load_scope(filename: str | Path | None, sandbox: Sandbox | None, mode: str) -> LiveScope:
    """Load the owner-approved enforcement scope."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class EnforcementClient(OrgPolicyClient):
    """OrgPolicyClient that also admits the owned bucket's and service account's paths."""

    def allowed_paths(self) -> tuple[str, ...]:
        """Return the Storage and IAM paths this scenario sends."""
        return ('/storage/v1/b', '/v1/projects/' + self.target.project + '/serviceAccounts',
                '/v3/projects/' + self.target.project)


class EnforcementRun(OwnedRun):
    """One bucket and one service account, named by the run, under the Storage lock.

    The lock is the Storage scenarios' (storage.lock in the project's state
    directory), since the sandbox holds at most one bucket at a time.
    """

    def __init__(self, client: EnforcementClient, state_dir: Path, *,
                 manifest: str | None = None) -> None:
        """Name the run's bucket and account, or reload them from a manifest."""
        super().__init__(client, state_dir, 'storage', LIMITS['max_run_seconds'],
                         manifest=manifest,
                         check=lambda v: v.get('account_id') == 'gl-op-' + v['run_id'][:10])
        run = self.manifest['run_id']
        self.manifest.setdefault('account_id', 'gl-op-' + run[:10])
        self.manifest.setdefault('bucket', 'gcp-local-' + client.number + '-' + run)
        self.manifest.setdefault('keys', [])
        self.bucket, self.account_id = self.manifest['bucket'], self.manifest['account_id']
        self.email = f'{self.account_id}@{self.project}.iam.gserviceaccount.com'
        self.accounts = f'/v1/projects/{self.project}/serviceAccounts'
        self.account = f'{self.accounts}/{self.email}'
        self.bucket_path = '/storage/v1/b/' + self.bucket
        self.description = 'gcp-local conformance run ' + run

    def call(self, actor: str, method: str, path: str, body: object = None,
             query: dict | None = None) -> Response:
        """Send one request within the run's deadline."""
        return self.client.request(actor, method, path, body=body, query=query,
                                   timeout=self.remaining())

    def bucket_body(self, iam_configuration: dict) -> dict:
        """Return a create body for the owned bucket: labeled, regional, without soft delete."""
        region = self.client.sandbox.region if self.client.sandbox else 'us-central1'
        return {'name': self.bucket, 'location': region.upper(), 'storageClass': 'STANDARD',
                'labels': {'managed-by': 'gcp-local', 'gcp-local-run': self.manifest['run_id']},
                'softDeletePolicy': {'retentionDurationSeconds': '0'},
                'iamConfiguration': iam_configuration}

    def setup(self) -> None:
        """Take the lock, confirm the sandbox holds no bucket, and record intent."""
        self.acquire()
        self.client.verify_project()
        listed = self.call('fixture', 'GET', '/storage/v1/b',
                           query={'project': self.project, 'maxResults': 2})
        if listed.status != 200:
            raise Inconclusive(f'Bucket list returned HTTP {listed.status}')
        if listed.json().get('items') or listed.json().get('nextPageToken'):
            raise Inconclusive('Sandbox already has a bucket; clean owned resources first')
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.

    def cleanup(self) -> None:
        """Delete the owned bucket, any key, and the account, then require them gone."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            self.delete_bucket()
            self.delete_account()
            if (self.client.request('fixture', 'GET', self.bucket_path).status != 404
                    or self.remains(self.account)):
                raise ValueError('An owned resource remains after cleanup')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()

    def delete_bucket(self) -> None:
        """Delete the owned bucket if it exists."""
        bucket = self.client.request('fixture', 'GET', self.bucket_path)
        if bucket.status != 200:
            return
        if bucket.json().get('labels', {}).get('gcp-local-run') != self.manifest['run_id']:
            raise ValueError('Refusing bucket cleanup: ownership does not match the manifest')
        if self.client.request('fixture', 'DELETE', self.bucket_path).status != 204:
            raise ValueError('Owned bucket delete failed')

    def delete_account(self) -> None:
        """Delete any key the run recorded and the owned account, if it exists."""
        account = self.client.request('fixture', 'GET', self.account)
        if account.status != 200:
            return
        if account.json().get('description') != self.description:
            raise ValueError('Refusing account cleanup: ownership does not match the manifest')
        for key in self.manifest['keys']:
            self.client.request('fixture', 'DELETE', '/v1/' + key)
        if self.client.request('fixture', 'DELETE', self.account).status not in {200, 404}:
            raise ValueError('Owned service account delete failed')

    def remains(self, path: str) -> bool:
        """Report whether path still reads after the account's delete propagation."""
        response = self.wait_until(lambda: self.client.request('fixture', 'GET', path), {200}, 60)
        return response.status != 404


def storage_error(observe: Observations, response: Response) -> dict:
    """Select a Storage JSON API error: status, message, and reasons."""
    error = response.json().get('error', {})
    selected = {'http_status': response.status,
                'message': observe.text(error.get('message', '')),
                'reasons': [item.get('reason') for item in error.get('errors', [])]}
    check_observation(selected)
    return selected


def detail(observe: Observations, value: dict) -> dict:
    """Select an ErrorInfo's reason, domain, and metadata, or a PreconditionFailure's violations."""
    kind = value.get('@type', '').rsplit('.', 1)[-1]
    if kind == 'ErrorInfo':
        metadata = {key: observe.text(text) for key, text in value.get('metadata', {}).items()
                    if key not in {'troubleshooter_url', 'error_info_id'}}
        return {'type': kind, 'reason': value.get('reason'), 'domain': value.get('domain'),
                'metadata': metadata}
    if kind == 'PreconditionFailure':
        violations = [observe.value(item) for item in value.get('violations', [])]
        return {'type': kind, 'violations': violations}
    return {'type': kind}


def api_error(observe: Observations, response: Response) -> dict:
    """Select a Google API error: code, status, message, and details."""
    error = response.json().get('error', {})
    message = error.get('message', '').split(TROUBLESHOOTER)[0]
    selected = {'http_status': response.status, 'code': error.get('code'),
                'status': error.get('status'), 'message': observe.text(message),
                'details': [detail(observe, value) for value in error.get('details', [])]}
    check_observation(selected)
    return selected


def bindings(observe: Observations, response: Response) -> dict:
    """Select a policy's bindings, sorted."""
    if response.status != 200:
        return storage_error(observe, response)
    shapes = sorted([item['role'], sorted(observe.text(member) for member in item['members'])]
                    for item in response.json().get('bindings', []))
    return {'http_status': response.status, 'bindings': shapes}


def run_bucket_policy(run: EnforcementRun, observe: Observations, snapshot: Snapshot) -> None:
    """Observe public members refused on the bucket's policy, for the fixture and denied caller."""
    policy_path = run.bucket_path + '/iam'
    policy = run.wait_until(lambda: run.call('fixture', 'GET', policy_path), {403, 404}, 120)
    current = policy.json()
    for label, member in (('all-users', 'allUsers'),
                          ('all-authenticated-users', 'allAuthenticatedUsers')):
        binding = {'role': 'roles/storage.objectViewer', 'members': [member]}
        body = {**current, 'bindings': [*current.get('bindings', []), binding]}
        refused = run.call('fixture', 'PUT', policy_path, body)
        snapshot.match('bucket-policy-' + label, storage_error(observe, refused))
        denied = run.call('denied', 'PUT', policy_path, body)
        snapshot.match('bucket-policy-' + label + '-denied-caller', storage_error(observe, denied))
    after = run.call('fixture', 'GET', policy_path)
    # The default legacy bindings (projectOwner: and the like) aren't modeled;
    # what matters is that no public member got in.
    public = sorted({member for item in after.json().get('bindings', [])
                     for member in item['members'] if member in PUBLIC})
    snapshot.match('bucket-policy-without-public-members',
                   {'http_status': after.status, 'public_members': public})


def run_storage(run: EnforcementRun, observe: Observations, snapshot: Snapshot) -> None:
    """Observe bucket creates, IAM writes, and an update the constraints refuse."""
    project = {'project': run.project}
    disabled = run.bucket_body({'uniformBucketLevelAccess': {'enabled': False},
                                'publicAccessPrevention': 'inherited'})
    refused = run.call('fixture', 'POST', '/storage/v1/b', disabled, project)
    snapshot.match('bucket-create-uniform-access-disabled', storage_error(observe, refused))
    omitted = run.bucket_body({'publicAccessPrevention': 'inherited'})
    created = run.call('fixture', 'POST', '/storage/v1/b', omitted, project)
    if created.status != 200:
        raise AssertionError(f'Bucket create returned HTTP {created.status}')
    access = created.json().get('iamConfiguration', {})
    snapshot.match('bucket-create-access-omitted', {
        'http_status': created.status,
        'public_access_prevention': access.get('publicAccessPrevention'),
        'uniform_enabled': access.get('uniformBucketLevelAccess', {}).get('enabled'),
        'bucket_policy_only_enabled': access.get('bucketPolicyOnly', {}).get('enabled')})
    run_bucket_policy(run, observe, snapshot)
    patch = {'iamConfiguration': {'uniformBucketLevelAccess': {'enabled': False}}}
    patched = run.call('fixture', 'PATCH', run.bucket_path, patch)
    snapshot.match('bucket-disable-uniform-access', storage_error(observe, patched))
    deleted = run.call('fixture', 'DELETE', run.bucket_path)
    snapshot.match('bucket-deleted', {'http_status': deleted.status})


def run_account(run: EnforcementRun, observe: Observations, snapshot: Snapshot) -> None:
    """Observe a public member on the account's policy, then key creation as each caller."""
    policy = run.wait_until(lambda: run.call('fixture', 'POST', run.account + ':getIamPolicy', {}),
                            {403, 404}, 120)
    etag = policy.json().get('etag', '')
    public = {'policy': {'etag': etag, 'bindings': [
        {'role': 'roles/iam.serviceAccountUser', 'members': ['allUsers']}]}}
    refused = run.call('fixture', 'POST', run.account + ':setIamPolicy', public)
    snapshot.match('account-policy-all-users', api_error(observe, refused))
    fixture = 'serviceAccount:' + run.client.emails['fixture']
    grant = {'policy': {'etag': etag, 'bindings': [
        {'role': 'roles/iam.serviceAccountKeyAdmin', 'members': [fixture]}]}}
    granted = run.call('fixture', 'POST', run.account + ':setIamPolicy', grant)
    snapshot.match('account-key-admin-granted', bindings(observe, granted))
    denied = run.call('denied', 'POST', run.account + '/keys', {})
    snapshot.match('key-create-denied-caller', observe.error(denied))
    # Until the grant propagates key creation is 403; a new account can also
    # read as missing (404) for a while.
    created = run.wait_until(lambda: run.call('fixture', 'POST', run.account + '/keys', {}),
                             {403, 404}, 300)
    if created.status == 200:
        run.manifest['keys'].append(created.json().get('name', ''))
        run.save()
        raise AssertionError('The sandbox created a service account key; the constraint is gone')
    snapshot.match('key-create-refused', api_error(observe, created))


def run_scenario(run: EnforcementRun, snapshot: Snapshot) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = run.client
    names = {client.emails[actor]: '<' + actor + '>' for actor in client.emails}
    names |= {run.bucket: '<bucket>', run.email: '<account>', run.manifest['run_id']: '<run>',
              client.target.project: '<project>', client.number: '<project-number>'}
    run_storage(run, Observations(names), snapshot)
    created = run.call('fixture', 'POST', run.accounts, {
        'accountId': run.account_id,
        'serviceAccount': {'displayName': 'gcp-local conformance', 'description': run.description}})
    if created.status != 200 or created.json().get('description') != run.description:
        raise AssertionError(f'Service account create returned HTTP {created.status}')
    unique = created.json().get('uniqueId', '')
    if not re.fullmatch('[0-9]{21}', unique):
        raise AssertionError('Service account unique ID is not numeric')
    run_account(run, Observations({**names, unique: '<account-unique-id>'}), snapshot)
    deleted = run.call('fixture', 'DELETE', run.account)
    snapshot.match('account-deleted', {'http_status': deleted.status})


def main(argv: list[str] | None = None) -> None:
    """Clean only the owned bucket and account of an interrupted run."""
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
    client = EnforcementClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    state = Path.home() / '.local/state/gcp-local' / sandbox.project
    EnforcementRun(client, state, manifest=args.manifest).cleanup()
    sys.stdout.write('Owned bucket and service-account cleanup passed.\n')


if __name__ == '__main__':
    main()
