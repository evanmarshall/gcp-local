"""Secret Manager ``versions.access`` as the first permission-probe target.

The fixture identity creates one labeled secret with one enabled version and one
project-level custom role, and binds that role to the probe identity (the sandbox
``allowed`` caller) at an explicit attachment point: the secret itself by default, or the
project. Every resource is named in an owner-only manifest before it is created, and the
cleanup deletes only those resources after verifying their ownership marks.
"""

import base64
import json
import re
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request

from .evidence import Inconclusive, now
from .live_rest import LiveScope, OwnedRun, Response, RestClient
from .permission_probe import (
    Budget,
    BudgetedTransport,
    Clock,
    ProbeRunner,
    Transport,
    role_snapshots,
)

SERVICE = 'permissiondiscovery'
METHOD = 'secretmanager.versions.access'
LIMITS = {
    'max_trials': 16,
    'max_role_updates': 12,
    'max_custom_roles': 1,
    'max_secrets': 1,
    'max_requests': 800,
    'max_run_seconds': 7200,
    'max_propagation_seconds': 420,
    'max_operation_seconds': 60,
    'poll_interval_seconds': 15,
    'removal_confirmations': 3,
}
LIVE_ENDPOINTS = {
    'secretmanager': 'https://secretmanager.googleapis.com',
    'iam': 'https://iam.googleapis.com',
    'crm': 'https://cloudresourcemanager.googleapis.com',
}
TESTABLE_PATH = '/v1/permissions:queryTestablePermissions'
MAX_TESTABLE_PAGES = 20
SEED_ROLES = ('roles/secretmanager.secretAccessor', 'roles/secretmanager.viewer')
ROLE_DESCRIPTION = 'gcp-local permission probe run '
PAYLOAD = b'gcp-local synthetic permission-probe payload'
ATTACHMENTS = ('secret', 'project')
ROLES_DIR = Path(__file__).resolve().parents[1] / 'internal/iam/roles'


class _GlobalIamClient(RestClient):
    """RestClient plus exactly one global, read-only IAM path outside the project prefix."""

    def request(
        self,
        actor: str,
        method: str,
        path: str,
        *,
        query: dict | None = None,
        body: dict | None = None,
        timeout: int = 30,
    ) -> Response:
        """Forward project paths to RestClient; send only queryTestablePermissions globally."""
        if path != TESTABLE_PATH:
            return super().request(actor, method, path, query=query, body=body, timeout=timeout)
        if method != 'POST' or query:
            raise ValueError('queryTestablePermissions is a POST without a query string')
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.token(actor),
            'x-goog-user-project': self.target.project,
        }
        data = json.dumps(body or {}).encode()
        # The endpoint is the fixed https IAM URL, never caller-supplied.
        request = Request(self.endpoint + path, data=data, headers=headers, method='POST')  # noqa: S310
        try:
            response = self.opener.open(request, timeout=timeout)
        except HTTPError as error:
            response = error
        except OSError:
            raise Inconclusive('Transport failed; no cloud fallback is available') from None
        with response:
            return Response(response.code, response.read(1 << 20))


class LiveTransport:
    """Sandbox ADC per API endpoint; every path stays in the selected project."""

    def __init__(self, target: object, sandbox: object, scope: LiveScope) -> None:
        """Build one explicit-identity client per API; local mode is refused."""
        if target.mode not in {'record', 'revalidate'}:
            raise ValueError('Permission probes run only against an approved live sandbox')
        self.clients = {
            api: (_GlobalIamClient if api == 'iam' else RestClient)(
                target, endpoint, sandbox=sandbox, scope=scope
            )
            for api, endpoint in LIVE_ENDPOINTS.items()
        }
        first = self.clients['secretmanager']
        self.target, self.sandbox, self.number, self.emails = (
            target,
            sandbox,
            first.number,
            first.emails,
        )

    def request(
        self,
        actor: str,
        api: str,
        method: str,
        path: str,
        *,
        query: dict | None = None,
        body: dict | None = None,
    ) -> Response:
        """Send one request through the named API's client."""
        return self.clients[api].request(actor, method, path, query=query, body=body)

    def verify_project(self) -> None:
        """Confirm the live project number matches the approved scope."""
        self.clients['crm'].verify_project()


def manifest_names(project: str, run: str) -> dict[str, str]:
    """Derive every owned resource name from the run ID alone."""
    return {
        'secret': f'projects/{project}/secrets/gcp-local-probe-{run}',
        'role': f'projects/{project}/roles/gcp_local_probe_{run}',
    }


def owned_run(
    transport: Transport,
    state_dir: Path,
    *,
    attachment: str = 'secret',
    manifest: Path | None = None,
) -> OwnedRun:
    """Open a new or existing ownership manifest whose names derive from its run ID."""

    def check(value: dict) -> bool:
        names = manifest_names(value['project'], value['run_id'])
        return (
            value.get('method') == METHOD
            and value.get('attachment') in ATTACHMENTS
            and value.get('secret') == names['secret']
            and value.get('role') == names['role']
            and all(
                type(value.get(key)) is bool
                for key in ('secret_attempted', 'role_attempted', 'bound_attempted')
            )
        )

    owned = OwnedRun(
        transport, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest, check=check
    )
    if manifest is None:
        if attachment not in ATTACHMENTS:
            raise ValueError('Select an explicit attachment point: secret or project')
        owned.manifest.update(
            manifest_names(owned.project, owned.manifest['run_id']),
            method=METHOD,
            attachment=attachment,
            secret_attempted=False,
            role_attempted=False,
            bound_attempted=False,
        )
        owned.save()
    return owned


def numbered(name: str, project: str, number: str) -> set[str]:
    """GCP may answer with the project number in place of its ID."""
    return {name, name.replace('projects/' + project + '/', 'projects/' + number + '/', 1)}


def query_testable(transport: Transport, full_resource_name: str) -> dict[str, str]:
    """Map each testable permission on a resource to its custom-role support level."""
    levels, token = {}, None
    # Observed live: service resources (secrets) must name the project by number
    # here, or the call is 400 INVALID_ARGUMENT; project resources accept the ID.
    if full_resource_name.startswith('//secretmanager.googleapis.com/projects/'):
        project = full_resource_name.split('/')[4]
        full_resource_name = full_resource_name.replace(
            f'/projects/{project}/', f'/projects/{transport.number}/', 1)
    for _ in range(MAX_TESTABLE_PAGES):
        body = {'fullResourceName': full_resource_name, 'pageSize': 1000}
        if token:
            body['pageToken'] = token
        response = transport.request('fixture', 'iam', 'POST', TESTABLE_PATH, body=body)
        if response.status != 200:
            raise Inconclusive(f'queryTestablePermissions returned HTTP {response.status}')
        value = response.json()
        for item in value.get('permissions', []):
            # An omitted level is the enum default, SUPPORTED.
            levels[item['name']] = item.get('customRolesSupportLevel', 'SUPPORTED')
        token = value.get('nextPageToken')
        if not token:
            return levels
    raise Inconclusive('Testable permission listing exceeded its page bound')


class PolicyAttachment:
    """Read-modify-write of one resource's IAM policy, touching only the probe binding."""

    def __init__(self, transport: Transport, kind: str, resource: str) -> None:
        """Select the secret or project policy by its resource name."""
        self.transport, self.kind, self.resource = transport, kind, resource
        if kind == 'secret':
            self.api, self.full_name = 'secretmanager', '//secretmanager.googleapis.com/' + resource
        else:
            self.api, self.full_name = 'crm', '//cloudresourcemanager.googleapis.com/' + resource

    def read(self) -> Response:
        """Read the version 3 policy so conditional bindings are preserved."""
        if self.kind == 'secret':
            return self.transport.request(
                'fixture',
                self.api,
                'GET',
                f'/v1/{self.resource}:getIamPolicy',
                query={'options.requestedPolicyVersion': 3},
            )
        return self.transport.request(
            'fixture',
            self.api,
            'POST',
            f'/v1/{self.resource}:getIamPolicy',
            body={'options': {'requestedPolicyVersion': 3}},
        )

    def change(self, role: str, member: str, *, add: bool) -> None:
        """Add or remove one member of one role binding, with etag protection."""
        current = self.read()
        if current.status != 200:
            raise Inconclusive(f'{self.kind} getIamPolicy returned HTTP {current.status}')
        policy = current.json()
        bindings = [dict(item) for item in policy.get('bindings', [])]
        ours = [item for item in bindings if item.get('role') == role]
        if any(item.get('condition') or set(item.get('members', [])) - {member} for item in ours):
            raise ValueError(
                'Refusing to change a probe-role binding with foreign members or conditions'
            )
        bindings = [item for item in bindings if item.get('role') != role]
        if add:
            bindings.append({'role': role, 'members': [member]})
        policy['bindings'] = bindings
        changed = self.transport.request(
            'fixture',
            self.api,
            'POST',
            f'/v1/{self.resource}:setIamPolicy',
            body={'policy': policy},
        )
        if changed.status != 200:
            raise Inconclusive(f'{self.kind} setIamPolicy returned HTTP {changed.status}')

    def foreign_bindings(self, role: str) -> list:
        """Return the policy's bindings other than the probe role, for state snapshots."""
        current = self.read()
        if current.status != 200:
            raise Inconclusive(f'{self.kind} getIamPolicy returned HTTP {current.status}')
        return sorted(
            (item for item in current.json().get('bindings', []) if item.get('role') != role),
            key=lambda item: json.dumps(item, sort_keys=True),
        )


class CustomRoleGrant:
    """One project-level custom role, bound to the probe identity at one attachment point."""

    def __init__(self, transport: Transport, owned: OwnedRun, budget: Budget) -> None:
        """Bind the run's manifest; nothing is created until the first non-empty grant."""
        self.transport, self.owned, self.budget = transport, owned, budget
        manifest = owned.manifest
        self.role, self.run = manifest['role'], manifest['run_id']
        self.member = 'serviceAccount:' + transport.emails['allowed']
        resource = (
            manifest['secret']
            if manifest['attachment'] == 'secret'
            else 'projects/' + owned.project
        )
        self.attachment = PolicyAttachment(transport, manifest['attachment'], resource)
        self.current: frozenset[str] = frozenset()
        self.created, self.bound, self.etag = False, False, None

    def support(self, permissions: list[str]) -> dict[str, str]:
        """Permissions not testable at the attachment point cannot take effect there."""
        levels = query_testable(self.transport, self.attachment.full_name)
        return {permission: levels.get(permission, 'NOT_APPLICABLE') for permission in permissions}

    def ensure(self, permissions: frozenset[str]) -> bool:
        """Create, update, bind, or unbind so the probe identity holds exactly these permissions."""
        permissions = frozenset(permissions)
        if permissions == self.current and self.bound == bool(permissions):
            return False
        if not permissions:
            # An empty role is never sent; the binding is removed instead.
            self.attachment.change(self.role, self.member, add=False)
            self.bound = False
            return True
        if not self.created:
            self._create(permissions)
        elif permissions != self.current:
            self._update(permissions)
        if not self.bound:
            self.owned.manifest['bound_attempted'] = True
            self.owned.save()
            self.attachment.change(self.role, self.member, add=True)
            self.bound = True
        return True

    def _create(self, permissions: frozenset[str]) -> None:
        self.budget.spend('custom_role')
        self.owned.manifest['role_attempted'] = True
        self.owned.save()  # Creation may succeed even if the response is lost.
        project = 'projects/' + self.owned.project
        response = self.transport.request(
            'fixture',
            'iam',
            'POST',
            f'/v1/{project}/roles',
            body={
                'roleId': self.role.rsplit('/', 1)[1],
                'role': {
                    'title': 'gcp-local permission probe',
                    'description': ROLE_DESCRIPTION + self.run,
                    'includedPermissions': sorted(permissions),
                    'stage': 'GA',
                },
            },
        )
        if response.status != 200:
            raise Inconclusive(f'Custom role create returned HTTP {response.status}')
        self.created, self.current, self.etag = True, permissions, response.json().get('etag')

    def _update(self, permissions: frozenset[str]) -> None:
        self.budget.spend('role_update')
        response = self.transport.request(
            'fixture',
            'iam',
            'PATCH',
            '/v1/' + self.role,
            query={'updateMask': 'includedPermissions'},
            body={'includedPermissions': sorted(permissions), 'etag': self.etag},
        )
        if response.status != 200:
            raise Inconclusive(f'Custom role update returned HTTP {response.status}')
        self.current, self.etag = permissions, response.json().get('etag')

    def describe(self) -> dict:
        """Name the grant mechanism and the attachment point separately from the check target."""
        return {
            'mechanism': 'custom-role',
            'role_level': 'project',
            'role': self.role,
            'attachment': self.attachment.kind,
            'attachment_resource': self.attachment.full_name,
            'member': self.member,
            'grantor': 'fixture',
        }


class SecretAccessTarget:
    """``GET .../versions/1:access`` by the probe identity on a fixture-owned secret."""

    method = METHOD
    long_running = False

    def __init__(
        self, transport: Transport, owned: OwnedRun, budget: Budget, *, roles_dir: Path = ROLES_DIR
    ) -> None:
        """Bind the run manifest; the fixture is created by prepare()."""
        self.transport, self.owned, self.budget = transport, owned, budget
        self.secret, self.role = owned.manifest['secret'], owned.manifest['role']
        self.labels = {'managed-by': 'gcp-local', 'gcp-local-run': owned.manifest['run_id']}
        self.policy = PolicyAttachment(transport, 'secret', self.secret)
        self.roles_dir, self.testable = Path(roles_dir), {}

    def _call(
        self,
        actor: str,
        method: str,
        suffix: str = '',
        *,
        body: dict | None = None,
        query: dict | None = None,
    ) -> Response:
        return self.transport.request(
            actor, 'secretmanager', method, '/v1/' + self.secret + suffix, query=query, body=body
        )

    def prepare(self) -> None:
        """Create the labeled secret and one enabled version as the fixture identity."""
        self.owned.acquire()
        self.transport.verify_project()
        self.budget.spend('secret')
        self.owned.manifest['secret_attempted'] = True
        self.owned.save()  # Creation may succeed even if the response is lost.
        created = self.transport.request(
            'fixture',
            'secretmanager',
            'POST',
            f'/v1/projects/{self.owned.project}/secrets',
            query={'secretId': self.secret.rsplit('/', 1)[1]},
            body={'replication': {'automatic': {}}, 'labels': self.labels},
        )
        if created.status != 200:
            raise Inconclusive(f'Secret create returned HTTP {created.status}')
        added = self._call(
            'fixture',
            'POST',
            ':addVersion',
            body={'payload': {'data': base64.b64encode(PAYLOAD).decode()}},
        )
        if added.status != 200:
            raise Inconclusive(f'Secret addVersion returned HTTP {added.status}')
        if self.policy.foreign_bindings(self.role):
            raise Inconclusive('New secret unexpectedly has a direct IAM policy')
        self.testable = query_testable(self.transport, self.policy.full_name)

    def describe(self) -> dict:
        """Return the scenario's request features, check target, identity, and preconditions."""
        return {
            'service': 'secretmanager',
            'method': METHOD,
            'api_version': 'v1',
            'request_features': {
                'http_method': 'GET',
                'path': '/v1/projects/{project}/secrets/{secret}/versions/{version}:access',
                'version_reference': 'numeric',
                'features': {},
            },
            'check_target': '//secretmanager.googleapis.com/' + self.secret,
            'acting_identity': 'allowed',
            'preconditions': {
                'secret_replication': 'automatic',
                'version_state': 'ENABLED',
                'versions': 1,
                'secret_policy': 'probe binding only',
                'caller_quota_project': 'Service Usage Consumer on the sandbox',
                'grantor': 'fixture',
            },
        }

    def seed_permissions(self) -> dict:
        """Seed candidates from the committed, checksummed predefined-role snapshots."""
        roles = role_snapshots(self.roles_dir)
        permissions = sorted({p for name in SEED_ROLES for p in roles[name]['permissions']})
        return {
            'sources': {name: roles[name]['sha256'] for name in SEED_ROLES},
            'permissions': permissions,
        }

    def snapshot(self) -> dict:
        """Secret metadata, version states, and non-probe policy bindings; never payloads."""
        secret = self._call('fixture', 'GET')
        versions = self._call('fixture', 'GET', '/versions')
        if secret.status != 200 or versions.status != 200:
            raise Inconclusive('Fixture state could not be read')
        value = secret.json()
        return {
            'labels': value.get('labels'),
            'replication': value.get('replication'),
            'versions': sorted(
                (item['name'].rsplit('/', 1)[1], item['state'])
                for item in versions.json().get('versions', [])
            ),
            'bindings': self.policy.foreign_bindings(self.role),
        }

    def invoke(self) -> Response:
        """Access version 1 as the probe identity."""
        return self._call('allowed', 'GET', '/versions/1:access')

    def operation(self, name: str) -> Response:
        """Payload access is synchronous; there is no operation to read."""
        raise ValueError('versions.access returns no long-running operation: ' + name)

    def hintable(self, permissions: list[str]) -> list[str]:
        """Only permissions testable on the secret can be hinted there."""
        return [permission for permission in permissions if permission in self.testable]

    def hint(self, permissions: list[str]) -> set[str] | None:
        """Ask testIamPermissions, as the probe identity, which candidates it holds."""
        response = self._call(
            'allowed', 'POST', ':testIamPermissions', body={'permissions': permissions}
        )
        if response.status != 200:
            return None
        return set(response.json().get('permissions', []))

    def restore(self) -> None:
        """Payload access changes no state; the snapshots verify that."""


def aliases(transport: Transport, owned: OwnedRun) -> dict[str, str]:
    """Replace identifiers in reports while keeping their relationships."""
    manifest, number = owned.manifest, transport.number
    values = {owned.project: '<project>', number: '<project-number>', manifest['run_id']: '<run>'}
    values.update({email: '<' + actor + '>' for actor, email in transport.emails.items()})
    return values


def _verify_role(transport: Transport, manifest: dict) -> None:
    current = transport.request('fixture', 'iam', 'GET', '/v1/' + manifest['role'])
    if current.status == 404:
        return
    if current.status != 200:
        raise ValueError(f'Owned custom role read returned HTTP {current.status}')
    value = current.json()
    if (
        value.get('name') != manifest['role']
        or value.get('description') != ROLE_DESCRIPTION + manifest['run_id']
    ):
        raise ValueError('Refusing cleanup: custom role ownership does not match the manifest')
    if not value.get('deleted'):
        deleted = transport.request('fixture', 'iam', 'DELETE', '/v1/' + manifest['role'])
        if deleted.status not in {200, 404}:
            raise ValueError(f'Owned custom role delete returned HTTP {deleted.status}')
    after = transport.request('fixture', 'iam', 'GET', '/v1/' + manifest['role'])
    if after.status != 404 and not (after.status == 200 and after.json().get('deleted')):
        raise ValueError('Owned custom role remains after deletion')


def _delete_secret(transport: Transport, owned: OwnedRun) -> None:
    manifest, path = owned.manifest, '/v1/' + owned.manifest['secret']
    current = transport.request('fixture', 'secretmanager', 'GET', path)
    if current.status == 200:
        labels = {'managed-by': 'gcp-local', 'gcp-local-run': manifest['run_id']}
        names = numbered(manifest['secret'], owned.project, transport.number)
        if current.json().get('name') not in names or current.json().get('labels') != labels:
            raise ValueError('Refusing cleanup: secret ownership does not match the manifest')
        deleted = transport.request('fixture', 'secretmanager', 'DELETE', path)
        if deleted.status not in {200, 404}:
            raise ValueError(f'Owned secret delete returned HTTP {deleted.status}')
    elif current.status != 404:
        raise ValueError(f'Owned secret read returned HTTP {current.status}')
    if transport.request('fixture', 'secretmanager', 'GET', path).status != 404:
        raise ValueError('Owned secret remains after deletion')


def _unbind(transport: Transport, owned: OwnedRun) -> None:
    manifest = owned.manifest
    if manifest['attachment'] == 'secret':
        probe = transport.request('fixture', 'secretmanager', 'GET', '/v1/' + manifest['secret'])
        if probe.status == 404:
            return
    resource = (
        manifest['secret'] if manifest['attachment'] == 'secret' else 'projects/' + owned.project
    )
    PolicyAttachment(transport, manifest['attachment'], resource).change(
        manifest['role'], 'serviceAccount:' + transport.emails['allowed'], add=False
    )


def cleanup(transport: Transport, owned: OwnedRun) -> None:
    """Remove the probe binding, then the custom role, then the secret; owned resources only."""
    manifest = owned.manifest
    if not any(manifest[key] for key in ('secret_attempted', 'role_attempted', 'bound_attempted')):
        manifest.update(cleanup='passed', finished_at=now())
        owned.save()
        owned.release()
        return
    owned.acquire()
    try:
        transport.verify_project()
        if manifest['bound_attempted']:
            _unbind(transport, owned)
        if manifest['role_attempted']:
            _verify_role(transport, manifest)
        if manifest['secret_attempted']:
            _delete_secret(transport, owned)
        manifest.update(cleanup='passed', finished_at=now())
        owned.save()
    finally:
        owned.release()


def scope_document(project: str, project_number: str) -> dict:
    """Return the exact scope file LiveScope accepts, left unapproved for the owner."""
    return {
        'schema': 1,
        'approved': False,
        'services': [SERVICE],
        'project': project,
        'project_number': project_number,
        'location': 'global',
        'limits': dict(LIMITS),
        'cleanup_required': True,
        'scheduled_runs': False,
    }


def live_plan(project: str, project_number: str, emails: dict[str, str], attachment: str) -> dict:
    """Describe the bounded live run without contacting GCP."""
    if attachment not in ATTACHMENTS or not re.fullmatch(r'[a-z][a-z0-9-]{4,28}[a-z0-9]', project):
        raise ValueError('Select a sandbox project and an explicit attachment point')
    fixture = 'serviceAccount:' + emails['fixture']
    grants = [
        {
            'member': fixture,
            'role': 'roles/secretmanager.admin',
            'on': 'project',
            'temporary': True,
            'why': 'create, read, and delete the one labeled secret, add one version, '
            'and set its IAM policy',
        },
        {
            'member': fixture,
            'role': 'roles/iam.roleAdmin',
            'on': 'project',
            'temporary': True,
            'why': 'create, update, and delete the one project-level custom role',
        },
    ]
    if attachment == 'project':
        grants.append(
            {
                'member': fixture,
                'role': 'roles/resourcemanager.projectIamAdmin',
                'on': 'project',
                'temporary': True,
                'why': 'add and remove the probe binding on the project policy',
            }
        )
    commands = [
        f'gcloud projects {verb} {project} --member={grant["member"]} '
        f'--role={grant["role"]} --condition=None'
        for verb in ('add-iam-policy-binding', 'remove-iam-policy-binding')
        for grant in grants
    ]
    return {
        'status': 'prepared, not executed',
        'method': METHOD,
        'attachment': attachment,
        'apis': [
            'secretmanager.googleapis.com',
            'iam.googleapis.com',
            'cloudresourcemanager.googleapis.com',
        ],
        'probe_identity': {
            'member': 'serviceAccount:' + emails['allowed'],
            'new_grants': 'none',
            'must_not_hold': 'any candidate permission on the secret (the control trial checks)',
            'keeps': 'Service Usage Consumer for the x-goog-user-project quota header',
        },
        'temporary_fixture_grants': grants,
        'owner_commands': commands,
        'seed_roles': list(SEED_ROLES),
        'limits': dict(LIMITS),
        'scope_file': scope_document(project, project_number),
        'creates': {'secrets': 1, 'secret_versions': 1, 'custom_roles': 1, 'bindings': 1},
        'notes': [
            'Deleted custom roles count toward the 300-per-project quota until purged, and '
            'their IDs cannot be reused; each run uses a new run-ID role.',
            'Revoke the temporary fixture grants after cleanup passes.',
        ],
    }


def run_probe(
    transport: Transport,
    state_dir: Path,
    *,
    attachment: str,
    clock: Clock,
    source: str,
    limits: dict = LIMITS,
    roles_dir: Path = ROLES_DIR,
) -> tuple[dict, Path]:
    """Run one bounded probe and always attempt owned cleanup; return the report and manifest."""
    owned = owned_run(transport, state_dir, attachment=attachment)
    budget = Budget(limits, clock)
    budgeted = BudgetedTransport(transport, budget)
    target = SecretAccessTarget(budgeted, owned, budget, roles_dir=roles_dir)
    grant = CustomRoleGrant(budgeted, owned, budget)
    runner = ProbeRunner(
        target, grant, budget, clock=clock, aliases=aliases(transport, owned), source=source
    )
    report = None
    try:
        report = runner.run()
    finally:
        # Cleanup uses the unbudgeted transport so exhausted limits never block it.
        status = 'failed'
        try:
            cleanup(transport, owned)
            status = 'passed'
        except (Inconclusive, ValueError, OSError):
            owned.release()
        if report is not None:
            report['cleanup'] = status
    return report, owned.path
