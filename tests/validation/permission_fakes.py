"""Controlled fake Secret Manager/IAM/CRM responses for permission-probe tests.

The fake models what the probe must cope with: grant and revocation propagation (with a
testIamPermissions hint that can converge before the data plane), custom-role support
levels, non-IAM errors, lost responses, and a transport that goes dark mid-run.
"""

import base64
import json
import re
from types import SimpleNamespace as ns  # noqa: N813 -- short alias keeps fakes readable

from gcp_validation.evidence import Inconclusive
from gcp_validation.live_rest import Response

PROJECT, NUMBER = 'sandbox-project', '123456789'
EMAILS = {
    actor: f'{actor}-sa@{PROJECT}.iam.gserviceaccount.com'
    for actor in ('fixture', 'allowed', 'denied')
}
ALLOWED_MEMBER = 'serviceAccount:' + EMAILS['allowed']
SECRET_TESTABLE = {
    'secretmanager.secrets.delete': 'SUPPORTED',
    'secretmanager.secrets.get': 'SUPPORTED',
    'secretmanager.secrets.getIamPolicy': 'SUPPORTED',
    'secretmanager.secrets.setIamPolicy': 'SUPPORTED',
    'secretmanager.secrets.update': 'SUPPORTED',
    'secretmanager.secrets.listEffectiveTags': 'TESTING',
    'secretmanager.secrets.listTagBindings': 'SUPPORTED',
    'secretmanager.versions.access': 'SUPPORTED',
    'secretmanager.versions.add': 'SUPPORTED',
    'secretmanager.versions.destroy': 'SUPPORTED',
    'secretmanager.versions.get': 'SUPPORTED',
    'secretmanager.versions.list': 'SUPPORTED',
}
PROJECT_TESTABLE = {
    **SECRET_TESTABLE,
    'resourcemanager.projects.get': 'SUPPORTED',
    'resourcemanager.projects.list': 'NOT_SUPPORTED',
    'secretmanager.locations.get': 'SUPPORTED',
    'secretmanager.locations.list': 'SUPPORTED',
    'secretmanager.secrets.create': 'SUPPORTED',
    'secretmanager.secrets.list': 'SUPPORTED',
}
PREDEFINED = {
    'roles/secretmanager.secretAccessor': frozenset({'secretmanager.versions.access'}),
    'roles/viewer': frozenset({'resourcemanager.projects.get'}),
}


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def error(code, status, reason=None):
    body = {'error': {'code': code, 'status': status, 'message': 'synthetic'}}
    if reason:
        body['error']['details'] = [
            {'@type': 'type.googleapis.com/google.rpc.ErrorInfo', 'reason': reason}
        ]
    return Response(code, json.dumps(body).encode())


def ok(value):
    return Response(200, json.dumps(value).encode())


class FakeCloud:
    """One sandbox project; every request is routed like the real REST surfaces."""

    def __init__(
        self,
        clock,
        *,
        required=frozenset({'secretmanager.versions.access'}),
        data_delay=60,
        hint_delay=30,
    ):
        self.clock, self.required = clock, frozenset(required)
        self.data_delay, self.hint_delay = data_delay, hint_delay
        self.target = ns(project=PROJECT, mode='local')
        self.number, self.emails, self.sandbox = NUMBER, dict(EMAILS), None
        self.secrets, self.roles = (
            {},
            {
                f'projects/{PROJECT}/roles/someoneElse': {
                    'perms': frozenset({'secretmanager.versions.access'}),
                    'description': 'not ours',
                    'deleted': False,
                    'etag': 'e0',
                }
            },
        )
        self.project_bindings = [{'role': 'roles/viewer', 'members': ['user:owner@example.com']}]
        self.history = [(float('-inf'), frozenset())]
        self.faults, self.lose, self.dark_after, self.requests = {}, set(), None, 0
        self.role_updates = 0

    # Propagation model -------------------------------------------------
    def _role_perms(self, role):
        if role in PREDEFINED:
            return PREDEFINED[role]
        value = self.roles.get(role)
        return value['perms'] if value and not value['deleted'] else frozenset()

    def _changed(self):
        bindings = [b for s in self.secrets.values() for b in s['bindings']] + self.project_bindings
        effective = frozenset().union(
            *(self._role_perms(b['role']) for b in bindings if ALLOWED_MEMBER in b['members'])
        )
        self.history.append((self.clock.monotonic(), effective))

    def effective(self, delay):
        moment = self.clock.monotonic() - delay
        return [perms for at, perms in self.history if at <= moment][-1]

    # Transport ---------------------------------------------------------
    def verify_project(self):
        self._gate()

    def _gate(self):
        if self.dark_after is not None and self.requests >= self.dark_after:
            raise Inconclusive('Transport failed; no cloud fallback is available')

    def request(self, actor, api, method, path, *, query=None, body=None):
        self._gate()
        self.requests += 1
        key, response = self._route(actor, api, method, path, query or {}, body or {})
        queued = self.faults.get(key)
        if queued:
            fault = queued.pop(0)
            if isinstance(fault, Exception):
                raise fault
            return fault
        if key in self.lose:
            self.lose.discard(key)
            raise Inconclusive('Transport failed; no cloud fallback is available')
        return response

    def _route(self, actor, api, method, path, query, body):
        handler = getattr(self, f'_{api}')
        return handler(actor, method, path, query, body)

    # Secret Manager ----------------------------------------------------
    def _secretmanager(self, actor, method, path, query, body):
        prefix = f'/v1/projects/{PROJECT}/secrets'
        if path == prefix and method == 'POST':
            return 'secret_create', self._create_secret(actor, query, body)
        match = re.fullmatch(prefix + r'/([a-z0-9-]+)(.*)', path)
        name = f'projects/{PROJECT}/secrets/{match.group(1)}'
        suffix, secret = match.group(2), self.secrets.get(name)
        if actor == 'allowed' and suffix == '/versions/1:access':
            return 'access', self._access(secret)
        if actor == 'allowed' and suffix == ':testIamPermissions':
            return 'hint', self._hint(body)
        if actor != 'fixture':
            return 'denied', error(403, 'PERMISSION_DENIED', 'IAM_PERMISSION_DENIED')
        if secret is None:
            return 'secret_missing', error(404, 'NOT_FOUND')
        return self._secret_admin(name, secret, method, suffix, body)

    def _create_secret(self, actor, query, body):
        name = f'projects/{PROJECT}/secrets/{query["secretId"]}'
        if actor != 'fixture':
            return error(403, 'PERMISSION_DENIED', 'IAM_PERMISSION_DENIED')
        if name in self.secrets:
            return error(409, 'ALREADY_EXISTS')
        self.secrets[name] = {
            'labels': body['labels'],
            'replication': body['replication'],
            'versions': {},
            'bindings': [],
        }
        return ok(
            {'name': f'projects/{NUMBER}/secrets/{query["secretId"]}', 'labels': body['labels']}
        )

    def _secret_admin(self, name, secret, method, suffix, body):
        numbered = name.replace(PROJECT, NUMBER)
        if suffix == '' and method == 'GET':
            return 'secret_get', ok(
                {'name': numbered, 'labels': secret['labels'], 'replication': secret['replication']}
            )
        if suffix == '' and method == 'DELETE':
            del self.secrets[name]
            self._changed()
            return 'secret_delete', ok({})
        if suffix == ':addVersion':
            secret['versions'][str(len(secret['versions']) + 1)] = 'ENABLED'
            return 'add_version', ok({'name': numbered + '/versions/1', 'state': 'ENABLED'})
        if suffix == '/versions':
            return 'versions', ok(
                {
                    'versions': [
                        {'name': f'{numbered}/versions/{number}', 'state': state}
                        for number, state in secret['versions'].items()
                    ]
                }
            )
        if suffix == ':getIamPolicy':
            return 'secret_policy', ok({'bindings': secret['bindings'], 'etag': 'BwE='})
        if suffix == ':setIamPolicy':
            secret['bindings'] = body['policy'].get('bindings', [])
            self._changed()
            return 'secret_set_policy', ok(body['policy'])
        raise AssertionError('unrouted ' + method + ' ' + suffix)

    def _access(self, secret):
        if secret is None:
            return error(404, 'NOT_FOUND')
        if not self.required <= self.effective(self.data_delay):
            return error(403, 'PERMISSION_DENIED', 'IAM_PERMISSION_DENIED')
        return ok({'name': 'v1', 'payload': {'data': base64.b64encode(b'x').decode()}})

    def _hint(self, body):
        requested = set(body['permissions'])
        if requested - set(SECRET_TESTABLE):
            return error(400, 'INVALID_ARGUMENT')
        return ok({'permissions': sorted(requested & self.effective(self.hint_delay))})

    # IAM ---------------------------------------------------------------
    def _iam(self, actor, method, path, query, body):  # noqa: PLR0911 -- one route per call
        assert actor == 'fixture'
        if path == '/v1/permissions:queryTestablePermissions':
            return 'testable', self._testable(body)
        if path == f'/v1/projects/{PROJECT}/roles' and method == 'POST':
            return 'role_create', self._create_role(body)
        role = self.roles.get(path[len('/v1/') :])
        if role is None:
            return 'role_missing', error(404, 'NOT_FOUND')
        if method == 'GET':
            return 'role_get', ok(
                {
                    'name': path[len('/v1/') :],
                    'description': role['description'],
                    'includedPermissions': sorted(role['perms']),
                    **({'deleted': True} if role['deleted'] else {}),
                }
            )
        if method == 'DELETE':
            role['deleted'] = True
            self._changed()
            return 'role_delete', ok({})
        assert method == 'PATCH'
        assert query == {'updateMask': 'includedPermissions'}
        if body['etag'] != role['etag']:
            return 'role_update', error(409, 'ABORTED')
        self.role_updates += 1
        role.update(perms=frozenset(body['includedPermissions']), etag=f'e{self.role_updates}')
        self._changed()
        return 'role_update', ok({'etag': role['etag']})

    def _testable(self, body):
        resource = body['fullResourceName']
        table = (
            PROJECT_TESTABLE if resource.startswith('//cloudresourcemanager') else SECRET_TESTABLE
        )
        names = sorted(table)
        start = int(body.get('pageToken', 0))
        page = names[start : start + 5]
        value = {
            'permissions': [
                {
                    'name': n,
                    **({} if table[n] == 'SUPPORTED' else {'customRolesSupportLevel': table[n]}),
                }
                for n in page
            ]
        }
        if start + 5 < len(names):
            value['nextPageToken'] = str(start + 5)
        return ok(value)

    def _create_role(self, body):
        name = f'projects/{PROJECT}/roles/{body["roleId"]}'
        perms = body['role']['includedPermissions']
        if any(PROJECT_TESTABLE.get(p) == 'NOT_SUPPORTED' for p in perms):
            return error(400, 'INVALID_ARGUMENT')
        if name in self.roles:
            return error(409, 'ALREADY_EXISTS')
        self.roles[name] = {
            'perms': frozenset(perms),
            'description': body['role']['description'],
            'deleted': False,
            'etag': 'e0',
        }
        self._changed()
        return ok({'name': name, 'etag': 'e0'})

    # Resource Manager --------------------------------------------------
    def _crm(self, actor, method, path, query, body):
        assert actor == 'fixture'
        assert method == 'POST'
        if path == f'/v1/projects/{PROJECT}:getIamPolicy':
            return 'project_policy', ok(
                {'bindings': [dict(b) for b in self.project_bindings], 'etag': 'p'}
            )
        assert path == f'/v1/projects/{PROJECT}:setIamPolicy'
        self.project_bindings = body['policy']['bindings']
        self._changed()
        return 'project_set_policy', ok(body['policy'])
