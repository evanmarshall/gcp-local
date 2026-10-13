"""Read-only Organization Policy recordings: the sandbox organization's policies.

The scenarios observe what the sandbox project's callers see. Nothing is
created or changed: writes are sent only after testIamPermissions shows the
caller lacks the permission they need, so they can only be denied. The viewer
scenario needs roles/orgpolicy.policyViewer granted to the allowed identity on
the sandbox project for the run (docs/contributing/gcp-sandbox.md).
"""

import json
from collections.abc import Callable
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

from gcp_validation.evidence import Inconclusive, check_observation
from gcp_validation.live_rest import LiveScope, Observations, Response, RestClient
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.security.hierarchy import Snapshot, refusal

SERVICE = 'orgpolicy-visibility'
LIMITS = {'max_resource_writes': 0, 'max_run_seconds': 300}
# Live hosts by path prefix; locally every path goes to the one endpoint.
HOSTS = {'/v2/': 'https://orgpolicy.googleapis.com',
         '/v3/': 'https://cloudresourcemanager.googleapis.com',
         '/storage/v1/': 'https://storage.googleapis.com',
         '/v1/projects/': 'https://iam.googleapis.com'}
LIVE_ENDPOINT = HOSTS['/v2/']
MISSING_PROJECT = 'gcp-local-missing-project'
MISSING_FOLDER = 'folders/1'
LOCATIONS = '/policies/gcp.resourceLocations'
# The constraints gcp-local enforces, in GCP's constraints.list order.
CONSTRAINTS = ('gcp.resourceLocations', 'iam.allowedPolicyMemberDomains',
               'iam.disableServiceAccountKeyCreation', 'storage.uniformBucketLevelAccess',
               'storage.publicAccessPrevention',
               'iam.managed.disableServiceAccountKeyCreation')
PERMISSIONS = ['orgpolicy.policy.get', 'orgpolicy.policies.list', 'orgpolicy.constraints.list',
               'orgpolicy.policies.create', 'orgpolicy.policies.update',
               'orgpolicy.policies.delete']
WRITES = {'orgpolicy.policies.create', 'orgpolicy.policies.update', 'orgpolicy.policies.delete'}

Call = Callable[..., Response]


def load_scope(filename: str | Path | None, sandbox: Sandbox | None, mode: str) -> LiveScope:
    """Load the owner-approved read-only scope."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class OrgPolicyClient(RestClient):
    """RestClient admitting only this recording's paths, each sent to its API's live host."""

    def allowed_paths(self) -> tuple[str, ...]:
        """Return the path prefixes this scenario may send."""
        project, number = self.target.project, self.number
        return ('/v2/projects/' + project + '/', '/v2/projects/' + number + '/',
                '/v2/projects/' + MISSING_PROJECT + '/', '/v2/organizations/',
                '/v2/' + MISSING_FOLDER + '/', '/v3/projects/' + project)

    def request(self, actor: str, method: str, path: str, *, query: dict | None = None,
                body: object = None, timeout: float = 30) -> Response:
        """Send one request as a scenario identity to an allowed path."""
        if actor not in self.emails:
            raise ValueError('Unknown scenario identity')
        if not path.startswith(self.allowed_paths()):
            raise ValueError('Select a path this scenario sends')
        headers = {'Content-Type': 'application/json',
                   'Authorization': 'Bearer ' + self.token(actor)}
        endpoint = self.endpoint
        if self.target.mode != 'local':
            headers['x-goog-user-project'] = self.target.project
            endpoint = next(host for prefix, host in HOSTS.items() if path.startswith(prefix))
        empty = b'' if method == 'POST' else None
        data = json.dumps(body).encode() if body is not None else empty
        url = endpoint + path + ('?' + urlencode(query) if query else '')
        # The endpoint is a fixed https live host or the loopback target.
        request = Request(url, data=data, headers=headers, method=method)  # noqa: S310
        try:
            response = self.opener.open(request, timeout=timeout)
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError):
            raise Inconclusive('Transport failed; no cloud fallback is available') from None
        with response:
            return Response(response.code, response.read(4 << 20))


def aliases(client: RestClient, organization: str, effective: dict) -> Observations:
    """Alias identities, the project, the organization, and the permitted customers."""
    names = {client.emails[actor]: '<' + actor + '>' for actor in client.emails}
    names |= {client.target.project: '<project>', client.number: '<project-number>',
              organization.rsplit('/', 1)[-1]: '<organization>'}
    for rule in effective.get('spec', {}).get('rules', []):
        for index, value in enumerate(rule.get('values', {}).get('allowedValues', [])):
            names[value] = f'<customer-{index}>'
    return Observations(names)


def effective_view(observe: Observations, response: Response) -> dict:
    """Select an effective policy's status, name, and spec (whole, aliased)."""
    if response.status != 200:
        return refusal(observe, response)
    value = response.json()
    selected = {'http_status': response.status, 'fields': sorted(value),
                'name': observe.text(value.get('name', '')),
                'spec': observe.value(value.get('spec'))}
    check_observation(selected)
    return selected


def held(call: Call, actor: str) -> list:
    """Return the Organization Policy permissions actor holds on the sandbox project."""
    tested = call(actor, 'POST', '/v3/projects/{project}:testIamPermissions',
                  {'permissions': PERMISSIONS})
    if tested.status != 200:
        raise Inconclusive(f'testIamPermissions returned HTTP {tested.status}')
    return tested.json().get('permissions', [])


def project_parent(client: RestClient) -> tuple[Call, str]:
    """Return the call helper and the sandbox project's organization."""
    project = client.target.project

    def call(actor: str, method: str, path: str, body: object = None,
             query: dict | None = None) -> Response:
        path = path.replace('{project}', project).replace('{number}', client.number)
        return client.request(actor, method, path, body=body, query=query)

    found = call('fixture', 'GET', '/v3/projects/{project}')
    organization = found.json().get('parent', '') if found.status == 200 else ''
    if not organization.startswith('organizations/'):
        raise Inconclusive('The sandbox project is not directly under an organization')
    return call, organization


def parent_requests(parent: str, project: str) -> dict:
    """Return the list, write, and read requests a parent's denials are observed with."""
    prefix = '/v2/' + parent
    policy = {'name': parent + LOCATIONS,
              'spec': {'rules': [{'values': {'allowedValues': ['in:us-locations']}}]}}
    requests = {'list': ('GET', prefix + '/policies', None),
                'constraints': ('GET', prefix + '/constraints', None),
                'create': ('POST', prefix + '/policies', policy)}
    if parent != MISSING_FOLDER:
        requests |= {'patch': ('PATCH', prefix + LOCATIONS, policy),
                     'delete': ('DELETE', prefix + LOCATIONS, None)}
    if parent != project:
        requests['effective'] = ('GET', prefix + LOCATIONS + ':getEffectivePolicy', None)
    return requests


def run_visibility(client: OrgPolicyClient, snapshot: Snapshot) -> None:
    """Observe the fixture's effective policies and the refusals of every write and list."""
    call, organization = project_parent(client)
    permissions = held(call, 'fixture')
    snapshot.match('fixture-permissions', {'permissions': permissions})
    if set(permissions) - {'orgpolicy.policy.get'}:
        raise Inconclusive('The fixture may list or write policies; refusing to send the writes')
    base = '/v2/projects/{project}/policies/'
    domains = call('fixture', 'GET', base + 'iam.allowedPolicyMemberDomains:getEffectivePolicy')
    observe = aliases(client, organization, domains.json())
    for constraint in CONSTRAINTS:
        response = call('fixture', 'GET', base + constraint + ':getEffectivePolicy')
        label = 'effective-' + constraint.lower().replace('.', '-')
        snapshot.match(label, effective_view(observe, response))
    by_number = call('fixture', 'GET', '/v2/projects/{number}' + LOCATIONS + ':getEffectivePolicy')
    snapshot.match('effective-by-number', effective_view(observe, by_number))
    unset = call('fixture', 'GET', base + 'iam.allowedPolicyMemberDomains')
    snapshot.match('get-unset', refusal(observe, unset))
    denied = call('denied', 'GET', base + 'iam.allowedPolicyMemberDomains:getEffectivePolicy')
    snapshot.match('effective-denied', refusal(observe, denied))
    project = 'projects/' + client.target.project
    parents = (('project', project), ('organization', organization), ('folder', MISSING_FOLDER))
    for label, parent in parents:
        for name, (method, path, body) in parent_requests(parent, project).items():
            response = call('fixture', method, path, body)
            snapshot.match(f'{label}-{name}-denied', refusal(observe, response))
    missing = call('fixture', 'GET',
                   f'/v2/projects/{MISSING_PROJECT}{LOCATIONS}:getEffectivePolicy')
    snapshot.match('missing-project-effective', refusal(observe, missing))


def constraint_view(observe: Observations, response: Response) -> dict:
    """Select a constraints.list response: its keys and every enforced constraint, in order."""
    value = response.json()
    if response.status != 200:
        return refusal(observe, response)
    kept = [observe.value(item) for item in value.get('constraints', [])
            if item.get('name', '').rsplit('/', 1)[-1] in CONSTRAINTS]
    selected = {'http_status': response.status, 'fields': sorted(value), 'constraints': kept}
    check_observation(selected)
    return selected


def run_viewer(client: OrgPolicyClient, snapshot: Snapshot) -> None:
    """Observe constraint definitions and policy lists as a project policy viewer reads them."""
    call, organization = project_parent(client)
    permissions = held(call, 'allowed')
    if {'orgpolicy.constraints.list', 'orgpolicy.policies.list'} - set(permissions):
        raise Inconclusive('Grant the allowed identity roles/orgpolicy.policyViewer first')
    if set(permissions) & WRITES:
        raise Inconclusive('The allowed identity may write policies; this scenario only reads')
    snapshot.match('allowed-permissions', {'permissions': sorted(permissions)})
    observe = aliases(client, organization, {})
    constraints = '/v2/projects/{project}/constraints'
    snapshot.match('constraints', constraint_view(observe, call('allowed', 'GET', constraints)))
    paged = call('allowed', 'GET', constraints, query={'pageSize': '2', 'pageToken': 'bogus'})
    snapshot.match('constraints-page-options-ignored', constraint_view(observe, paged))
    by_number = call('allowed', 'GET', '/v2/projects/{number}/constraints')
    snapshot.match('constraints-by-number', constraint_view(observe, by_number))
    ignored = {'pageSize': '-1', 'pageToken': 'bogus'}
    for label, query in (('', None), ('-page-options-ignored', ignored)):
        listed = call('allowed', 'GET', '/v2/projects/{project}/policies', query=query)
        snapshot.match('policies' + label, {'http_status': listed.status, 'body': listed.json()})
