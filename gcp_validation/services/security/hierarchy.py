"""Read-only Resource Manager recording: the hierarchy as callers without organization roles see it.

Nothing is created or changed. The folder create and project move requests run
only after testIamPermissions shows the caller lacks the permissions they need,
so they can only be denied; every other mutation names folders/1, which no
caller can reach.
"""

import json
from collections.abc import Callable
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

from gcp_validation.evidence import Inconclusive, check_observation
from gcp_validation.live_rest import LiveScope, Observations, Response, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = 'resourcemanager-hierarchy'
LIMITS = {'max_resource_writes': 0, 'max_run_seconds': 300}
LIVE_ENDPOINT = 'https://cloudresourcemanager.googleapis.com'
MISSING_FOLDER = 'folders/1'
MISSING_PROJECT = 'gcp-local-missing-project'
DENIED_NAME = 'gcp-local-denied'
# The path prefixes this scenario may send, beyond the sandbox project's own.
READ_PATHS = ('/v3/organizations', '/v3/folders', '/v3/projects:search',
              '/v3/projects/' + MISSING_PROJECT, '/v3/operations/', '/v1/operations/')
PROJECT_PERMISSIONS = ['resourcemanager.projects.get', 'resourcemanager.projects.move',
                       'resourcemanager.projects.update']
ORGANIZATION_PERMISSIONS = ['resourcemanager.organizations.get',
                            'resourcemanager.organizations.getIamPolicy',
                            'resourcemanager.organizations.setIamPolicy',
                            'resourcemanager.folders.create', 'resourcemanager.folders.list']
TROUBLESHOOTER = ' Remediate access with this Troubleshooter URL'


class Snapshot(Protocol):
    """The snapshot fixture's matching interface."""

    def match(self, name: str, value: object) -> None:
        """Record or compare one observation."""


Call = Callable[..., Response]


def load_scope(filename: str | None, sandbox: Sandbox | None, mode: str) -> LiveScope:
    """Load the owner-approved read-only scope."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class HierarchyClient(RestClient):
    """RestClient whose path guard admits the hierarchy requests this scenario sends."""

    def request(self, actor: str, method: str, path: str, *, query: dict | None = None,
                body: object = None, timeout: float = 30) -> Response:
        """Send one request as the fixture or denied caller to an allowed path."""
        if actor not in ('fixture', 'denied'):
            raise ValueError('Unknown scenario identity')
        own = ('/v3/projects/' + self.target.project, '/v1/projects/' + self.target.project)
        if not (path == '/v3/projects' or path.startswith(READ_PATHS) or path.startswith(own)):
            raise ValueError('Select a Resource Manager path this scenario reads')
        headers = {'Content-Type': 'application/json',
                   'Authorization': 'Bearer ' + self.token(actor)}
        if self.target.mode != 'local':
            headers['x-goog-user-project'] = self.target.project
        empty = b'' if method == 'POST' else None
        data = json.dumps(body).encode() if body is not None else empty
        url = self.endpoint + path + ('?' + urlencode(query) if query else '')
        # The endpoint is the fixed https live endpoint or the loopback target.
        request = Request(url, data=data, headers=headers, method=method)  # noqa: S310
        try:
            response = self.opener.open(request, timeout=timeout)
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError):
            raise Inconclusive('Transport failed; no cloud fallback is available') from None
        with response:
            return Response(response.code, response.read(1 << 20))


def refusal(observe: Observations, response: Response) -> dict:
    """Select a refusal's code, message (without the troubleshooter link), and details."""
    error = response.json().get('error')
    if not isinstance(error, dict):
        raise TypeError('Unexpected error structure')
    message = error.get('message', '').split(TROUBLESHOOTER)[0]
    selected = {'http_status': response.status, 'code': error.get('code'),
                'status': error.get('status'), 'message': observe.text(message), 'details': []}
    for detail in error.get('details', []):
        selected['details'].append(detail_view(observe, detail))
    check_observation(selected)
    return selected


def detail_view(observe: Observations, detail: dict) -> dict:
    """Select an ErrorInfo's reason, domain, permission, and resource, or a BadRequest's fields."""
    kind = detail.get('@type', '').rsplit('.', 1)[-1]
    if kind == 'ErrorInfo':
        metadata = detail.get('metadata', {})
        return {'type': kind, 'reason': detail.get('reason'), 'domain': detail.get('domain'),
                'permission': metadata.get('permission'),
                'resource': observe.text(metadata.get('resource', ''))}
    if kind == 'BadRequest':
        violations = [[item.get('field'), observe.text(item.get('description', ''))]
                      for item in detail.get('fieldViolations', [])]
        return {'type': kind, 'violations': violations}
    return {'type': kind}


def project_view(observe: Observations, project: dict) -> dict:
    """Select a v3 project's identity, parent, and field names. Labels aren't modeled."""
    return {'name': observe.text(project.get('name', '')),
            'parent': observe.text(project.get('parent', '')),
            'projectId': observe.text(project.get('projectId', '')), 'state': project.get('state'),
            'fields': sorted(set(project) - {'labels'}),
            'weak_etag': project.get('etag', '').startswith('W/"')}


def listing(observe: Observations, response: Response, project: str) -> dict:
    """Select a project search: its keys and the sandbox project's entry."""
    value = response.json()
    own = [project_view(observe, item) for item in value.get('projects', [])
           if item.get('projectId') == project]
    return {'http_status': response.status, 'fields': sorted(value), 'sandbox_project': own}


def body_of(response: Response) -> dict:
    """Select a successful call's status and whole body (empty for these calls)."""
    return {'http_status': response.status, 'body': response.json()}


def observe_projects(call: Call, observe: Observations, snapshot: Snapshot, project: str,
                     organization: str, found: Response) -> None:
    """Project reads, searches, lists, permissions, and a denied move."""
    snapshot.match('project-get', {'http_status': found.status,
                                   **project_view(observe, found.json())})
    v1 = call('fixture', 'GET', '/v1/projects/' + project).json()
    snapshot.match('project-get-v1', {'projectId': observe.text(v1.get('projectId', '')),
                                      'projectNumber': observe.text(v1.get('projectNumber', '')),
                                      'lifecycleState': v1.get('lifecycleState'),
                                      'parent': observe.value(v1.get('parent'))})
    denied_get = call('denied', 'GET', '/v3/projects/' + project)
    snapshot.match('project-get-denied', refusal(observe, denied_get))
    missing = call('fixture', 'GET', '/v3/projects/' + MISSING_PROJECT)
    snapshot.match('project-get-missing', refusal(observe, missing))
    search = '/v3/projects:search'
    snapshot.match('project-search', listing(observe, call('fixture', 'GET', search), project))
    by_parent = call('fixture', 'GET', search, query={'query': 'parent:' + organization})
    snapshot.match('project-search-parent', listing(observe, by_parent, project))
    bad_token = call('fixture', 'GET', search, query={'pageToken': 'bogus'})
    snapshot.match('project-search-bad-token', refusal(observe, bad_token))
    negative = call('fixture', 'GET', search, query={'pageSize': '-1'})
    snapshot.match('project-search-negative-size', refusal(observe, negative))
    listed = call('fixture', 'GET', '/v3/projects', query={'parent': organization})
    snapshot.match('project-list-denied', refusal(observe, listed))
    no_parent = call('fixture', 'GET', '/v3/projects')
    snapshot.match('project-list-no-parent', refusal(observe, no_parent))
    test = f'/v3/projects/{project}:testIamPermissions'
    held = call('fixture', 'POST', test, {'permissions': PROJECT_PERMISSIONS})
    snapshot.match('project-permissions', {'http_status': held.status,
                                           'permissions': held.json().get('permissions', [])})
    denied = call('denied', 'POST', test, {'permissions': PROJECT_PERMISSIONS})
    snapshot.match('project-permissions-denied', body_of(denied))
    if set(held.json().get('permissions', [])) & set(PROJECT_PERMISSIONS[1:]):
        raise Inconclusive('The fixture may move the sandbox project; refusing to send the move')
    move = call('fixture', 'POST', f'/v3/projects/{project}:move',
                {'destinationParent': MISSING_FOLDER})
    snapshot.match('project-move-denied', refusal(observe, move))


def observe_organization(call: Call, observe: Observations, snapshot: Snapshot,
                         organization: str) -> None:
    """Organization permissions, denied reads, and searches."""
    path = '/v3/' + organization
    held = call('fixture', 'POST', path + ':testIamPermissions',
                {'permissions': ORGANIZATION_PERMISSIONS})
    snapshot.match('organization-permissions', body_of(held))
    if held.json().get('permissions'):
        raise Inconclusive('The fixture holds organization permissions; refusing the requests')
    snapshot.match('organization-get-denied', refusal(observe, call('fixture', 'GET', path)))
    policy = call('fixture', 'POST', path + ':getIamPolicy', {})
    snapshot.match('organization-policy-denied', refusal(observe, policy))
    for actor in ('fixture', 'denied'):
        searched = call(actor, 'GET', '/v3/organizations:search')
        snapshot.match(f'organization-search-{actor}', body_of(searched))
    bad_token = call('fixture', 'GET', '/v3/organizations:search', query={'pageToken': 'bogus'})
    snapshot.match('organization-search-bad-token', refusal(observe, bad_token))


def observe_folders(call: Call, observe: Observations, snapshot: Snapshot,
                    organization: str) -> None:
    """Folder lists, searches, a denied create, and every folder method on folders/1."""
    listed = call('fixture', 'GET', '/v3/folders', query={'parent': organization})
    snapshot.match('folder-list-denied', refusal(observe, listed))
    snapshot.match('folder-list-no-parent', refusal(observe, call('fixture', 'GET', '/v3/folders')))
    snapshot.match('folder-search-denied', body_of(call('denied', 'GET', '/v3/folders:search')))
    creates = {'denied': {'parent': organization, 'displayName': DENIED_NAME},
               'no-parent': {'displayName': DENIED_NAME},
               'unknown-field': {'parent': organization, 'displayName': DENIED_NAME, 'bogus': 1}}
    for label, body in creates.items():
        created = call('fixture', 'POST', '/v3/folders', body)
        snapshot.match('folder-create-' + label, refusal(observe, created))
    missing = '/v3/' + MISSING_FOLDER
    requests = {'get': ('GET', missing, None, None),
                'patch': ('PATCH', missing, {'displayName': DENIED_NAME},
                          {'updateMask': 'display_name'}),
                'delete': ('DELETE', missing, None, None),
                'undelete': ('POST', missing + ':undelete', {}, None),
                'policy': ('POST', missing + ':getIamPolicy', {}, None),
                'set-policy': ('POST', missing + ':setIamPolicy', {'policy': {}}, None),
                'move': ('POST', missing + ':move', {'destinationParent': organization}, None)}
    for label, (method, path, body, query) in requests.items():
        response = call('fixture', method, path, body, query)
        snapshot.match(f'folder-{label}-missing', refusal(observe, response))
    tested = call('fixture', 'POST', missing + ':testIamPermissions',
                  {'permissions': ['resourcemanager.folders.get']})
    snapshot.match('folder-permissions-missing', body_of(tested))
    for version in ('v1', 'v3'):
        unknown = call('fixture', 'GET', f'/{version}/operations/fc.1')
        snapshot.match(f'operation-unknown-{version}', refusal(observe, unknown))


def run_scenario(client: HierarchyClient, snapshot: Snapshot) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    project = client.target.project

    def call(actor: str, method: str, path: str, body: object = None,
             query: dict | None = None) -> Response:
        return client.request(actor, method, path, body=body, query=query)

    found = call('fixture', 'GET', '/v3/projects/' + project)
    if found.status != 200:
        raise Inconclusive(f'The fixture cannot read the sandbox project (HTTP {found.status})')
    organization = found.json().get('parent', '')
    if not organization.startswith('organizations/'):
        raise Inconclusive('The sandbox project is not directly under an organization')
    aliases = {client.emails[actor]: '<' + actor + '>' for actor in client.emails}
    aliases |= {project: '<project>', client.number: '<project-number>',
                organization.split('/')[1]: '<organization>'}
    observe = Observations(aliases)
    observe_projects(call, observe, snapshot, project, organization, found)
    observe_organization(call, observe, snapshot, organization)
    observe_folders(call, observe, snapshot, organization)
