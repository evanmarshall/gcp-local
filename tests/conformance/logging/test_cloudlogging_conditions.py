"""Bounded recording of the resource.service attribute project-level IAM Conditions see.

The fixture adds conditional bindings for the allowed caller on the sandbox project, then
calls as that caller: Cloud Logging entries.write (dry run) and entries.list on a log
with no entries, Artifact Registry repositories.get on a run-owned empty repository, and
Vertex AI countTokens. Phase one binds roles/logging.logWriter, roles/logging.viewer,
roles/artifactregistry.reader, and roles/aiplatform.user, each on
`resource.service == "<service host>"`. Phase two replaces the last two with an
always-true time condition, which shows those bindings propagate. Cleanup removes
exactly the bindings this run added, by title.

The fixture identity needs roles/resourcemanager.projectIamAdmin for the run; it is
not a standing role. The run takes the project IAM scenario's lock, and the live batch
runs it alone, because it rewrites the project policy.
"""

import time
from pathlib import Path
from typing import Any

import pytest

from gcp_validation.evidence import Inconclusive, SnapshotCase, now
from gcp_validation.live_rest import LiveScope, OwnedRun, Response, RestClient
from gcp_validation.services.artifactregistry import LIVE_ENDPOINT as REGISTRY_ENDPOINT
from gcp_validation.services.artifactregistry import RepositoryFixture
from gcp_validation.services.cloudlogging import LIVE_ENDPOINT as LOGGING_ENDPOINT
from gcp_validation.services.cloudlogging import LoggingClient
from gcp_validation.services.vertexai import LIVE_ENDPOINT as VERTEX_ENDPOINT
from gcp_validation.services.vertexai import MODEL, PROMPT, REGION

SERVICE = 'iam-conditions'
LIMITS = {'max_added_bindings': 4, 'max_policy_writes': 4, 'max_repositories': 1,
          'max_run_seconds': 900}
POLICY_ENDPOINT = 'https://cloudresourcemanager.googleapis.com'
TITLE = 'gcp-local-conditions-'
ALWAYS = 'request.time < timestamp("2100-01-01T00:00:00Z")'
# Phase one: each check's role, bound on its service's resource.service value.
CHECKS = {
    'logging-write': ('roles/logging.logWriter', 'logging.googleapis.com'),
    'logging-list': ('roles/logging.viewer', 'logging.googleapis.com'),
    'artifactregistry-get': ('roles/artifactregistry.reader', 'artifactregistry.googleapis.com'),
    'vertexai-count': ('roles/aiplatform.user', 'aiplatform.googleapis.com'),
}
CONTROLS = ('artifactregistry-get', 'vertexai-count')
PROPAGATION_SECONDS = 300


def load_scope(filename: str, sandbox: Any, mode: str) -> LiveScope:
    """Load the approved scope file for this scenario's limits."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


def ours(binding: dict) -> bool:
    """Report a binding this scenario added."""
    return binding.get('condition', {}).get('title', '').startswith(TITLE)


class ConditionalGrants(OwnedRun):
    """Adds titled conditional bindings for the allowed caller and always removes them."""

    def __init__(self, client: RestClient, state_dir: Path) -> None:
        """Share the project IAM scenario's lock directory."""
        super().__init__(client, state_dir, 'projectiam', LIMITS['max_run_seconds'])
        self.manifest.setdefault('writes', 0)
        self.member = 'serviceAccount:' + client.emails['allowed']
        self.api = f'/v1/projects/{self.project}'

    def read(self) -> dict:
        """Read the project policy with its conditions."""
        response = self.client.request('fixture', 'POST', f'{self.api}:getIamPolicy',
                                       body={'options': {'requestedPolicyVersion': 3}},
                                       timeout=self.remaining())
        if response.status != 200:
            raise ValueError(f'Project policy read returned HTTP {response.status}')
        return response.json()

    def write(self, bindings: dict[str, str]) -> Response:
        """Replace this run's bindings with one per check: check -> condition expression."""
        if self.manifest['writes'] >= LIMITS['max_policy_writes']:
            raise ValueError('Project policy write limit reached')
        self.manifest['writes'] += 1
        self.save()
        policy = self.read()
        kept = [binding for binding in policy.get('bindings', []) if not ours(binding)]
        added = [{'role': CHECKS[check][0], 'members': [self.member],
                  'condition': {'title': TITLE + check, 'expression': expression}}
                 for check, expression in bindings.items()]
        body = {'version': 3, 'etag': policy['etag'], 'bindings': kept + added}
        return self.client.request('fixture', 'POST', f'{self.api}:setIamPolicy',
                                   body={'policy': body}, timeout=self.remaining())

    def setup(self) -> None:
        """Refuse a policy that already holds this scenario's bindings."""
        self.acquire()
        self.client.verify_project()
        if any(ours(binding) for binding in self.read().get('bindings', [])):
            raise ValueError('The project policy already holds bindings this scenario adds')
        self.manifest['attempted'] = True
        self.save()

    def cleanup(self) -> None:
        """Remove every binding this scenario added and confirm none remains."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            # Remove this run's bindings at version 3, then return to version 1 once no
            # conditional binding remains, so the project IAM scenario reads the policy it
            # expects. GCP refuses a version 1 write while conditions are stored.
            for _ in range(4):
                policy = self.read()
                kept = [binding for binding in policy.get('bindings', []) if not ours(binding)]
                conditional = any('condition' in binding for binding in kept)
                removing = len(kept) != len(policy.get('bindings', []))
                if not removing and (conditional or policy.get('version') == 1):
                    break
                version = 3 if conditional or removing else 1
                body = {'version': version, 'etag': policy['etag'], 'bindings': kept}
                self.client.request('fixture', 'POST', f'{self.api}:setIamPolicy',
                                    body={'policy': body})
            if any(ours(binding) for binding in self.read().get('bindings', [])):
                raise ValueError('Scenario bindings remain in the project policy')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()


def settle(probes: dict, seconds: float) -> dict[str, int]:
    """Call each probe until it stops answering 403 or the window ends; return the statuses."""
    deadline = time.monotonic() + seconds
    statuses = {}
    while True:
        statuses = {name: statuses[name] if statuses.get(name, 403) != 403 else probe().status
                    for name, probe in probes.items()}
        if 403 not in statuses.values() or time.monotonic() >= deadline:
            return statuses
        time.sleep(10)


@pytest.fixture
def conditions(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'iam-conditions'
    else:
        scope = load_scope(pytestconfig.getoption('gcp_live_scope_file'), sandbox, gcp_target.mode)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project
        gcp_snapshot.context['scope'] = scope.evidence()
    endpoints = (
        ('policy', RestClient, POLICY_ENDPOINT), ('logging', LoggingClient, LOGGING_ENDPOINT),
        ('registry', RestClient, REGISTRY_ENDPOINT), ('vertex', RestClient, VERTEX_ENDPOINT))
    clients = {name: cls(gcp_target, endpoint, sandbox=sandbox, scope=scope)
               for name, cls, endpoint in endpoints}
    grants = ConditionalGrants(clients['policy'], state / 'projectiam')
    repository = RepositoryFixture(clients['registry'], state / 'artifactregistry')
    try:
        grants.setup()
        repository.finish(repository.setup())
        yield grants, repository, clients
    finally:
        try:
            grants.cleanup()
        finally:
            repository.cleanup()
        if grants.manifest['cleanup'] == 'passed' and repository.manifest['cleanup'] == 'passed':
            gcp_snapshot.match('cleanup', {'bindings_removed': True, 'repository_absent': True})


@pytest.mark.gcp_scenario(
    id='logging-conditional-access', service='logging', transport='rest-v2',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/artifactregistry.py',
        'gcp_validation/services/cloudlogging.py',
        'gcp_validation/services/vertexai.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
    ),
)
def test_cloudlogging_conditions(conditions, gcp_snapshot: SnapshotCase):
    grants, repository, clients = conditions
    project = grants.project
    log_name = f'projects/{project}/logs/gcp-local-conditions'
    model = (f'/v1/projects/{project}/locations/{REGION}/publishers/google/models/'
             f'{MODEL}:countTokens')
    logging = clients['logging']
    write = {'logName': log_name, 'resource': {'type': 'global'}, 'dryRun': True,
             'entries': [{'textPayload': 'dry run'}]}
    listing = {'resourceNames': [f'projects/{project}'], 'filter': f'logName="{log_name}"',
               'pageSize': 1}
    probes = {
        'logging-write': lambda: logging.request('allowed', 'POST', '/v2/entries:write',
                                                 body=write),
        'logging-list': lambda: logging.request('allowed', 'POST', '/v2/entries:list',
                                                body=listing),
        'artifactregistry-get': lambda: repository.call('GET', actor='allowed'),
        'vertexai-count': lambda: clients['vertex'].request('allowed', 'POST', model, body=PROMPT),
    }
    window = PROPAGATION_SECONDS if grants.client.sandbox else 0
    before = {name: probe().status for name, probe in probes.items()}
    if set(before.values()) != {403}:
        # A removed grant can keep working for minutes after an earlier run.
        raise Inconclusive(f'The allowed caller already has access: {before}')
    gcp_snapshot.match('before', before)
    service = {check: f'resource.service == "{host}"' for check, (_, host) in CHECKS.items()}
    gcp_snapshot.match('service-conditions-written', {'http_status': grants.write(service).status})
    gcp_snapshot.match('service-conditions', settle(probes, window))
    controls = {**service, **dict.fromkeys(CONTROLS, ALWAYS)}
    gcp_snapshot.match('controls-written', {'http_status': grants.write(controls).status})
    gcp_snapshot.match('controls', settle({name: probes[name] for name in CONTROLS}, window))
