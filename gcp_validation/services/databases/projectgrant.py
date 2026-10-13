"""One conditional project binding for the ``allowed`` caller, added and removed exactly.

Cloud SQL and Memorystore have no resource-level IAM policy, so an IAM Condition on their
resources is a project binding. The grant adds one binding whose condition title carries the
run ID, and cleanup removes only bindings with that title, retrying a stale policy etag
(other runs write the same project policy).
"""

import time
from collections.abc import Callable

from gcp_validation.evidence import Inconclusive
from gcp_validation.live_rest import OwnedRun, Response, RestClient

CRM_ENDPOINT = 'https://cloudresourcemanager.googleapis.com'
ATTEMPTS = 5


class ConditionalGrant:
    """Adds ``role`` for the allowed caller under one condition; ``remove`` takes it back."""

    def __init__(self, run: OwnedRun, role: str, expression: str) -> None:
        """Bind the grant to an owned run, whose manifest records that it was attempted."""
        client = run.client
        self.run = run
        self.crm = RestClient(client.target, CRM_ENDPOINT, sandbox=client.sandbox,
                              scope=client.scope)
        self.api = f'/v1/projects/{run.project}'
        self.member = 'serviceAccount:' + client.emails['allowed']
        self.role = role
        self.condition = {'title': 'gcp-local-' + run.manifest['run_id'], 'expression': expression}

    def owned(self, binding: dict) -> bool:
        """Report a binding this grant created."""
        return (binding.get('role') == self.role and binding.get('members') == [self.member]
                and (binding.get('condition') or {}).get('title') == self.condition['title'])

    def rewrite(self, change: Callable[[list], list | None]) -> None:
        """Read the version 3 policy, apply ``change`` to its bindings, and write it back."""
        for _ in range(ATTEMPTS):
            read = self.crm.request('fixture', 'POST', self.api + ':getIamPolicy',
                                    body={'options': {'requestedPolicyVersion': 3}},
                                    timeout=30)
            if read.status != 200:
                raise Inconclusive(f'Project policy read returned HTTP {read.status}')
            policy = read.json()
            bindings = change(policy.get('bindings', []))
            if bindings is None:
                return
            # Version 3 only while a conditional binding remains, as other runs expect.
            version = 3 if any(binding.get('condition') for binding in bindings) else 1
            written = self.crm.request('fixture', 'POST', self.api + ':setIamPolicy', body={
                'policy': {'version': version, 'etag': policy['etag'], 'bindings': bindings}},
                timeout=30)
            if written.status == 200:
                return
            if written.status != 409:
                raise Inconclusive(f'Project policy write returned HTTP {written.status}')
            time.sleep(2 if self.crm.sandbox else 0)
        raise Inconclusive('Project policy stayed busy; the grant was not changed')

    def add(self) -> None:
        """Add the binding once; the manifest records the attempt first."""
        self.run.manifest['grant_attempted'] = True
        self.run.save()
        binding = {'role': self.role, 'members': [self.member], 'condition': self.condition}
        self.rewrite(lambda bindings: None if any(map(self.owned, bindings))
                     else [*bindings, binding])

    def remove(self) -> None:
        """Remove only the bindings this grant added."""
        if not self.run.manifest.get('grant_attempted'):
            return
        self.rewrite(lambda bindings: [b for b in bindings if not self.owned(b)]
                     if any(map(self.owned, bindings)) else None)

    def wait(self, operation: Callable[[], Response], seconds: int = 480) -> Response:
        """Poll ``operation`` until it is allowed or ``seconds`` pass; live only."""
        deadline = time.monotonic() + (seconds if self.crm.sandbox else 0)
        while True:
            response = operation()
            if response.status != 403 or time.monotonic() >= deadline:
                return response
            time.sleep(15)
