"""Bounded conditional bucket bindings: CEL conditions, policy versions, and expiry."""

import time

from gcp_validation.evidence import Inconclusive
from gcp_validation.services.storage.authorization import StorageAuthorization

PREFIX = 'public-'
EXPIRED = 'request.time < timestamp("2000-01-01T00:00:00Z")'


class ConditionalAuthorization(StorageAuthorization):
    """Writes one conditional objectViewer binding for the allowed caller."""

    def prefix_expression(self):
        return f'resource.name.startsWith("projects/_/buckets/{self.storage.bucket}/objects/{PREFIX}")'

    def policy_v3(self):
        return self.storage.client.request('fixture', 'GET', self.storage.path + '/iam',
                                           query={'optionsRequestedPolicyVersion': '3'})

    def put(self, expression, *, version=3, title='gcp-local-conformance'):
        """Replace only the allowed caller's bindings; keep every other binding."""
        member = self.member('allowed')
        policy = self.policy_v3().require(200)
        bindings = []
        for original in policy.get('bindings', []):
            binding = dict(original)
            binding['members'] = [value for value in binding['members'] if value != member]
            if binding['members']:
                bindings.append(binding)
        bindings.append({'role': 'roles/storage.objectViewer', 'members': [member],
                         'condition': {'title': title, 'expression': expression}})
        policy.update(bindings=bindings, version=version)
        return self.storage.client.request('fixture', 'PUT', self.storage.path + '/iam', body=policy)

    def wait_status(self, name, expected):
        """Poll an allowed read until it reaches the expected status, live only."""
        deadline = time.monotonic() + (420 if self.storage.sandbox else 0)
        while True:
            response = self.storage.read(name, actor='allowed')
            if response.status == expected:
                return response
            if response.status not in {200, 403}:
                response.require(expected)
            if time.monotonic() >= deadline:
                raise Inconclusive('Conditional grant did not converge within the bounded deadline')
            time.sleep(5)


def binding_view(observed, policy):
    """The scenario's own binding, with the bucket name aliased."""
    member_bindings = [b for b in policy.get('bindings', []) if 'condition' in b]
    return {'version': policy.get('version'),
            'conditional_bindings': [{'role': b['role'], 'condition_fields': sorted(b['condition']),
                                      'expression': observed.text(b['condition']['expression'])}
                                     for b in member_bindings]}
