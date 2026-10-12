"""Bounded project IAM policy recording: one added binding, every other binding preserved."""

import argparse
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = 'projectiam'
LIMITS = {'max_added_bindings': 1, 'max_policy_writes': 3, 'max_run_seconds': 600}
LIVE_ENDPOINT = 'https://cloudresourcemanager.googleapis.com'
ROLE = 'roles/iam.serviceAccountViewer'


def load_scope(filename, sandbox):
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox)


def bindings_of(policy):
    """Normalize bindings to comparable (role, member) pairs."""
    return {(binding['role'], member) for binding in policy.get('bindings', [])
            for member in binding.get('members', []) if 'condition' not in binding}


def with_member(policy, role, member, present):
    """Return a policy body that adds or removes exactly one role/member pair."""
    bindings = [dict(binding, members=list(binding['members'])) for binding in policy.get('bindings', [])]
    target = next((b for b in bindings if b['role'] == role and 'condition' not in b), None)
    if present:
        if target is None:
            bindings.append({'role': role, 'members': [member]})
        elif member not in target['members']:
            target['members'].append(member)
    elif target is not None:
        target['members'] = [item for item in target['members'] if item != member]
        if not target['members']:
            bindings.remove(target)
    return {'version': policy.get('version', 1), 'etag': policy['etag'], 'bindings': bindings}


class ProjectPolicyFixture(OwnedRun):
    """Adds one unconditional binding for the allowed caller and always removes it."""

    def __init__(self, client, state_dir, *, manifest=None):
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: type(v.get('writes', 0)) is int)
        self.manifest.setdefault('writes', 0)
        self.member = 'serviceAccount:' + client.emails['allowed']
        self.api = f'/v1/projects/{self.project}'
        self.original = None

    def call(self, verb, *, actor='fixture', body=None):
        return self.client.request(actor, 'POST', f'{self.api}:{verb}IamPolicy', body=body or {},
                                   timeout=self.remaining())

    def read(self, actor='fixture'):
        return self.call('get', actor=actor, body={'options': {'requestedPolicyVersion': 1}})

    def write(self, policy):
        if self.manifest['writes'] >= LIMITS['max_policy_writes']:
            raise ValueError('Project policy write limit reached')
        self.manifest['writes'] += 1
        self.save()
        return self.call('set', body={'policy': policy})

    def setup(self):
        self.acquire()
        self.client.verify_project()
        current = self.read()
        if current.status != 200:
            raise AssertionError(f'Fixture project policy read returned HTTP {current.status}')
        self.original = current.json()
        if any(binding.get('condition') for binding in self.original.get('bindings', [])):
            raise ValueError('Conditional project bindings are outside this bounded scenario')
        if (ROLE, self.member) in bindings_of(self.original):
            raise ValueError('The allowed caller already holds the scenario role; refusing to proceed')
        self.manifest['attempted'] = True
        self.manifest['preserved'] = sorted(map(list, bindings_of(self.original)))
        self.save()
        return current

    def cleanup(self):
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            current = self.read()
            if current.status != 200:
                raise ValueError(f'Cleanup policy read returned HTTP {current.status}')
            policy = current.json()
            if (ROLE, self.member) in bindings_of(policy):
                removed = self.client.request('fixture', 'POST', f'{self.api}:setIamPolicy',
                                              body={'policy': with_member(policy, ROLE, self.member, False)})
                if removed.status != 200:
                    raise ValueError(f'Removing the scenario binding returned HTTP {removed.status}')
                policy = removed.json()
            preserved = {tuple(pair) for pair in self.manifest['preserved']}
            if (ROLE, self.member) in bindings_of(policy) or not preserved <= bindings_of(policy):
                raise ValueError('Project policy does not match its pre-run bindings; inspect it manually')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()


def run_scenario(fixture, snapshot, initial):
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({client.emails[actor]: '<' + actor + '>' for actor in client.emails})
    original = initial.json()
    snapshot.match('fixture-read', {'http_status': initial.status, 'fields': sorted(original),
                                    'version': original.get('version'),
                                    'etag_present': bool(original.get('etag'))})
    snapshot.match('denied-read', observe.error(fixture.read(actor='denied')))
    snapshot.match('allowed-read-before-grant', observe.error(fixture.read(actor='allowed')))
    added = fixture.write(with_member(original, ROLE, fixture.member, True))
    value = added.json()
    snapshot.match('binding-added', {'http_status': added.status, 'fields': sorted(value),
                                     'scenario_binding': (ROLE, fixture.member) in bindings_of(value),
                                     'preserved': bindings_of(original) <= bindings_of(value),
                                     'etag_changed': value.get('etag') != original.get('etag')})
    stale = fixture.write(with_member(original, ROLE, fixture.member, True))
    snapshot.match('stale-etag', observe.error(stale))
    removed = fixture.write(with_member(value, ROLE, fixture.member, False))
    after = removed.json()
    snapshot.match('binding-removed', {'http_status': removed.status,
                                       'scenario_binding': (ROLE, fixture.member) in bindings_of(after),
                                       'preserved': bindings_of(original) <= bindings_of(after)})


def main(argv=None):
    parser = argparse.ArgumentParser(description='Remove only the scenario binding after an interrupted run')
    parser.add_argument('command', choices=['cleanup'])
    parser.add_argument('--profile', required=True)
    parser.add_argument('--scope', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--allow-live', action='store_true', required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox)
    target = Target.select('record', project=sandbox.project, credentials_file=sandbox.identities['allowed'].credentials_file,
                           live_scope=sandbox.live_scope, allow_live=args.allow_live)
    client = RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    ProjectPolicyFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                         manifest=args.manifest).cleanup()
    print('Owned project-policy cleanup passed.')


if __name__ == '__main__':
    main()
