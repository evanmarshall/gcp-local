"""Deny policy enforcement recording: a project deny policy over a custom-role grant.

The fixture creates one service account and one project custom role holding
iam.serviceAccounts.get and iam.serviceAccounts.getIamPolicy, and grants the
role to the allowed identity on the account. A deny policy on the project
then denies the allowed identity iam.serviceAccounts.get: directly, with the
identity exempted, and through the standing group that contains it. Each
change is polled until it takes effect, within PROPAGATION_SECONDS. The
fixture needs roles/iam.roleAdmin on the project for the run.
"""

import argparse
import sys
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive
from gcp_validation.live_rest import LiveScope, Response
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.security.hierarchy import Snapshot
from gcp_validation.services.security.iam.denypolicies import (
    ALLOWED_GROUP,
    LIVE_ENDPOINT,
    SERVICE_ACCOUNT,
    DenyClient,
    DenyRun,
    aliases,
    deny_rule,
    operation_view,
)
from gcp_validation.services.security.refusals import outcome

SERVICE = 'denyenforcement'
LIMITS = {'max_deny_policies': 1, 'max_custom_roles': 1, 'max_service_accounts': 1,
          'max_run_seconds': 3600}
GET = 'iam.serviceAccounts.get'
PROPAGATION_SECONDS = 600


def load_scope(filename: str | Path | None, sandbox: Sandbox | None, mode: str) -> LiveScope:
    """Load the owner-approved enforcement scope."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class EnforcementRun(DenyRun):
    """DenyRun that also owns one service account and one custom role.

    A deleted role's ID stays reserved for weeks, so every run uses a new ID.
    """

    def __init__(self, client: DenyClient, state_dir: Path, *, manifest: str | None = None) -> None:
        """Name the run's account and role, or reload them from a manifest."""
        super().__init__(client, state_dir, service=SERVICE, max_seconds=LIMITS['max_run_seconds'],
                         manifest=manifest,
                         check=lambda v: v.get('account_id') == 'gl-dn-' + v['run_id'][:10])
        run = self.manifest['run_id']
        self.manifest.setdefault('account_id', 'gl-dn-' + run[:10])
        self.manifest.setdefault('role_id', 'gcl_dn_' + run[:10])
        self.email = f"{self.manifest['account_id']}@{self.project}.iam.gserviceaccount.com"
        self.account = f'/v1/projects/{self.project}/serviceAccounts/{self.email}'
        self.role = f"/v1/projects/{self.project}/roles/{self.manifest['role_id']}"
        self.role_name = f"projects/{self.project}/roles/{self.manifest['role_id']}"

    def prepare(self) -> None:
        """Create the account and role and grant the role to the allowed identity."""
        created = self.call('fixture', 'POST', f'/v1/projects/{self.project}/serviceAccounts', {
            'accountId': self.manifest['account_id'],
            'serviceAccount': {'displayName': 'gcp-local conformance', 'description': self.marker}})
        if created.status != 200:
            raise Inconclusive(f'Service-account create returned HTTP {created.status}')
        self.manifest['unique_id'] = created.json().get('uniqueId', '')
        self.save()
        role = self.call('fixture', 'POST', f'/v1/projects/{self.project}/roles', {
            'roleId': self.manifest['role_id'],
            'role': {'title': 'gcp-local conformance', 'description': self.marker, 'stage': 'GA',
                     'includedPermissions': [GET, 'iam.serviceAccounts.getIamPolicy']}})
        if role.status != 200:
            raise Inconclusive(f'Custom-role create returned HTTP {role.status}')
        member = 'serviceAccount:' + self.client.emails['allowed']
        policy = {'bindings': [{'role': self.role_name, 'members': [member]}]}
        # A new account or role can take a moment to be visible to setIamPolicy.
        path = self.account + ':setIamPolicy'
        granted = self.wait_until(lambda: self.call('fixture', 'POST', path, {'policy': policy}),
                                  {400, 404}, 120)
        if granted.status != 200:
            raise Inconclusive(f'Granting the custom role returned HTTP {granted.status}')

    def cleanup_resources(self) -> None:
        """Delete the deny policies, the role (its ID stays reserved), and the account."""
        super().cleanup_resources()
        role = self.call('fixture', 'GET', self.role)
        if role.status == 200 and not role.json().get('deleted'):
            if role.json().get('description') != self.marker:
                raise ValueError('Refusing custom-role cleanup: ownership does not match')
            if self.call('fixture', 'DELETE', self.role).status != 200:
                raise ValueError('Owned custom-role delete failed')
        elif role.status not in {200, 404}:
            raise ValueError(f'Owned custom-role read returned HTTP {role.status}')
        account = self.call('fixture', 'GET', self.account)
        if account.status == 200:
            if account.json().get('description') != self.marker:
                raise ValueError('Refusing service-account cleanup: ownership does not match')
            if self.call('fixture', 'DELETE', self.account).status not in {200, 404}:
                raise ValueError('Owned service-account delete failed')
        elif account.status != 404:
            raise ValueError(f'Owned service-account read returned HTTP {account.status}')
        if self.call('fixture', 'GET', self.account).status != 404:
            raise ValueError('Owned service account remains after deletion')


def read_until(run: EnforcementRun, expected: int) -> Response:
    """Poll the allowed identity's account read until it returns expected."""
    other = {200: {403}, 403: {200}}[expected]
    response = run.wait_until(lambda: run.call('allowed', 'GET', run.account), other,
                              PROPAGATION_SECONDS)
    if response.status != expected:
        raise AssertionError(f'Allowed read returned HTTP {response.status}, not {expected}')
    return response


def write(run: EnforcementRun, rules: list, etag: str | None) -> Response:
    """Create the run's deny policy, or replace its rules at etag."""
    policy_id = run.policy_id('enf')
    if etag is None:
        return run.create(policy_id, rules)
    return run.call('fixture', 'PUT', run.collection + '/' + policy_id,
                    {'displayName': run.marker, 'etag': etag, 'rules': rules})


def run_enforcement(run: EnforcementRun, snapshot: Snapshot) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    run.prepare()
    observe = aliases(run, {run.email: '<account>', run.manifest['role_id']: '<role>',
                            run.manifest['unique_id']: '<unique-id>',
                            run.policy_id('enf'): '<policy>'})
    allowed = SERVICE_ACCOUNT + run.client.emails['allowed']
    read = read_until(run, 200)
    email = observe.text(read.json().get('email', ''))
    snapshot.match('granted-read', {'http_status': read.status, 'email': email})
    snapshot.match('ungranted-read', outcome(observe, run.call('denied', 'GET', run.account)))
    denied = ['iam.googleapis.com/serviceAccounts.get']
    phases = [
        ('denied', [deny_rule([allowed], denied)], 403),
        ('exempted', [deny_rule([allowed], denied, exceptionPrincipals=[allowed])], 200),
        ('group-denied', [deny_rule([ALLOWED_GROUP], denied)], 403),
    ]
    etag = None
    for name, rules, expected in phases:
        written = write(run, rules, etag)
        snapshot.match(name + '-written', operation_view(observe, written))
        etag = written.json().get('response', {}).get('etag', '')
        response = read_until(run, expected)
        snapshot.match(name + '-read', outcome(observe, response) if expected == 403 else
                       {'http_status': response.status})
        policy = run.call('allowed', 'POST', run.account + ':getIamPolicy', {})
        snapshot.match(name + '-other-permission', {'http_status': policy.status})
    deleted = run.call('fixture', 'DELETE', run.collection + '/' + run.policy_id('enf'))
    snapshot.match('deleted', operation_view(observe, deleted))
    snapshot.match('restored-read', {'http_status': read_until(run, 200).status})


def main(argv: list[str] | None = None) -> None:
    """Clean only the owned policies, role, and account of an interrupted run."""
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
    EnforcementRun(client, state, manifest=args.manifest).cleanup()
    sys.stdout.write('Owned deny enforcement cleanup passed.\n')


if __name__ == '__main__':
    main()
