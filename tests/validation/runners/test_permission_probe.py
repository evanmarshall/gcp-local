import hashlib
import json
import shutil
from pathlib import Path
from types import SimpleNamespace as ns  # noqa: N813 -- short alias keeps fakes readable

import pytest
from permission_fakes import (
    ALLOWED_MEMBER,
    EMAILS,
    NUMBER,
    PROJECT,
    FakeClock,
    FakeCloud,
    error,
    ok,
)

from gcp_validation.evidence import Inconclusive
from gcp_validation.live_rest import LiveScope, Response
from gcp_validation.permissions import cli as permission_cli
from gcp_validation.permissions.drift import drift, markdown, write
from gcp_validation.permissions.probe import (
    ALLOWED,
    DENIED,
    INCONCLUSIVE,
    Budget,
    LimitReachedError,
    ProbeRunner,
    classify,
    operation_outcome,
    role_snapshots,
)
from gcp_validation.permissions.secretmanager import (
    LIMITS,
    ROLES_DIR,
    SERVICE,
    cleanup,
    live_plan,
    owned_run,
    run_probe,
    scope_document,
)

ROOT = Path(__file__).resolve().parents[3]
ROLE = f'projects/{PROJECT}/roles/gcp_local_probe_'


def probe(cloud, clock, tmp_path, *, attachment='secret', limits=LIMITS):
    return run_probe(
        cloud,
        tmp_path / 'state',
        attachment=attachment,
        clock=clock,
        source='synthetic',
        limits=limits,
    )


def owned_roles(cloud):
    return {name: role for name, role in cloud.roles.items() if name.startswith(ROLE)}


def assert_clean(cloud):
    assert cloud.secrets == {}
    assert all(role['deleted'] for role in owned_roles(cloud).values())
    assert not cloud.roles[f'projects/{PROJECT}/roles/someoneElse']['deleted']
    assert cloud.project_bindings[0] == {
        'role': 'roles/viewer',
        'members': ['user:owner@example.com'],
    }
    assert not any(b['role'].startswith(ROLE) for b in cloud.project_bindings)


# Classification ----------------------------------------------------------------


def test_only_iam_denial_and_success_are_conclusive():
    assert classify(ok({})).kind == ALLOWED
    assert classify(error(403, 'PERMISSION_DENIED', 'IAM_PERMISSION_DENIED')).kind == DENIED
    assert classify(error(403, 'PERMISSION_DENIED')).kind == DENIED
    for response in (
        error(403, 'PERMISSION_DENIED', 'SERVICE_DISABLED'),
        error(403, 'PERMISSION_DENIED', 'USER_PROJECT_DENIED'),
        error(409, 'ALREADY_EXISTS'),
        error(400, 'INVALID_ARGUMENT'),
        error(429, 'RESOURCE_EXHAUSTED'),
        error(404, 'NOT_FOUND'),
        error(401, 'UNAUTHENTICATED'),
        Response(502, b'<html>bad gateway</html>'),
    ):
        outcome = classify(response)
        assert outcome.kind == INCONCLUSIVE
        assert outcome.detail == 'non-iam-error'


def test_operation_outcome_uses_the_completed_status():
    assert operation_outcome({'done': True, 'response': {}}).kind == ALLOWED
    assert operation_outcome({'done': True, 'error': {'code': 7}}).kind == DENIED
    assert operation_outcome({'done': True, 'error': {'code': 6}}).kind == INCONCLUSIVE


def test_budget_refuses_past_each_limit():
    clock = FakeClock()
    budget = Budget(dict(LIMITS, max_trials=1), clock)
    budget.spend('trial')
    with pytest.raises(LimitReachedError, match='max_trials'):
        budget.spend('trial')
    clock.sleep(LIMITS['max_run_seconds'])
    with pytest.raises(LimitReachedError, match='max_run_seconds'):
        budget.spend('request')


# Secret Manager target against the controlled fake ------------------------------


def test_conclusive_minimization_restores_state_and_cleans_up(tmp_path):
    clock = FakeClock()
    cloud = FakeCloud(clock)
    report, manifest = probe(cloud, clock, tmp_path)
    result = report['result']
    assert result['status'] == 'conclusive', result
    assert result['minimal_permissions'] == ['secretmanager.versions.access']
    assert result['conclusive_findings']['required'] == ['secretmanager.versions.access']
    assert 'secretmanager.versions.get' in result['conclusive_findings']['not_required']
    assert result['inconclusive'] == {
        'permissions': [],
        'unconfirmed_not_required': [],
        'trials': [],
    }
    # Permissions that cannot take effect on a secret binding are excluded, not minimized.
    assert report['excluded']['resourcemanager.projects.get'] == 'NOT_APPLICABLE'
    assert report['excluded']['secretmanager.secrets.list'] == 'NOT_APPLICABLE'
    kinds = [trial['kind'] for trial in report['trials']]
    assert kinds[:2] == ['control', 'baseline']
    assert kinds[-1] == 'confirmation'
    assert kinds.count('restoration') == 1
    for trial in report['trials']:
        assert trial['state'] == {'before_matches_initial': True, 'after_matches_initial': True}
        assert trial['acting_identity'] == 'allowed'
        assert trial['propagation']['waited_seconds'] <= LIMITS['max_propagation_seconds']
    removal = next(t for t in report['trials'] if t['removed'] == 'secretmanager.versions.access')
    assert removal['outcome'] == DENIED
    assert removal['propagation']['hint'] == 'converged'
    assert report['grant']['attachment'] == 'secret'
    assert report['grant']['member'] == 'serviceAccount:<allowed>'
    assert report['scenario']['check_target'].startswith(
        '//secretmanager.googleapis.com/projects/<project>/'
    )
    assert report['cleanup'] == 'passed'
    text = json.dumps(report)
    for identifier in (PROJECT, *EMAILS.values(), json.loads(manifest.read_text())['run_id']):
        assert identifier not in text
    assert json.loads(manifest.read_text())['cleanup'] == 'passed'
    assert_clean(cloud)
    for name, limit in (
        ('trial', 'max_trials'),
        ('role_update', 'max_role_updates'),
        ('request', 'max_requests'),
    ):
        assert report['spent'][name] <= LIMITS[limit]


def test_two_required_permissions_each_restore_to_success(tmp_path):
    clock = FakeClock()
    cloud = FakeCloud(
        clock, required={'secretmanager.versions.access', 'secretmanager.secrets.get'}
    )
    report, _ = probe(cloud, clock, tmp_path)
    assert report['result']['minimal_permissions'] == [
        'secretmanager.secrets.get',
        'secretmanager.versions.access',
    ]
    restorations = [t for t in report['trials'] if t['kind'] == 'restoration']
    assert len(restorations) == 2
    assert all(t['outcome'] == ALLOWED for t in restorations)


def test_project_attachment_excludes_permissions_unavailable_in_custom_roles(tmp_path):
    clock = FakeClock()
    cloud = FakeCloud(clock)
    report, _ = probe(cloud, clock, tmp_path, attachment='project')
    assert report['excluded']['resourcemanager.projects.list'] == 'NOT_SUPPORTED'
    assert 'resourcemanager.projects.get' in report['result']['conclusive_findings']['not_required']
    assert report['result']['minimal_permissions'] == ['secretmanager.versions.access']
    assert report['grant']['attachment'] == 'project'
    assert_clean(cloud)


def test_grant_that_never_propagates_is_inconclusive(tmp_path):
    clock = FakeClock()
    cloud = FakeCloud(clock, data_delay=LIMITS['max_propagation_seconds'] * 10)
    report, _ = probe(cloud, clock, tmp_path)
    result = report['result']
    assert result['status'] == 'inconclusive'
    assert result['minimal_permissions'] is None
    assert result['stopped'] == 'baseline-not-allowed'
    baseline = report['trials'][1]
    assert baseline['outcome'] == INCONCLUSIVE
    assert baseline['detail'] == 'propagation-timeout'
    assert baseline['propagation']['waited_seconds'] >= LIMITS['max_propagation_seconds']
    assert result['inconclusive']['trials'] == [1]
    assert_clean(cloud)


def test_revocation_lagging_the_hint_never_yields_a_wrong_conclusive_set(tmp_path):
    # The hint converges long before the data plane revokes access, so a required
    # permission looks removable; the confirmation trial must catch it.
    clock = FakeClock()
    cloud = FakeCloud(clock, data_delay=150, hint_delay=10)
    report, _ = probe(cloud, clock, tmp_path)
    result = report['result']
    assert result['status'] == 'inconclusive'
    assert result['stopped'] == 'confirmation-not-allowed'
    assert result['minimal_permissions'] is None
    assert result['conclusive_findings']['not_required'] == []
    assert 'secretmanager.versions.access' in result['inconclusive']['unconfirmed_not_required']
    confirmation = report['trials'][-1]
    assert confirmation['kind'] == 'confirmation'
    assert confirmation['outcome'] == DENIED
    assert_clean(cloud)


@pytest.mark.parametrize(
    'fault',
    [
        error(429, 'RESOURCE_EXHAUSTED'),
        error(409, 'ALREADY_EXISTS'),
        error(400, 'INVALID_ARGUMENT'),
        error(403, 'PERMISSION_DENIED', 'SERVICE_DISABLED'),
        Inconclusive('Transport failed'),
    ],
)
def test_non_iam_errors_do_not_establish_requirements(tmp_path, monkeypatch, fault):
    clock = FakeClock()
    cloud = FakeCloud(clock)
    runner_trials = []
    original = ProbeRunner._trial

    def trial(self, kind, granted, removed=None):
        if kind == 'removal' and not runner_trials:
            # The first removal trial's first attempt meets the fault.
            runner_trials.append(removed)
            cloud.faults['access'] = [fault]
        return original(self, kind, granted, removed)

    monkeypatch.setattr(ProbeRunner, '_trial', trial)
    report, _ = probe(cloud, clock, tmp_path)
    result = report['result']
    assert result['status'] == 'inconclusive'
    assert result['minimal_permissions'] is None
    assert result['inconclusive']['permissions'] == runner_trials
    assert runner_trials[0] not in result['conclusive_findings']['required']
    failed = next(t for t in report['trials'] if t['removed'] == runner_trials[0])
    assert failed['outcome'] == INCONCLUSIVE
    assert failed['conclusive'] is False
    assert failed['detail'] in {'non-iam-error', 'transport'}
    assert_clean(cloud)


def test_preexisting_access_stops_before_any_grant(tmp_path):
    clock = FakeClock()
    cloud = FakeCloud(clock)
    cloud.project_bindings.append(
        {'role': 'roles/secretmanager.secretAccessor', 'members': [ALLOWED_MEMBER]}
    )
    cloud._changed()
    clock.sleep(100)
    report, _ = probe(cloud, clock, tmp_path)
    assert report['result']['stopped'].startswith('control-not-denied')
    assert report['trials'][0]['detail'] == 'preexisting-access'
    assert owned_roles(cloud) == {}
    assert cloud.secrets == {}


@pytest.mark.parametrize(
    ('limit', 'value'),
    [('max_trials', 4), ('max_role_updates', 2), ('max_requests', 40), ('max_run_seconds', 200)],
)
def test_limits_stop_the_run_and_cleanup_still_passes(tmp_path, limit, value):
    clock = FakeClock()
    cloud = FakeCloud(clock)
    report, _ = probe(cloud, clock, tmp_path, limits=dict(LIMITS, **{limit: value}))
    assert report['result']['status'] == 'inconclusive'
    assert report['result']['stopped'] == 'limit-reached: ' + limit
    assert report['cleanup'] == 'passed'
    assert_clean(cloud)


def test_interrupted_run_is_cleaned_from_its_manifest_only(tmp_path):
    clock = FakeClock()
    cloud = FakeCloud(clock)
    cloud.dark_after = 40  # The run loses its transport mid-minimization.
    report, manifest = probe(cloud, clock, tmp_path)
    assert report['result']['status'] == 'inconclusive'
    assert report['cleanup'] == 'failed'
    pending = json.loads(manifest.read_text())
    assert pending['cleanup'] == 'pending'
    assert pending['secret_attempted'] and pending['role_attempted'] and pending['bound_attempted']
    assert cloud.secrets and owned_roles(cloud)
    # A tampered manifest naming someone else's role is refused before any request.
    tampered = tmp_path / 'tampered.json'
    tampered.write_text(json.dumps(dict(pending, role=f'projects/{PROJECT}/roles/someoneElse')))
    tampered.chmod(0o600)
    with pytest.raises(ValueError):
        owned_run(cloud, tmp_path / 'state', manifest=tampered)
    cloud.dark_after = None
    owned = owned_run(cloud, tmp_path / 'state', manifest=manifest)
    cleanup(cloud, owned)
    assert_clean(cloud)
    assert json.loads(manifest.read_text())['cleanup'] == 'passed'
    cleanup(cloud, owned_run(cloud, tmp_path / 'state', manifest=manifest))  # idempotent


def test_cleanup_refuses_a_role_whose_ownership_mark_differs(tmp_path):
    clock = FakeClock()
    cloud = FakeCloud(clock)
    cloud.dark_after = 40
    _, manifest = probe(cloud, clock, tmp_path)
    cloud.dark_after = None
    (role,) = owned_roles(cloud).values()
    role['description'] = 'someone else reused this name'
    with pytest.raises(ValueError, match='ownership'):
        cleanup(cloud, owned_run(cloud, tmp_path / 'state', manifest=manifest))
    assert not role['deleted']


def test_lost_create_response_is_still_cleaned(tmp_path):
    clock = FakeClock()
    cloud = FakeCloud(clock)
    cloud.lose.add('secret_create')
    report, _ = probe(cloud, clock, tmp_path)
    assert report['result']['stopped'].startswith('inconclusive')
    assert report['cleanup'] == 'passed'
    assert cloud.secrets == {}


def test_scope_requires_the_exact_approved_limits(tmp_path):
    sandbox = ns(project=PROJECT, project_number=NUMBER)
    document = dict(scope_document(PROJECT, NUMBER))
    path = tmp_path / 'scope.json'
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError):  # Plans are written unapproved.
        LiveScope.load(SERVICE, LIMITS, path, sandbox)
    path.write_text(json.dumps(dict(document, approved=True)))
    assert LiveScope.load(SERVICE, LIMITS, path, sandbox).limits == LIMITS
    path.write_text(json.dumps(dict(document, approved=True, limits=dict(LIMITS, max_trials=99))))
    with pytest.raises(ValueError):
        LiveScope.load(SERVICE, LIMITS, path, sandbox)


def test_plan_names_temporary_grants_and_runs_nothing():
    plan = live_plan(PROJECT, NUMBER, EMAILS, 'secret')
    assert plan['status'] == 'prepared, not executed'
    assert [g['role'] for g in plan['temporary_fixture_grants']] == [
        'roles/secretmanager.admin',
        'roles/iam.roleAdmin',
    ]
    assert plan['probe_identity']['new_grants'] == 'none'
    assert plan['scope_file']['approved'] is False
    project = live_plan(PROJECT, NUMBER, EMAILS, 'project')
    assert 'roles/resourcemanager.projectIamAdmin' in [
        g['role'] for g in project['temporary_fixture_grants']
    ]
    with pytest.raises(ValueError):
        live_plan(PROJECT, NUMBER, EMAILS, 'folder')


# Generic runner: long-running operations and restoration --------------------------


class FakeGrant:
    def __init__(self, clock, delay=0):
        self.clock, self.delay, self.history = clock, delay, [(float('-inf'), frozenset())]

    def support(self, permissions):
        return dict.fromkeys(permissions, 'SUPPORTED')

    def ensure(self, permissions):
        if permissions == self.history[-1][1]:
            return False
        self.history.append((self.clock.monotonic(), frozenset(permissions)))
        return True

    def held(self):
        moment = self.clock.monotonic() - self.delay
        return [perms for at, perms in self.history if at <= moment][-1]

    def describe(self):
        return {'mechanism': 'fake'}


class WidgetTarget:
    """A create method that answers with an operation and must be undone each trial."""

    method = 'fake.widgets.create'
    long_running = True

    def __init__(self, grant, *, polls_to_done=2, operation_error=None, sticky_restores=0):
        self.grant, self.polls_to_done, self.operation_error = grant, polls_to_done, operation_error
        self.sticky_restores, self.widgets, self.operations = sticky_restores, set(), {}

    def prepare(self):
        pass

    def describe(self):
        return {
            'method': self.method,
            'check_target': 'widgets',
            'request_features': {'features': {}},
        }

    def seed_permissions(self):
        return {'sources': {}, 'permissions': ['fake.widgets.create', 'fake.widgets.get']}

    def snapshot(self):
        return sorted(self.widgets)

    def invoke(self):
        if 'fake.widgets.create' not in self.grant.held():
            return error(403, 'PERMISSION_DENIED')
        name = f'operations/{len(self.operations)}'
        self.operations[name] = 0
        self.widgets.add(name)
        return ok({'name': name})

    def operation(self, name):
        self.operations[name] += 1
        if self.polls_to_done is None or self.operations[name] < self.polls_to_done:
            return ok({'name': name})
        if self.operation_error:
            return ok({'name': name, 'done': True, 'error': self.operation_error})
        return ok({'name': name, 'done': True, 'response': {}})

    def hintable(self, permissions):
        return []

    def hint(self, permissions):
        return None

    def restore(self):
        if self.sticky_restores and self.widgets:
            self.sticky_restores -= 1
            return
        self.widgets.clear()


def widget_run(target_options=None, limits=None):
    clock = FakeClock()
    grant = FakeGrant(clock)
    target = WidgetTarget(grant, **(target_options or {}))
    budget = Budget(dict(LIMITS, **(limits or {})), clock)
    return ProbeRunner(target, grant, budget, clock=clock, aliases={}, source='synthetic').run()


def test_operations_complete_before_a_trial_counts_as_success():
    report = widget_run()
    assert report['result']['minimal_permissions'] == ['fake.widgets.create']
    baseline = report['trials'][1]
    assert baseline['propagation']['operation'] == {'polls': 2, 'done': True}
    assert baseline['propagation']['hint'] == 'unavailable'


def test_unfinished_operation_is_inconclusive():
    report = widget_run({'polls_to_done': None})
    baseline = report['trials'][1]
    assert baseline['detail'] == 'operation-timeout'
    assert baseline['propagation']['operation']['done'] is False
    assert report['result']['minimal_permissions'] is None


def test_operation_errors_decide_only_for_permission_denied():
    # A completed operation's PERMISSION_DENIED is a denial, so the baseline keeps
    # waiting for the grant and then times out; it never counts as success.
    denied = widget_run({'operation_error': {'code': 7, 'message': 'synthetic'}})
    assert denied['trials'][1]['detail'] == 'propagation-timeout'
    assert denied['trials'][1]['propagation']['operation']['done'] is True
    assert denied['result']['stopped'] == 'baseline-not-allowed'
    exists = widget_run({'operation_error': {'code': 6, 'message': 'synthetic'}})
    assert exists['trials'][1]['outcome'] == INCONCLUSIVE
    assert exists['trials'][1]['detail'] == 'operation-error'


def test_flaky_restore_stops_the_run_with_state_evidence():
    report = widget_run({'sticky_restores': 1})
    baseline = report['trials'][1]
    assert baseline['state'] == {'before_matches_initial': True, 'after_matches_initial': False}
    assert baseline['detail'] == 'state-not-restored'
    assert report['result']['stopped'] == 'state-not-restored'
    assert report['result']['minimal_permissions'] is None


# Drift ---------------------------------------------------------------------------

PLANS = {
    'schema': 1,
    'service': 'secretmanager',
    'plans': {
        'secretmanager.versions.access': {'baseline': ['secretmanager.versions.access'], 'when': {}}
    },
}


def conclusive_report(tmp_path):
    clock = FakeClock()
    report, _ = probe(FakeCloud(clock), clock, tmp_path)
    return report


def tree_digest(directory):
    return {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(directory.rglob('*'))
        if p.is_file()
    }


def test_drift_matches_the_registered_plan_and_role_snapshots(tmp_path):
    proposal = drift(conclusive_report(tmp_path), PLANS, ROLES_DIR)
    assert proposal['status'] == 'no-drift'
    assert proposal['applied'] is False
    assert 'roles/secretmanager.secretAccessor' in proposal['role_snapshots']['roles_granting_all']
    assert proposal['comparison']['missing_from_local'] == []


def test_drift_proposes_reviewed_changes_without_editing(tmp_path):
    report = conclusive_report(tmp_path)
    plans = {
        **PLANS,
        'plans': {
            'secretmanager.versions.access': {
                'baseline': ['secretmanager.versions.get'],
                'when': {'uses_alias': ['secretmanager.versions.list']},
            }
        },
    }
    protected = [ROLES_DIR, ROOT / 'internal/providers/security/secretmanager']
    before = [tree_digest(path) for path in protected]
    proposal = drift(report, plans, ROLES_DIR)
    assert proposal['status'] == 'drift'
    assert proposal['comparison']['missing_from_local'] == ['secretmanager.versions.access']
    assert proposal['comparison']['extra_in_local'] == ['secretmanager.versions.get']
    assert proposal['comparison']['unresolved_features'] == ['uses_alias']
    assert any('Review adding' in action for action in proposal['actions'])
    directory = write(proposal, tmp_path / 'drift')
    assert json.loads((directory / 'drift.json').read_text())['applied'] is False
    assert 'Nothing was applied' in (directory / 'drift.md').read_text()
    with pytest.raises(FileExistsError):
        write(proposal, tmp_path / 'drift')
    assert [tree_digest(path) for path in protected] == before


def test_drift_refuses_to_propose_from_an_inconclusive_report(tmp_path):
    clock = FakeClock()
    report, _ = probe(FakeCloud(clock, data_delay=10_000), clock, tmp_path)
    proposal = drift(report, PLANS, ROLES_DIR)
    assert proposal['status'] == 'inconclusive'
    assert 'comparison' not in proposal
    assert 'never establishes' in markdown(proposal)


def test_drift_flags_changed_seed_snapshots_and_missing_plans(tmp_path):
    report = conclusive_report(tmp_path)
    report['seed']['sources']['roles/secretmanager.viewer'] = '0' * 64
    proposal = drift(report, {**PLANS, 'plans': {}}, ROLES_DIR)
    assert proposal['status'] == 'drift'
    assert proposal['comparison']['local_plan'] is None
    assert proposal['role_snapshots']['seed_snapshots_changed_since_probe'] == [
        'roles/secretmanager.viewer'
    ]


def test_role_snapshots_verify_checksums(tmp_path):
    copy = tmp_path / 'roles'
    shutil.copytree(ROLES_DIR, copy)
    assert 'roles/secretmanager.viewer' in role_snapshots(copy)
    (copy / 'security/secretmanager/secretmanager.viewer.json').write_text('{}')
    with pytest.raises(ValueError, match='checksum'):
        role_snapshots(copy)


def test_cli_drift_and_plan_run_offline(tmp_path, capsys, monkeypatch):
    report_path = tmp_path / 'report.json'
    report_path.write_text(json.dumps(conclusive_report(tmp_path)))
    plans_path = tmp_path / 'plans.json'
    plans_path.write_text(json.dumps(PLANS))
    assert (
        permission_cli.main(
            [
                'drift',
                '--report',
                str(report_path),
                '--plans',
                str(plans_path),
                '--out',
                str(tmp_path / 'out'),
            ]
        )
        == 0
    )
    assert 'no-drift' in capsys.readouterr().out
    sandbox = ns(
        project=PROJECT,
        project_number=NUMBER,
        identities={actor: ns(email=email) for actor, email in EMAILS.items()},
    )
    monkeypatch.setattr(permission_cli.Sandbox, 'load', lambda _: sandbox)
    scope = tmp_path / 'scope.json'
    assert permission_cli.main(['plan', '--profile', 'unused', '--write-scope', str(scope)]) == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'prepared, not executed'
    assert json.loads(scope.read_text())['approved'] is False
    with pytest.raises(SystemExit):
        permission_cli.main(['plan', '--profile', 'unused', '--write-scope', str(scope)])


def test_cli_live_commands_require_explicit_flags():
    for argv in (
        ['run', '--profile', 'p', '--scope', 's', '--attachment', 'secret', '--out', 'o'],
        ['cleanup', '--profile', 'p', '--scope', 's', '--manifest', 'm'],
    ):
        with pytest.raises(SystemExit):
            permission_cli.main(argv)
