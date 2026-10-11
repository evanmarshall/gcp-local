"""Bounded permission minimization for one method: trials, propagation, and outcomes.

The runner never opens a connection itself. A target fixture performs the request and
reports state, a grant changes the probe identity's custom role, and an injected clock
bounds every wait. The same orchestration therefore runs against controlled fake
responses offline and against an approved sandbox live. Only an IAM
``PERMISSION_DENIED`` or a completed success decides a trial; everything else, including
propagation timeouts, is recorded as inconclusive and kept apart from conclusive results.
"""

import contextlib
import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .evidence import Inconclusive, check_observation, now
from .live_rest import Observations, Response

ALLOWED, DENIED, INCONCLUSIVE = 'allowed', 'denied', 'inconclusive'
EXPECT_ALLOWED = frozenset({'baseline', 'restoration', 'confirmation'})
# ErrorInfo reasons GCP attaches to 403 PERMISSION_DENIED for causes other than a
# missing IAM permission; they never establish a permission requirement.
NON_IAM_REASONS = frozenset(
    {
        'ACCESS_TOKEN_SCOPE_INSUFFICIENT',
        'API_KEY_SERVICE_BLOCKED',
        'BILLING_DISABLED',
        'CONSUMER_INVALID',
        'SECURITY_POLICY_VIOLATED',
        'SERVICE_DISABLED',
        'USER_PROJECT_DENIED',
    }
)
GRANTABLE = frozenset({'SUPPORTED', 'TESTING'})
# Spend counters and the scope limits that bound them.
COUNTERS = {
    'trial': 'max_trials',
    'role_update': 'max_role_updates',
    'request': 'max_requests',
    'custom_role': 'max_custom_roles',
    'secret': 'max_secrets',
}
CLAIM = (
    'Scenario-scoped: one acting identity, one request shape, one initial state, and one '
    'grant location. A minimal set found by greedy removal is one sufficient set, not '
    'proof of every authorization path.'
)


class LimitReachedError(Exception):
    """A scope limit stopped the run; the run is inconclusive and still needs cleanup."""


class _StopError(Exception):
    """The run cannot continue from an equivalent, trusted state."""


class Clock:
    """Real time; tests inject a fake clock that advances on sleep."""

    def monotonic(self) -> float:
        """Return a monotonic timestamp in seconds."""
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        """Block for the given number of seconds."""
        time.sleep(seconds)


class Budget:
    """Explicit run limits; every spend is checked before the side effect it guards."""

    def __init__(self, limits: dict, clock: Clock) -> None:
        """Bind the approved limits and start the run deadline."""
        self.limits, self.clock = dict(limits), clock
        self.spent = dict.fromkeys(COUNTERS, 0)
        self.deadline = clock.monotonic() + self.limits['max_run_seconds']

    def spend(self, kind: str) -> None:
        """Count one unit of a bounded resource, refusing past its limit."""
        self.check_time()
        if self.spent[kind] >= self.limits[COUNTERS[kind]]:
            raise LimitReachedError(COUNTERS[kind])
        self.spent[kind] += 1

    def check_time(self) -> None:
        """Refuse further work after the run deadline."""
        if self.clock.monotonic() >= self.deadline:
            raise LimitReachedError('max_run_seconds')


class Transport(Protocol):
    """Explicit-identity JSON transport selecting one API by name."""

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
        """Send one request as a named scenario identity."""


class BudgetedTransport:
    """Count every request of the probe phase against the run limits."""

    def __init__(self, transport: Transport, budget: Budget) -> None:
        """Wrap a transport; cleanup uses the unwrapped one so it is never blocked."""
        self.transport, self.budget = transport, budget

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
        """Spend one request, then forward it."""
        self.budget.spend('request')
        return self.transport.request(actor, api, method, path, query=query, body=body)

    def __getattr__(self, name: str) -> object:
        """Expose the wrapped transport's identities, project number, and project check."""
        return getattr(self.transport, name)


@dataclass(frozen=True)
class Outcome:
    """One classified attempt; only ALLOWED and DENIED are conclusive."""

    kind: str
    http_status: int | None = None
    status: str | None = None
    reason: str | None = None
    detail: str | None = None

    def record(self) -> dict:
        """Return the public fields; error messages are never recorded."""
        return {
            'outcome': self.kind,
            'http_status': self.http_status,
            'status': self.status,
            'reason': self.reason,
            'detail': self.detail,
        }


def error_body(response: Response) -> dict:
    """Return the google.rpc error object of a response, or an empty mapping."""
    try:
        value = response.json()
    except ValueError:
        return {}
    error = value.get('error') if isinstance(value, dict) else None
    return error if isinstance(error, dict) else {}


def error_reason(error: dict) -> str | None:
    """Return the ErrorInfo reason of a google.rpc error, if present."""
    for detail in error.get('details') or ():
        if isinstance(detail, dict) and str(detail.get('@type', '')).endswith('ErrorInfo'):
            return detail.get('reason')
    return None


def classify(response: Response) -> Outcome:
    """Classify a final HTTP response: IAM denial, success, or inconclusive."""
    if 200 <= response.status < 300:
        return Outcome(ALLOWED, response.status)
    error = error_body(response)
    status, reason = error.get('status'), error_reason(error)
    if response.status == 403 and status == 'PERMISSION_DENIED' and reason not in NON_IAM_REASONS:
        return Outcome(DENIED, 403, status, reason)
    return Outcome(INCONCLUSIVE, response.status, status, reason, 'non-iam-error')


def operation_outcome(operation: dict) -> Outcome:
    """Classify a completed google.longrunning.Operation."""
    error = operation.get('error')
    if not error:
        return Outcome(ALLOWED, 200)
    code = error.get('code') if isinstance(error, dict) else None
    if code == 7:
        return Outcome(DENIED, None, 'PERMISSION_DENIED', error_reason(error), 'operation-error')
    return Outcome(INCONCLUSIVE, None, f'code-{code}', error_reason(error), 'operation-error')


def digest(value: object) -> str:
    """Hash a JSON-shaped state snapshot for equivalence checks."""
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def role_snapshots(directory: Path) -> dict[str, dict]:
    """Load committed predefined-role snapshots, verifying their manifest checksums."""
    directory = Path(directory)
    manifest = json.loads((directory / 'manifest.json').read_text())
    roles = {}
    for entry in manifest['roles']:
        raw = (directory / entry['file']).read_bytes()
        if hashlib.sha256(raw).hexdigest() != entry['sha256']:
            raise ValueError('Role snapshot checksum mismatch: ' + entry['file'])
        value = json.loads(raw)
        if value.get('name') != entry['name']:
            raise ValueError('Role snapshot name mismatch: ' + entry['file'])
        roles[entry['name']] = {
            'permissions': sorted(value['includedPermissions']),
            'sha256': entry['sha256'],
        }
    return roles


class Target(Protocol):
    """The fixture, request, and state of one probed method."""

    method: str
    long_running: bool

    def prepare(self) -> None:
        """Create the fixture preconditions; record ownership before creating anything."""

    def describe(self) -> dict:
        """Return request features, check target, acting identity, and preconditions."""

    def seed_permissions(self) -> dict:
        """Return the initial permission candidates and their sources."""

    def snapshot(self) -> object:
        """Return the comparable fixture state, without payloads or credentials."""

    def invoke(self) -> Response:
        """Call the probed method once as the probe identity."""

    def operation(self, name: str) -> Response:
        """Read a long-running operation returned by the probed method."""

    def hintable(self, permissions: list[str]) -> list[str]:
        """Return the permissions testIamPermissions accepts on the check target."""

    def hint(self, permissions: list[str]) -> set[str] | None:
        """Return the probe identity's testIamPermissions answer, or None if unavailable."""

    def restore(self) -> None:
        """Undo any side effect of one invocation."""


class Grant(Protocol):
    """A custom role bound to the probe identity at one attachment point."""

    def support(self, permissions: list[str]) -> dict[str, str]:
        """Map each permission to its custom-role support level at the attachment point."""

    def ensure(self, permissions: frozenset[str]) -> bool:
        """Make the grant hold exactly these permissions; report whether it changed."""

    def describe(self) -> dict:
        """Return the grant mechanism and attachment point."""


class ProbeRunner:
    """Greedy one-at-a-time removal from a seeded custom role, under explicit limits."""

    def __init__(
        self,
        target: Target,
        grant: Grant,
        budget: Budget,
        *,
        clock: Clock,
        aliases: dict[str, str],
        source: str,
    ) -> None:
        """Bind one target, its grant, the run budget, and the report aliases."""
        if source not in {'gcp', 'synthetic'}:
            raise ValueError('Select an explicit evidence source')
        self.target, self.grant, self.budget, self.clock = target, grant, budget, clock
        self.limits = budget.limits
        self.observe = Observations(aliases)
        self.source = source
        self.trials, self.candidates, self.excluded, self.seed = [], [], {}, {}
        self.required, self.not_required, self.undetermined = [], [], []
        self.current: frozenset[str] = frozenset()
        self.initial_digest = None
        self.stopped = None
        self.last_change = None

    def run(self) -> dict:
        """Run the probe; a stop or limit makes the whole result inconclusive."""
        try:
            self._execute()
        except _StopError as stop:
            self.stopped = str(stop)
        except LimitReachedError as limit:
            self.stopped = 'limit-reached: ' + str(limit)
        except Inconclusive as error:
            self.stopped = 'inconclusive: ' + str(error)
        return self.report()

    def _execute(self) -> None:
        self.target.prepare()
        self.initial_digest = digest(self.target.snapshot())
        self.seed = self.target.seed_permissions()
        support = self.grant.support(self.seed['permissions'])
        self.candidates = [p for p in self.seed['permissions'] if support[p] in GRANTABLE]
        self.excluded = {
            p: support[p] for p in self.seed['permissions'] if support[p] not in GRANTABLE
        }
        if not self.candidates:
            raise _StopError('no-grantable-candidates')
        if self._trial('control', frozenset()).kind != DENIED:
            raise _StopError('control-not-denied: the probe identity already has access')
        self.current = frozenset(self.candidates)
        if self._trial('baseline', self.current).kind != ALLOWED:
            raise _StopError('baseline-not-allowed')
        for permission in self.candidates:
            outcome = self._trial('removal', self.current - {permission}, removed=permission)
            if outcome.kind == ALLOWED:
                self.not_required.append(permission)
                self.current -= {permission}
                continue
            (self.required if outcome.kind == DENIED else self.undetermined).append(permission)
            if self._trial('restoration', self.current).kind != ALLOWED:
                raise _StopError('restoration-not-allowed')
        if self._trial('confirmation', self.current).kind != ALLOWED:
            raise _StopError('confirmation-not-allowed')

    def _trial(self, kind: str, granted: frozenset[str], removed: str | None = None) -> Outcome:
        self.budget.spend('trial')
        record = {
            'index': len(self.trials),
            'kind': kind,
            'removed': removed,
            'granted': sorted(granted),
            'acting_identity': 'allowed',
        }
        before = digest(self.target.snapshot())
        record['state'] = {'before_matches_initial': before == self.initial_digest}
        if before != self.initial_digest:
            record.update(Outcome(INCONCLUSIVE, detail='initial-state-mismatch').record())
            self.trials.append(record)
            raise _StopError('initial-state-mismatch')
        record['grant_changed'] = self.grant.ensure(granted)
        if record['grant_changed']:
            self.last_change = self.clock.monotonic()
        outcome, propagation = self._observe(kind, granted)
        after = digest(self.target.snapshot())
        record['state']['after_matches_initial'] = after == self.initial_digest
        record['propagation'] = propagation
        if after != self.initial_digest:
            outcome = Outcome(
                INCONCLUSIVE,
                outcome.http_status,
                outcome.status,
                outcome.reason,
                'state-not-restored',
            )
        record.update(outcome.record())
        record['conclusive'] = outcome.kind != INCONCLUSIVE
        self.trials.append(record)
        if after != self.initial_digest:
            raise _StopError('state-not-restored')
        return outcome

    def _observe(self, kind: str, granted: frozenset[str]) -> tuple[Outcome, dict]:
        """Poll the hint, then the real operation, until the trial decides or times out."""
        start = self.clock.monotonic()
        deadline = start + self.limits['max_propagation_seconds']
        hintable = self.target.hintable(self.candidates)
        expected = sorted(granted & set(hintable))
        hint, attempts, successes, operation = 'pending', 0, 0, None
        while True:
            if hint == 'pending':
                hint = self._hint_state(kind, hintable, expected)
            if hint == 'preexisting':
                outcome = Outcome(INCONCLUSIVE, detail='preexisting-access')
                break
            if hint != 'pending':
                attempts += 1
                outcome, operation = self._attempt()
                successes += outcome.kind == ALLOWED
                if self._decided(kind, outcome, successes):
                    break
            if self.clock.monotonic() >= deadline:
                outcome = Outcome(INCONCLUSIVE, detail='propagation-timeout')
                break
            self.budget.check_time()
            self.clock.sleep(self.limits['poll_interval_seconds'])
        return outcome, {
            'max_seconds': self.limits['max_propagation_seconds'],
            'poll_seconds': self.limits['poll_interval_seconds'],
            'waited_seconds': round(self.clock.monotonic() - start),
            'hint': hint,
            'attempts': attempts,
            'operation': operation,
        }

    def _hint_state(self, kind: str, hintable: list[str], expected: list[str]) -> str:
        """Use testIamPermissions as a readiness hint only; the real call decides."""
        observed = self.target.hint(hintable) if hintable else None
        if observed is None:
            return 'unavailable'
        if sorted(observed) == expected:
            return 'converged'
        # The probe identity must start with none of the candidates.
        return 'preexisting' if kind == 'control' else 'pending'

    def _decided(self, kind: str, outcome: Outcome, successes: int) -> bool:
        if outcome.kind == INCONCLUSIVE or kind == 'control':
            return True
        if kind == 'confirmation':
            # Soak: success must persist for the full propagation bound after the last
            # grant change, so a revocation that lagged its hint surfaces here.
            soaked = self.last_change is None or (
                self.clock.monotonic() >= self.last_change + self.limits['max_propagation_seconds'])
            return outcome.kind == DENIED or soaked
        if kind in EXPECT_ALLOWED:
            return outcome.kind == ALLOWED
        # A removal is decided by a denial, or by repeated success after revocation.
        return outcome.kind == DENIED or successes >= self.limits['removal_confirmations']

    def _attempt(self) -> tuple[Outcome, dict | None]:
        operation = None
        try:
            response = self.target.invoke()
            outcome = classify(response)
            if self.target.long_running and outcome.kind == ALLOWED:
                outcome, operation = self._complete(response.json())
        except Inconclusive:
            outcome = Outcome(INCONCLUSIVE, detail='transport')
        # The after-trial snapshot decides whether state was restored.
        with contextlib.suppress(Inconclusive):
            self.target.restore()
        return outcome, operation

    def _complete(self, value: dict) -> tuple[Outcome, dict]:
        """Poll a long-running operation to completion before counting success."""
        name = value.get('name') if isinstance(value, dict) else None
        if not isinstance(name, str) or not name:
            return Outcome(INCONCLUSIVE, detail='operation-missing'), {'polls': 0, 'done': False}
        deadline = self.clock.monotonic() + self.limits['max_operation_seconds']
        polls = 0
        while not value.get('done'):
            if self.clock.monotonic() >= deadline:
                timeout = Outcome(INCONCLUSIVE, detail='operation-timeout')
                return timeout, {'polls': polls, 'done': False}
            self.clock.sleep(self.limits['poll_interval_seconds'])
            polls += 1
            response = self.target.operation(name)
            if response.status != 200:
                return classify(response), {'polls': polls, 'done': False}
            value = response.json()
        return operation_outcome(value), {'polls': polls, 'done': True}

    def report(self) -> dict:
        """Return the aliased run record; inconclusive trials stay apart from conclusive ones."""
        conclusive = self.stopped is None and not self.undetermined
        result = {
            'status': 'conclusive' if conclusive else 'inconclusive',
            'stopped': self.stopped,
            'minimal_permissions': sorted(self.current) if conclusive else None,
            # Removals that looked unnecessary hold only after the confirmation soak passes.
            'conclusive_findings': {
                'required': sorted(self.required),
                'not_required': sorted(self.not_required) if conclusive else [],
            },
            'inconclusive': {
                'permissions': sorted(self.undetermined),
                'unconfirmed_not_required': [] if conclusive else sorted(self.not_required),
                'trials': [trial['index'] for trial in self.trials if not trial.get('conclusive')],
            },
        }
        report = {
            'schema': 1,
            'kind': 'gcp-local-permission-probe',
            'source': self.source,
            'reviewed': False,
            'executed_at': now(),
            'scenario': self.target.describe(),
            'grant': self.grant.describe(),
            'limits': dict(self.limits),
            'spent': dict(self.budget.spent),
            'seed': self.seed,
            'excluded': dict(sorted(self.excluded.items())),
            'trials': self.trials,
            'result': result,
            'claim': CLAIM,
        }
        report = self.observe.value(json.loads(json.dumps(report)))
        check_observation(report)
        return report
