"""Compare a probe report with local permission plans and role snapshots; propose, never edit.

The local plans come from ``tools/permission-plans``, which exports the rules providers
register with the IAM engine. The role snapshots are the committed, checksummed files the
engine embeds. The output is a reviewed proposal (JSON and Markdown) written to a new
directory; providers, plans, and snapshots are never changed by this module.
"""

import json
from pathlib import Path

from gcp_validation.permissions.probe import role_snapshots


def load_plans(path: Path) -> dict:
    """Read the exported plans for one service."""
    value = json.loads(Path(path).read_text())
    if value.get('schema') != 1 or not isinstance(value.get('plans'), dict):
        raise ValueError('Expected tools/permission-plans output')
    return value


def planned(rule: dict, features: dict) -> tuple[list[str], list[str]]:
    """Return the plan's permissions for these request features, and unresolved features."""
    permissions = set(rule.get('baseline', []))
    unresolved = []
    for feature, extra in sorted((rule.get('when') or {}).items()):
        if feature not in features:
            unresolved.append(feature)
        elif features[feature]:
            permissions.update(extra)
    return sorted(permissions), unresolved


def _role_findings(report: dict, discovered: list[str], roles: dict) -> dict:
    granting = sorted(
        name for name, role in roles.items() if set(discovered) <= set(role['permissions'])
    )
    known = {permission for role in roles.values() for permission in role['permissions']}
    changed = sorted(
        name
        for name, digest in report['seed']['sources'].items()
        if roles.get(name, {}).get('sha256') != digest
    )
    return {
        'roles_granting_all': granting,
        'absent_from_snapshots': sorted(set(discovered) - known),
        'seed_snapshots_changed_since_probe': changed,
    }


def _actions(method: str, comparison: dict, snapshots: dict, excluded: dict) -> list[str]:
    actions = []
    if comparison['missing_from_local']:
        actions.append(
            f'Review adding {", ".join(comparison["missing_from_local"])} to the local '
            f'{method} plan for these request features.'
        )
    if comparison['extra_in_local']:
        actions.append(
            f'The local {method} plan also checks {", ".join(comparison["extra_in_local"])}, '
            'which this scenario did not require; keep it unless other scenarios agree, '
            'because one sufficient set does not exclude other authorization paths.'
        )
    if comparison['unresolved_features']:
        actions.append(
            'The report does not resolve plan features '
            f'{", ".join(comparison["unresolved_features"])}; probe those shapes separately.'
        )
    if snapshots['absent_from_snapshots']:
        actions.append(
            'Re-record role snapshots with scripts/live/record_iam_roles.py: '
            f'{", ".join(snapshots["absent_from_snapshots"])} is in no committed role.'
        )
    if snapshots['seed_snapshots_changed_since_probe']:
        actions.append(
            'Seed role snapshots changed after the probe; re-run it before relying on it.'
        )
    if excluded:
        actions.append(
            f'Not probed at this attachment point: {", ".join(sorted(excluded))}; '
            'local checks of these remain unverified by this report.'
        )
    return actions


def drift(report: dict, plans: dict, roles_dir: Path) -> dict:
    """Build a review proposal from one probe report; inconclusive reports propose nothing."""
    if report.get('kind') != 'gcp-local-permission-probe' or report.get('schema') != 1:
        raise ValueError('Expected a permission-probe report')
    scenario, result = report['scenario'], report['result']
    method = scenario['method']
    base = {
        'schema': 1,
        'kind': 'gcp-local-permission-drift',
        'method': method,
        'source': report['source'],
        'report_executed_at': report['executed_at'],
        'review_required': True,
        'applied': False,
    }
    if result['status'] != 'conclusive':
        return {
            **base,
            'status': 'inconclusive',
            'stopped': result['stopped'],
            'inconclusive': result['inconclusive'],
            'actions': [
                'No proposal: an inconclusive probe never establishes permission completeness.'
            ],
        }
    rule = plans['plans'].get(method)
    discovered = result['minimal_permissions']
    local, unresolved = (
        planned(rule, scenario['request_features'].get('features', {})) if rule else ([], [])
    )
    comparison = {
        'discovered': discovered,
        'local_plan': local if rule else None,
        'missing_from_local': sorted(set(discovered) - set(local)),
        'extra_in_local': sorted(set(local) - set(discovered)),
        'unresolved_features': unresolved,
    }
    snapshots = _role_findings(report, discovered, role_snapshots(roles_dir))
    actions = _actions(method, comparison, snapshots, report['excluded'])
    if rule is None:
        actions.insert(0, f'No local plan is registered for {method}.')
    drifted = bool(
        rule is None
        or comparison['missing_from_local']
        or comparison['extra_in_local']
        or snapshots['absent_from_snapshots']
        or snapshots['seed_snapshots_changed_since_probe']
    )
    return {
        **base,
        'status': 'drift' if drifted else 'no-drift',
        'comparison': comparison,
        'role_snapshots': snapshots,
        'excluded_from_probe': report['excluded'],
        'grant': report['grant'],
        'check_target': scenario['check_target'],
        'claim': report['claim'],
        'actions': actions or ['No change proposed.'],
    }


def markdown(proposal: dict) -> str:
    """Render the proposal for a reviewer; it states that nothing was applied."""
    lines = [
        f'# Permission drift: {proposal["method"]}',
        '',
        f'Status: **{proposal["status"]}** (source: {proposal["source"]}; '
        f'probe executed {proposal["report_executed_at"]}). Nothing was applied; '
        'review before changing providers, plans, or snapshots.',
        '',
    ]
    comparison = proposal.get('comparison')
    if comparison:
        lines += ['| | Permissions |', '| --- | --- |']
        for label, key in (
            ('Discovered minimal set', 'discovered'),
            ('Local plan', 'local_plan'),
            ('Missing from local', 'missing_from_local'),
            ('Extra in local', 'extra_in_local'),
        ):
            lines.append(f'| {label} | {", ".join(comparison[key] or []) or "-"} |')
        lines.append('')
    lines += ['## Proposed actions', '']
    lines += [f'- {action}' for action in proposal['actions']]
    if proposal.get('claim'):
        lines += ['', proposal['claim']]
    return '\n'.join(lines) + '\n'


def write(proposal: dict, directory: Path) -> Path:
    """Write drift.json and drift.md into a new directory, never over existing evidence."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    (directory / 'drift.json').write_text(json.dumps(proposal, indent=2, sort_keys=True) + '\n')
    (directory / 'drift.md').write_text(markdown(proposal))
    return directory
