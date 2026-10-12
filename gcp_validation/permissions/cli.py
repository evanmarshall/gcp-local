"""Permission discovery commands: plan and drift run offline; run and cleanup need --allow-live.

``plan`` prints the bounded live plan and the unapproved scope file. ``run`` executes one
probe against the approved sandbox and writes an unreviewed report candidate. ``cleanup``
deletes only the resources an interrupted run's manifest owns. ``drift`` compares a report
with exported local plans and committed role snapshots and writes a review proposal.
"""

import argparse
import json
import sys
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.live_rest import LiveScope
from gcp_validation.permissions.drift import drift, load_plans, write
from gcp_validation.permissions.probe import Clock
from gcp_validation.permissions.secretmanager import (
    LIMITS,
    ROLES_DIR,
    SERVICE,
    LiveTransport,
    cleanup,
    live_plan,
    owned_run,
    run_probe,
    scope_document,
)
from gcp_validation.sandbox import Sandbox


def state_dir(project: str) -> Path:
    """Return the owner-only manifest directory for this sandbox."""
    return Path.home() / '.local/state/gcp-local' / project / SERVICE


def _live(args: argparse.Namespace) -> LiveTransport:
    sandbox = Sandbox.load(args.profile)
    scope = LiveScope.load(SERVICE, LIMITS, args.scope, sandbox)
    target = Target.select(
        'record',
        project=sandbox.project,
        credentials_file=sandbox.identities['allowed'].credentials_file,
        live_scope=sandbox.live_scope,
        allow_live=args.allow_live,
    )
    return LiveTransport(target, sandbox, scope)


def _plan(args: argparse.Namespace) -> int:
    sandbox = Sandbox.load(args.profile)
    emails = {actor: identity.email for actor, identity in sandbox.identities.items()}
    plan = live_plan(sandbox.project, sandbox.project_number, emails, args.attachment)
    if args.write_scope:
        path = Path(args.write_scope)
        if path.exists():
            raise SystemExit('Refusing to overwrite an existing scope file')
        path.write_text(
            json.dumps(scope_document(sandbox.project, sandbox.project_number), indent=2) + '\n'
        )
    sys.stdout.write(json.dumps(plan, indent=2) + '\n')
    return 0


def _run(args: argparse.Namespace) -> int:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    transport = _live(args)
    report, manifest = run_probe(
        transport,
        state_dir(transport.target.project),
        attachment=args.attachment,
        clock=Clock(),
        source='gcp',
    )
    (out / 'report.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    sys.stdout.write(f'Report candidate (unreviewed): {out / "report.json"}\n')
    sys.stdout.write(f'Result: {report["result"]["status"]}; cleanup: {report["cleanup"]}\n')
    if report['cleanup'] != 'passed':
        sys.stdout.write(f'Run cleanup with --manifest {manifest}\n')
        return 1
    return 0


def _cleanup(args: argparse.Namespace) -> int:
    transport = _live(args)
    owned = owned_run(transport, state_dir(transport.target.project), manifest=Path(args.manifest))
    cleanup(transport, owned)
    sys.stdout.write('Owned permission-probe cleanup passed.\n')
    return 0


def _drift(args: argparse.Namespace) -> int:
    report = json.loads(Path(args.report).read_text())
    proposal = drift(report, load_plans(args.plans), Path(args.roles))
    directory = write(proposal, Path(args.out))
    sys.stdout.write(f'Drift proposal ({proposal["status"]}, not applied): {directory}\n')
    return 0


def parser() -> argparse.ArgumentParser:
    """Build the command parser; live commands require explicit profile, scope, and flag."""
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest='command', required=True)
    plan = commands.add_parser('plan', help='print the bounded live plan; no network access')
    plan.add_argument('--profile', required=True)
    plan.add_argument('--attachment', choices=['secret', 'project'], default='secret')
    plan.add_argument('--write-scope', help='write the unapproved scope file for owner review')
    plan.set_defaults(handler=_plan)
    for name, handler in (('run', _run), ('cleanup', _cleanup)):
        live = commands.add_parser(name)
        live.add_argument('--profile', required=True)
        live.add_argument('--scope', required=True)
        live.add_argument('--allow-live', action='store_true', required=True)
        live.set_defaults(handler=handler)
        if name == 'run':
            live.add_argument('--attachment', choices=['secret', 'project'], required=True)
            live.add_argument('--out', required=True)
        else:
            live.add_argument('--manifest', required=True)
    check = commands.add_parser('drift', help='compare a report with local plans; never edits')
    check.add_argument('--report', required=True)
    check.add_argument('--plans', required=True, help='output of tools/permission-plans')
    check.add_argument('--roles', default=str(ROLES_DIR))
    check.add_argument('--out', required=True)
    check.set_defaults(handler=_drift)
    return root


def main(argv: list[str] | None = None) -> int:
    """Run one permission discovery command."""
    args = parser().parse_args(argv)
    return args.handler(args)


if __name__ == '__main__':
    raise SystemExit(main())
