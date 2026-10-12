"""The one contract every owner-approved live scope file must satisfy.

A scope file names one service, the sandbox project and number, fixed limits,
and the cleanup contract. ``scheduled_runs: true`` marks a scope for the
scheduled drift job; it authorizes ``revalidate`` runs and the manifest-owned
``cleanup`` after an interruption, never ``record``.
"""

import json
from pathlib import Path
from typing import Any

from gcp_validation.sandbox import Sandbox

# Run modes a scope can be checked for; cleanup is the manual recovery CLI.
MODES = frozenset({"record", "revalidate", "cleanup"})


def scope_document(
    service: str,
    project: str,
    project_number: str,
    limits: dict[str, int],
    *,
    approved: bool,
    scheduled: bool = False,
) -> dict[str, Any]:
    """Return the exact scope file the loaders accept; planners write it unapproved."""
    return {
        "schema": 1,
        "approved": approved,
        "services": [service],
        "project": project,
        "project_number": project_number,
        "location": "global",
        "limits": dict(limits),
        "cleanup_required": True,
        "scheduled_runs": scheduled,
    }


def check_scope(
    value: object,
    *,
    service: str,
    project: str,
    project_number: str,
    limits: dict[str, int],
    mode: str,
    label: str,
) -> None:
    """Raise ValueError unless the document is the approved scope for this run mode."""
    if mode not in MODES:
        raise ValueError(f"Unknown live mode {mode!r} for the {label} scope")
    scheduled = value.get("scheduled_runs") if isinstance(value, dict) else None
    expected = scope_document(
        service, project, project_number, limits, approved=True, scheduled=scheduled
    )
    if (
        not isinstance(value, dict)
        or value != expected
        or type(value["schema"]) is not int
        or value["approved"] is not True
        or value["cleanup_required"] is not True
        or type(value["scheduled_runs"]) is not bool
        or any(type(item) is not int for item in value["limits"].values())
    ):
        raise ValueError(
            f"{label} scope must match the approved project, fixed limits, and cleanup contract"
        )
    if value["scheduled_runs"] and mode == "record":
        raise ValueError(f"A scheduled {label} scope never authorizes record runs")


def load_scope_file(
    filename: str | Path | None,
    sandbox: Sandbox | None,
    *,
    service: str,
    limits: dict[str, int],
    mode: str,
    label: str,
    option: str,
) -> dict[str, Any]:
    """Read one scope file and check it against the sandbox and run mode."""
    if not filename or sandbox is None:
        raise ValueError(f"{label} requires a separate explicitly approved {option}")
    value = json.loads(Path(filename).read_text())
    check_scope(
        value,
        service=service,
        project=sandbox.project,
        project_number=sandbox.project_number,
        limits=limits,
        mode=mode,
        label=label,
    )
    return value
