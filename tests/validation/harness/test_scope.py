import json
from types import SimpleNamespace

import pytest

from gcp_validation.scope import check_scope, load_scope_file, scope_document

LIMITS = {"max_things": 1, "max_run_seconds": 600}
SANDBOX = SimpleNamespace(project="sandbox-project", project_number="123456")


def approved(**changes):
    value = scope_document("thing", "sandbox-project", "123456", LIMITS, approved=True)
    value.update(changes)
    return value


def check(value, mode="record"):
    check_scope(
        value,
        service="thing",
        project="sandbox-project",
        project_number="123456",
        limits=LIMITS,
        mode=mode,
        label="Thing",
    )


def test_unscheduled_scope_authorizes_record_revalidate_and_cleanup():
    for mode in ("record", "revalidate", "cleanup"):
        check(approved(), mode)


def test_scheduled_scope_authorizes_revalidate_and_cleanup():
    for mode in ("revalidate", "cleanup"):
        check(approved(scheduled_runs=True), mode)


def test_scheduled_scope_never_authorizes_record():
    with pytest.raises(ValueError, match="never authorizes record"):
        check(approved(scheduled_runs=True), "record")


@pytest.mark.parametrize("value", [1, 0, "true", None])
def test_scheduled_runs_must_be_a_bool(value):
    with pytest.raises(ValueError, match="cleanup contract"):
        check(approved(scheduled_runs=value), "revalidate")


@pytest.mark.parametrize(
    "changes",
    [
        {"approved": False},
        {"approved": 1},
        {"cleanup_required": False},
        {"cleanup_required": 1},
        {"schema": True},
        {"schema": 2},
        {"project": "other-project"},
        {"project_number": "654321"},
        {"location": "us-central1"},
        {"services": ["other"]},
        {"limits": {"max_things": 2, "max_run_seconds": 600}},
        {"limits": {"max_things": True, "max_run_seconds": 600}},
        {"extra": 1},
    ],
)
def test_every_other_field_stays_strict(changes):
    with pytest.raises(ValueError, match="cleanup contract"):
        check(approved(**changes))


def test_missing_field_and_non_mapping_are_rejected():
    value = approved()
    del value["scheduled_runs"]
    for document in (value, [], "scope"):
        with pytest.raises(ValueError, match="cleanup contract"):
            check(document)


def test_unknown_mode_is_rejected():
    with pytest.raises(ValueError, match="Unknown live mode"):
        check(approved(), "offline")


def test_planner_document_is_unapproved_and_unscheduled():
    document = scope_document("thing", "sandbox-project", "123456", LIMITS, approved=False)
    assert document["approved"] is False
    assert document["scheduled_runs"] is False
    with pytest.raises(ValueError, match="cleanup contract"):
        check(document)


def test_load_scope_file_requires_a_file_and_a_sandbox(tmp_path):
    with pytest.raises(ValueError, match="explicitly approved --scope"):
        load_scope_file(None, SANDBOX, service="thing", limits=LIMITS, mode="record",
                        label="Thing", option="--scope")
    path = tmp_path / "scope.json"
    path.write_text(json.dumps(approved(scheduled_runs=True)))
    assert load_scope_file(path, SANDBOX, service="thing", limits=LIMITS, mode="revalidate",
                           label="Thing", option="--scope")["scheduled_runs"] is True
    with pytest.raises(ValueError, match="never authorizes record"):
        load_scope_file(path, SANDBOX, service="thing", limits=LIMITS, mode="record",
                        label="Thing", option="--scope")
