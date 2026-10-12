import json
import runpy
from pathlib import Path
from types import SimpleNamespace

import pytest

from gcp_validation.services.messaging.pubsub_schemas import (
    SCHEMA_LIMITS,
    SchemaScope,
    definition_tag,
)
from gcp_validation.services.messaging.pubsub_scope import private_json

SANDBOX = SimpleNamespace(project="project-a", project_number="123456")
PROFILE = SimpleNamespace(
    project="project-a",
    project_number="123456",
    identities={"fixture": SimpleNamespace(email="fixture@project-a.iam.gserviceaccount.com")},
)


def planner():
    return runpy.run_path(str(Path(__file__).resolve().parents[3] / "scripts/live/plan_pubsub.py"))[
        "plan"
    ]


def test_planner_writes_an_unapproved_schema_scope():
    result = planner()(PROFILE, "owner@example.com", "schemas")
    assert result["scope"]["approved"] is False
    assert result["scope"]["services"] == ["pubsub-schemas"]
    assert result["scope"]["limits"] == SCHEMA_LIMITS
    assert result["resource_grants"]["Protocol Buffer schema"] == {
        "role": "roles/pubsub.viewer",
        "identity": "allowed",
    }


def test_schema_scope_requires_explicit_approval_and_its_own_service(tmp_path):
    value = planner()(PROFILE, "owner@example.com", "schemas")["scope"]
    path = tmp_path / "scope.json"
    private_json(path, value)
    with pytest.raises(ValueError, match="must match the approved project"):
        SchemaScope.load(path, SANDBOX)
    value["approved"] = True
    path.write_text(json.dumps(value))
    assert SchemaScope.load(path, SANDBOX).project_number == "123456"
    value["services"] = ["pubsub-delivery"]
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="must match the approved project"):
        SchemaScope.load(path, SANDBOX)
    with pytest.raises(ValueError, match="separately approved"):
        SchemaScope.load(None, SANDBOX)


def test_definitions_are_named_by_revision():
    assert definition_tag("avro", "") == "empty"
    assert definition_tag("proto", "syntax = 'proto2';") == "other"
