import json
import runpy
from pathlib import Path
from types import SimpleNamespace

import pytest

from gcp_validation.pubsub_delivery import DELIVERY_LIMITS, DeliveryScope
from gcp_validation.pubsub_scope import private_json

SANDBOX = SimpleNamespace(project="project-a", project_number="123456")


def test_planner_writes_an_unapproved_delivery_scope():
    plan = runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts/plan_pubsub.py"))[
        "plan"
    ]
    profile = SimpleNamespace(
        project="project-a",
        project_number="123456",
        identities={"fixture": SimpleNamespace(email="fixture@project-a.iam.gserviceaccount.com")},
    )
    result = plan(profile, "owner@example.com", "delivery")
    assert result["scope"]["approved"] is False
    assert result["scope"]["services"] == ["pubsub-delivery"]
    assert result["scope"]["limits"] == DELIVERY_LIMITS
    agent = "service-123456@gcp-sa-pubsub.iam.gserviceaccount.com"
    assert result["resource_grants"]["dead-letter topic"] == {
        "role": "roles/pubsub.publisher",
        "identity": agent,
    }


def test_delivery_scope_requires_explicit_approval_and_its_own_service(tmp_path):
    plan = runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts/plan_pubsub.py"))[
        "plan"
    ]
    profile = SimpleNamespace(
        project="project-a",
        project_number="123456",
        identities={"fixture": SimpleNamespace(email="f@x.y")},
    )
    value = plan(profile, "owner@example.com", "delivery")["scope"]
    path = tmp_path / "scope.json"
    private_json(path, value)
    with pytest.raises(ValueError, match="must match the approved project"):
        DeliveryScope.load(path, SANDBOX)
    value["approved"] = True
    path.write_text(json.dumps(value))
    assert DeliveryScope.load(path, SANDBOX).project_number == "123456"
    value["services"] = ["pubsub"]
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="must match the approved project"):
        DeliveryScope.load(path, SANDBOX)
    with pytest.raises(ValueError, match="separately approved"):
        DeliveryScope.load(None, SANDBOX)
