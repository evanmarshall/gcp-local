import json
import runpy
from pathlib import Path
from types import SimpleNamespace as ns  # noqa: N813 -- short alias keeps fake constructors readable

import pytest

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive
from gcp_validation.services.messaging.pubsub import (
    LIMITS,
    LiveScope,
    PubSubFixture,
    RPCFailure,
    local_credentials,
    private_json,
    run_scenario,
)


def _unused(*args: object) -> None:
    """Deliberately ignore matcher callbacks."""


class SyntheticClient:
    """Fixture-failure tests only; no service or cloud fidelity evidence."""

    target = Target.select("local", project="project-a", endpoint="http://127.0.0.1:4567")
    sandbox = None
    members = {"allowed": "serviceAccount:allowed@project-a.iam.gserviceaccount.com"}  # noqa: RUF012 -- class-level constant

    def __init__(self):
        self.resources, self.calls, self.messages = {}, [], []
        self.fail_delete = self.ambiguous_create = self.bad_denied_write = False

    def verify_project(self):
        pass

    def call(self, actor, kind, method, request, timeout=10):
        self.calls.append((actor, kind, method, request))
        if actor == "denied":
            return self.denied(method, request)
        return self.dispatch(kind, method, request)

    def denied(self, method, request):
        """Reject as the real service would, keeping the poisoned message first."""
        if self.bad_denied_write and method == "publish":
            self.messages.append(
                ns(ack_id="bad-ack", message=ns(message_id="bad-id", **request["messages"][0]))
            )
        raise RPCFailure("PERMISSION_DENIED")

    def dispatch(self, kind, method, request):
        """Answer the method families of the generated Pub/Sub client."""
        if method.startswith("list_"):
            return self.list(kind)
        if method.startswith("create_"):
            return self.create(kind, request)
        if method.startswith("get_") and method != "get_iam_policy":
            return self.get(kind, request)
        if method.startswith("delete_"):
            return self.delete(kind, request)
        if method == "publish":
            return self.publish(request)
        return self.default(method)

    def create(self, kind, request):
        """Store the resource, maybe failing like an ambiguous commit."""
        value = ns(**request)
        self.resources[kind, request["name"]] = value
        if self.ambiguous_create:
            raise RPCFailure("DEADLINE_EXCEEDED")
        return value

    def get(self, kind, request):
        """Return the stored resource or the service's not-found error."""
        try:
            return self.resources[kind, request[kind]]
        except KeyError:
            raise RPCFailure("NOT_FOUND") from None

    def list(self, kind):
        """Page the resources of one kind."""
        return ns(
            **{kind + "s": [value for (k, _), value in self.resources.items() if k == kind]},
            next_page_token="",
        )

    def delete(self, kind, request):
        """Delete or fail with the service's transient error."""
        if self.fail_delete:
            raise RPCFailure("UNAVAILABLE")
        del self.resources[kind, request[kind]]
        return ns()

    def publish(self, request):
        """Record the message and echo its id like the real backend."""
        self.messages.append(
            ns(ack_id="ack", message=ns(message_id="msg-1", **request["messages"][0]))
        )
        return ns(message_ids=["msg-1"])

    def default(self, method):
        """Drain queued messages on pull; answer everything else with an empty result."""
        if method == "pull":
            values, self.messages = self.messages, []
            return ns(received_messages=values)
        if method == "get_iam_policy":
            return ns(bindings=[], etag=b"local-etag")
        return ns()


def test_separate_pubsub_approval_is_required(tmp_path):
    sandbox = ns(project="project-a", project_number="123456")
    with pytest.raises(ValueError, match="separate explicitly approved"):
        LiveScope.load(None, sandbox, "record")
    value = {
        "schema": 1,
        "approved": False,
        "services": ["pubsub"],
        "project": sandbox.project,
        "project_number": sandbox.project_number,
        "location": "global",
        "limits": LIMITS,
        "cleanup_required": True,
        "scheduled_runs": False,
    }
    path = tmp_path / "scope.json"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="scope must match"):
        LiveScope.load(path, sandbox, "record")
    value["approved"] = True
    path.write_text(json.dumps(value))
    assert LiveScope.load(path, sandbox, "record").project == sandbox.project
    value["services"] = ["storage"]
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="must match the approved project"):
        LiveScope.load(path, sandbox, "record")
    value["services"], value["project"] = ["pubsub"], "foreign-project"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="must match the approved project"):
        LiveScope.load(path, sandbox, "record")


def test_local_transport_refuses_cloud_adc_before_loading_credentials(tmp_path):
    path = tmp_path / "adc.json"
    private_json(
        path,
        {"type": "impersonated_service_account", "source_credentials": {"secret": "never loaded"}},
    )
    with pytest.raises(ValueError, match="synthetic local ADC"):
        local_credentials(path, SyntheticClient.target)


def test_planner_keeps_cloud_scope_unapproved_and_grants_separate():
    plan = runpy.run_path(str(Path(__file__).resolve().parents[3] / "scripts/live/plan_pubsub.py"))[
        "plan"
    ]
    profile = ns(
        project="project-a",
        project_number="123456",
        identities={"fixture": ns(email="fixture@project-a.iam.gserviceaccount.com")},
    )
    result = plan(profile, "owner@example.com")
    assert result["scope"]["approved"] is False
    assert result["scope"]["limits"] == LIMITS
    assert len(result["setup"]) == 2
    for command in result["setup"] + [result["restore_new_fixture_grant"]]:
        assert "--project=project-a" in command and "--account=owner@example.com" in command
    assert "--role=roles/pubsub.admin" in result["setup"][1]
    assert "--member=serviceAccount:fixture@project-a.iam.gserviceaccount.com" in result["setup"][1]
    assert result["resource_grants"]["topic"]["role"] == "roles/pubsub.publisher"
    assert result["resource_grants"]["subscription"]["role"] == "roles/pubsub.subscriber"


def test_cleanup_manifest_cannot_retarget_another_project(tmp_path):
    client = SyntheticClient()
    fixture = PubSubFixture(client, tmp_path)
    fixture.setup()
    original = fixture.path.read_bytes()
    value = json.loads(original)
    value["topic"] = "projects/another/topics/foreign"
    private_json(fixture.path, value)
    with pytest.raises(ValueError, match="ownership scope"):
        PubSubFixture(client, tmp_path, manifest=fixture.path)
    assert client.resources
    fixture.path.write_bytes(original)
    fixture.cleanup()


def test_owned_lifecycle_and_scoped_grants(tmp_path):
    client = SyntheticClient()
    fixture = PubSubFixture(client, tmp_path)
    observations = {}
    try:
        fixture.setup()
        run_scenario(fixture, ns(match=observations.setdefault))
    finally:
        fixture.cleanup()
    assert client.resources == {}
    assert json.loads(fixture.path.read_text())["cleanup"] == "passed"
    assert fixture.path.stat().st_mode & 0o077 == 0
    assert fixture.manifest["publish_attempts"] == 2
    assert observations["denied-consumption"]["grpc_status"] == "PERMISSION_DENIED"
    grants = [request for _, _, method, request in client.calls if method == "set_iam_policy"]
    assert {item["resource"] for item in grants} == {fixture.topic, fixture.subscription}
    assert {item["policy"]["bindings"][0]["role"] for item in grants} == {
        "roles/pubsub.publisher",
        "roles/pubsub.subscriber",
    }
    assert all("project-a" not in json.dumps(observation) for observation in observations.values())


@pytest.mark.parametrize("corruption", ["denied_write", "foreign_labels", "changed_topic"])
def test_scenario_and_cleanup_reject_corruption(tmp_path, corruption):
    client = SyntheticClient()
    fixture = PubSubFixture(client, tmp_path)
    fixture.setup()
    if corruption == "denied_write":
        client.bad_denied_write = True
        with pytest.raises(AssertionError):
            run_scenario(fixture, ns(match=_unused))
        fixture.cleanup()
    else:
        value = client.resources["subscription", fixture.subscription]
        if corruption == "foreign_labels":
            value.labels = {"gcp-local-run": "someone-else"}
        else:
            value.topic = "projects/foreign/topics/foreign"
        with pytest.raises(ValueError, match="ownership"):
            fixture.cleanup()
        assert client.resources
        assert json.loads(fixture.path.read_text())["cleanup"] == "pending"
        value.labels, value.topic = fixture.labels, fixture.topic
        fixture.cleanup()


def test_failed_cleanup_can_be_recovered_from_manifest(tmp_path):
    client = SyntheticClient()
    fixture = PubSubFixture(client, tmp_path)
    fixture.setup()
    client.fail_delete = True
    with pytest.raises(RPCFailure, match="UNAVAILABLE"):
        fixture.cleanup()
    assert json.loads(fixture.path.read_text())["cleanup"] == "pending"
    assert fixture.lock is None
    client.fail_delete = False
    recovery = PubSubFixture(client, tmp_path, manifest=fixture.path)
    recovery.cleanup()
    assert not client.resources
    assert json.loads(fixture.path.read_text())["cleanup"] == "passed"


def test_ambiguous_creation_records_intent_before_mutation(tmp_path):
    client = SyntheticClient()
    fixture = PubSubFixture(client, tmp_path)
    client.ambiguous_create = True
    with pytest.raises(RPCFailure, match="DEADLINE_EXCEEDED"):
        fixture.setup()
    assert json.loads(fixture.path.read_text())["attempted"] == ["topic"]
    fixture.cleanup()
    assert not client.resources


def test_bounds_project_lock_and_occupied_sandbox(tmp_path):
    client = SyntheticClient()
    first, second = PubSubFixture(client, tmp_path), PubSubFixture(client, tmp_path)
    first.setup()
    with pytest.raises(Inconclusive, match="lock"):
        second.setup()
    second.cleanup()
    before = len(client.calls)
    with pytest.raises(ValueError, match="payload"):
        first.publish(b"x" * 1025)
    first.manifest["publish_attempts"] = 10
    with pytest.raises(Inconclusive, match="attempt limit"):
        first.publish(b"small")
    assert len(client.calls) == before
    first.cleanup()
    client.resources["topic", "foreign"] = ns(name="foreign")
    third = PubSubFixture(client, tmp_path)
    with pytest.raises(Inconclusive, match="occupied"):
        third.setup()
    third.cleanup()
    assert ("topic", "foreign") in client.resources
