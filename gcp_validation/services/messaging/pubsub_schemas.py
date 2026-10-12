"""Bounded Pub/Sub schema scenario: schemas, revisions, validation, typed topics, IAM.

Live runs need a separately approved scope whose services entry is
``pubsub-schemas`` (``scripts/live/plan_pubsub.py --scenario schemas``).
"""

import fcntl
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, Self
from uuid import uuid4

from gcp_validation.evidence import Inconclusive, now
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.messaging.pubsub import PubSubClient, RPCFailure
from gcp_validation.services.messaging.pubsub_scope import ACTORS, private_json

SCHEMA_LIMITS = {
    "max_schemas": 3,
    "max_topics": 4,
    "max_subscriptions": 3,
    "max_messages": 60,
    "max_message_bytes": 1024,
    "max_run_seconds": 900,
}
AVRO_V1 = json.dumps(
    {
        "type": "record",
        "name": "Event",
        "fields": [{"name": "id", "type": "string"}, {"name": "count", "type": "int"}],
    }
)
AVRO_V2 = json.dumps(
    {
        "type": "record",
        "name": "Event",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "count", "type": "int"},
            {"name": "note", "type": ["null", "string"], "default": None},
        ],
    }
)
PROTO_V1 = 'syntax = "proto3";\nmessage Event {\n  string id = 1;\n  int32 count = 2;\n}\n'
PROTO_V2 = (
    'syntax = "proto3";\nmessage Event {\n  string id = 1;\n  int32 count = 2;\n'
    "  string note = 3;\n}\n"
)
DEFINITIONS = {"avro": (AVRO_V1, AVRO_V2), "proto": (PROTO_V1, PROTO_V2)}
TYPES = {"avro": "AVRO", "proto": "PROTOCOL_BUFFER"}
# id="a", count=1 in protobuf binary.
PROTO_BINARY = b"\x0a\x01a\x10\x01"
# Owned topics: short name -> (schema, encoding, pin to the first revision).
TOPICS = {
    "avro-json": ("avro", "JSON", True),
    "proto-json": ("proto", "JSON", True),
    "proto-binary": ("proto", "BINARY", False),
}
GRANT_WAIT_SECONDS = 180
# Publishing keeps working for a short time after a schema is deleted.
DELETION_WAIT_SECONDS = 120


class Snapshot(Protocol):
    """The observation sink: a SnapshotCase or a local preparation recorder."""

    def match(self, label: str, observation: object) -> None:
        """Record one observation."""


@dataclass(frozen=True)
class SchemaScope:
    """A separately approved live scope for this scenario."""

    project: str
    project_number: str

    @classmethod
    def load(cls, filename: str | None, sandbox: Sandbox | None) -> Self:
        """Load the scope file and require it to match the fixed contract."""
        if not filename or sandbox is None:
            raise ValueError("Pub/Sub schemas requires a separately approved --gcp-pubsub-scope")
        value = json.loads(Path(filename).read_text())
        if value != schema_scope(sandbox.project, sandbox.project_number, approved=True):
            raise ValueError(
                "Pub/Sub schema scope must match the approved project, limits, and cleanup"
            )
        return cls(sandbox.project, sandbox.project_number)

    def evidence(self) -> dict[str, Any]:
        """Scope facts recorded with the evidence."""
        return {
            "services": ["pubsub-schemas"],
            "location": "global",
            "limits": dict(SCHEMA_LIMITS),
            "cleanup_required": True,
            "identities": list(ACTORS),
        }


def schema_scope(project: str, project_number: str, *, approved: bool) -> dict[str, Any]:
    """Return the scope document; the planner writes it unapproved."""
    return {
        "schema": 1,
        "approved": approved,
        "services": ["pubsub-schemas"],
        "project": project,
        "project_number": project_number,
        "location": "global",
        "limits": SCHEMA_LIMITS,
        "cleanup_required": True,
        "scheduled_runs": False,
    }


def attach_schema_clients(client: PubSubClient) -> None:
    """Add a SchemaService client per actor over the actor's existing channel."""
    from google.pubsub_v1.services.schema_service.client import (  # noqa: PLC0415 -- SDK is optional offline
        SchemaServiceClient,
    )
    from google.pubsub_v1.services.schema_service.transports.grpc import (  # noqa: PLC0415
        SchemaServiceGrpcTransport,
    )

    # PubSubClient opens one channel per actor, in the order of its client map.
    for actor, channel in zip(client.clients, client.channels, strict=True):
        client.clients[actor]["schema"] = SchemaServiceClient(
            transport=SchemaServiceGrpcTransport(channel=channel)
        )


class SchemaFixture:
    """Owns the scenario's schemas, topics, and subscriptions through a private manifest."""

    def __init__(self, client: PubSubClient, state_dir: Path) -> None:
        """Prepare an ownership manifest; nothing is created until setup."""
        attach_schema_clients(client)
        self.client, self.project = client, client.target.project
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.deadline = time.monotonic() + SCHEMA_LIMITS["max_run_seconds"]
        run = uuid4().hex
        self.manifest = {
            "schema": 1,
            "kind": "pubsub-schemas",
            "project": self.project,
            "run_id": run,
            "attempted": [],
            "publish_attempts": 0,
            "cleanup": "pending",
            "started_at": now(),
        }
        self.path = self.state_dir / (run + ".json")
        self.labels = {"managed-by": "gcp-local", "gcp-local-run": run}
        self.lock = None
        self.revision_labels: dict[str, str] = {}

    def short(self, short: str) -> str:
        """Return the owned resource ID for one scenario resource."""
        return f"gcp-local-{short}-{self.manifest['run_id'][:12]}"

    def name(self, kind: str, short: str) -> str:
        """Return the owned resource name for one scenario resource."""
        return f"projects/{self.project}/{kind}s/{self.short(short)}"

    def call(self, kind: str, method: str, request: dict, *, actor: str = "fixture") -> Any:  # noqa: ANN401 -- SDK response types vary per method
        """One bounded RPC; failures become RPCFailure."""
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise Inconclusive(
                "Pub/Sub schema scenario exceeded its deadline; cleanup is still required"
            )
        return self.client.call(actor, kind, method, request, timeout=min(10, remaining))

    def outcome(self, kind: str, method: str, request: dict, *, actor: str = "fixture") -> str:
        """Run one call and return OK or its gRPC status name."""
        try:
            self.call(kind, method, request, actor=actor)
        except RPCFailure as error:
            return error.status
        return "OK"

    def remember(self, name: str) -> None:
        """Persist intent before a create whose response may be lost."""
        self.manifest["attempted"].append(name)
        private_json(self.path, self.manifest)

    def label(self, revision_id: str) -> str:
        """Name revision IDs by first appearance: <revision:1>, <revision:2>, and so on."""
        if not revision_id:
            return ""
        if revision_id not in self.revision_labels:
            self.revision_labels[revision_id] = f"<revision:{len(self.revision_labels) + 1}>"
        return self.revision_labels[revision_id]

    def lock_project(self) -> None:
        """Take the per-project lock and confirm the live project."""
        self.lock = (self.state_dir / "pubsub-schemas.lock").open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            self.lock = None
            raise Inconclusive("Another Pub/Sub schema run holds this project lock") from None
        self.client.verify_project()

    def create_schema(self, kind: str) -> Any:  # noqa: ANN401 -- SDK Schema
        """Create one owned schema with its first definition."""
        self.remember(self.name("schema", kind))
        request = {
            "parent": f"projects/{self.project}",
            "schema_id": self.short(kind),
            "schema": {"type_": TYPES[kind], "definition": DEFINITIONS[kind][0]},
        }
        return self.call("schema", "create_schema", request)

    def create_topic(self, short: str, settings: dict) -> Any:  # noqa: ANN401 -- SDK Topic
        """Create one owned topic with schema settings and its subscription."""
        topic = self.name("topic", short)
        self.remember(topic)
        created = self.call(
            "topic",
            "create_topic",
            {"name": topic, "labels": self.labels, "schema_settings": settings},
        )
        subscription = self.name("subscription", short)
        self.remember(subscription)
        request = {"name": subscription, "topic": topic, "labels": self.labels}
        self.call("subscription", "create_subscription", request)
        return created

    def publish(self, short: str, payload: bytes, *, actor: str = "allowed") -> str:
        """Publish one bounded message and return OK or the failure status."""
        if len(payload) > SCHEMA_LIMITS["max_message_bytes"]:
            raise ValueError("Payload exceeds the bounded scope")
        if self.manifest["publish_attempts"] >= SCHEMA_LIMITS["max_messages"]:
            raise Inconclusive("Pub/Sub schema publish limit reached")
        self.manifest["publish_attempts"] += 1
        private_json(self.path, self.manifest)
        request = {"topic": self.name("topic", short), "messages": [{"data": payload}]}
        return self.outcome("topic", "publish", request, actor=actor)

    def grant(self, kind: str, resource: str, role: str) -> None:
        """Grant the allowed caller one role on one owned resource."""
        policy = self.call(kind, "get_iam_policy", {"resource": resource})
        bindings = [{"role": b.role, "members": list(b.members)} for b in policy.bindings]
        bindings.append({"role": role, "members": [self.client.members["allowed"]]})
        policy = {"version": 1, "etag": policy.etag, "bindings": bindings}
        self.call(kind, "set_iam_policy", {"resource": resource, "policy": policy})

    def until_allowed(self, operation: Any) -> str:  # noqa: ANN401 -- zero-argument callable
        """Retry an operation while a fresh grant propagates; return its final outcome."""
        deadline = min(self.deadline, time.monotonic() + GRANT_WAIT_SECONDS)
        while True:
            result = operation()
            if result != "PERMISSION_DENIED" or time.monotonic() >= deadline:
                return result
            time.sleep(10)

    def pull(self, short: str, count: int) -> list:
        """Pull until count messages arrive or 60 seconds pass; acknowledge them."""
        subscription = self.name("subscription", short)
        received, deadline = [], min(self.deadline, time.monotonic() + 60)
        while len(received) < count and time.monotonic() < deadline:
            request = {"subscription": subscription, "max_messages": 10, "return_immediately": True}
            try:
                batch = list(self.call("subscription", "pull", request).received_messages)
            except RPCFailure as error:
                if error.status != "DEADLINE_EXCEEDED":
                    raise
                batch = []
            if batch:
                ids = [m.ack_id for m in batch]
                ack = {"subscription": subscription, "ack_ids": ids}
                self.call("subscription", "acknowledge", ack)
            received.extend(batch)
            time.sleep(0.5 if batch else 2)
        return received

    def cleanup(self) -> None:
        """Delete owned resources in reverse creation order, verifying ownership."""
        try:
            for name in reversed(self.manifest["attempted"]):
                self.delete_owned(name)
            self.manifest.update(cleanup="passed", finished_at=now())
            private_json(self.path, self.manifest)
        finally:
            if self.lock:
                self.lock.close()
                self.lock = None

    def delete_owned(self, name: str) -> None:
        """Delete one owned resource unless it is already gone."""
        kind = name.split("/")[2][:-1]
        if not name.rsplit("/", maxsplit=1)[-1].endswith(self.manifest["run_id"][:12]):
            raise ValueError("Refusing Pub/Sub cleanup: name is outside the manifest")
        field = "name" if kind == "schema" else kind
        try:
            resource = self.client.call("fixture", kind, "get_" + kind, {field: name})
        except RPCFailure as error:
            if error.status == "NOT_FOUND":
                return
            raise
        # Schemas carry no labels; their run-scoped name is the ownership check.
        if kind != "schema" and dict(resource.labels) != self.labels:
            raise ValueError("Refusing Pub/Sub cleanup: ownership does not match the manifest")
        self.client.call("fixture", kind, "delete_" + kind, {field: name})


def definition_tag(kind: str, definition: str) -> str:
    """Name a returned definition: v1, v2, empty, or other."""
    first, second = DEFINITIONS[kind]
    return {first: "v1", second: "v2", "": "empty"}.get(definition, "other")


def schema_ref(fixture: SchemaFixture, name: str) -> str:
    """Normalize a schema name: its project form, owned schema, and revision label."""
    match = re.fullmatch(r"projects/([^/]+)/schemas/([^@]+)(?:@(.+))?", name)
    if not match:
        return name
    project = match[1]
    project = (
        "<project>"
        if project == fixture.project
        else "<project-number>"
        if project.isdigit()
        else "other"
    )
    kinds = {fixture.short(kind): kind for kind in DEFINITIONS}
    reference = f"{project}/<schema:{kinds.get(match[2], 'other')}>"
    return reference + (f"@{fixture.label(match[3])}" if match[3] else "")


def schema_shape(fixture: SchemaFixture, kind: str, schema: Any) -> dict[str, Any]:  # noqa: ANN401 -- SDK Schema
    """Select the public fields of a schema."""
    return {
        "name": schema_ref(fixture, schema.name),
        "type": schema.type_.name,
        "definition": definition_tag(kind, schema.definition),
        "revision": fixture.label(schema.revision_id),
        "revision_id_length": len(schema.revision_id),
        "has_revision_create_time": bool(
            schema.revision_create_time and schema.revision_create_time.timestamp() > 0
        ),
    }


# A field type change in each schema's second definition, and the other type.
INCOMPATIBLE = {
    "avro": AVRO_V1.replace('"count", "type": "int"', '"count", "type": "string"'),
    "proto": PROTO_V1.replace("int32 count", "string count"),
}
OTHER = {"avro": "proto", "proto": "avro"}


def commits(fixture: SchemaFixture, kind: str) -> dict[str, Any]:
    """Commit a new revision, the same revision again, and three refused commits."""
    name = fixture.name("schema", kind)

    def commit(definition: str, schema_type: str = TYPES[kind], schema_name: str = name) -> Any:  # noqa: ANN401 -- SDK Schema
        request = {"name": name, "schema": {"type_": schema_type, "definition": definition}}
        if schema_name:
            request["schema"]["name"] = schema_name
        return fixture.call("schema", "commit_schema", request)

    def refused(*args: str) -> str:
        try:
            commit(*args)
        except RPCFailure as error:
            return error.status
        return "OK"

    second = commit(DEFINITIONS[kind][1])
    other = OTHER[kind]
    return {
        "second": second,
        "committed": schema_shape(fixture, kind, second),
        "repeated_commit": schema_ref(fixture, commit(DEFINITIONS[kind][1]).name),
        "commit_without_schema_name": refused(DEFINITIONS[kind][1], TYPES[kind], ""),
        "commit_incompatible_field_type": refused(INCOMPATIBLE[kind]),
        "commit_other_schema_type": refused(DEFINITIONS[other][0], TYPES[other]),
    }


def revisions(fixture: SchemaFixture, kind: str, snapshot: Snapshot) -> None:
    """Commit, list, get, roll back, and delete revisions of one schema."""
    name = fixture.name("schema", kind)
    first = fixture.call("schema", "get_schema", {"name": name}).revision_id
    observation = commits(fixture, kind)
    second = observation.pop("second")

    def listed(view: str | None) -> list[dict[str, str]]:
        request = {"name": name, **({"view": view} if view else {})}
        page = fixture.call("schema", "list_schema_revisions", request)
        return [
            {"name": schema_ref(fixture, s.name), "definition": definition_tag(kind, s.definition)}
            for s in page.schemas
        ]

    pinned = fixture.call("schema", "get_schema", {"name": f"{name}@{first}", "view": "BASIC"})
    observation.update(
        list_default=listed(None),
        list_full=listed("FULL"),
        get_latest=schema_shape(
            fixture, kind, fixture.call("schema", "get_schema", {"name": name})
        ),
        get_first_basic=schema_shape(fixture, kind, pinned),
    )
    rolled = fixture.call("schema", "rollback_schema", {"name": name, "revision_id": first})
    observation["rolled_back"] = schema_shape(fixture, kind, rolled)
    deleted = fixture.call(
        "schema", "delete_schema_revision", {"name": f"{name}@{second.revision_id}"}
    )
    observation["deleted_revision"] = schema_shape(fixture, kind, deleted)
    observation["get_deleted_revision"] = fixture.outcome(
        "schema", "get_schema", {"name": f"{name}@{second.revision_id}"}
    )
    observation["after_delete"] = listed(None)
    snapshot.match(f"{kind}-revisions", observation)


def listing(fixture: SchemaFixture, snapshot: Snapshot) -> None:
    """List the project's schemas in the default and FULL views."""
    ours = {fixture.name("schema", kind): kind for kind in DEFINITIONS}
    observation = {}
    for view in (None, "BASIC", "FULL"):
        request = {"parent": f"projects/{fixture.project}", **({"view": view} if view else {})}
        page = fixture.call("schema", "list_schemas", request)
        observation[view or "default"] = sorted(
            [
                schema_ref(fixture, s.name),
                s.type_.name,
                definition_tag(ours[s.name], s.definition),
                bool(s.revision_id),
            ]
            for s in page.schemas
            if s.name in ours
        )
    snapshot.match("list-schemas", observation)


def validation(fixture: SchemaFixture, snapshot: Snapshot) -> None:
    """ValidateSchema and ValidateMessage outcomes for both schema types."""
    parent = f"projects/{fixture.project}"

    def schema(kind: str, definition: str) -> str:
        request = {"parent": parent, "schema": {"type_": TYPES[kind], "definition": definition}}
        return fixture.outcome("schema", "validate_schema", request)

    def message(data: bytes, encoding: str | None, **spec: object) -> str:
        request = {"parent": parent, "message": data, **spec}
        if encoding:
            request["encoding"] = encoding
        return fixture.outcome("schema", "validate_message", request)

    proto, avro = fixture.name("schema", "proto"), fixture.name("schema", "avro")
    inline = {"type_": "AVRO", "definition": AVRO_V1}
    snapshot.match(
        "validate-schema",
        {
            "avro_valid": schema("avro", AVRO_V2),
            "avro_unparsable": schema("avro", '{"type": "record"'),
            "proto_valid": schema("proto", PROTO_V2),
            "proto_unparsable": schema("proto", "message {"),
            "proto_two_messages": schema(
                "proto", 'syntax = "proto3";\nmessage A {}\nmessage B {}\n'
            ),
            "proto_import": schema(
                "proto",
                'syntax = "proto3";\nimport "google/protobuf/timestamp.proto";\n'
                "message Event { google.protobuf.Timestamp at = 1; }\n",
            ),
        },
    )
    snapshot.match(
        "validate-message",
        {
            "proto_json_valid": message(b'{"id": "a", "count": 1}', "JSON", name=proto),
            "proto_json_wrong_type": message(b'{"id": 5}', "JSON", name=proto),
            "proto_json_unknown_field": message(b'{"id": "a", "other": 1}', "JSON", name=proto),
            "proto_binary_valid": message(PROTO_BINARY, "BINARY", name=proto),
            "proto_binary_garbage": message(b"\xff\xff", "BINARY", name=proto),
            "avro_named_valid": message(b'{"id": "a", "count": 1}', "JSON", name=avro),
            "avro_inline_valid": message(b'{"id": "a", "count": 1}', "JSON", schema=inline),
            "avro_inline_missing_field": message(b'{"id": "a"}', "JSON", schema=inline),
            "avro_no_encoding": message(b'{"id": "a", "count": 1}', None, name=avro),
        },
    )


def topics(fixture: SchemaFixture, first: dict[str, str], snapshot: Snapshot) -> None:
    """Create topics bound to the schemas and record their settings."""
    observation = {}
    for short, (kind, encoding, pinned) in TOPICS.items():
        settings = {"schema": fixture.name("schema", kind), "encoding": encoding}
        if pinned:
            revision = first[kind]
            settings.update(first_revision_id=revision, last_revision_id=revision)
        created = fixture.create_topic(short, settings).schema_settings
        observation[short] = {
            "schema_matches": created.schema == settings["schema"],
            "encoding": created.encoding.name,
            "first_revision_id": fixture.label(created.first_revision_id),
            "last_revision_id": fixture.label(created.last_revision_id),
        }
    missing = fixture.name("topic", "missing-schema")
    fixture.remember(missing)
    request = {
        "name": missing,
        "labels": fixture.labels,
        "schema_settings": {"schema": fixture.name("schema", "absent"), "encoding": "JSON"},
    }
    observation["missing_schema"] = fixture.outcome("topic", "create_topic", request)
    snapshot.match("topics", observation)


def authorization(fixture: SchemaFixture, snapshot: Snapshot) -> None:
    """Denied and schema-scoped grants, then publish grants on the topics."""
    proto, avro = fixture.name("schema", "proto"), fixture.name("schema", "avro")
    parent = f"projects/{fixture.project}"
    # Remembered so cleanup removes it should the denied create succeed.
    fixture.remember(fixture.name("schema", "denied"))
    denied = {
        "get": fixture.outcome("schema", "get_schema", {"name": proto}, actor="denied"),
        "create": fixture.outcome(
            "schema",
            "create_schema",
            {
                "parent": parent,
                "schema_id": fixture.short("denied"),
                "schema": {"type_": "AVRO", "definition": AVRO_V1},
            },
            actor="denied",
        ),
        "validate_message": fixture.outcome(
            "schema",
            "validate_message",
            {"parent": parent, "name": proto, "message": PROTO_BINARY, "encoding": "BINARY"},
            actor="denied",
        ),
        "publish": fixture.publish("avro-json", b'{"id": "a", "count": 1}', actor="denied"),
    }
    fixture.grant("schema", proto, "roles/pubsub.viewer")
    for short in TOPICS:
        fixture.grant("topic", fixture.name("topic", short), "roles/pubsub.publisher")
    get = fixture.until_allowed(
        lambda: fixture.outcome("schema", "get_schema", {"name": proto}, actor="allowed")
    )
    schema = {"name": proto, "type_": "PROTOCOL_BUFFER", "definition": PROTO_V2}
    commit = {"name": proto, "schema": schema}
    allowed = {
        "get_granted_schema": get,
        "get_other_schema": fixture.outcome(
            "schema", "get_schema", {"name": avro}, actor="allowed"
        ),
        "commit_with_viewer": fixture.outcome("schema", "commit_schema", commit, actor="allowed"),
    }
    snapshot.match("schema-authorization", {"denied": denied, "allowed": allowed})


VALID_JSON = b'{"id": "a", "count": 1}'
# Publishes as (observation, topic, payload). The pinned topics accept only the
# first revision, which has no note field.
PUBLISHES = (
    ("avro_valid", "avro-json", VALID_JSON),
    ("avro_newer_revision_field", "avro-json", b'{"id": "a", "count": 1, "note": {"string": "x"}}'),
    ("avro_missing_field", "avro-json", b'{"id": "a"}'),
    ("proto_json_valid", "proto-json", VALID_JSON),
    ("proto_json_newer_revision_field", "proto-json", b'{"id": "a", "count": 1, "note": "x"}'),
    ("proto_json_wrong_type", "proto-json", b'{"id": 5}'),
    ("proto_binary_valid", "proto-binary", PROTO_BINARY),
    ("proto_binary_garbage", "proto-binary", b"\xff\xff"),
)


def publishing(fixture: SchemaFixture, snapshot: Snapshot) -> None:
    """Publish valid and invalid messages, then read the delivered attributes."""
    outcomes, expected = {}, dict.fromkeys(TOPICS, 0)
    for key, short, payload in PUBLISHES:
        outcomes[key] = fixture.until_allowed(lambda s=short, d=payload: fixture.publish(s, d))
        expected[short] += outcomes[key] == "OK"
    snapshot.match("publish", outcomes)
    schemas = {fixture.name("schema", kind): f"<schema:{kind}>" for kind in DEFINITIONS}
    delivered = {}
    for short, count in expected.items():
        messages = fixture.pull(short, count) if count else []
        delivered[short] = sorted(
            (
                {
                    "data_length": len(m.message.data),
                    "schemaname": schemas.get(
                        m.message.attributes.get("googclient_schemaname"), "other"
                    ),
                    "schemaencoding": m.message.attributes.get("googclient_schemaencoding", ""),
                    "schemarevisionid": fixture.label(
                        m.message.attributes.get("googclient_schemarevisionid", "")
                    ),
                    "attribute_names": sorted(m.message.attributes),
                }
                for m in messages
            ),
            key=json.dumps,
        )
    snapshot.match("delivered", delivered)


def until_refused(fixture: SchemaFixture, short: str) -> str:
    """Publish until the deletion reaches the publish path; return the final outcome."""
    deadline = min(fixture.deadline, time.monotonic() + DELETION_WAIT_SECONDS)
    while True:
        result = fixture.publish(short, b'{"id": "a", "count": 1}')
        if result != "OK" or time.monotonic() >= deadline:
            return result
        time.sleep(10)


def deleted_schemas(fixture: SchemaFixture, snapshot: Snapshot) -> None:
    """Delete each schema while a topic still names it."""
    observation = {}
    for kind, short in (("proto", "proto-json"), ("avro", "avro-json")):
        fixture.call("schema", "delete_schema", {"name": fixture.name("schema", kind)})
        topic = fixture.call("topic", "get_topic", {"topic": fixture.name("topic", short)})
        settings = topic.schema_settings
        observation[kind] = {
            "schema": settings.schema,
            "encoding": settings.encoding.name,
            "first_revision_id": settings.first_revision_id,
            "last_revision_id": settings.last_revision_id,
            "publish": until_refused(fixture, short),
        }
    snapshot.match("deleted-schemas", observation)


def run_schema_scenario(fixture: SchemaFixture, snapshot: Snapshot) -> None:
    """Run the same observations in local, recording, and revalidation modes."""
    fixture.lock_project()
    created = {kind: fixture.create_schema(kind) for kind in DEFINITIONS}
    snapshot.match(
        "created-schemas", {kind: schema_shape(fixture, kind, s) for kind, s in created.items()}
    )
    first = {kind: s.revision_id for kind, s in created.items()}
    for kind in DEFINITIONS:
        revisions(fixture, kind, snapshot)
    listing(fixture, snapshot)
    validation(fixture, snapshot)
    topics(fixture, first, snapshot)
    authorization(fixture, snapshot)
    publishing(fixture, snapshot)
    deleted_schemas(fixture, snapshot)
