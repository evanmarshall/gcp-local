"""Python Pub/Sub SchemaServiceClient and typed topics through gcp-local with local ADC.

Covers Avro and Protocol Buffer schemas: create, commit, revisions, rollback,
ValidateMessage, a topic bound to each type, the publisher client's handling
of a rejected message, and the schema attributes on delivered messages.
"""

import ipaddress
import json
import os
import socket
import sys

import google.auth
import grpc
import requests
from google.api_core.exceptions import InvalidArgument, NotFound
from google.auth.transport.grpc import secure_authorized_channel
from google.cloud import pubsub_v1
from google.pubsub_v1 import Encoding, Schema, SchemaServiceClient, SchemaView
from google.pubsub_v1.services.publisher.transports.grpc import PublisherGrpcTransport
from google.pubsub_v1.services.schema_service.transports.grpc import SchemaServiceGrpcTransport
from google.pubsub_v1.services.subscriber.transports.grpc import SubscriberGrpcTransport
from sdk import LocalRequest

PROJECT = "projects/project-a"
AVRO = json.dumps(
    {
        "type": "record",
        "name": "Event",
        "fields": [{"name": "id", "type": "string"}, {"name": "count", "type": "int"}],
    }
)
PROTO = 'syntax = "proto3";\nmessage Event {\n  string id = 1;\n  int32 count = 2;\n}\n'
PROTO_V2 = PROTO.replace("}\n", "  string note = 3;\n}\n")


def main():
    assert os.environ.get("GCP_LOCAL_ISOLATED") == "1"
    assert {name for _, name in socket.if_nameindex()} == {"lo"}
    assert "PUBSUB_EMULATOR_HOST" not in os.environ
    endpoint = sys.argv[1]
    assert ipaddress.ip_address(endpoint.rsplit(":", 1)[0]).is_loopback
    session = requests.Session()
    session.trust_env = False
    request = LocalRequest(session=session)
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    credentials, project = google.auth.default(scopes=scopes, request=request)
    assert project == "project-a"
    channel = secure_authorized_channel(
        credentials, request, endpoint, ssl_credentials=grpc.local_channel_credentials()
    )
    schemas = SchemaServiceClient(transport=SchemaServiceGrpcTransport(channel=channel))
    pub = pubsub_v1.PublisherClient(transport=PublisherGrpcTransport(channel=channel))
    sub = pubsub_v1.SubscriberClient(transport=SubscriberGrpcTransport(channel=channel))
    made = []
    try:
        run(schemas, pub, sub, made)
    finally:
        for kind, name in reversed(made):
            try:
                if kind == "schema":
                    schemas.delete_schema(name=name, retry=None, timeout=5)
                elif kind == "topic":
                    pub.delete_topic(topic=name, retry=None, timeout=5)
                else:
                    sub.delete_subscription(subscription=name, retry=None, timeout=5)
            except NotFound:
                pass
        channel.close()
        session.close()
    print(json.dumps({"kind": "python-sdk-schemas", "gcp_execution": False}))


def run(schemas, pub, sub, made):
    proto = schemas.create_schema(
        parent=PROJECT,
        schema=Schema(type_=Schema.Type.PROTOCOL_BUFFER, definition=PROTO),
        schema_id="sdk-proto",
        retry=None,
        timeout=5,
    )
    made.append(("schema", proto.name))
    avro = schemas.create_schema(
        parent=PROJECT,
        schema=Schema(type_=Schema.Type.AVRO, definition=AVRO),
        schema_id="sdk-avro",
        retry=None,
        timeout=5,
    )
    made.append(("schema", avro.name))
    first = proto.revision_id
    committed = schemas.commit_schema(
        name=proto.name,
        schema=Schema(name=proto.name, type_=Schema.Type.PROTOCOL_BUFFER, definition=PROTO_V2),
        retry=None,
        timeout=5,
    )
    revisions = list(
        schemas.list_schema_revisions(
            request={"name": proto.name, "view": SchemaView.FULL}, retry=None, timeout=5
        )
    )
    # Newest first, named SCHEMA@REVISION, as GCP lists them.
    assert [r.name for r in revisions] == [committed.name, f"{proto.name}@{first}"], revisions
    rolled = schemas.rollback_schema(name=proto.name, revision_id=first, retry=None, timeout=5)
    assert rolled.definition == PROTO and rolled.revision_id not in (first, committed.revision_id)
    listed = {s.name: s for s in schemas.list_schemas(parent=PROJECT, retry=None, timeout=5)}
    assert set(listed) == {proto.name, avro.name} and not listed[proto.name].definition
    schemas.validate_message(
        request={
            "parent": PROJECT,
            "name": proto.name,
            "message": b'{"id": "a"}',
            "encoding": Encoding.JSON,
        },
        retry=None,
        timeout=5,
    )
    try:
        schemas.validate_message(
            request={
                "parent": PROJECT,
                "name": avro.name,
                "message": b'{"id": "a"}',
                "encoding": Encoding.JSON,
            },
            retry=None,
            timeout=5,
        )
        raise AssertionError("Avro accepted a message without a required field")
    except InvalidArgument:
        pass
    for kind, schema, encoding in (
        ("proto", proto, Encoding.BINARY),
        ("avro", avro, Encoding.JSON),
    ):
        typed_topic(pub, sub, made, kind, schema, encoding)


def typed_topic(pub, sub, made, kind, schema, encoding):
    """Publish a valid and an invalid message to a topic bound to one schema."""
    topic = f"{PROJECT}/topics/sdk-{kind}"
    pub.create_topic(
        request={"name": topic, "schema_settings": {"schema": schema.name, "encoding": encoding}},
        retry=None,
        timeout=5,
    )
    made.append(("topic", topic))
    created = pub.get_topic(topic=topic, retry=None, timeout=5)
    assert (
        created.schema_settings.schema == schema.name
        and created.schema_settings.encoding == encoding
    )
    subscription = f"{PROJECT}/subscriptions/sdk-{kind}"
    sub.create_subscription(request={"name": subscription, "topic": topic}, retry=None, timeout=5)
    made.append(("subscription", subscription))
    valid = b"\x0a\x01a\x10\x01" if encoding == Encoding.BINARY else b'{"id": "a", "count": 1}'
    pub.publish(topic, valid).result(timeout=10)
    try:
        pub.publish(topic, b"\xff\xff").result(timeout=10)
        raise AssertionError("A nonconforming message was published")
    except InvalidArgument:
        pass
    response = sub.pull(
        request={"subscription": subscription, "max_messages": 10}, retry=None, timeout=10
    )
    assert len(response.received_messages) == 1, response
    attributes = response.received_messages[0].message.attributes
    assert attributes["googclient_schemaname"] == schema.name, attributes
    assert attributes["googclient_schemaencoding"] == Encoding(encoding).name, attributes
    sub.acknowledge(
        request={"subscription": subscription, "ack_ids": [response.received_messages[0].ack_id]},
        retry=None,
        timeout=5,
    )


if __name__ == "__main__":
    main()
