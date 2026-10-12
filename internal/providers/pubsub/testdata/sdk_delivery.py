"""Python Pub/Sub client delivery features through gcp-local with local ADC.

Covers ordered publishing (including resume_publish after a failed key),
ordered redelivery after a nack, dead-lettering gated by the Pub/Sub service
agent's grants, filters, and exactly-once acknowledgement results.
"""

import ipaddress
import itertools
import json
import os
import socket
import sys
import threading
import time
from concurrent.futures import CancelledError

import google.auth
import grpc
import requests
from google.api_core.exceptions import InvalidArgument, NotFound
from google.auth.transport.grpc import secure_authorized_channel
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.publisher.exceptions import PublishToPausedOrderingKeyException
from google.cloud.pubsub_v1.subscriber import exceptions as sub_exceptions
from google.pubsub_v1.services.publisher.transports.grpc import PublisherGrpcTransport
from google.pubsub_v1.services.subscriber.transports.grpc import SubscriberGrpcTransport
from sdk import LocalRequest

PROJECT = "projects/project-a"


def name(kind, short):
    return f"{PROJECT}/{kind}/sdk-{short}"


class Session:
    """Clients over one authorized loopback channel, plus owned cleanup."""

    def __init__(self, endpoint):
        self.http = requests.Session()
        self.http.trust_env = False
        self.request = LocalRequest(session=self.http)
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        credentials, project = google.auth.default(scopes=scopes, request=self.request)
        assert project == "project-a"
        self.channel = secure_authorized_channel(
            credentials, self.request, endpoint, ssl_credentials=grpc.local_channel_credentials()
        )
        transport = PublisherGrpcTransport(channel=self.channel)
        self.pub = pubsub_v1.PublisherClient(transport=transport)
        self.ordered_pub = pubsub_v1.PublisherClient(
            publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True),
            transport=PublisherGrpcTransport(channel=self.channel),
        )
        self.sub = pubsub_v1.SubscriberClient(
            transport=SubscriberGrpcTransport(channel=self.channel)
        )
        self.topics, self.subscriptions = [], []

    def topic(self, short):
        topic = name("topics", short)
        self.pub.create_topic(request={"name": topic}, retry=None, timeout=5)
        self.topics.append(topic)
        return topic

    def subscription(self, short, topic, **fields):
        subscription = name("subscriptions", short)
        self.sub.create_subscription(
            request={"name": subscription, "topic": topic, **fields}, retry=None, timeout=5
        )
        self.subscriptions.append(subscription)
        return subscription

    def pull(self, subscription, wait=5.0):
        deadline = time.monotonic() + wait
        while True:
            response = self.sub.pull(
                request={
                    "subscription": subscription,
                    "max_messages": 10,
                    "return_immediately": True,
                },
                retry=None,
                timeout=5,
            )
            if response.received_messages or time.monotonic() >= deadline:
                return list(response.received_messages)
            time.sleep(0.1)

    def stream(self, subscription, callback, until, wait):
        """Run a streaming subscriber until the predicate holds or wait elapses."""
        future = self.sub.subscribe(
            subscription, callback=callback, await_callbacks_on_shutdown=True
        )
        deadline = time.monotonic() + wait
        try:
            while not until() and time.monotonic() < deadline:
                time.sleep(0.1)
        finally:
            future.cancel()
            try:
                future.result(timeout=5)
            except CancelledError:
                pass
        return until()

    def close(self):
        for subscription in self.subscriptions:
            self.sub.delete_subscription(
                request={"subscription": subscription}, retry=None, timeout=5
            )
        for topic in self.topics:
            self.pub.delete_topic(request={"topic": topic}, retry=None, timeout=5)
        self.pub.stop()
        self.ordered_pub.stop()
        self.sub.close()
        self.channel.close()
        self.http.close()


def ordering(s):
    """Ordered publish, resume_publish after a failed key, and ordered redelivery."""
    missing = name("topics", "not-yet")
    try:
        s.ordered_pub.publish(missing, b"lost", ordering_key="k").result(timeout=10)
        raise AssertionError("publish to a missing topic succeeded")
    except NotFound:
        pass
    try:
        s.ordered_pub.publish(missing, b"paused", ordering_key="k").result(timeout=10)
        raise AssertionError("a failed ordering key was not paused")
    except PublishToPausedOrderingKeyException:
        pass
    topic = s.topic("not-yet")
    assert topic == missing
    ordered = s.subscription(
        "ordered", topic, enable_message_ordering=True, ack_deadline_seconds=10
    )
    s.ordered_pub.resume_publish(topic, "k")
    for value in "12345":
        s.ordered_pub.publish(topic, value.encode(), ordering_key="k")
    s.ordered_pub.publish(topic, b"x", ordering_key="other").result(timeout=10)

    seen, acked, lock = [], [], threading.Lock()
    nacked = threading.Event()

    def receive(message):
        value = message.data.decode()
        with lock:
            seen.append(value)
        if value == "1" and not nacked.is_set():
            nacked.set()
            message.nack()
            return
        with lock:
            acked.append(value)
        message.ack()

    def done():
        with lock:
            keyed = [value for value in acked if value != "x"]
            return nacked.is_set() and keyed[-5:] == list("12345")

    assert s.stream(ordered, receive, done, 20), f"ordered deliveries {seen}"
    keyed = [value for value in seen if value != "x"]
    for before, after in itertools.pairwise(keyed):
        assert after == "1" or int(after) == int(before) + 1, f"key order broken: {keyed}"
    return {"ordered_deliveries": keyed, "resume_publish": True}


def dead_lettering(s, project_number):
    """No forwarding without the service agent's grants; forwarding after them."""
    agent = f"serviceAccount:service-{project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
    source, dead = s.topic("source"), s.topic("dead-letters")
    collected = s.subscription("collected", dead)
    subscription = s.subscription(
        "dead-lettered",
        source,
        ack_deadline_seconds=10,
        dead_letter_policy={"dead_letter_topic": dead, "max_delivery_attempts": 5},
    )
    s.pub.publish(source, b"poison").result(timeout=10)
    attempts, lock = [], threading.Lock()

    def reject(message):
        with lock:
            attempts.append(message.delivery_attempt)
        message.nack()

    def seen(count):
        def check():
            with lock:
                return len(attempts) >= count

        return check

    assert s.stream(subscription, reject, seen(7), 20), f"ungranted attempts {attempts}"
    assert set(attempts) == {None}, f"attempts counted without grants: {attempts}"
    assert not s.pull(collected, wait=0.5), "dead-lettered without the service agent's grants"

    for kind, resource, role in (
        ("topic", dead, "roles/pubsub.publisher"),
        ("subscription", subscription, "roles/pubsub.subscriber"),
    ):
        client = s.pub if kind == "topic" else s.sub
        policy = client.get_iam_policy(request={"resource": resource}, retry=None, timeout=5)
        policy.bindings.add(role=role, members=[agent])
        client.set_iam_policy(
            request={"resource": resource, "policy": policy}, retry=None, timeout=5
        )
    with lock:
        attempts.clear()
    forwarded = []

    def arrived():
        if not forwarded:
            forwarded.extend(s.pull(collected, wait=0))
        return bool(forwarded)

    assert s.stream(subscription, reject, arrived, 20), (
        f"no dead-letter forwarding, attempts {attempts}"
    )
    message = forwarded[0].message
    assert message.data == b"poison"
    assert message.attributes["CloudPubSubDeadLetterSourceDeliveryCount"] == "5"
    counted = [value for value in attempts if value is not None]
    assert counted and counted[0] == 1 and max(counted) == 5, f"granted attempts {attempts}"
    s.sub.acknowledge(
        request={"subscription": collected, "ack_ids": [forwarded[0].ack_id]}, retry=None, timeout=5
    )
    return {"ungranted_attempts": 7, "granted_attempts": counted}


def filtering(s):
    topic = s.topic("filtered")
    filtered = s.subscription("filtered", topic, filter='attributes.kind = "match"')
    try:
        s.subscription("bad-filter", topic, filter="attributes.kind = =")
        raise AssertionError("an unparsable filter was accepted")
    except InvalidArgument:
        pass
    s.pub.publish(topic, b"match", kind="match").result(timeout=10)
    s.pub.publish(topic, b"other", kind="other").result(timeout=10)
    received = s.pull(filtered)
    assert [m.message.data for m in received] == [b"match"], received
    s.sub.acknowledge(
        request={"subscription": filtered, "ack_ids": [m.ack_id for m in received]},
        retry=None,
        timeout=5,
    )
    return {"filtered": ["match"]}


def exactly_once(s):
    topic = s.topic("exact")
    exact = s.subscription(
        "exact", topic, enable_exactly_once_delivery=True, ack_deadline_seconds=10
    )
    s.pub.publish(topic, b"once").result(timeout=10)
    results, lock = [], threading.Lock()

    def receive(message):
        first = message.ack_with_response().result(timeout=10)
        try:
            message.ack_with_response().result(timeout=10)
            second = "SUCCESS"
        except sub_exceptions.AcknowledgeError as error:
            second = error.error_code.name
        with lock:
            results.append((first.name, second))

    def done():
        with lock:
            return bool(results)

    assert s.stream(exact, receive, done, 20), "exactly-once message not delivered"
    assert results == [("SUCCESS", "INVALID_ACK_ID")], results
    assert not s.pull(exact, wait=1), "exactly-once message was redelivered after a successful ack"
    return {"exactly_once": results[0]}


def main():
    assert os.environ.get("GCP_LOCAL_ISOLATED") == "1"
    assert {name for _, name in socket.if_nameindex()} == {"lo"}
    assert "PUBSUB_EMULATOR_HOST" not in os.environ
    endpoint, project_number = sys.argv[1], sys.argv[2]
    assert ipaddress.ip_address(endpoint.rsplit(":", 1)[0]).is_loopback
    s = Session(endpoint)
    try:
        report = {
            "kind": "local-sdk-delivery",
            "cloud_fidelity": "unverified",
            "gcp_execution": False,
        }
        report.update(ordering(s))
        report.update(dead_lettering(s, project_number))
        report.update(filtering(s))
        report.update(exactly_once(s))
    finally:
        s.close()
    print(json.dumps(report))


if __name__ == "__main__":
    main()
