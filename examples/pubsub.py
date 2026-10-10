"""Run after local_env.py up; publishes and pulls one message through the local gRPC gateway."""

import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import google.auth
import grpc
import requests
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
from google.cloud import pubsub_v1
from google.pubsub_v1.services.publisher.transports.grpc import PublisherGrpcTransport
from google.pubsub_v1.services.subscriber.transports.grpc import SubscriberGrpcTransport


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path.cwd() / ".gcp-local/environment")
    args = parser.parse_args()
    directory = args.directory.expanduser().resolve()
    settings = json.loads((directory / "environment.json").read_text())
    endpoint = f"http://127.0.0.1:{settings['port']}"
    grpc_endpoint = f"127.0.0.1:{settings['grpc_port']}"
    adc = directory / "identity/adc.json"
    profile = json.loads(adc.read_text())
    if (
        settings.get("owner") != "gcp-local"
        or settings.get("directory") != str(directory)
        or profile.get("type") != "external_account"
        or profile.get("audience") != "//gcp-local/identity"
        or profile.get("token_url") != endpoint + "/v1/token"
        or profile.get("credential_source") != {"file": str(directory / "identity/subject.txt")}
        or profile.get("service_account_impersonation_url")
    ):
        raise ValueError("Select a generated local environment, not a real GCP credential profile")

    # The runtime has no real-cloud credentials. This example also confines its
    # token refresh to the selected local endpoint.
    class LocalSession(requests.Session):
        def __init__(self):
            super().__init__()
            self.trust_env = False

        def request(self, method, url, **kwargs):
            if urlsplit(url).scheme != "http" or urlsplit(url).netloc != urlsplit(endpoint).netloc:
                raise ValueError("Request left the local environment")
            kwargs.update(allow_redirects=False, timeout=5)
            return super().request(method, url, **kwargs)

    request = Request(session=LocalSession())
    credentials, _ = google.auth.load_credentials_from_file(
        str(adc), scopes=["https://www.googleapis.com/auth/cloud-platform"], request=request
    )
    channel = secure_authorized_channel(
        credentials, request, grpc_endpoint, ssl_credentials=grpc.local_channel_credentials()
    )
    publisher = pubsub_v1.PublisherClient(transport=PublisherGrpcTransport(channel=channel))
    subscriber = pubsub_v1.SubscriberClient(transport=SubscriberGrpcTransport(channel=channel))

    project = settings["project"]
    name = "gcp-local-example-" + uuid4().hex[:16]
    topic = f"projects/{project}/topics/{name}"
    subscription = f"projects/{project}/subscriptions/{name}"
    try:
        publisher.create_topic(request={"name": topic}, retry=None, timeout=10)
        subscriber.create_subscription(
            request={"name": subscription, "topic": topic, "ack_deadline_seconds": 10},
            retry=None,
            timeout=10,
        )
        publisher.publish(topic, b"Hello from local Pub/Sub.", source="gcp-local-example").result(
            timeout=10
        )
        received = subscriber.pull(
            request={"subscription": subscription, "max_messages": 1}, retry=None, timeout=10
        ).received_messages
        if len(received) != 1 or received[0].message.data != b"Hello from local Pub/Sub.":
            raise AssertionError("The local subscription did not return the published message")
        subscriber.acknowledge(
            request={"subscription": subscription, "ack_ids": [received[0].ack_id]},
            retry=None,
            timeout=10,
        )
        print("Hello from local Pub/Sub.")
    finally:
        try:
            subscriber.delete_subscription(subscription=subscription, retry=None, timeout=10)
        except Exception:
            pass
        try:
            publisher.delete_topic(topic=topic, retry=None, timeout=10)
        except Exception:
            pass
        channel.close()


if __name__ == "__main__":
    main()
