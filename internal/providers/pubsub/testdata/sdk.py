"""Actual Python Pub/Sub client with production local ADC, in loopback isolation."""

from concurrent.futures import CancelledError
import importlib.metadata
import ipaddress
import json
import os
import socket
import sys
import threading

import google.auth
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
from google.api_core.exceptions import PermissionDenied, Unauthenticated
from google.cloud import pubsub_v1
from google.pubsub_v1.services.publisher.transports.grpc import PublisherGrpcTransport
from google.pubsub_v1.services.subscriber.transports.grpc import SubscriberGrpcTransport
import grpc
import requests


class LocalRequest(Request):
    def __call__(self, url, *args, **kwargs):
        from urllib.parse import urlsplit
        parsed = urlsplit(url)
        if parsed.scheme != 'http' or not ipaddress.ip_address(parsed.hostname).is_loopback:
            raise AssertionError('SDK tried a nonlocal credential request')
        kwargs['timeout'] = 5
        kwargs['allow_redirects'] = False
        return super().__call__(url, *args, **kwargs)


def main():
    assert os.environ.get('GCP_LOCAL_ISOLATED') == '1'
    assert {name for _, name in socket.if_nameindex()} == {'lo'}
    assert 'PUBSUB_EMULATOR_HOST' not in os.environ
    endpoint = sys.argv[1]
    assert ipaddress.ip_address(endpoint.rsplit(':', 1)[0]).is_loopback
    session = requests.Session()
    session.trust_env = False
    request = LocalRequest(session=session)
    scopes = ['https://www.googleapis.com/auth/cloud-platform']
    credentials, project = google.auth.default(scopes=scopes, request=request)
    assert project == 'project-a'
    channel = secure_authorized_channel(credentials, request, endpoint, ssl_credentials=grpc.local_channel_credentials())
    pub = pubsub_v1.PublisherClient(transport=PublisherGrpcTransport(channel=channel))
    sub = pubsub_v1.SubscriberClient(transport=SubscriberGrpcTransport(channel=channel))
    topic = 'projects/project-a/topics/python-sdk'
    subscription = 'projects/project-a/subscriptions/python-sdk'
    made_topic = made_sub = False
    stream = None
    try:
        pub.create_topic(request={'name': topic}, retry=None, timeout=5)
        made_topic = True
        sub.create_subscription(request={'name': subscription, 'topic': topic, 'ack_deadline_seconds': 10}, retry=None, timeout=5)
        made_sub = True
        denied, _ = google.auth.load_credentials_from_file(os.environ['GCP_LOCAL_DENIED_ADC'], scopes=scopes, request=request)
        denied_channel = secure_authorized_channel(denied, request, endpoint, ssl_credentials=grpc.local_channel_credentials())
        try:
            denied_pub = pubsub_v1.PublisherClient(transport=PublisherGrpcTransport(channel=denied_channel))
            try:
                denied_pub.publish(topic, b'denied').result(timeout=5)
                raise AssertionError('denied publisher succeeded')
            except PermissionDenied:
                pass
            finally:
                denied_pub.stop()
            denied_sub = pubsub_v1.SubscriberClient(transport=SubscriberGrpcTransport(channel=denied_channel))
            try:
                denied_sub.pull(request={'subscription': subscription, 'max_messages': 1}, retry=None, timeout=5)
                raise AssertionError('denied subscriber succeeded')
            except PermissionDenied:
                pass
        finally:
            denied_channel.close()
        # Anonymous emulator mode is explicitly unsuitable for IAM testing.
        with grpc.insecure_channel(endpoint) as anonymous_channel:
            anonymous = pubsub_v1.PublisherClient(transport=PublisherGrpcTransport(channel=anonymous_channel))
            try:
                anonymous.get_topic(request={'topic': topic}, retry=None, timeout=5)
                raise AssertionError('anonymous client succeeded')
            except Unauthenticated:
                pass
        message_id = pub.publish(topic, b'python-pull', source='sdk').result(timeout=5)
        pulled = sub.pull(request={'subscription': subscription, 'max_messages': 1}, retry=None, timeout=5)
        assert len(pulled.received_messages) == 1
        message = pulled.received_messages[0]
        assert message.message.message_id == message_id
        assert message.message.data == b'python-pull'
        assert message.message.attributes == {'source': 'sdk'}
        sub.acknowledge(request={'subscription': subscription, 'ack_ids': [message.ack_id]}, retry=None, timeout=5)

        arrived = threading.Event()
        messages = []
        def receive(message):
            messages.append((message.message_id, message.data))
            message.ack()
            arrived.set()
        stream = sub.subscribe(subscription, callback=receive,
                               flow_control=pubsub_v1.types.FlowControl(max_messages=10, max_bytes=10240),
                               await_callbacks_on_shutdown=True)
        message_id = pub.publish(topic, b'python-stream').result(timeout=5)
        assert arrived.wait(10), 'SDK stream did not deliver'
        assert messages == [(message_id, b'python-stream')]
        stream.cancel()
        try:
            stream.result(timeout=5)
        except CancelledError:
            pass
        stream = None
        # Refresh credentials rather than replacing them with fixed bearer text.
        credentials.token = None
        credentials.expiry = None
        assert pub.get_topic(request={'topic': topic}, retry=None, timeout=5).name == topic
    finally:
        if stream is not None:
            stream.cancel()
            try:
                stream.result(timeout=5)
            except CancelledError:
                pass
        if made_sub:
            sub.delete_subscription(request={'subscription': subscription}, retry=None, timeout=5)
        if made_topic:
            pub.delete_topic(request={'topic': topic}, retry=None, timeout=5)
        pub.stop()
        sub.close()
        channel.close()
        session.close()
    print(json.dumps({'kind': 'local-sdk', 'cloud_fidelity': 'unverified', 'gcp_execution': False,
                      'adc_refresh': True, 'publish_pull_ack': True, 'streaming_cancel': True,
                      'denials': True, 'cleanup': True,
                      'versions': {name: importlib.metadata.version(name) for name in ('google-cloud-pubsub', 'grpcio', 'google-auth')}}))


if __name__ == '__main__':
    main()
