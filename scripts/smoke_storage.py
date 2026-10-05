#!/usr/bin/env python3
"""Use standard local ADC and the Python SDK against the production binary."""

import json
import os
from pathlib import Path
import queue
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
from unittest.mock import patch
from urllib.parse import urlsplit

import google.auth
from google.auth.transport.requests import AuthorizedSession, Request
from google.api_core.exceptions import Forbidden, PreconditionFailed
from google.cloud import storage
import requests


def private_json(path, value):
    path.write_text(json.dumps(value))
    path.chmod(0o600)


def main():
    assert sys.platform == 'linux' and os.environ.get('GCP_LOCAL_ISOLATED') == '1'
    assert {name for _, name in socket.if_nameindex()} == {'lo'}
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix='gcp-local-storage-') as temporary:
        directory = Path(temporary)
        principals = [{'subject': f'serviceAccount:{actor}@synthetic.iam.gserviceaccount.com',
                       'project': 'synthetic', 'subject_token': secrets.token_urlsafe(40)}
                      for actor in ('fixture', 'allowed', 'denied')]
        private_json(directory / 'identity.json', {'principals': principals})
        project = '//cloudresourcemanager.googleapis.com/projects/synthetic'
        private_json(directory / 'iam.json', {
            'resources': [{'name': project, 'service': 'resourcemanager', 'kind': 'project',
                           'number': '123456', 'scope': {'project': 'synthetic', 'location': 'global'}}],
            'policies': {project: {'bindings': [{'role': 'roles/storage.admin', 'members': [principals[0]['subject']]}]}}})
        process = subprocess.Popen([str(root / 'bin/gcp-local'), '-services', 'storage',
                                    '-identity-config', str(directory / 'identity.json'),
                                    '-iam-config', str(directory / 'iam.json'),
                                    '-listen', '127.0.0.1:0', '-grpc-listen', '127.0.0.1:0'],
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        lines, events = [], queue.Queue()

        def logs():
            for line in process.stdout:
                lines.append(line)
                events.put(line)

        reader = threading.Thread(target=logs, daemon=True)
        reader.start()
        clients, sessions = [], []
        try:
            started = json.loads(events.get(timeout=10))
            assert started['msg'] == 'gcp-local listening'
            endpoint = 'http://' + started['address']
            origin = urlsplit(endpoint).netloc

            class LocalSession(requests.Session):
                def __init__(self):
                    super().__init__()
                    self.trust_env = False

                def request(self, method, url, **kwargs):
                    parsed = urlsplit(url)
                    assert parsed.scheme == 'http' and parsed.netloc == origin
                    kwargs['allow_redirects'] = False
                    kwargs.setdefault('timeout', 5)
                    return super().request(method, url, **kwargs)

            transport_session = LocalSession()
            sessions.append(transport_session)
            transport = Request(session=transport_session)
            for principal in principals:
                actor = principal['subject'].split(':')[1].split('@')[0]
                subject = directory / (actor + '.txt')
                subject.write_text(principal['subject_token'])
                subject.chmod(0o600)
                adc = directory / (actor + '.json')
                private_json(adc, {'type': 'external_account', 'audience': '//gcp-local/identity',
                                  'subject_token_type': 'urn:ietf:params:oauth:token-type:access_token',
                                  'token_url': endpoint + '/v1/token', 'credential_source': {'file': str(subject)}})
                with patch.dict(os.environ, {'GOOGLE_APPLICATION_CREDENTIALS': str(adc),
                                             'GOOGLE_CLOUD_PROJECT': 'synthetic'}):
                    credentials, selected = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'], request=transport)
                assert selected == 'synthetic'
                session = AuthorizedSession(credentials, auth_request=transport)
                session.trust_env = False
                sessions.append(session)
                clients.append(storage.Client(project=selected, credentials=credentials, _http=session,
                                              client_options={'api_endpoint': endpoint}))
            fixture, allowed, denied = clients
            bucket = fixture.bucket('sdk-authenticated-bucket')
            bucket.iam_configuration.uniform_bucket_level_access_enabled = True
            bucket.iam_configuration.public_access_prevention = 'enforced'
            bucket.soft_delete_policy.retention_duration_seconds = 0
            bucket = fixture.create_bucket(bucket, location='us-central1', retry=None)
            policy = bucket.get_iam_policy(requested_policy_version=1)
            policy.bindings.append({'role': 'roles/storage.objectUser', 'members': {principals[1]['subject']}})
            bucket.set_iam_policy(policy)
            blob = allowed.bucket(bucket.name).blob('nested/name %?.txt')
            blob.upload_from_string(b'original', if_generation_match=0, retry=None)
            original_generation = blob.generation
            assert blob.download_as_bytes() == b'original'
            try:
                denied.bucket(bucket.name).blob(blob.name).upload_from_string(b'denied', retry=None)
                raise AssertionError('Denied upload succeeded')
            except Forbidden:
                pass
            blob.upload_from_string(b'replacement', if_generation_match=original_generation, retry=None)
            assert blob.generation != original_generation
            try:
                blob.upload_from_string(b'stale', if_generation_match=original_generation, retry=None)
                raise AssertionError('Stale generation succeeded')
            except PreconditionFailed:
                pass
            policy = bucket.get_iam_policy(requested_policy_version=1)
            policy.bindings.clear()
            bucket.set_iam_policy(policy)
            try:
                blob.delete(retry=None)
                raise AssertionError('Revoked deletion succeeded')
            except Forbidden:
                pass
            admin_blob = bucket.blob(blob.name)
            assert admin_blob.download_as_bytes() == b'replacement'
            admin_blob.delete(retry=None)
            bucket.delete(retry=None)
            assert list(fixture.list_buckets()) == []
            assert transport_session.get(endpoint + '/storage/v1/b?project=synthetic',
                                         headers={'Authorization': 'Bearer gcp-local-test:fixture'}).status_code == 401
            assert all(client._credentials.token.startswith('gl.') for client in clients)
            process.terminate()
            assert process.wait(timeout=12) == 0
            reader.join(timeout=2)
            text = ''.join(lines)
            assert all(principal['subject_token'] not in text for principal in principals)
            assert all(client._credentials.token not in text for client in clients)
        finally:
            for client in clients:
                client.close()
            for session in sessions:
                session.close()
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            reader.join(timeout=2)
            process.stdout.close()
    print('Production binary: local ADC, SDK bucket/object CRUD, replacement, revocation, and cleanup passed.')


if __name__ == '__main__':
    main()
