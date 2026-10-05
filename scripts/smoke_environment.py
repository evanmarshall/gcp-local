#!/usr/bin/env python3
"""Exercise packaged startup, standard local ADC, isolated reset, and teardown."""

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit

import google.auth
from google.auth.transport.requests import AuthorizedSession, Request
from google.api_core.exceptions import NotFound
from google.cloud import storage
import requests

ROOT = Path(__file__).resolve().parents[1]


def cli(directory, action, *options, check=True):
    result = subprocess.run([sys.executable, str(ROOT / 'scripts/local_env.py'), '--directory', str(directory),
                             *options, action], capture_output=True, text=True, timeout=600)
    if check and result.returncode:
        raise AssertionError(result.stdout + result.stderr)
    return result


def unused_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def client_for(directory):
    env = json.loads(cli(directory, 'env').stdout)
    endpoint = env['GCP_LOCAL_ENDPOINT']

    def scoped(url):
        parsed = urlsplit(url)
        expected = urlsplit(endpoint)
        if (parsed.scheme, parsed.netloc) != (expected.scheme, expected.netloc):
            raise ValueError('The SDK attempted to leave the selected local environment')

    class LocalSession(requests.Session):
        def __init__(self):
            super().__init__()
            self.trust_env = False

        def request(self, method, url, **kwargs):
            scoped(url)
            kwargs.update(allow_redirects=False, timeout=5)
            return super().request(method, url, **kwargs)

    transport_session = LocalSession()
    transport = Request(session=transport_session)
    credentials, _ = google.auth.load_credentials_from_file(env['GOOGLE_APPLICATION_CREDENTIALS'],
        scopes=['https://www.googleapis.com/auth/cloud-platform'], request=transport)

    class API(AuthorizedSession):
        def request(self, method, url, **kwargs):
            scoped(url)
            kwargs.update(allow_redirects=False, timeout=5)
            return super().request(method, url, **kwargs)

    session = API(credentials, auth_request=transport)
    session.trust_env = False
    client = storage.Client(project=env['GOOGLE_CLOUD_PROJECT'], credentials=credentials, _http=session,
                            client_options={'api_endpoint': endpoint}, use_auth_w_custom_endpoint=True)
    return client, session, transport_session


def create(client):
    bucket = client.bucket('packaged-smoke-bucket')
    bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    bucket.iam_configuration.public_access_prevention = 'enforced'
    bucket.soft_delete_policy.retention_duration_seconds = 0
    return client.create_bucket(bucket, location='us-central1', retry=None, timeout=5)


def main():
    probe_image = (ROOT / 'Dockerfile').read_text().splitlines()[0].split()[1]
    subprocess.run(['docker', 'pull', probe_image], check=True, stdout=subprocess.DEVNULL, timeout=120)
    with tempfile.TemporaryDirectory(prefix='gcp-local-package-') as temporary:
        directories = [Path(temporary) / name for name in ('first', 'second', 'failed')]
        resources = []
        try:
            for directory in directories[:2]:
                cli(directory, 'up', '--port', str(unused_port()))
                status = json.loads(cli(directory, 'status').stdout)
                assert status['ready'] and status['providers'] == ['storage'] and status['backends'] == []
                # This is a private, synthetic file only. Source credentials are never copied.
                assert (directory / 'identity/adc.json').stat().st_mode & 0o077 == 0
                client, session, transport = client_for(directory)
                resources.extend((client, session, transport))
                bucket = create(client)
                blob = bucket.blob('sample.txt')
                blob.upload_from_string(b'packaged local storage', if_generation_match=0, retry=None, timeout=5)
                assert blob.download_as_bytes(retry=None, timeout=5) == b'packaged local storage'
                configuration = json.loads((directory / 'environment.json').read_text())
                network = subprocess.check_output(['docker', 'network', 'inspect', configuration['compose_project'] + '_backend'], text=True)
                assert json.loads(network)[0]['Internal'] is True
                probe = '''import errno,socket
for family,address in ((socket.AF_INET,('192.0.2.1',443)),(socket.AF_INET6,('2001:db8::1',443))):
 with socket.socket(family,socket.SOCK_STREAM) as connection:
  connection.settimeout(2)
  assert connection.connect_ex(address) in {errno.ENETUNREACH,errno.EHOSTUNREACH}
'''
                subprocess.run(['docker', 'run', '--rm', '--pull=never', '--cap-drop=ALL',
                                '--network', configuration['compose_project'] + '_backend',
                                probe_image, 'python3', '-c', probe], check=True, timeout=10)
                container = configuration['compose_project'] + '-gcp-local-1'
                state = json.loads(subprocess.check_output(['docker', 'inspect', container], text=True))[0]
                assert state['HostConfig']['ReadonlyRootfs'] and state['Config']['User'] == f'{os.getuid()}:{os.getgid()}'
                assert len(state['NetworkSettings']['Networks']) == 1
                entrypoint = json.loads(subprocess.check_output(['docker', 'inspect', configuration['compose_project'] + '-entrypoint-1'], text=True))[0]
                assert entrypoint['NetworkSettings']['Ports']['4566/tcp'][0]['HostIp'] == '127.0.0.1'
            first, second = directories[:2]
            cli(first, 'reset')
            fresh, session, transport = client_for(first)
            resources.extend((fresh, session, transport))
            assert list(fresh.list_buckets(retry=None, timeout=5)) == []
            try:
                fresh.get_bucket('packaged-smoke-bucket', retry=None, timeout=5)
                raise AssertionError('Reset retained the first environment bucket')
            except NotFound:
                pass
            other, session, transport = client_for(second)
            resources.extend((other, session, transport))
            assert other.bucket('packaged-smoke-bucket').blob('sample.txt').download_as_bytes(retry=None, timeout=5) == b'packaged local storage'
            create(fresh).delete(retry=None, timeout=5)
            example = subprocess.run([sys.executable, str(ROOT / 'examples/storage.py'), '--directory', str(first)],
                                     capture_output=True, text=True, timeout=15)
            assert example.returncode == 0, example.stderr
            assert 'Hello from local GCP.' in example.stdout
            assert list(fresh.list_buckets(retry=None, timeout=5)) == []
            # Bind a port to require a visible startup failure without affecting the working environment.
            with socket.socket() as occupied:
                occupied.bind(('127.0.0.1', 0))
                occupied.listen()
                failed = cli(directories[2], 'up', '--port', str(occupied.getsockname()[1]), check=False)
                assert failed.returncode != 0
            # Exercise the actual portable doctor's supported and unsupported service decisions.
            app = Path(temporary) / 'application'
            app.mkdir()
            (app / 'requirements.txt').write_text('google-cloud-storage==3.10.1\n')
            endpoint = json.loads(cli(first, 'env').stdout)['GCP_LOCAL_ENDPOINT']
            doctor = [sys.executable, str(ROOT / 'skills/gcp-local-setup/scripts/inspect_repo.py'),
                      '--repo', str(app), '--doctor', '--endpoint', endpoint]
            assert subprocess.run(doctor, capture_output=True, timeout=10).returncode == 0
            (app / 'requirements.txt').write_text('google-cloud-storage==3.10.1\ngoogle-cloud-pubsub\n')
            result = subprocess.run(doctor, capture_output=True, text=True, timeout=10)
            assert result.returncode == 2
            assert json.loads(result.stdout)['profiles']['local']['missing_services'] == ['pubsub']
        finally:
            for resource in resources:
                resource.close()
            failures = []
            for directory in directories:
                if cli(directory, 'down', check=False).returncode:
                    failures.append(str(directory))
                if (directory / 'environment.json').exists():
                    project = json.loads((directory / 'environment.json').read_text())['compose_project']
                    remaining = subprocess.check_output(['docker', 'ps', '-aq', '--filter', 'label=com.docker.compose.project=' + project], text=True)
                    assert remaining.strip() == '', 'Packaged teardown left an owned container'
                    networks = subprocess.check_output(['docker', 'network', 'ls', '-q', '--filter', 'label=com.docker.compose.project=' + project], text=True)
                    assert networks.strip() == '', 'Packaged teardown left an owned network'
            assert not failures, 'Teardown failed for: ' + ', '.join(failures)
    print('Packaged workflow passed: local ADC/Storage, isolated reset, failed startup, portable doctor, and teardown.')


if __name__ == '__main__':
    main()
