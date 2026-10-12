"""Bounded Cloud KMS symmetric-key recording. Keys cannot be deleted in GCP, so each
run creates one labeled key in a shared persistent ring and schedules every version
for destruction."""

import argparse
import base64
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = 'cloudkms'
LIMITS = {'max_key_rings': 1, 'max_crypto_keys': 1, 'max_key_versions': 2, 'max_run_seconds': 600}
LIVE_ENDPOINT = 'https://cloudkms.googleapis.com'
LOCATION = 'us-central1'
RING = 'gcp-local-conformance'
PLAINTEXT = b'gcp-local synthetic plaintext'
AAD = b'gcp-local-aad'


def load_scope(filename, sandbox):
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox)


def b64(data):
    return base64.b64encode(data).decode()


class KeyFixture(OwnedRun):
    """One labeled ENCRYPT_DECRYPT key per run; cleanup destroys its versions."""

    def __init__(self, client, state_dir, *, manifest=None):
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('key') == 'gl-' + v['run_id'][:12])
        self.manifest.setdefault('key', 'gl-' + self.manifest['run_id'][:12])
        self.key = self.manifest['key']
        self.ring_path = f'/v1/projects/{self.project}/locations/{LOCATION}/keyRings/{RING}'
        self.labels = {'managed-by': 'gcp-local', 'gcp-local-run': self.manifest['run_id']}

    def call(self, method, suffix='', *, actor='fixture', body=None, query=None):
        return self.client.request(actor, method, f'{self.ring_path}/cryptoKeys/{self.key}{suffix}',
                                   body=body, query=query, timeout=self.remaining())

    def encrypt(self, actor, aad=AAD):
        return self.call('POST', ':encrypt', actor=actor, body={
            'plaintext': b64(PLAINTEXT), 'additionalAuthenticatedData': b64(aad)})

    def decrypt(self, actor, ciphertext, aad=AAD):
        return self.call('POST', ':decrypt', actor=actor, body={
            'ciphertext': ciphertext, 'additionalAuthenticatedData': b64(aad)})

    def set_state(self, version, state):
        return self.call('PATCH', f'/cryptoKeyVersions/{version}', query={'updateMask': 'state'},
                         body={'state': state})

    def setup(self):
        self.acquire()
        self.client.verify_project()
        ring = self.client.request('fixture', 'GET', self.ring_path)
        if ring.status == 404:
            created = self.client.request('fixture', 'POST', self.ring_path.rsplit('/', 1)[0],
                                          query={'keyRingId': RING}, body={})
            if created.status not in {200, 409}:
                raise ValueError(f'Shared key ring create returned HTTP {created.status}')
        elif ring.status != 200:
            raise ValueError(f'Shared key ring read returned HTTP {ring.status}')
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.client.request('fixture', 'POST', f'{self.ring_path}/cryptoKeys', timeout=self.remaining(),
                                   query={'cryptoKeyId': self.key},
                                   body={'purpose': 'ENCRYPT_DECRYPT', 'labels': self.labels})

    def cleanup(self):
        """Keys persist in GCP; destroy every version that is not already destroyed."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            key = self.client.request('fixture', 'GET', f'{self.ring_path}/cryptoKeys/{self.key}')
            if key.status == 200:
                if key.json().get('labels') != self.labels:
                    raise ValueError('Refusing KMS cleanup: key ownership does not match the manifest')
                versions = self.client.request('fixture', 'GET', f'{self.ring_path}/cryptoKeys/{self.key}/cryptoKeyVersions')
                for version in versions.json().get('cryptoKeyVersions', []):
                    if version['state'] in {'ENABLED', 'DISABLED'}:
                        number = version['name'].rsplit('/', 1)[1]
                        destroyed = self.client.request(
                            'fixture', 'POST', f'{self.ring_path}/cryptoKeys/{self.key}/cryptoKeyVersions/{number}:destroy', body={})
                        if destroyed.status != 200:
                            raise ValueError(f'Version destroy returned HTTP {destroyed.status}')
                remaining = self.client.request('fixture', 'GET', f'{self.ring_path}/cryptoKeys/{self.key}/cryptoKeyVersions')
                if any(v['state'] in {'ENABLED', 'DISABLED'} for v in remaining.json().get('cryptoKeyVersions', [])):
                    raise ValueError('Owned key versions remain usable after cleanup')
            elif key.status != 404:
                raise ValueError(f'Owned key read returned HTTP {key.status}')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()


def version_of(name):
    return name.rsplit('/', 1)[1] if name else None


def run_scenario(fixture, snapshot, created):
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.key: '<key>', fixture.manifest['run_id']: '<run>', RING: '<ring>',
                            fixture.project: '<project>', client.number: '<project-number>',
                            **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    key = created.json()
    snapshot.match('key-created', {**observe.select(created, ('name', 'purpose', 'labels')),
                                   'primary_fields': sorted(key.get('primary', {})),
                                   'primary_state': key.get('primary', {}).get('state'),
                                   'primary_version': version_of(key.get('primary', {}).get('name'))})
    snapshot.match('denied-encrypt', observe.error(fixture.encrypt('denied')))
    snapshot.match('admin-cannot-encrypt', observe.error(fixture.encrypt('fixture')))
    policy = fixture.call('GET', ':getIamPolicy')
    snapshot.match('policy-read', observe.select(policy, ('bindings',)))
    granted = fixture.call('POST', ':setIamPolicy', body={'policy': {'etag': policy.json().get('etag', ''), 'bindings': [
        {'role': 'roles/cloudkms.cryptoKeyEncrypterDecrypter', 'members': ['serviceAccount:' + client.emails['allowed']]}]}})
    snapshot.match('encrypter-granted', observe.select(granted, ('bindings',)))
    first = fixture.wait_until(lambda: fixture.encrypt('allowed'), {403}, 180)
    sealed = first.json()
    snapshot.match('encrypted', {'http_status': first.status, 'fields': sorted(sealed),
                                 'version': version_of(sealed.get('name')),
                                 'protectionLevel': sealed.get('protectionLevel'),
                                 'verifiedPlaintextCrc32c': sealed.get('verifiedPlaintextCrc32c'),
                                 'verifiedAdditionalAuthenticatedDataCrc32c': sealed.get('verifiedAdditionalAuthenticatedDataCrc32c')})
    opened = fixture.decrypt('allowed', sealed['ciphertext'])
    snapshot.match('decrypted', {'http_status': opened.status, 'fields': sorted(opened.json()),
                                 'matches': base64.b64decode(opened.json().get('plaintext', '')) == PLAINTEXT,
                                 'usedPrimary': opened.json().get('usedPrimary'),
                                 'protectionLevel': opened.json().get('protectionLevel')})
    snapshot.match('wrong-aad', observe.error(fixture.decrypt('allowed', sealed['ciphertext'], b'other')))
    added = fixture.call('POST', '/cryptoKeyVersions', body={})
    snapshot.match('version-added', {**observe.select(added, ('state', 'protectionLevel', 'algorithm')),
                                     'version': version_of(added.json().get('name'))})
    promoted = fixture.call('POST', ':updatePrimaryVersion', body={'cryptoKeyVersionId': '2'})
    snapshot.match('primary-updated', {'http_status': promoted.status,
                                       'primary_version': version_of(promoted.json().get('primary', {}).get('name'))})
    second = fixture.encrypt('allowed')
    snapshot.match('encrypted-v2', {'http_status': second.status, 'version': version_of(second.json().get('name'))})
    old = fixture.decrypt('allowed', sealed['ciphertext'])
    snapshot.match('old-ciphertext-decrypts', {'http_status': old.status, 'usedPrimary': old.json().get('usedPrimary')})
    disabled = fixture.set_state('1', 'DISABLED')
    snapshot.match('version-disabled', observe.select(disabled, ('state',)))
    snapshot.match('disabled-decrypt', observe.error(fixture.decrypt('allowed', sealed['ciphertext'])))
    destroyed = fixture.call('POST', '/cryptoKeyVersions/1:destroy', body={})
    snapshot.match('version-destroyed', observe.select(destroyed, ('state',)))
    listed = fixture.call('GET', '/cryptoKeyVersions')
    snapshot.match('versions-listed', {'http_status': listed.status, 'states': sorted(
        item['state'] for item in listed.json().get('cryptoKeyVersions', []))})


def main(argv=None):
    parser = argparse.ArgumentParser(description='Destroy the owned key versions after an interrupted run')
    parser.add_argument('command', choices=['cleanup'])
    parser.add_argument('--profile', required=True)
    parser.add_argument('--scope', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--allow-live', action='store_true', required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox)
    target = Target.select('record', project=sandbox.project, credentials_file=sandbox.identities['allowed'].credentials_file,
                           live_scope=sandbox.live_scope, allow_live=args.allow_live)
    client = RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    KeyFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
               manifest=args.manifest).cleanup()
    print('Owned KMS cleanup passed.')


if __name__ == '__main__':
    main()
