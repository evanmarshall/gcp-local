"""Bounded Firestore document recording in a run-owned collection of the default database."""

import argparse
import sys
import time
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.errors import detailed
from gcp_validation.evidence import Inconclusive, SnapshotCase, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = 'firestore'
LIMITS = {'max_documents': 2, 'max_collections': 1, 'max_ttl_fields': 1,
          'max_run_seconds': 900}
LIVE_ENDPOINT = 'https://firestore.googleapis.com'
FIELDS = {
    'title': {'stringValue': 'gcp-local conformance'},
    'count': {'integerValue': '42'},
    'ratio': {'doubleValue': 0.5},
    'enabled': {'booleanValue': True},
    'missing': {'nullValue': None},
    'when': {'timestampValue': '2026-01-02T03:04:05.123456Z'},
    'tags': {'arrayValue': {'values': [{'stringValue': 'a'}, {'integerValue': '7'}]}},
    'nested': {'mapValue': {'fields': {'inner': {'stringValue': 'x'}}}},
}


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the separately approved live scope for this scenario."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class DocumentFixture(OwnedRun):
    """Documents live only in the run's own collection; cleanup deletes only that collection."""

    def __init__(self, client: RestClient, state_dir: Path, *, manifest: str | None = None) -> None:
        """Bind the run manifest, the collection, and its `when` field."""
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('collection') == 'gl-' + v['run_id'])
        self.manifest.setdefault('collection', 'gl-' + self.manifest['run_id'])
        self.collection = self.manifest['collection']
        database = f'/v1/projects/{self.project}/databases/(default)'
        self.base = f'{database}/documents/{self.collection}'
        self.manifest.setdefault('ttl', False)
        self.field = f'{database}/collectionGroups/{self.collection}/fields/when'

    def patch_ttl(self, body: dict, *, actor: str = 'fixture') -> Response:
        """Change the collection group's TTL field as Terraform does: updateMask=ttlConfig."""
        return self.client.request(actor, 'PATCH', self.field, body=body,
                                   query={'updateMask': 'ttlConfig'}, timeout=self.remaining())

    def finish(self, response: Response) -> Response:
        """Poll a field operation to completion; return the final operation."""
        operation = response.json()
        while response.status == 200 and not operation.get('done'):
            time.sleep(5 if self.client.sandbox else 0.2)
            response = self.client.request('fixture', 'GET', '/v1/' + operation['name'],
                                           timeout=self.remaining())
            operation = response.json()
        return response

    def call(self, method: str, suffix: str = '', *, actor: str = 'fixture',
             body: dict | None = None, query: dict | None = None) -> Response:
        """Send one request to the run's collection or a document in it."""
        return self.client.request(actor, method, self.base + suffix, body=body, query=query,
                                   timeout=self.remaining())

    def create(self, document_id: str = 'doc1') -> Response:
        """Create one document with every value type."""
        return self.call('POST', query={'documentId': document_id}, body={'fields': FIELDS})

    def setup(self) -> Response:
        """Create the first document after writing the manifest."""
        self.acquire()
        self.client.verify_project()
        listing = self.call('GET', query={'pageSize': 1})
        if listing.status != 200:
            raise Inconclusive(f'Collection listing returned HTTP {listing.status}')
        if listing.json().get('documents'):
            raise Inconclusive('The run collection unexpectedly holds documents')
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.create()

    def cleanup(self) -> None:
        """Delete the run collection's documents and turn its TTL policy off."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            self.empty_collection()
            if self.manifest['ttl']:
                disabled = self.finish(self.patch_ttl({}))
                if disabled.status != 200 or disabled.json().get('error'):
                    raise ValueError(f'Owned TTL field reset returned HTTP {disabled.status}')
                self.manifest['ttl'] = False
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()

    def empty_collection(self) -> None:
        """Delete every document of the run collection, and only those."""
        query = {'pageSize': LIMITS['max_documents'] + 1}
        listing = self.client.request('fixture', 'GET', self.base, query=query)
        if listing.status != 200:
            raise ValueError(f'Owned collection listing returned HTTP {listing.status}')
        for document in listing.json().get('documents', []):
            name = document['name']
            if f'/documents/{self.collection}/' not in name:
                raise ValueError('Refusing Firestore cleanup outside the run collection')
            path = self.base + '/' + name.rsplit('/', 1)[1]
            deleted = self.client.request('fixture', 'DELETE', path)
            if deleted.status != 200:
                raise ValueError(f'Owned document delete returned HTTP {deleted.status}')
        if self.client.request('fixture', 'GET', self.base).json().get('documents'):
            raise ValueError('Owned documents remain after deletion')


def document(observe: Observations, response: Response) -> dict:
    """Select a document's name and field values."""
    value = response.json()
    selected = observe.select(response, ('name',))
    selected['field_values'] = observe.value(value.get('fields'))
    return selected


def run_scenario(fixture: DocumentFixture, snapshot: SnapshotCase, created: Response) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.collection: '<collection>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>'})
    snapshot.match('created', document(observe, created))
    snapshot.match('read', document(observe, fixture.call('GET', '/doc1')))
    snapshot.match('denied-read', detailed(observe, fixture.call('GET', '/doc1', actor='denied')))
    snapshot.match('denied-write', observe.error(fixture.call(
        'PATCH', '/doc1', actor='denied', body={'fields': {'title': {'stringValue': 'no'}}})))
    masked = fixture.call('PATCH', '/doc1', query={'updateMask.fieldPaths': 'title'}, body={
        'fields': {'title': {'stringValue': 'renamed'}, 'count': {'integerValue': '1'}}})
    snapshot.match('masked-patch', document(observe, masked))
    replaced = fixture.call('PATCH', '/doc1', body={'fields': {'only': {'stringValue': 'left'}}})
    snapshot.match('unmasked-patch', document(observe, replaced))
    snapshot.match('duplicate-create', observe.error(fixture.create()))
    second = fixture.create('doc2')
    snapshot.match('second-created', {'http_status': second.status})
    listed = fixture.call('GET')
    snapshot.match('listed', {
        'http_status': listed.status, 'fields': sorted(listed.json()),
        'names': sorted(observe.text(item['name']) for item in listed.json().get('documents', []))})
    snapshot.match('missing-read', observe.error(fixture.call('GET', '/absent')))
    deleted = fixture.call('DELETE', '/doc1')
    snapshot.match('deleted', {'http_status': deleted.status, 'body': deleted.json()})
    again = fixture.call('DELETE', '/doc1')
    snapshot.match('delete-missing', {'http_status': again.status, 'body': again.json()})
    snapshot.match('absent', observe.error(fixture.call('GET', '/doc1')))
    observe_fields(fixture, snapshot, observe)


def field_operation(response: Response) -> dict:
    """Describe an operation's shape without its generated ID or timestamps."""
    value = response.json()
    metadata = value.get('metadata', {})
    return {'http_status': response.status, 'fields': sorted(value), 'done': value.get('done'),
            'database_operation': '/databases/(default)/operations/' in value.get('name', ''),
            'metadata_type': metadata.get('@type'), 'metadata_fields': sorted(metadata),
            'response_type': value.get('response', {}).get('@type'),
            'error': value.get('error', {}).get('code')}


def field_view(observe: Observations, response: Response) -> dict:
    """Select a field's name and its TTL and index configuration."""
    value = response.json()
    return {**observe.select(response, ('name', 'indexConfig')),
            'ttl_fields': sorted(value.get('ttlConfig', {}))}


def observe_fields(fixture: DocumentFixture, snapshot: SnapshotCase,
                   observe: Observations) -> None:
    """Enable then disable TTL on the run collection group's `when` field.

    This is what Terraform's google_firestore_field with ttl_config {} does; the
    operations and field reads are observed.
    """
    def read() -> Response:
        return fixture.client.request('fixture', 'GET', fixture.field, timeout=fixture.remaining())

    snapshot.match('field-default', field_view(observe, read()))
    denied = fixture.patch_ttl({'ttlConfig': {}}, actor='denied')
    snapshot.match('ttl-denied', detailed(observe, denied))
    unknown = fixture.patch_ttl({'ttlConfig': {}, 'unknownField': 1})
    snapshot.match('ttl-unknown-field', detailed(observe, unknown))
    fixture.manifest['ttl'] = True
    fixture.save()  # The change may land even if the response is lost.
    enabled = fixture.patch_ttl({'ttlConfig': {}})
    snapshot.match('ttl-enable-operation', field_operation(enabled))
    snapshot.match('ttl-enable-done', field_operation(fixture.finish(enabled)))
    snapshot.match('ttl-field', field_view(observe, read()))
    disabled = fixture.patch_ttl({})
    snapshot.match('ttl-disable-operation', field_operation(disabled))
    finished = fixture.finish(disabled)
    snapshot.match('ttl-disable-done', field_operation(finished))
    if finished.status == 200 and not finished.json().get('error'):
        fixture.manifest['ttl'] = False
        fixture.save()
    snapshot.match('field-after-disable', field_view(observe, read()))


def main(argv: list[str] | None = None) -> None:
    """Delete only the run collection after an interrupted run."""
    parser = argparse.ArgumentParser(
        description='Delete only the run collection after an interrupted run')
    parser.add_argument('command', choices=['cleanup'])
    parser.add_argument('--profile', required=True)
    parser.add_argument('--scope', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--allow-live', action='store_true', required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox, 'cleanup')
    target = Target.select('record', project=sandbox.project,
                           credentials_file=sandbox.identities['allowed'].credentials_file,
                           live_scope=sandbox.live_scope, allow_live=args.allow_live)
    client = RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    DocumentFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                    manifest=args.manifest).cleanup()
    sys.stdout.write('Owned Firestore cleanup passed.\n')


if __name__ == '__main__':
    main()
