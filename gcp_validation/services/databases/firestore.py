"""Bounded Firestore document recording in a run-owned collection of the default database."""

import argparse
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = 'firestore'
LIMITS = {'max_documents': 2, 'max_collections': 1, 'max_run_seconds': 600}
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


def load_scope(filename, sandbox, mode: str):
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class DocumentFixture(OwnedRun):
    """Documents live only in the run's own collection; cleanup deletes only that collection."""

    def __init__(self, client, state_dir, *, manifest=None):
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest,
                         check=lambda v: v.get('collection') == 'gl-' + v['run_id'])
        self.manifest.setdefault('collection', 'gl-' + self.manifest['run_id'])
        self.collection = self.manifest['collection']
        self.base = f'/v1/projects/{self.project}/databases/(default)/documents/{self.collection}'

    def call(self, method, suffix='', *, actor='fixture', body=None, query=None):
        return self.client.request(actor, method, self.base + suffix, body=body, query=query,
                                   timeout=self.remaining())

    def create(self, document_id='doc1'):
        return self.call('POST', query={'documentId': document_id}, body={'fields': FIELDS})

    def setup(self):
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

    def cleanup(self):
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            listing = self.client.request('fixture', 'GET', self.base, query={'pageSize': LIMITS['max_documents'] + 1})
            if listing.status != 200:
                raise ValueError(f'Owned collection listing returned HTTP {listing.status}')
            for document in listing.json().get('documents', []):
                name = document['name']
                if f'/documents/{self.collection}/' not in name:
                    raise ValueError('Refusing Firestore cleanup outside the run collection')
                deleted = self.client.request('fixture', 'DELETE', self.base + '/' + name.rsplit('/', 1)[1])
                if deleted.status != 200:
                    raise ValueError(f'Owned document delete returned HTTP {deleted.status}')
            if self.client.request('fixture', 'GET', self.base).json().get('documents'):
                raise ValueError('Owned documents remain after deletion')
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()


def document(observe, response):
    value = response.json()
    selected = observe.select(response, ('name',))
    selected['field_values'] = observe.value(value.get('fields'))
    return selected


def run_scenario(fixture, snapshot, created):
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations({fixture.collection: '<collection>', fixture.manifest['run_id']: '<run>',
                            fixture.project: '<project>', client.number: '<project-number>'})
    snapshot.match('created', document(observe, created))
    snapshot.match('read', document(observe, fixture.call('GET', '/doc1')))
    snapshot.match('denied-read', observe.error(fixture.call('GET', '/doc1', actor='denied')))
    snapshot.match('denied-write', observe.error(fixture.call(
        'PATCH', '/doc1', actor='denied', body={'fields': {'title': {'stringValue': 'no'}}})))
    masked = fixture.call('PATCH', '/doc1', query={'updateMask.fieldPaths': 'title'},
                          body={'fields': {'title': {'stringValue': 'renamed'}, 'count': {'integerValue': '1'}}})
    snapshot.match('masked-patch', document(observe, masked))
    replaced = fixture.call('PATCH', '/doc1', body={'fields': {'only': {'stringValue': 'left'}}})
    snapshot.match('unmasked-patch', document(observe, replaced))
    snapshot.match('duplicate-create', observe.error(fixture.create()))
    second = fixture.create('doc2')
    snapshot.match('second-created', {'http_status': second.status})
    listed = fixture.call('GET')
    snapshot.match('listed', {'http_status': listed.status, 'fields': sorted(listed.json()),
                              'names': sorted(observe.text(item['name']) for item in listed.json().get('documents', []))})
    snapshot.match('missing-read', observe.error(fixture.call('GET', '/absent')))
    deleted = fixture.call('DELETE', '/doc1')
    snapshot.match('deleted', {'http_status': deleted.status, 'body': deleted.json()})
    again = fixture.call('DELETE', '/doc1')
    snapshot.match('delete-missing', {'http_status': again.status, 'body': again.json()})
    snapshot.match('absent', observe.error(fixture.call('GET', '/doc1')))


def main(argv=None):
    parser = argparse.ArgumentParser(description='Delete only the run collection after an interrupted run')
    parser.add_argument('command', choices=['cleanup'])
    parser.add_argument('--profile', required=True)
    parser.add_argument('--scope', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--allow-live', action='store_true', required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox, 'cleanup')
    target = Target.select('record', project=sandbox.project, credentials_file=sandbox.identities['allowed'].credentials_file,
                           live_scope=sandbox.live_scope, allow_live=args.allow_live)
    client = RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    DocumentFixture(client, Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE,
                    manifest=args.manifest).cleanup()
    print('Owned Firestore cleanup passed.')


if __name__ == '__main__':
    main()
