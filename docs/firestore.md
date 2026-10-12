# Firestore (local slice)

Firestore v1 documents, subcollections, queries, aggregations, transactions, batch writes, and listeners work over REST and gRPC in the `(default)` database, with IAM checked on every request. Only document create/read/update/delete is recorded against GCP; queries, transactions, batch writes, subcollections, and listeners are modeled locally, not recorded.

## Served methods

| Method | REST | gRPC | Permissions |
| --- | --- | --- | --- |
| get | `GET documents/{path}` (even segment count) | `GetDocument` | `datastore.entities.get` |
| list | `GET documents/{path}` (odd segment count) | `ListDocuments` | `datastore.entities.get`, `datastore.entities.list` |
| createDocument | `POST documents/{collection path}` | `CreateDocument` | `datastore.entities.create` |
| patch | `PATCH documents/{path}` | `UpdateDocument` | `datastore.entities.update` |
| delete | `DELETE documents/{path}` | `DeleteDocument` | `datastore.entities.delete` |
| batchGet | `POST documents:batchGet` | `BatchGetDocuments` | `datastore.entities.get` |
| beginTransaction, rollback | `POST documents:beginTransaction`, `:rollback` | `BeginTransaction`, `Rollback` | `datastore.databases.get` |
| commit, batchWrite | `POST documents:commit`, `:batchWrite` | `Commit`, `BatchWrite` | per write, below |
| runQuery, runAggregationQuery | `POST documents[/{doc}]:runQuery`, `:runAggregationQuery` | `RunQuery`, `RunAggregationQuery` | `datastore.entities.get`, `datastore.entities.list` |
| listCollectionIds | `POST documents[/{doc}]:listCollectionIds` | `ListCollectionIds` | `datastore.entities.list` |
| listen | not routed | `Listen` | `datastore.entities.get`, `datastore.entities.list` |
| fields.get, fields.patch | `GET`/`PATCH collectionGroups/{c}/fields/{f}` | not routed | `datastore.schemas.get`, `datastore.schemas.update` |

`{path}` spans any depth, so subcollection documents such as `cities/sf/landmarks/ggb` use the same routes. Permissions follow Google's [required permissions table](https://docs.cloud.google.com/firestore/native/docs/security/iam) (read 2026-10-11) and authorize against the project's `(default)` database. Commit and BatchWrite need the permission for each write kind they carry:

| Write | Permissions |
| --- | --- |
| update or transform with `exists: false` | `datastore.entities.create` |
| update or transform with `exists: true` or an `updateTime` precondition | `datastore.entities.update` |
| update or transform with no precondition (`set()`) | `datastore.entities.create`, `datastore.entities.update` |
| delete | `datastore.entities.delete` |

A read that starts a transaction (`newTransaction`) also needs `datastore.databases.get`. Local choices where Google's table has no row: Listen uses the query permissions; a commit or batch write with no writes needs `datastore.databases.get`; BatchWrite deletes use the Commit delete row. These plans are not recorded against GCP.

Every document name in a request (batch get, writes, listen targets) must be in the request project's `(default)` database; other names return `INVALID_ARGUMENT`, and a named database returns `UNIMPLEMENTED`. Streaming RPCs authorize before the backend stream opens. Listen also authorizes each later request and each response, so revoking a grant ends delivery.

## Python client

The official `google-cloud-firestore` client (pinned 2.34.1 in `requirements-firestore.txt`) works over the gRPC gateway with local ADC. Keep `FIRESTORE_EMULATOR_HOST` unset: that mode sends the emulator's anonymous `owner` token and bypasses IAM. The client has no public channel argument, so attach an authenticated loopback channel through its private `_firestore_api_internal` attribute:

```python
import google.auth
import grpc
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
from google.cloud import firestore
from google.cloud.firestore_v1.services.firestore import FirestoreClient
from google.cloud.firestore_v1.services.firestore.transports.grpc import FirestoreGrpcTransport

request = Request()
credentials, project = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"], request=request)
channel = secure_authorized_channel(credentials, request, "127.0.0.1:4567", ssl_credentials=grpc.local_channel_credentials())
db = firestore.Client(project=project, credentials=credentials)
db._firestore_api_internal = FirestoreClient(transport=FirestoreGrpcTransport(channel=channel))
```

The [SDK test](../internal/providers/firestore/testdata/sdk.py) uses this setup for filtered, ordered, and limited queries, `count()`, `get_all`, a `@transactional` read-modify-write, a rolled-back transaction, `batch()`, `bulk_writer()`, subcollections, `collections()`, collection-group queries, `on_snapshot`, `recursive_delete`, and denial of another project's principal.

## Backend: the official emulator

Documents are served by Google's official Cloud Firestore emulator, pinned in [toolchains/firestore.json](../toolchains/firestore.json) (version **1.19.9**, the `cloud-firestore-emulator` component of Cloud SDK 529.0.0). `python3 scripts/install_firestore.py` downloads the component from `dl.google.com` without gcloud, verifies the archive size/SHA-256 and the JAR SHA-256, and installs only `.tools/firestore/emulator.jar`. The provider re-hashes the JAR before every start.

Version choice: the current release, 1.22.0 (SDK 588.0.0), and every release since 1.20.0 (SDK 530.0.0) ship Java 21 class files (class-file major version 65), so they cannot run on the pinned Java 17 runtime. Google's versioned SDK manifests (`components-v<sdk>.json`) list 1.19.9 for SDK 500.0.0 through 529.0.0. Its class files are Java 11 (major 55). Moving to a newer emulator requires changing the Java 21 runtime used by Moo, CI, and the packaged image.

The provider remains the authentication and authorization gate:

1. Validate every resource name in the request (REST custom-method bodies are decoded strictly into the Firestore protobuf request first), then authorize each permission plan against the `(default)` database resource with the IAM engine. A denied or invalid request returns before the backend manager is involved, so it never starts or reaches the emulator.
2. Run unary requests inside `Manager.Use` and streaming requests (`BatchGetDocuments`, `RunQuery`, `RunAggregationQuery`, `Listen`) inside `Manager.Session` for the project scope, so reset cancels open streams. Each resource project gets its own lazily started JVM on a random loopback port (`--single_project_mode_error` rejects any other project ID). The child gets a private temporary directory, a minimal environment (`PATH`, `TMPDIR`, `HOME`), a 256 MiB heap, and no caller credentials, proxy settings, or ambient ADC.
3. Forward REST requests to the emulator's v1 REST endpoint and gRPC requests to its gRPC service on the same port. REST paths are rebuilt from validated route parameters; document methods forward only documented query parameters (`documentId`, `mask.fieldPaths`, `updateMask.fieldPaths`, `currentDocument.*`, `pageSize`, `pageToken`, `orderBy`, `showMissing`, `readTime`). Both transports send the fixed `Bearer owner` credential, the emulator's documented administrative bypass of security rules; gRPC also sends the validated database as `google-cloud-resource-prefix`, which the emulator's Listen requires. The IAM decision has already been made, and the caller's token and metadata are never forwarded. Emulator statuses and bodies return unchanged. A non-JSON REST reply becomes an explicit `UNIMPLEMENTED` coverage gap.

The emulator owns all document state. The provider has no document map and publishes no per-document index entries. The shared index holds only the project's `(default)` database reference that anchors IAM. Reset stops the scope's JVM, which discards its documents, then clears the provider's field configuration, Firestore IAM policies, and index entries for that project only. Other projects keep their emulators. There is no persistence: a restart begins empty.

Resource names need no rewriting. The emulator runs under the request's project ID and returns `projects/<project>/databases/(default)/documents/...`, which matches the recorded GCP shape.

### Provider-owned: collection-group fields

`firestore.fields.get`/`update` (`collectionGroups/{c}/fields/{f}`, used by `google_firestore_field` in the Terraform case) belong to the Firestore Admin API. The pinned emulator returns a plain-text `404 Not Found` for both (checked directly against 1.19.9), so the provider keeps that configuration in its scoped store. Similarly, Pub/Sub keeps push configuration in its provider. An unconfigured field reads as automatic index settings. An update stores the TTL/index configuration and returns a completed operation. These calls still pass through `Manager.Use`, so they serialize against reset.

## Recorded scenario

`firestore-document-lifecycle` was recorded and revalidated against real GCP on 2026-10-11 in the sandbox's `(default)` Native-mode database (us-central1). The emulator-backed provider matches it in comparison mode (`sh scripts/check_rest_recordings.sh`). No response patching is involved. A negative control that points the provider at the wrong JAR makes this scenario fail. Documents live in a run-owned collection that cleanup empties. The fixture identity holds a temporary project-level `roles/datastore.user`; the denied identity has no grant. Observed and compared:

- typed values round-trip unchanged: string, integer (as a decimal string), double, boolean, null, timestamp with microseconds, array, and map
- documents carry `name`, `fields`, `createTime`, and `updateTime`
- denied read and write are 403 (provider IAM)
- PATCH with `updateMask.fieldPaths=title` changes only that field; PATCH without a mask replaces the document
- duplicate create is 409; a missing document is 404; deleting a missing document returns 200 `{}`

All emulator responses in this scenario matched the recording. Error messages differ from GCP; for example, the emulator reports `entity already exists: EntityRef{...}`. The recording compares only HTTP status, `code`, and `status`.

The Terraform acceptance case `TestAccFirestoreDocument` (pinned google 8.5.0 provider) passes against the emulator-backed provider.

## Unrecorded scenario

`firestore-query-transaction` ([recorder](../gcp_validation/firestore_query.py), [scenario](../tests/conformance/firestorequery/test_firestore_query.py)) covers a filtered, ordered, limited query, a count aggregation, a subcollection query and `listCollectionIds`, a transaction read and commit, a rollback, and denied query and commit, in a run-owned collection. It has not run against GCP, so it has no baseline: in local mode pytest reports it `unverified`, and it is not in `check_rest_recordings.sh`. Recording it needs its own live scope file with the limits in the recorder.

## Verification

```sh
python3 scripts/install_firestore.py
.venv-tools/bin/python -m pip install -r requirements-firestore.txt
sh scripts/check_firestore.sh
```

This runs inside Moo or Linux CI with only loopback available. Against the real emulator, the race-enabled Go integration tests check that a denied cross-project write starts no backend, document create/duplicate/read, project isolation, reset, REST queries/transactions/subcollections, and a gRPC Listen through the gateway. The Python SDK test runs the official client with local ADC through the core server's gRPC listener. Unit tests use a fake per-scope backend; they pin every permission plan, deny each commit write kind before the backend, reject names outside the project's database, and confirm that only the `owner` credential reaches the emulator on both transports. These are local tests, not GCP authorization evidence.

Run the server with `-services firestore -firestore-java "$(command -v java)" -firestore-jar "$PWD/.tools/firestore/emulator.jar"`. The packaged image ships the JAR at `/opt/firestore/emulator.jar`, and Compose passes both flags.

## Explicit gaps

- Queries, aggregations, transactions, batch writes, subcollections, and listeners are modeled locally, not recorded.
- Not served: the `Write` streaming RPC, `PartitionQuery` (the emulator returns `UNIMPLEMENTED`), `ExecutePipeline`, REST `:listen`, named databases, and the Admin API beyond collection-group fields (indexes, databases, backups, import/export).
- The Python client needs the private `_firestore_api_internal` attribute to use an authenticated local channel.
- Firestore IAM is project/database-level; no per-document grants exist to record. Security rules are bypassed; IAM is the only gate.
- Emulator fidelity is limited to what the recording covers; Google documents the emulator as a development tool, not a production-equivalent backend.
