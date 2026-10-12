# Firestore

Firestore v1 documents, subcollections, queries, aggregations, transactions, batch writes, and listeners over REST and gRPC in the `(default)` database, with IAM checked on every request. Google's official emulator (1.19.9, [pin](../../../toolchains/firestore.json)) stores documents; each project gets its own JVM. CRUD, queries, aggregations, subcollections, and transactions are recorded against GCP; batch writes and listeners are modeled locally.

## Use

```sh
python3 scripts/install/emulator_jar.py firestore
bin/gcp-local -services firestore \
  -identity-config /absolute/private/server.json \
  -iam-config /absolute/private/iam.json \
  -firestore-java "$(command -v java)" \
  -firestore-jar "$PWD/.tools/firestore/emulator.jar"
```

The packaged image ships the JAR and passes both flags. Java 17 is required; emulator releases after 1.19.9 need Java 21.

Keep `FIRESTORE_EMULATOR_HOST` unset; it sends an anonymous token that bypasses IAM. `google-cloud-firestore` (2.34.1) has no public channel argument, so set the private `_firestore_api_internal` attribute:

```python
import google.auth, grpc
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

[`sdk.py`](../../../internal/providers/databases/firestore/testdata/sdk.py) exercises queries, `count()`, `get_all`, transactions, `batch()`, `bulk_writer()`, subcollections, collection-group queries, `on_snapshot`, and `recursive_delete` with this setup.

## Methods

`{path}` spans any depth, so `cities/sf/landmarks/ggb` uses the same routes.

| Method | REST | gRPC | Permissions |
| --- | --- | --- | --- |
| get | `GET documents/{path}` (even segments) | `GetDocument` | `datastore.entities.get` |
| list | `GET documents/{path}` (odd segments) | `ListDocuments` | `datastore.entities.get`, `.list` |
| createDocument | `POST documents/{collection}` | `CreateDocument` | `datastore.entities.create` |
| patch | `PATCH documents/{path}` | `UpdateDocument` | `datastore.entities.update` |
| delete | `DELETE documents/{path}` | `DeleteDocument` | `datastore.entities.delete` |
| batchGet | `POST documents:batchGet` | `BatchGetDocuments` | `datastore.entities.get` |
| beginTransaction, rollback | `POST documents:beginTransaction`, `:rollback` | `BeginTransaction`, `Rollback` | `datastore.databases.get` |
| commit, batchWrite | `POST documents:commit`, `:batchWrite` | `Commit`, `BatchWrite` | Per write, below |
| runQuery, runAggregationQuery | `POST documents[/{doc}]:runQuery`, `:runAggregationQuery` | `RunQuery`, `RunAggregationQuery` | `datastore.entities.get`, `.list` |
| listCollectionIds | `POST documents[/{doc}]:listCollectionIds` | `ListCollectionIds` | `datastore.entities.list` |
| listen | Not routed | `Listen` | `datastore.entities.get`, `.list` |
| fields.get, fields.patch | `GET`/`PATCH collectionGroups/{c}/fields/{f}` | Not routed | `datastore.schemas.get`, `.update` |

Collection-group fields are stored by the provider, since the emulator doesn't serve them. An unset field reads as automatic index settings; patch returns a completed operation.

## IAM

Permissions follow Google's [required permissions table](https://docs.cloud.google.com/firestore/native/docs/security/iam) (read 2026-10-11) and are checked on the project's `(default)` database. Commit and BatchWrite need a permission per write:

| Write | Permissions |
| --- | --- |
| update or transform with `exists: false` | `datastore.entities.create` |
| update or transform with `exists: true` or `updateTime` | `datastore.entities.update` |
| update or transform with no precondition (`set()`) | `datastore.entities.create`, `.update` |
| delete | `datastore.entities.delete` |

- A read that starts a transaction also needs `datastore.databases.get`, as does an empty commit.
- Listen uses the query permissions and rechecks on each response, so revoking ends delivery.
- Documents outside the request project's `(default)` database return `INVALID_ARGUMENT`.
- Security rules are bypassed; IAM is the only gate. There are no per-document grants.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `firestore-document-lifecycle` | 2026-10-12 | Typed values round-trip, masked and unmasked PATCH, 409 duplicate, 404 missing, 200 on deleting a missing doc, 403 denied read and write |
| `firestore-query-transaction` | 2026-10-12 | Filtered limited query, count, subcollection query, `listCollectionIds`, transaction commit and rollback, denied query and commit |

Recordings compare status and error `code`/`status`, not messages. Run in Linux or moo:

```sh
sh scripts/check/recordings.sh   # both recordings
sh scripts/check/backend/firestore.sh         # provider and Python SDK against the emulator
```

`TestLocalFirestoreDocument` passes ([Terraform](../../contributing/terraform-compatibility.md)).

## Gaps

- Batch writes and listeners are not recorded.
- Composite index requirements are not enforced.
- Not served: `Write` stream, `PartitionQuery`, `ExecutePipeline`, REST `:listen`, named databases, and Admin API other than collection-group fields.
- Documents are in memory; reset or restart discards them.
