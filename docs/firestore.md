# Firestore (local slice)

Firestore v1 documents control plane: collection-level creates, document upsert/get/list/delete with database-attached IAM (recorded `roles/datastore.*` snapshots pin the baselines; Firestore permission names are the Cloud Datastore ones, and field/TTL configuration checks `datastore.schemas.*`, the current name for `datastore.indexes.*`).

## Local-modeled surfaces

- Creates: POST to the collection path with a `documentId` query, matching the real surface.
- Upserts: a PATCH to a document path creates it when absent and replaces it when present; create-time is preserved across upserts.
- Reads: get and list (collection-scoped); typed-value field objects pass through verbatim.
- Deletes: document delete with 404 confirmation.
- IAM attaches to the default database resource `(default)` — document permissions evaluate against it, matching the real surface's database-attached policies.

The Terraform acceptance case `TestAccFirestoreDocument` drives google_firestore_document with the pinned google 8.5.0 provider and passes locally.

## Explicit gaps

- No cloud recording backs these methods; issue #40 tracks the recording. No supported claim is made.
- runQuery, subcollections, transactions, batch writes, listeners, and the full typed-value encoding (timestamps, references, geo points) are unimplemented.
