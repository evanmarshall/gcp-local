# Firestore (local slice)

Firestore v1 documents control plane: collection-level creates, document upsert/get/list/delete with database-attached IAM (recorded `roles/datastore.*` snapshots pin the baselines; Firestore permission names are the Cloud Datastore ones, and field/TTL configuration checks `datastore.schemas.*`, the current name for `datastore.indexes.*`).

## Local-modeled surfaces

- Creates: POST to the collection path with a `documentId` query, matching the real surface.
- Upserts: a PATCH to a document path creates it when absent and replaces it when present; create-time is preserved across upserts.
- Reads: get and list (collection-scoped); typed-value field objects pass through verbatim.
- Deletes: document delete with 404 confirmation.
- IAM attaches to the default database resource `(default)` — document permissions evaluate against it, matching the real surface's database-attached policies.

The Terraform acceptance case `TestAccFirestoreDocument` drives google_firestore_document with the pinned google 8.5.0 provider and passes locally.

## Recorded scenario

`firestore-document-lifecycle` was recorded and revalidated against real GCP on
2026-10-11 in the sandbox's `(default)` Native-mode database (us-central1), and
the local provider matches it in comparison mode
(`sh scripts/check_rest_recordings.sh`). Documents live in a run-owned
collection that cleanup empties. The fixture identity holds a temporary
project-level `roles/datastore.user`; the denied identity has no grant.
Observed and compared:

- typed values round-trip unchanged: string, integer (as a decimal string),
  double, boolean, null, timestamp with microseconds, array, and map
- documents carry `name`, `fields`, `createTime`, and `updateTime`
- denied read and write are 403
- PATCH with `updateMask.fieldPaths=title` changes only that field; PATCH
  without a mask replaces the document
- duplicate create is 409; a missing document is 404; deleting a missing
  document returns 200 `{}`

The recording fixed two local divergences: PATCH ignored the field mask and
replaced the document, and deleting a missing document returned 404. Nested
(dotted) mask paths are an explicit `UNIMPLEMENTED` gap.

## Explicit gaps

- runQuery, subcollections, transactions, batch writes, listeners, and
  references/geo points are unimplemented and unrecorded.
- Firestore IAM is project-level; no per-document grants exist to record.
