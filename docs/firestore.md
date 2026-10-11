# Firestore (local slice)

Firestore v1 documents: collection-level creates, document upsert/get/list/delete with database-attached IAM (recorded `roles/datastore.*` snapshots pin the baselines; Firestore permission names are the Cloud Datastore ones, and field/TTL configuration checks `datastore.schemas.*`, the current name for `datastore.indexes.*`).

## Backend: the official emulator

Documents are served by Google's official Cloud Firestore emulator, pinned in [toolchains/firestore.json](../toolchains/firestore.json) (version **1.19.9**, the `cloud-firestore-emulator` component of Cloud SDK 529.0.0). `python3 scripts/install_firestore.py` downloads the component from `dl.google.com` without gcloud, verifies the archive size/SHA-256 and the JAR SHA-256, and installs only `.tools/firestore/emulator.jar`. The provider re-hashes the JAR before every start.

Version choice: the current release, 1.22.0 (SDK 588.0.0), and every release since 1.20.0 (SDK 530.0.0) ship Java 21 class files (class-file major version 65), so they cannot run on the pinned Java 17 runtime. Google's versioned SDK manifests (`components-v<sdk>.json`) list 1.19.9 for SDK 500.0.0 through 529.0.0. Its class files are Java 11 (major 55). Moving to a newer emulator requires changing the Java 21 runtime used by Moo, CI, and the packaged image.

The provider remains the authentication and authorization gate:

1. Resolve the project, collection, and document from the route, then authorize against the `(default)` database resource with the IAM engine. A denied or invalid request returns before the backend manager is involved, so it never starts or reaches the emulator.
2. Run the request inside `Manager.Use` for the project scope. Each resource project gets its own lazily started JVM on a random loopback port (`--single_project_mode_error` rejects any other project ID). The child gets a private temporary directory, a minimal environment (`PATH`, `TMPDIR`, `HOME`), a 256 MiB heap, and no caller credentials, proxy settings, or ambient ADC.
3. Forward the documents request to the emulator's v1 REST endpoint. The provider rebuilds the path from the route parameters, forwards only documented document query parameters (`documentId`, `mask.fieldPaths`, `updateMask.fieldPaths`, `currentDocument.*`, `pageSize`, `pageToken`, `orderBy`, `showMissing`, `readTime`), and sends the fixed `Bearer owner` header. That header is the emulator's documented administrative bypass of security rules. The IAM decision has already been made, and the caller's token is never forwarded. The emulator's status code and JSON body are returned unchanged. A non-JSON reply becomes an explicit `UNIMPLEMENTED` coverage gap.

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

## Verification

```sh
python3 scripts/install_firestore.py
sh scripts/check_firestore.sh
```

This runs inside Moo or Linux CI with only loopback available. The race-enabled Go integration test starts the real emulator. It checks that a denied cross-project write starts no backend, verifies create/duplicate/read through the emulator, shows that two projects with the same collection and document ID stay separate, and confirms that reset clears one project's documents without affecting the other. Unit tests use a fake per-scope backend. They verify that denied requests never create a backend, that only allowlisted query parameters and the fixed `owner` bearer reach it, that emulator errors pass through, that each project uses its own instance, and that reset restarts the instance and clears field configuration. These are local tests, not GCP authorization evidence.

Run the server with `-services firestore -firestore-java "$(command -v java)" -firestore-jar "$PWD/.tools/firestore/emulator.jar"`. The packaged image ships the JAR at `/opt/firestore/emulator.jar`, and Compose passes both flags. The packaged `-services` list does not select Firestore yet.

## Explicit gaps

- Only the routed documents methods are exposed. runQuery, batch get/write, transactions, listeners, subcollection paths, and the other Admin API surfaces are not routed, even though the emulator implements some of them. They remain unrecorded.
- Firestore IAM is project/database-level; no per-document grants exist to record.
- Emulator fidelity is limited to what the recording covers; Google documents the emulator as a development tool, not a production-equivalent backend.
