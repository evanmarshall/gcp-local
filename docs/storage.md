# Cloud Storage

Private, uniform-access buckets and objects over the JSON API, with IAM enforced on every call. The backend is [fake-gcs-server v1.56.1](https://github.com/fsouza/fake-gcs-server/tree/896ece51e49eed0946089f3089f6343855c0d106) running in-process with no listener ([pin](../third_party/fake-gcs-server/manifest.json)). Seven scenarios are recorded against GCP.

## Use

Create [local ADC](local-identity.md) and an [IAM configuration](iam.md), or let the [quickstart](quickstart.md) do both. Each Storage project needs its ID and numeric `number`; grant the fixture administrator `roles/storage.admin` on it.

```sh
bin/gcp-local -services storage \
  -identity-config .gcp-local/identity/server.json \
  -iam-config .gcp-local/iam.json
```

Set the SDK's `api_endpoint` to `http://127.0.0.1:4566` and use the local ADC file. [`smoke_storage.py`](../scripts/smoke_storage.py) is a complete Python example.

Bucket create must set `location`, `iamConfiguration.uniformBucketLevelAccess.enabled: true`, `iamConfiguration.publicAccessPrevention: enforced`, and `softDeletePolicy.retentionDurationSeconds: "0"`. Storage class is `STANDARD`; versioning and default holds must stay off.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Buckets | `insert`, `list`, `get`, `delete` | Global name uniqueness across projects |
| Buckets | `patch` | `labels` only: merges keys, null removes a key, advances metageneration and etag |
| Buckets | `getIamPolicy`, `setIamPolicy` | Version-3 conditions on `resource.name`; public members rejected |
| Objects | `insert` | Media and multipart uploads, up to 8 MiB |
| Objects | `list`, `get` (metadata and `alt=media`), `delete` | Generation, metageneration, and etag preconditions |
| Folders | `insert`/`create`, `get`, `list`, `delete` | Modeled locally, not recorded; deleted with their bucket |

## IAM

- Uploads of a new object need `storage.objects.create`; replacing an existing object also needs `storage.objects.delete`. Denials match GCP's distinct missing-create and missing-delete errors.
- Project-level grants are inherited; bucket policies return only bucket bindings. A role in the caller's own project grants nothing on a foreign bucket; an explicit bucket binding does.
- Anonymous callers get 401, authenticated callers without permission get 403.
- Responses carry `X-Gcp-Local-Decision-Id`; `gcp-local why-denied ID` explains it.

## Verification

All recorded against GCP in `us-central1` ([recordings](../tests/conformance/recordings/)):

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `storage-object-lifecycle` | 2026-10-05 | Create, list, read, delete, failed conditional upload, denied write |
| `storage-authorization` | 2026-10-05 | Bucket Object User, inherited project admin, anonymous denial, grant and revoke transitions |
| `storage-cross-project` | 2026-10-05 | Foreign caller denied, allowed by bucket Object Viewer, denied after revoke |
| `storage-object-replacement` | 2026-10-05 | Creator can add but not replace, Object User can replace, stale generation rejected |
| `storage-bucket-updates` | 2026-10-05 | Label merge and removal, stale and denied PATCH |
| `storage-multipart-query-name` | 2026-10-05 | Multipart upload with name in query, generation changes |
| `storage-python-sdk` | 2026-10-05 | Three upstream Python Storage tests, pagination, guarded cleanup |
| `storage-iam-conditions` | 2026-10-11 | Version-3 conditional `objectViewer`; conditional binding rejected in a version-1 policy |

Run in Linux or moo:

```sh
sh scripts/check_storage.sh
```

It runs the production ADC smoke and compares every recording with no egress. Three adapted Terraform acceptance cases also pass ([Terraform](terraform-compatibility.md)).

Re-recording `storage-cross-project` needs a peer project. Set `GCP_TEST_PEER_PROJECT_ID`, `GCP_TEST_PEER_SERVICE_ACCOUNT`, and an absolute `GCP_TEST_PEER_CREDENTIALS_FILE` in the sandbox profile ([sandbox](gcp-sandbox.md)). The peer account needs Service Usage Consumer on the primary project and no Storage roles; the setup user needs Token Creator on it. Grant/revoke polling waits up to seven minutes. See [validation](validation.md) for the commands.

## Gaps

- Upload limit is 8 MiB. No resumable, signed, or XML uploads.
- No ACLs, versioning, retention or holds, soft delete, lifecycle rules, requester pays, encryption keys, or notifications.
- Bucket `patch` accepts only `labels`. Only the `noAcl` projection and the `STANDARD` class.
- State is in memory and lost on restart. Reset clears one project's buckets and bucket policies.
