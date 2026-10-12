# Cloud Storage

Private, uniform-access buckets and objects over the JSON API and XML object paths, with IAM enforced on every call. Resumable uploads, V4 signed URLs, object metadata patches, and `objects.rewrite` work. The backend is [fake-gcs-server v1.56.1](https://github.com/fsouza/fake-gcs-server/tree/896ece51e49eed0946089f3089f6343855c0d106) running in-process with no listener ([pin](../third_party/fake-gcs-server/manifest.json)). Eleven scenarios are recorded against GCP; rewrite and the XML paths other than signed URLs are modeled locally, not recorded.

## Use

Create [local ADC](local-identity.md) and an [IAM configuration](iam.md), or let the [quickstart](quickstart.md) do both. Each Storage project needs its ID and numeric `number`; grant the fixture administrator `roles/storage.admin` on it. Add `iam` to `-services` for service-account keys and signBlob, which signed URLs need.

```sh
bin/gcp-local -services storage,iam \
  -identity-config .gcp-local/identity/server.json \
  -iam-config .gcp-local/iam.json
```

Point each client at the emulator and keep its normal credential loading:

| Client | Setting | Tested |
| --- | --- | --- |
| Python `google-cloud-storage` | `Client(client_options={"api_endpoint": "http://127.0.0.1:4566"})` | Yes ([`smoke_storage.py`](../scripts/smoke_storage.py)) |
| Go `cloud.google.com/go/storage` | `storage.NewClient(ctx, option.WithEndpoint("http://127.0.0.1:4566/storage/v1/"))` | Yes ([`sdk_test.go`](../internal/providers/storage/sdk_test.go)) |
| Node `@google-cloud/storage` | `new Storage({apiEndpoint: "http://127.0.0.1:4566", useAuthWithCustomEndpoint: true})`; upload with `save(data, {resumable: false})` | Yes ([client matrix](clients.md)) |
| rclone | `endpoint = http://127.0.0.1:4566/storage/v1/` in the `google cloud storage` remote | `mkdir` fails ([client matrix](coverage.md#known-client-failures)) |
| Terraform `hashicorp/google` | `storage_custom_endpoint = "http://127.0.0.1:4566/storage/v1/"` | Yes ([Terraform](terraform-compatibility.md)) |

- Without `useAuthWithCustomEndpoint`, the Node client sends no credentials. Its resumable uploads (the `save()` default) never send them to a custom endpoint: 8.4.0 omits the option in `File.startResumableUpload_`.
- Go and rclone need the `/storage/v1/` suffix; without it they request `/storage/b/...`. The Go client reads objects over the XML API at the endpoint's host.
- Signed URLs use the client's endpoint as their host: Python takes `client.api_endpoint`, Go takes `SignedURLOptions{Hostname: "127.0.0.1:4566", Insecure: true}`.

Bucket create must set `location`, `iamConfiguration.uniformBucketLevelAccess.enabled: true` (or its former name `bucketPolicyOnly`, which rclone sends), `iamConfiguration.publicAccessPrevention: enforced`, and `softDeletePolicy.retentionDurationSeconds: "0"`. Storage class is `STANDARD`; versioning and default holds must stay off. rclone sends neither public access prevention nor a soft-delete policy, so its `mkdir` is refused. Buckets report uniform access under both names, each with a `lockedTime` 90 days after creation.

## Methods

17 of 88 JSON API methods run.

| Resource | Methods | Notes |
| --- | --- | --- |
| Buckets | `insert`, `list`, `get`, `delete` | Global name uniqueness across projects |
| Buckets | `patch` | `labels` only: merges keys, null removes a key, advances metageneration and etag |
| Buckets | `getIamPolicy`, `setIamPolicy` | Version-3 conditions on `resource.name`; public members rejected |
| Objects | `insert` | Media, multipart, and resumable uploads, up to 128 MiB per object |
| Objects | `list`, `get` (metadata and `alt=media`), `delete` | Generation, metageneration, and etag preconditions |
| Objects | `patch` | `contentType`, `cacheControl`, `contentDisposition`, `contentEncoding`, `contentLanguage`, `metadata` (merges keys, null removes a key, null removes all); `generation` and the four precondition options; advances metageneration and etag |
| Objects | `rewrite` | Single call within one project; source metadata copied, body metadata replaces it |
| Objects (XML) | `GET`, `PUT /BUCKET/OBJECT` | Bearer token or V4 signature; `x-goog-meta-*`, `Content-MD5`, `response-content-*` |
| Folders | `insert`/`create`, `get`, `list`, `delete` | Modeled locally, not recorded; deleted with their bucket |

- `projection=full` returns the `noAcl` shape: uniform buckets have no ACLs. `returnPartialSuccess=false`, empty `matchGlob`, and `false` for `includeTrailingDelimiter`, `includeFoldersAsPrefixes`, and `softDeleted` are accepted as the defaults they are.
- A query option that Google's [discovery document](../api/discovery/storage-v1.json) does not define for the method is ignored, as GCP ignores it (recorded in `storage-go-client-projection`). A defined option the emulator does not serve, such as `userProject`, `predefinedAcl`, `quotaUser`, or `oauth_token`, returns the 400 gap, and so does `access_token`. The XML API has no discovery document and refuses every option it does not serve.
- Objects carry `selfLink` and `mediaLink` at the emulator's origin.
- Resumable sessions: initiate with `uploadType=resumable`, then `PUT` (Python) or `POST` (Go) chunks with `Content-Range` to the returned `Location`. A status query (`bytes */N`) returns 308 with `Range`; `X-GUploader-No-308: yes` gets 200 with `X-Http-Status-Code-Override: 308`. A retried chunk's repeated bytes are skipped. `X-Goog-Hash` on the last chunk is checked. `DELETE` cancels with 499, and every later request to that session also gets 499.
- Signed URLs: `GOOG4-RSA-SHA256` only. The signature must verify against a key of the signing service account: one created with `serviceAccountKeys.create`, or the account's managed key that `signBlob` uses ([service accounts](serviceaccounts.md)). Expired, future-dated, wrongly scoped, tampered, or other-method URLs are refused with XML errors.

## IAM

- Uploads of a new object need `storage.objects.create`; replacing an existing object also needs `storage.objects.delete`. Denials match GCP's distinct missing-create and missing-delete errors.
- A resumable session URI is the credential for its chunks, as in GCP. The commit is authorized again as the principal that started the session, against the object at commit time.
- A signed URL authorizes as its signer: the signing account's own Storage grants decide, and revoking them refuses a still-valid URL.
- Object `patch` needs `storage.objects.update`.
- `rewrite` needs `storage.objects.get` on the source and the insert permissions on the destination.
- Project-level grants are inherited; bucket policies return only bucket bindings. A role in the caller's own project grants nothing on a foreign bucket; an explicit bucket binding does.
- Anonymous callers get 401 on the JSON API and 403 `AccessDenied` on the XML API; authenticated callers without permission get 403.
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
| `storage-resumable-signed-url` | 2026-10-12 | Resumable session, status query, cancel and a request after it, signed PUT and GET, tampered, expired, and wrong-method URLs |
| `storage-bucket-policy-only` | 2026-10-12 | Bucket create with only `bucketPolicyOnly`; both names and their `lockedTime` on create and read |
| `storage-go-client-projection` | 2026-10-12 | The Go client's `projection=full` bucket and object calls, object `patch`, an undefined query option, an invalid projection |

Run in Linux or moo:

```sh
sh scripts/check_storage.sh
```

It runs the production ADC smoke (including a 9 MiB resumable upload and signed URLs from a created key and from signBlob) and compares every recording with no egress. Three adapted Terraform acceptance cases also pass ([Terraform](terraform-compatibility.md)).

The last two scenarios live in `tests/conformance/storageclients/`. Re-recording `storage-resumable-signed-url` needs the allowed identity to hold `roles/iam.serviceAccountTokenCreator` on itself.

Re-recording `storage-cross-project` needs a peer project. Set `GCP_TEST_PEER_PROJECT_ID`, `GCP_TEST_PEER_SERVICE_ACCOUNT`, and an absolute `GCP_TEST_PEER_CREDENTIALS_FILE` in the sandbox profile ([sandbox](gcp-sandbox.md)). The peer account needs Service Usage Consumer on the primary project and no Storage roles; the setup user needs Token Creator on it. Grant/revoke polling waits up to seven minutes. See [validation](validation.md) for the commands.

## Gaps

- A coverage gap returns 400 `Storage coverage gap: ...`, not 501. This is a deliberate exception to `UNIMPLEMENTED`: the Go client retries every 5xx without limit, so a 501 would hang it. A path no route claims (and no bucket name matches on the XML API) still returns 501.
- 128 MiB per object; uploads are buffered whole in memory. Chunk sizes are not checked against 256 KiB multiples.
- No ACLs, versioning, retention or holds, soft delete, lifecycle rules, requester pays, encryption keys, notifications, or compose.
- Object `patch` refuses every other field (`acl`, `customTime`, holds, `retention`, `storageClass`, contexts) and clearing a string field (null or empty). It does not change `updated`. `objects.update` (PUT) is a gap.
- `rewrite` never returns a `rewriteToken` and refuses one; it refuses buckets in different projects.
- XML API: object `GET` and `PUT` only. No HMAC signatures, signed resumable starts (`x-goog-resumable`), `x-goog-if-*` or `If-Match`/`If-None-Match` headers, signed payload hashes, or virtual-hosted URLs. A bucket named like an API path prefix (`storage`, `upload`, `v1`) is not reachable over XML.
- Bucket `patch` accepts only `labels`. Only the `STANDARD` class.
- State is in memory and lost on restart. Reset clears one project's buckets, bucket policies, and open upload sessions.
