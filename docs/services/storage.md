# Cloud Storage

Uniform-access buckets and objects over the JSON API and XML object paths, with IAM enforced on every call. Resumable uploads, V4 signed URLs, object metadata patches, `objects.rewrite`, soft delete and restore, and public access prevention work. The backend is [fake-gcs-server v1.56.1](https://github.com/fsouza/fake-gcs-server/tree/896ece51e49eed0946089f3089f6343855c0d106) running in-process with no listener ([pin](../../third_party/fake-gcs-server/manifest.json)). Seventeen scenarios are recorded against GCP; rewrite with a request body, public members under inherited prevention, and the XML paths other than signed URLs are modeled locally, not recorded.

## Use

Create [local ADC](../guides/local-identity.md) and an [IAM configuration](../architecture/iam.md), or let the [quickstart](../guides/quickstart.md) do both. Each Storage project needs its ID and numeric `number`; grant the fixture administrator `roles/storage.admin` on it. Add `iam` to `-services` for service-account keys and signBlob, which signed URLs need.

```sh
bin/gcp-local -services storage,iam \
  -identity-config .gcp-local/identity/server.json \
  -iam-config .gcp-local/iam.json
```

Point each client at the emulator and keep its normal credential loading:

| Client | Setting | Tested |
| --- | --- | --- |
| Python `google-cloud-storage` | `Client(client_options={"api_endpoint": "http://127.0.0.1:4566"})` | Yes ([`scripts/smoke/storage.py`](../../scripts/smoke/storage.py)) |
| Go `cloud.google.com/go/storage` | `storage.NewClient(ctx, option.WithEndpoint("http://127.0.0.1:4566/storage/v1/"))` | Yes ([`sdk_test.go`](../../internal/providers/storage/sdk_test.go)) |
| Node `@google-cloud/storage` | `new Storage({apiEndpoint: "http://127.0.0.1:4566", useAuthWithCustomEndpoint: true})`; upload with `save(data, {resumable: false})` | Yes ([client matrix](../contributing/clients.md)) |
| rclone | `endpoint = http://127.0.0.1:4566/storage/v1/` in the `google cloud storage` remote, with `bucket_policy_only = true` | Yes ([client matrix](../contributing/clients.md)) |
| gcloud | `CLOUDSDK_API_ENDPOINT_OVERRIDES_STORAGE=http://127.0.0.1:4566/storage/v1/`; create buckets with `--uniform-bucket-level-access` | Yes ([client matrix](../contributing/clients.md)) |
| Terraform `hashicorp/google` | `storage_custom_endpoint = "http://127.0.0.1:4566/storage/v1/"` | Yes ([Terraform](../contributing/terraform-compatibility.md)) |

- Without `useAuthWithCustomEndpoint`, the Node client sends no credentials. Its resumable uploads (the `save()` default) never send them to a custom endpoint: 8.4.0 omits the option in `File.startResumableUpload_`.
- Go and rclone need the `/storage/v1/` suffix; without it they request `/storage/b/...`. The Go client reads objects over the XML API at the endpoint's host.
- Signed URLs use the client's endpoint as their host: Python takes `client.api_endpoint`, Go takes `SignedURLOptions{Hostname: "127.0.0.1:4566", Insecure: true}`.

Bucket create must set `location` and `iamConfiguration.uniformBucketLevelAccess.enabled: true` (or its former name `bucketPolicyOnly`, which rclone sends). Everything else has GCP's defaults:

| Setting | Default | Accepted |
| --- | --- | --- |
| `iamConfiguration.publicAccessPrevention` | `inherited` | `inherited`, `enforced` |
| `softDeletePolicy.retentionDurationSeconds` | `604800` (7 days), `effectiveTime` = `timeCreated` | `0`, or 604800 to 7776000 (90 days) |
| `enableObjectRetention` query option | `false` | `false` in any case (gcloud sends `False`) |
| `storageClass` | `STANDARD` | `STANDARD` |

Versioning and default holds must stay off. Buckets report uniform access under both names, each with a `lockedTime` 90 days after creation.

## Methods

19 of 88 JSON API methods run.

| Resource | Methods | Notes |
| --- | --- | --- |
| Buckets | `insert`, `list`, `get`, `delete` | Global name uniqueness across projects; a bucket holding only soft-deleted objects deletes |
| Buckets | `patch` | `labels` (merges keys, null removes a key) and `softDeletePolicy.retentionDurationSeconds`; advances metageneration and etag |
| Buckets | `getIamPolicy`, `setIamPolicy` | Version-3 conditions on `resource.name`; public members per [public access prevention](#public-access-prevention) |
| Buckets | `getStorageLayout` | `locationType` and no `hierarchicalNamespace`, as gcloud reads them |
| Objects | `insert` | Media, multipart, and resumable uploads, up to 128 MiB per object |
| Objects | `list`, `get` (metadata and `alt=media`), `delete` | Generation, metageneration, and etag preconditions; `softDeleted=true` per [soft delete](#soft-delete) |
| Objects | `restore` | `generation`, preconditions on the live object |
| Objects | `patch` | `contentType`, `cacheControl`, `contentDisposition`, `contentEncoding`, `contentLanguage`, `metadata` (merges keys, null removes a key, null removes all); `generation` and the four precondition options; advances metageneration and etag |
| Objects | `rewrite` | Single call within one project; source metadata copied, body metadata replaces it. A JSON `null` body (rclone `moveto`) copies the source unchanged |
| Objects (XML) | `GET`, `PUT /BUCKET/OBJECT` | Bearer token or V4 signature; `x-goog-meta-*`, `Content-MD5`, `response-content-*` |
| Folders | `insert`/`create`, `get`, `list`, `delete` | Modeled locally, not recorded; deleted with their bucket |

- `projection=full` returns the `noAcl` shape: uniform buckets have no ACLs. `returnPartialSuccess=false`, empty `matchGlob`, and `includeTrailingDelimiter=false` are accepted as the defaults they are. Boolean options are case-insensitive.
- `includeFoldersAsPrefixes=true` (gcloud `ls`) is accepted in a bucket without folders; managed folders are not modeled. In a bucket with [folders](#methods) it returns the 400 gap.
- Multipart uploads accept the single-quoted boundary gcloud's apitools sends (`boundary='===...=='`).
- A query option that Google's [discovery document](../../api/discovery/storage-v1.json) does not define for the method is ignored, as GCP ignores it (recorded in `storage-go-client-projection`). A defined option the emulator does not serve, such as `userProject`, `predefinedAcl`, `quotaUser`, or `oauth_token`, returns the 400 gap, and so does `access_token`. The XML API has no discovery document and refuses every option it does not serve.
- Objects carry `selfLink` and `mediaLink` at the emulator's origin, with GCP's paths and `mediaLink` query (`?generation=N&alt=media`). GCP's `selfLink` origin is `https://www.googleapis.com`.
- Resumable sessions: initiate with `uploadType=resumable`, then `PUT` (Python) or `POST` (Go) chunks with `Content-Range` to the returned `Location`. A status query (`bytes */N`) returns 308 with `Range`; `X-GUploader-No-308: yes` gets 200 with `X-Http-Status-Code-Override: 308`. A retried chunk's repeated bytes are skipped. `X-Goog-Hash` on the last chunk is checked. `DELETE` cancels with 499, and every later request to that session also gets 499. A session expires a week after initiation, as GCP documents (not recorded); its URI then returns 404 and its bytes are freed.
- Signed URLs: `GOOG4-RSA-SHA256` only. The signature must verify against a key of the signing service account: one created with `serviceAccountKeys.create`, or the account's managed key that `signBlob` uses ([service accounts](security/serviceaccounts.md)). Expired, future-dated, wrongly scoped, tampered, or other-method URLs are refused with XML errors.

## Soft delete

A bucket with a soft-delete policy keeps every object generation that a delete, overwrite, rewrite, or restore removes, until its `hardDeleteTime`.

- `objects.list?softDeleted=true` lists them with `softDeleteTime` and `hardDeleteTime`; `prefix` and `maxResults` work. A bucket whose policy is `0` returns 400 `Soft delete policy is required to list soft-deleted versions`.
- `objects.get?softDeleted=true&generation=N` reads one's metadata.
- `objects.restore?generation=N` writes it as a new live generation with metageneration 1. The soft-deleted copy stays, as in GCP. A live object it replaces is soft-deleted.
- Changing the policy affects later deletions only.
- Soft-deleted bytes stay in memory until `hardDeleteTime` or bucket delete.

## Public access prevention

`allUsers` and `allAuthenticatedUsers` in a bucket policy follow the bucket's `publicAccessPrevention`:

- `enforced`: `setIamPolicy` returns 412 `The member bindings allUsers and allAuthenticatedUsers are not allowed since public access prevention is enforced.` A public binding inherited from the project grants nothing on the bucket.
- `inherited`: GCP applies the `storage.publicAccessPrevention` organization policy. The emulator models no organization policies, so, as in a hierarchy that sets none, public members are accepted and grant access: `allUsers` to anonymous callers on the JSON and XML APIs, `allAuthenticatedUsers` to any authenticated caller. A real project whose organization enforces that constraint, or domain-restricted sharing (`iam.allowedPolicyMemberDomains`), refuses these bindings. Not recorded: the sandbox organization enforces domain-restricted sharing.

## IAM

- Uploads of a new object need `storage.objects.create`; replacing an existing object also needs `storage.objects.delete`. Denials match GCP's distinct missing-create and missing-delete errors.
- `restore` needs `storage.objects.restore` and `storage.objects.create`, plus `storage.objects.delete` to replace a live object. `getStorageLayout` needs `storage.objects.list`.
- A resumable session URI is the credential for its chunks, as in GCP. The commit is authorized again as the principal that started the session, against the object at commit time.
- A signed URL authorizes as its signer: the signing account's own Storage grants decide, and revoking them refuses a still-valid URL.
- Object `patch` needs `storage.objects.update`.
- `rewrite` needs `storage.objects.get` on the source and the insert permissions on the destination.
- Project-level grants are inherited; bucket policies return only bucket bindings. A role in the caller's own project grants nothing on a foreign bucket; an explicit bucket binding does.
- Anonymous callers get 401 on the JSON API and 403 `AccessDenied` on the XML API; authenticated callers without permission get 403.
- Responses carry `X-Gcp-Local-Decision-Id`; `gcp-local why-denied ID` explains it.

## Verification

All recorded against GCP in `us-central1` ([recordings](../../tests/conformance/recordings/storage/clients/)):

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `storage-object-lifecycle` | 2026-10-12 | Create, list, read, delete, failed conditional upload, denied write |
| `storage-authorization` | 2026-10-12 | Bucket Object User, inherited project admin, anonymous denial, grant and revoke transitions |
| `storage-cross-project` | 2026-10-12 | Foreign caller denied, allowed by bucket Object Viewer, denied after revoke |
| `storage-object-replacement` | 2026-10-12 | Creator can add but not replace, Object User can replace, stale generation rejected |
| `storage-bucket-updates` | 2026-10-12 | Label merge and removal, stale and denied PATCH |
| `storage-multipart-query-name` | 2026-10-12 | Multipart upload with name in query, generation changes |
| `storage-python-sdk` | 2026-10-12 | Three upstream Python Storage tests, pagination, guarded cleanup |
| `storage-iam-conditions` | 2026-10-12 | Version-3 conditional `objectViewer`; conditional binding rejected in a version-1 policy |
| `storage-iam-directory` | 2026-10-12 | Bucket policy refusals for a nonexistent group (`group:` and `principalSet://goog/group/`) and domain, a `group:` naming a service account, empty group and domain; policy unchanged |
| `storage-resumable-signed-url` | 2026-10-12 | Resumable session, status query, cancel and a request after it, signed PUT and GET, tampered, expired, and wrong-method URLs |
| `storage-bucket-policy-only` | 2026-10-12 | Bucket create with only `bucketPolicyOnly`; both names and their `lockedTime` on create and read |
| `storage-go-client-projection` | 2026-10-12 | The Go client's `projection=full` bucket and object calls, object `patch`, an undefined query option, an invalid projection |
| `storage-bucket-defaults` | 2026-10-12 | rclone's bucket insert: inherited prevention, 7-day soft delete; soft-deleted list, read, restore, denied restore, overwrite; bucket delete with only soft-deleted objects |
| `storage-gcloud-bucket-defaults` | 2026-10-12 | gcloud's bucket insert with `enableObjectRetention=False`; a below-minimum retention refused; disabling soft delete; soft-deleted listing refused without a policy |
| `storage-public-access-enforced` | 2026-10-12 | `allUsers` and `allAuthenticatedUsers` refused under enforced prevention; policy unchanged |
| `storage-gcloud-object-requests` | 2026-10-12 | gcloud's `storageLayout` read and its denial, apitools multipart upload, `includeFoldersAsPrefixes=True` listing, stat, download, delete |
| `storage-rclone-object-requests` | 2026-10-12 | rclone's multipart upload, object read with `selfLink` and `mediaLink` paths, download from `mediaLink`, `rewrite` with a JSON `null` body, source delete |

Run in Linux or moo:

```sh
sh scripts/check/storage.sh     # production ADC smoke
sh scripts/check/recordings.sh  # every recording
```

The smoke covers a 9 MiB resumable upload and signed URLs from a created key and from signBlob. Both run with no egress. Three adapted Terraform acceptance cases also pass ([Terraform](../contributing/terraform-compatibility.md)).

Scenarios that send real-client request shapes, including `storage-gcloud-object-requests`, live in `tests/conformance/storage/clients/`; the other bucket-default scenarios are in `tests/conformance/storage/test_storage_bucket_defaults.py`. Each run of `storage-bucket-defaults`, `storage-gcloud-bucket-defaults`, or `storage-public-access-enforced` leaves its bucket soft-deleted in the sandbox for seven days: GCP soft-deletes a bucket created under a 7-day policy even after the policy is set to 0, and offers no way to purge it sooner. Re-recording `storage-resumable-signed-url` needs the allowed identity to hold `roles/iam.serviceAccountTokenCreator` on itself.

Re-recording `storage-cross-project` needs a peer project. Set `GCP_TEST_PEER_PROJECT_ID`, `GCP_TEST_PEER_SERVICE_ACCOUNT`, and an absolute `GCP_TEST_PEER_CREDENTIALS_FILE` in the sandbox profile ([sandbox](../contributing/gcp-sandbox.md)). The peer account needs Service Usage Consumer on the primary project and no Storage roles; the setup user needs Token Creator on it. Grant/revoke polling waits up to seven minutes. See [validation](../contributing/validation.md) for the commands.

## Gaps

- A coverage gap returns 400 `Storage coverage gap: ...`, not 501. This is a deliberate exception to `UNIMPLEMENTED`: the Go client retries every 5xx without limit, so a 501 would hang it. A path no route claims (and no bucket name matches on the XML API) still returns 501.
- 128 MiB per object; uploads are buffered whole in memory. Chunk sizes are not checked against 256 KiB multiples.
- No ACLs, versioning, object retention (`enableObjectRetention=true` returns the 400 gap) or holds, lifecycle rules, requester pays, encryption keys, notifications, or compose.
- Bucket soft delete is not modeled: a deleted bucket and its soft-deleted objects are gone at once. `softDeleted` on `buckets.list` and `buckets.get`, and `buckets.restore`, return the 400 gap. `objects.restore` in a bucket whose policy is `0` returns the gap.
- Soft-deleted media (`alt=media`), preconditions on a soft-deleted read, soft-deleted listings longer than one page or with other options, and `restoreToken` return the gap.
- Organization policies are not modeled ([public access prevention](#public-access-prevention)). `publicAccessPrevention: unspecified` returns the gap.
- Object `patch` refuses every other field (`acl`, `customTime`, holds, `retention`, `storageClass`, contexts) and clearing a string field (null or empty). It does not change `updated`. `objects.update` (PUT) is a gap.
- `rewrite` never returns a `rewriteToken` and refuses one; it refuses buckets in different projects.
- XML API: object `GET` and `PUT` only. No HMAC signatures, signed resumable starts (`x-goog-resumable`), `x-goog-if-*` or `If-Match`/`If-None-Match` headers, signed payload hashes, or virtual-hosted URLs. A bucket named like an API path prefix (`storage`, `upload`, `v1`) is not reachable over XML.
- Bucket `patch` accepts only `labels` and the soft-delete retention. Only the `STANDARD` class.
- State is in memory and lost on restart. Reset clears one project's buckets, bucket policies, soft-deleted objects, and open upload sessions.
