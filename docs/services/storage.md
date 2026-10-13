# Cloud Storage

Uniform-access buckets and objects over the JSON API and XML object paths, with IAM enforced on every call. Resumable uploads, V4 signed URLs, object metadata patches, `objects.rewrite`, object versioning, lifecycle rules, retention policies and Bucket Lock, object holds and object retention, Pub/Sub notifications, hierarchical-namespace folders, soft delete and restore, and public access prevention work. The backend is [fake-gcs-server v1.56.1](https://github.com/fsouza/fake-gcs-server/tree/896ece51e49eed0946089f3089f6343855c0d106) running in-process with no listener ([pin](../../third_party/fake-gcs-server/manifest.json)). Twenty-eight scenarios are recorded against GCP; rewrite with a request body, public members under inherited prevention, and the XML paths other than signed URLs are modeled locally, not recorded.

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
| Python `google-cloud-storage` | `Client(client_options={"api_endpoint": "http://127.0.0.1:4566"})` | Yes ([`scripts/smoke/storage.py`](../../scripts/smoke/storage.py); versioning in the [client matrix](../contributing/clients.md)) |
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
| `enableObjectRetention` query option | `false` | `true` or `false` in any case (gcloud sends `False`); `true` reports `objectRetention.mode: Enabled` |
| `retentionPolicy.retentionPeriod` | none | 1 second to under 100 years; `effectiveTime` = `timeCreated` |
| `defaultEventBasedHold` | off (omitted) | `true`, `false` |
| `lifecycle.rule` | none | Per [lifecycle](#lifecycle) |
| `hierarchicalNamespace.enabled` | off (omitted) | `true` with uniform access, per [hierarchical namespace](#hierarchical-namespace) |
| `storageClass` | `STANDARD` | `STANDARD` |

Buckets report uniform access under both names, each with a `lockedTime` 90 days after creation.

## Methods

25 of 88 JSON API methods run.

| Resource | Methods | Notes |
| --- | --- | --- |
| Buckets | `insert`, `get`, `delete` | Global name uniqueness across projects; a bucket holding only soft-deleted objects deletes |
| Buckets | `list` | `prefix`, [paging](#lists), `returnPartialSuccess` (every bucket is reachable, so `unreachable` is omitted) |
| Buckets | `patch` | `labels` (merges keys, null removes a key), `softDeletePolicy.retentionDurationSeconds`, `versioning.enabled`, `retentionPolicy` (null removes it), `defaultEventBasedHold`, and `lifecycle` per [lifecycle](#lifecycle); advances metageneration and etag |
| Buckets | `lockRetentionPolicy` | Requires `ifMetagenerationMatch`; locks the retention policy per [retention](#retention-and-holds) |
| Notifications | `insert`, `list`, `get`, `delete` | Per [notifications](#notifications); deleted with their bucket |
| Projects | `serviceAccount.get` | The project's Storage service agent, `service-NUMBER@gs-project-accounts.iam.gserviceaccount.com` |
| Buckets | `getIamPolicy`, `setIamPolicy` | Version-3 conditions on `resource.name`; public members per [public access prevention](#public-access-prevention) |
| Buckets | `getStorageLayout` | `locationType`, and `hierarchicalNamespace` when the bucket has it, as gcloud reads them |
| Objects | `insert` | Media, multipart, and resumable uploads, up to 128 MiB per object (larger is the 400 gap) |
| Objects | `list` | `prefix`, `delimiter`, `startOffset`, `endOffset`, `versions` per [versioning](#versioning), [paging](#lists); `softDeleted=true` per [soft delete](#soft-delete) |
| Objects | `get` (metadata and `alt=media`), `delete` | `generation` names a live or noncurrent generation; generation, metageneration, and etag preconditions; `softDeleted=true` per [soft delete](#soft-delete) |
| Objects | `restore` | `generation`, preconditions on the live object |
| Objects | `patch` | `contentType`, `cacheControl`, `contentDisposition`, `contentEncoding`, `contentLanguage`, `metadata` (merges keys, null removes a key, null removes all), `temporaryHold`, `eventBasedHold`, `retention`, `customTime`; `generation`, `overrideUnlockedRetention`, and the four precondition options; advances metageneration, etag, and `updated` |
| Objects | `rewrite` | Single call within one project; source metadata copied, body metadata replaces it. A JSON `null` body (rclone `moveto`) copies the source unchanged |
| Objects (XML) | `GET`, `PUT /BUCKET/OBJECT` | Bearer token or V4 signature; `x-goog-meta-*`, `Content-MD5`, `response-content-*` |
| Folders | `insert`/`create`, `get`, `list`, `delete` | Per [hierarchical namespace](#hierarchical-namespace); `insert` takes `recursive`, `list` takes `prefix` and `pageSize`/`pageToken` [paging](#lists) |

- `projection=full` returns the `noAcl` shape: uniform buckets have no ACLs. Empty `matchGlob` and `includeTrailingDelimiter=false` are accepted as the defaults they are. Boolean options are case-insensitive.
- `includeFoldersAsPrefixes=true` (gcloud `ls`) with delimiter `/` adds the folders directly under the prefix to `prefixes`, even when no object is under them. Managed folders are not modeled.
- Multipart uploads accept the single-quoted boundary gcloud's apitools sends (`boundary='===...=='`).
- A query option that Google's [discovery document](../../api/discovery/storage-v1.json) does not define for the method is ignored, as GCP ignores it (recorded in `storage-go-client-projection`). A defined option the emulator does not serve, such as `userProject`, `predefinedAcl`, `quotaUser`, or `oauth_token`, returns the 400 gap, and so does `access_token`. The XML API has no discovery document and refuses every option it does not serve.
- Objects carry `selfLink` and `mediaLink` at the emulator's origin, with GCP's paths and `mediaLink` query (`?generation=N&alt=media`). GCP's `selfLink` origin is `https://www.googleapis.com`.
- Resumable sessions: initiate with `uploadType=resumable`, then `PUT` (Python) or `POST` (Go) chunks with `Content-Range` to the returned `Location`. A status query (`bytes */N`) returns 308 with `Range`; `X-GUploader-No-308: yes` gets 200 with `X-Http-Status-Code-Override: 308`. A retried chunk's repeated bytes are skipped. `X-Goog-Hash` on the last chunk is checked. `DELETE` cancels with 499, and every later request to that session also gets 499. A session expires a week after initiation, as GCP documents (not recorded); its URI then returns 404 and its bytes are freed.
- Signed URLs: `GOOG4-RSA-SHA256` only. The signature must verify against a key of the signing service account: one created with `serviceAccountKeys.create`, or the account's managed key that `signBlob` uses ([service accounts](security/serviceaccounts.md)). Expired, future-dated, wrongly scoped, tampered, or other-method URLs are refused with XML errors.

## Lists

`objects.list`, `buckets.list`, and `folders.list` page as GCP does (recorded: `storage-list-paging`):

- Entries are in name order. With a `delimiter`, each prefix is one entry toward `maxResults`, in name order among the objects, and no later page repeats it. `startOffset` and `endOffset` select objects before they collapse into prefixes.
- `maxResults` (`pageSize` for folders) is an unsigned integer, capped at 1000. A negative or non-integer value is 400 `Invalid unsigned integer value: '-1'.` (reason `invalidParameter`). `maxResults=0` lists no objects but a full page of buckets.
- A page token resumes after the previous page's last entry, so entries added or removed between pages are neither repeated nor skipped. An object listing refuses a token it did not issue with 400 `Next page token not valid: TOKEN`; a bucket listing lists nothing after one.

## Versioning

A bucket with `versioning.enabled` keeps every live generation that an overwrite, rewrite, restore, or plain delete replaces, as noncurrent (recorded: `storage-object-versioning`):

- A noncurrent generation has `timeDeleted`. `objects.list` shows it only with `versions=true`, which lists each name's generations oldest first; a page can end between two generations of one name.
- `get` and `delete` with `generation` address that generation, live or noncurrent, and evaluate `ifGenerationMatch` and the other preconditions against it. Deleting a generation removes it permanently. A plain delete makes the live generation noncurrent.
- `ifGenerationMatch=0` on an insert checks only the live generation, so it succeeds once the live one is deleted.
- Noncurrent generations keep the bucket from deleting: 409 `The bucket you tried to delete is not empty.`
- Turning versioning off keeps the existing noncurrent generations; later overwrites replace the live one.
- With soft delete, only a generation that leaves the bucket is soft-deleted: a deleted noncurrent generation, never a live one that becomes noncurrent. Restoring one makes the live generation noncurrent (recorded: `storage-versioning-soft-delete`).

## Hierarchical namespace

A bucket created with `hierarchicalNamespace.enabled: true` serves folders (recorded: `storage-hierarchical-namespace`, `storage-folders-flat`).

- The bucket needs uniform bucket-level access: without it, 400 `Hierarchical namespace buckets must use uniform bucket-level access.` It reports `hierarchicalNamespace` on reads and in `getStorageLayout`.
- A folder name gets a trailing `/`. Creating an existing folder is 409 `The folder you tried to create already exists.`; creating one under a missing parent is 409 `The parent folder does not exist.` unless `recursive=true` creates the parents.
- Uploading `a/b/obj` creates `a/` and `a/b/`. Deleting the object leaves them.
- Deleting a folder with a folder or object under it is 409 `The folder you tried to delete is not empty.`; a missing folder is 404 `The folder does not exist.` A bucket with folders deletes, and its folders with it.
- A bucket without hierarchical namespace answers every folder method, after authorization, with 409 `The bucket does not support hierarchical namespace.`
- Folder `create`, `get`, and `delete` authorize on the folder (`.../buckets/B/folders/F/`); `list` on the bucket.

## Lifecycle

Bucket insert and patch take `lifecycle.rule` with the `Delete`, `SetStorageClass`, and `AbortIncompleteMultipartUpload` actions and every condition: `age`, `createdBefore`, `customTimeBefore`, `daysSinceCustomTime`, `daysSinceNoncurrentTime`, `isLive`, `matchesPrefix`, `matchesSuffix`, `matchesStorageClass`, `noncurrentTimeBefore`, and `numNewerVersions` (recorded: `storage-lifecycle-rules`).

- Rules read back as set, including zero values (`age: 0`, `isLive: false`). An empty rule list or `lifecycle: null` removes the field. Unknown condition names are ignored.
- Invalid rules get GCP's 400s: a missing action or condition, an unknown action type or storage class, `SetStorageClass` without a class or to `MULTI_REGIONAL` in a region, an `AbortIncompleteMultipartUpload` condition other than `age`, `matchesPrefix`, and `matchesSuffix`, a negative `age` or `numNewerVersions`, and a timestamp where a date belongs.
- A worker applies the rules once a minute, on the provider's clock (`storage.Config.Now`; tests advance it). GCP applies them asynchronously, typically within a day, so the timing differs: an object here is acted on within a minute of qualifying.
- `Delete` makes a versioned bucket's live generation noncurrent and otherwise deletes the generation, soft-deleting it when the bucket keeps soft-deleted objects. It never removes a held or retained generation. `Delete` wins over `SetStorageClass`; the coldest `SetStorageClass` wins over the others.
- `SetStorageClass` changes `storageClass` and `timeStorageClassUpdated` in place, with no new generation (not recorded). Uploads still take only `STANDARD`.
- `AbortIncompleteMultipartUpload` is accepted and does nothing: XML API multipart uploads are not modeled.
- The worker acts as the project's Storage service agent; GCP requires no role for lifecycle actions.
- `customTime` (object patch) moves only forward: an earlier time is 400 `Custom time cannot be decreased. Previously: T. Attempting to set: T.`, and `null` leaves it.

## Notifications

Notification configurations publish object changes to Pub/Sub through the local Pub/Sub provider, as the project's Storage service agent. Run with `-services storage,pubsub`. Recorded: `storage-notifications`, including the overwrite attributes; `OBJECT_ARCHIVE` and lifecycle events are modeled from GCP's documentation, not recorded.

Grant the agent `roles/pubsub.publisher` on the topic first, as in GCP.

- `insert` needs the agent to hold `pubsub.topics.publish` on the topic: without it, 403 `The service account 'AGENT' does not have permission to publish messages to to the Cloud Pub/Sub topic 'TOPIC', or that topic does not exist.`; for a topic that doesn't exist, 400 `Cloud Pub/Sub topic 'TOPIC' not found, or user 'AGENT' does not have permission to it.`
- `topic` takes `//pubsub.googleapis.com/projects/P/topics/T` or `projects/P/topics/T` and reports the first. `payload_format` is `JSON_API_V1` or `NONE`. Unknown `event_types` are dropped; none left means every event. `custom_attributes` and `object_name_prefix` are optional. A missing topic or payload format is 400 `required`; a malformed topic is 400 `invalid`.
- Events: `OBJECT_FINALIZE` for an upload, rewrite, or restore; `OBJECT_METADATA_UPDATE` for a patch; `OBJECT_DELETE` for a deletion or a replaced generation; `OBJECT_ARCHIVE` when a versioned bucket keeps the generation as noncurrent. Lifecycle deletions send them too.
- Attributes: `notificationConfig`, `eventType`, `payloadFormat`, `bucketId`, `objectId`, `objectGeneration`, `eventTime`, the custom attributes, and `overwroteGeneration` or `overwrittenByGeneration` on an overwrite. `JSON_API_V1` sends the object resource; its links use the origin of the request that created the configuration.
- Delivery is asynchronous through a queue. Pub/Sub authorizes each publish as the agent; a refused publish drops that message. A `SetStorageClass` lifecycle action sends nothing.
- `projects.serviceAccount.get` needs `resourcemanager.projects.get`; `notifications.insert` and `delete` need `storage.buckets.get` and `storage.buckets.update`; `list` and `get` need `storage.buckets.get`.

## Retention and holds

A retention policy, a hold, or an object's own retention refuses deleting, overwriting, or making noncurrent the object it protects, with GCP's errors (recorded: `storage-retention-holds`, `storage-object-retention`, `storage-bucket-lock`):

| Protection | Refusal |
| --- | --- |
| `temporaryHold` or `eventBasedHold` | 403 `forbidden`: `Object 'B/O' is under active Temporary hold and cannot be deleted, overwritten or archived until hold is removed.` (`Event-Based` for the other) |
| `retentionPolicy`, until the object's `retentionExpirationTime` | 403 `retentionPolicyNotMet`: `Object 'B/O' is subject to bucket's retention policy or object retention and cannot be deleted or overwritten until TIME` |
| Object `retention`, until `retainUntilTime` | The same 403 |

- `retentionExpirationTime` is `timeCreated` plus the period; it applies to objects created before the policy too. While an event-based hold is on the object has none; releasing the hold starts the period then.
- `defaultEventBasedHold: true` puts an event-based hold on every new object. Holds a patch never set are omitted.
- Metadata patches of a protected object succeed.
- `lockRetentionPolicy?ifMetagenerationMatch=M` locks the policy (`isLocked: true`). Without the option: 400 `Required parameter: ifMetagenerationMatch`. With no unlocked policy: 400 `Bucket 'B' does not have an unlocked retention policy.` A locked policy can't be removed (403 `retentionPolicyNotMet`) or shortened (403 `forbidden`); the same or a longer period is accepted. An empty locked bucket deletes.
- Object retention needs a bucket created with `enableObjectRetention=true`. `mode` is `Unlocked` or `Locked`; both fields are required and `retainUntilTime` must be in the future, each refused with GCP's 400. Unlocked retention can be shortened or removed only with `overrideUnlockedRetention=true`; locked retention can only be extended. Each is refused with GCP's 403.
- Not recorded: a plain delete in a versioned bucket of a retained object is refused too.

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
- Object `patch` needs `storage.objects.update`; a non-null `retention` adds `storage.objects.setRetention`, and `overrideUnlockedRetention=true` adds `storage.objects.overrideUnlockedRetention`. `lockRetentionPolicy` needs `storage.buckets.get` and `storage.buckets.update`.
- `rewrite` needs `storage.objects.get` on the source and the insert permissions on the destination.
- Project-level grants are inherited; bucket policies return only bucket bindings. A role in the caller's own project grants nothing on a foreign bucket; an explicit bucket binding does.
- Anonymous callers get 401 on the JSON API and 403 `AccessDenied` on the XML API; authenticated callers without permission get 403.
- Responses carry `X-Gcp-Local-Decision-Id`; `gcp-local why-denied ID` explains it.

## Verification

All recorded against GCP in `us-central1` ([recordings](../../tests/conformance/recordings/storage/)):

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
| `storage-iam-groups` | 2026-10-12 | `objectViewer` granted to a real group, to a group nesting it, and conditionally (object prefix) to the nested group: the member reads, the non-member gets 403, and each revocation returns 403 |
| `storage-resumable-signed-url` | 2026-10-12 | Resumable session, status query, cancel and a request after it, signed PUT and GET, tampered, expired, and wrong-method URLs |
| `storage-bucket-policy-only` | 2026-10-12 | Bucket create with only `bucketPolicyOnly`; both names and their `lockedTime` on create and read |
| `storage-go-client-projection` | 2026-10-13 | The Go client's `projection=full` bucket and object calls, a bucket list with `returnPartialSuccess=true`, object `patch`, an undefined query option, an XML read from a missing bucket, an invalid projection |
| `storage-object-versioning` | 2026-10-13 | Versioning on and off, noncurrent generations in listings and pages, reads, media, and deletes by generation with preconditions, create-only uploads, bucket delete refused while noncurrent generations remain |
| `storage-notifications` | 2026-10-13 | The Storage service agent and its denial; configurations refused without the publisher grant and for a missing topic; filtered, unfiltered, and short-topic configurations; invalid payload, event type, and topic; list, get, delete; finalize, metadata-update, overwrite (with `overwroteGeneration` and `overwrittenByGeneration`), and delete messages per configuration |
| `storage-hierarchical-namespace` | 2026-10-13 | Bucket create and layout, the uniform-access refusal; folder create, nested, duplicate, missing parent, recursive, trailing slash, denied; get, list, prefix, paging; folders an upload creates; non-empty deletes refused; folders as object prefixes; bucket delete with folders |
| `storage-folders-flat` | 2026-10-13 | Every folder method refused with 409 in a bucket without hierarchical namespace; the denied list |
| `storage-lifecycle-rules` | 2026-10-13 | Every action and condition set and read back, fourteen invalid rules, rules emptied and cleared, a denied update; `customTime` set, decreased, nulled, and advanced |
| `storage-retention-holds` | 2026-10-13 | Retention policy set, invalid, and removed; retained delete and overwrite refused; temporary, event-based, and default event-based holds; retention starting at an event-based hold's release; object retention refused without `enableObjectRetention` |
| `storage-object-retention` | 2026-10-13 | `enableObjectRetention=true`; unlocked retention set, refused deletes and overwrites, shortening and removal refused without override, extension, override, and the denials for both permissions; invalid values; locked retention refusals and extension |
| `storage-bucket-lock` | 2026-10-13 | `lockRetentionPolicy` without and with a stale precondition, denied, locked, locked again; locked policy removal and reduction refused; delete after expiry; locked bucket deleted |
| `storage-versioning-soft-delete` | 2026-10-13 | Soft delete in a versioned bucket: overwrites and plain deletes keep no soft-deleted copy, a deleted noncurrent generation is soft-deleted, restore makes the live generation noncurrent |
| `storage-list-paging` | 2026-10-13 | Delimiter pages (prefixes count toward `maxResults`, none repeated), flat pages, prefix and offset selection, invalid page tokens and `maxResults` values for objects and buckets |
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

Scenarios that send real-client request shapes, including `storage-gcloud-object-requests`, live in `tests/conformance/storage/clients/`; bucket scenarios, including the other bucket defaults, are in `tests/conformance/storage/buckets/`; versioning, retention, and hold scenarios are in `tests/conformance/storage/protection/`. Each run of `storage-bucket-defaults`, `storage-gcloud-bucket-defaults`, `storage-public-access-enforced`, or `storage-versioning-soft-delete` leaves its bucket soft-deleted in the sandbox for seven days: GCP soft-deletes a bucket created under a 7-day policy even after the policy is set to 0, and offers no way to purge it sooner. Re-recording `storage-resumable-signed-url` needs the allowed identity to hold `roles/iam.serviceAccountTokenCreator` on itself.

Re-recording `storage-cross-project` needs a peer project. Set `GCP_TEST_PEER_PROJECT_ID`, `GCP_TEST_PEER_SERVICE_ACCOUNT`, and an absolute `GCP_TEST_PEER_CREDENTIALS_FILE` in the sandbox profile ([sandbox](../contributing/gcp-sandbox.md)). The peer account needs Service Usage Consumer on the primary project and no Storage roles; the setup user needs Token Creator on it. Grant/revoke polling waits up to seven minutes. See [validation](../contributing/validation.md) for the commands.

## Gaps

- A coverage gap returns 400 `Storage coverage gap: ...`, not 501. This is a deliberate exception to `UNIMPLEMENTED`: the Go client retries every 5xx without limit, so a 501 would hang it.
- No Storage method answers 5xx or 429. Every refusal has the JSON API's body (`error.code`, `error.message`, `error.errors[]` with `domain` and `reason`), or an XML error document on the XML API. A failure inside the emulator that GCP would not have (a backend 5xx, a broken invariant) returns 400 `Storage emulator fault: ...`. A missing bucket on the XML API is 404 `NoSuchBucket`, as in GCP (recorded: `storage-go-client-projection`); only a path whose first segment can't name a bucket returns the gateway's 501.
- Outside the provider, the shared gateway still renders a malformed or rejected bearer token (401) and a JSON body over 2 MiB (429) in the generic error body.
- 128 MiB per object; uploads are buffered whole in memory, and a larger upload is the 400 gap. Chunk sizes are not checked against 256 KiB multiples.
- Folder `rename` and `deleteRecursive` return the gap; managed folders are not modeled.
- `folders.list` refuses `delimiter`, `startOffset`, and `endOffset`; `includeTrailingDelimiter=true` and `matchGlob` on `objects.list` return the gap.
- Patching a noncurrent generation returns the gap.
- A hold set to `null`, object retention in a bucket created without `enableObjectRetention`, and a `retentionPolicy` without `retentionPeriod` return the gap.
- Lifecycle values GCP may accept but the emulator does not model return the gap: a negative `daysSinceCustomTime` or `daysSinceNoncurrentTime`, a value of another JSON type, and `SetStorageClass` to `REGIONAL`, `DURABLE_REDUCED_AVAILABILITY`, or `MULTI_REGIONAL` outside a region.
- No ACLs, requester pays, encryption keys, or compose.
- Bucket soft delete is not modeled: a deleted bucket and its soft-deleted objects are gone at once. `softDeleted` on `buckets.list` and `buckets.get`, and `buckets.restore`, return the 400 gap. `objects.restore` in a bucket whose policy is `0` returns the gap.
- Soft-deleted media (`alt=media`), preconditions on a soft-deleted read, soft-deleted listings longer than one page or with other options, and `restoreToken` return the gap.
- Organization policies are not modeled ([public access prevention](#public-access-prevention)). `publicAccessPrevention: unspecified` returns the gap.
- Object `patch` refuses every other field (`acl`, `storageClass`, contexts) and clearing a string field (null or empty). `objects.update` (PUT) is a gap.
- `rewrite` never returns a `rewriteToken` and refuses one; it refuses buckets in different projects.
- XML API: object `GET` and `PUT` only. No HMAC signatures, signed resumable starts (`x-goog-resumable`), `x-goog-if-*` or `If-Match`/`If-None-Match` headers, signed payload hashes, or virtual-hosted URLs. A bucket named like an API path prefix (`storage`, `upload`, `v1`) is not reachable over XML.
- Bucket `patch` accepts only the fields in [methods](#methods). Only the `STANDARD` class.
- State is in memory and lost on restart. Reset clears one project's buckets, bucket policies, soft-deleted objects, and open upload sessions.
