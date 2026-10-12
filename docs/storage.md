# Cloud Storage provider

The first supported slice is private, uniform-access Storage: bucket create/list/get/delete and label PATCH, bucket get/set IAM policy, and object media/multipart upload, list, metadata/media get, and delete. It uses [fake-gcs-server v1.56.1](https://github.com/fsouza/fake-gcs-server/tree/896ece51e49eed0946089f3089f6343855c0d106) through its existing Go library. The [source and license pin](../third_party/fake-gcs-server/manifest.json) and `go.sum` make the backend reproducible.

## Run with local credentials

Build the [runtime](runtime.md), create [local ADC credentials](local-identity.md), and prepare an owner-only [IAM configuration](iam.md). Each configured Storage project needs its explicit numeric `number` as well as its project ID. Grant `roles/storage.admin` to the chosen fixture administrator on that project. Applications can instead receive bucket-level Object User/Viewer/Creator grants through the Storage API.

```sh
bin/gcp-local -services storage \
  -identity-config .gcp-local/identity/server.json \
  -iam-config .gcp-local/iam.json
```

Point the SDK's `api_endpoint` at `http://127.0.0.1:4566` and use the generated local ADC file. Do not use the real-cloud sandbox profile. The service flag requires both configurations; missing credentials never become an administrator. The default invocation remains a core-only process. Use the [packaged quickstart](quickstart.md) to generate the configurations and start Docker automatically.

Bucket creation explicitly sets `location`, `iamConfiguration.uniformBucketLevelAccess.enabled: true`, `iamConfiguration.publicAccessPrevention: enforced`, and `softDeletePolicy.retentionDurationSeconds: "0"`. Storage class defaults to `STANDARD`; versioning and default holds remain disabled. The Python SDK's numeric zero and null effective-time field are accepted. See [the production SDK smoke](../scripts/smoke_storage.py) for a complete fixture with ordinary `google.auth.default()` credentials, bucket creation, policy updates, upload/download, replacement, revocation, and cleanup.

## Ownership and authorization

The provider owns bucket placement, project number, labels, uniform-access/public-access settings, and policy metadata. Label PATCH merges keys and removes null-valued keys; successful updates advance provider-owned metageneration, etag, and updated time. The backend owns bucket/object data and object generation. The shared index owns the bucket's globally unique name and resource ancestry. A failed cross-project publication rolls back the losing backend's empty bucket. Operations are serialized within each project by the existing backend manager; different projects use separate in-memory backends.

Every operation performs an IAM preflight and a complete decision under the backend lifecycle gate. Uploads resolve whether an object exists before the complete decision: a new object requires create, and replacement adds delete without dropping create. Policy changes use the same gate, so a completed revocation precedes subsequent mutations. Anonymous and foreign-project principals receive only their explicit applicable grants. Public bindings are rejected because this slice enforces public access prevention. Unsupported authorization fails before mutation.

Responses carry `X-Gcp-Local-Decision-Id`; `gcp-local why-denied ID` explains a retained decision. The provider preserves the observed Storage denial formats, including the difference between missing-create and missing-delete upload errors. It enforces generation, metageneration, and etag preconditions. The backend's content-MD5 etag does not change on identical-content replacement, so the provider derives an opaque JSON etag from backend generation/metageneration. No second object-state store is introduced. Backend-origin URLs and unsupported ACL fields are omitted; the SDK uses its configured endpoint.

## Lifecycle and isolation

`NoListener: true` uses upstream's in-process HTTP transport and opens **no backend API port**. The gateway registers only the listed provider methods, and never mounts the backend handler. Its administrative, notification, ACL, and other routes cannot bypass authorization. No notification configuration, ambient ADC, proxy, or network HTTP transport is passed to the backend. This is the library reuse pattern described in the [LocalStack comparison](localstack-comparison.md), with existing lifecycle hooks retained.

Storage starts lazily after authorization. Reset clears one project's backend data, bucket catalog/index, and resource policies only after backend reset succeeds; project policies remain. Shutdown releases the backend. All Storage state is in memory; restart loses data and bucket policies. Snapshot/restore and persistence are explicitly unsupported. There is no public reset endpoint yet. Functional race tests cover reset, foreign-project preservation, revocation, canonical escaped object names, concurrent global-name collisions, and absence of a backend listener.

## Evidence and limits

Seven scenarios were recorded and revalidated against real GCP on **2026-10-05 UTC**, then matched by this provider without network egress:

| Scenario | Verified behavior |
| --- | --- |
| `storage-object-lifecycle` | Create/list/read/delete, failed conditional upload, denied write, cleanup |
| `storage-authorization` | Bucket grants, inherited project admin, anonymous denial, revoke/grant transitions, unchanged data after denial |
| `storage-cross-project` | Foreign caller denial, explicit bucket grant, revocation, preserved data |
| `storage-object-replacement` | Creator can add but cannot replace, Object User can replace, stale generation rejection, new etag for identical-content replacement |
| `storage-bucket-updates` | Label merge/removal, changing metageneration/etag, stale and denied PATCH without mutation, single-region defaults |
| `storage-multipart-query-name` | Multipart name in query, matching bucket metadata, explicit STANDARD replacement, generation changes and content preservation |
| `storage-python-sdk` | Three pinned upstream Python Storage tests plus pagination and generation-guarded cleanup |

`storage-iam-conditions` was recorded on **2026-10-11 UTC** and is matched the same way: a version-3 conditional `objectViewer` binding scoped by `resource.name`, and the rejection of a conditional binding in a version-1 policy ([IAM](iam.md)).

The cloud runs used one owned us-central1 bucket at a time, at most ten objects of at most 1 MiB, with successful cleanup. See [recordings](../tests/conformance/recordings/) for immutable observations, contract hashes, source versions, and dates. Ordinary local comparison uses a build-tagged **credential fixture** that resolves the runner's synthetic tokens; the gateway, IAM, provider, and backend are production code. Those fixed tokens are rejected by the production binary, which is separately exercised with real local STS/ADC credentials. Neither credential mode contacts real GCP during local tests.

Run in moo or Linux CI with namespace permission and the pinned SDK environment:

```sh
sh scripts/check_storage.sh
```

This builds both binaries, runs the production ADC/SDK smoke, and compares all seven recorded scenarios inside loopback-only namespaces. Reports under `.artifacts/storage-provider/` identify the implementation and backend versions separately from the cloud baseline. CI publishes the coverage and JUnit reports.

The initial upload limit is **8 MiB**. Resumable/signed/XML uploads, ACLs, versioning, retention/holds, soft delete, requester-pays, encryption keys, notifications, bucket updates other than labels, and broader methods are unsupported. Optional response-field projection and many validation/error combinations are not certified. Local reset/isolation and production ADC checks are functional evidence; they do not expand the seven cloud parity claims. Three adapted Google provider acceptance cases pass; see [Terraform compatibility](terraform-compatibility.md) for the exact limits. Other SDK languages, transports, advanced IAM policies, retries, and large-object streaming need their own recorded scenarios.
