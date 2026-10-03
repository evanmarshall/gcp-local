# Storage authorization baseline

The `storage-authorization` scenario records one private, uniform-access bucket with public access prevention enforced. The fixture administrator has `roles/storage.admin` on the selected project. The allowed caller initially has `roles/storage.objectUser` only on the temporary bucket; the denied caller initially has no Storage grants. Both callers may have `roles/serviceusage.serviceUsageConsumer` for the explicit quota project. Do not grant these callers project, folder, organization, group, or domain Storage access: inherited permissions would contaminate negative trials. The first denied reads and writes are a data-plane check of that assumption, not a complete IAM policy inventory.

| Observation | Initial grant / transition | Evidence |
| --- | --- | --- |
| Allowed read/write | Object User on bucket | Object metadata and payload digest |
| Missing read/create | No Storage grant | Actual status, reason, permission/resource text when disclosed |
| Anonymous read/write | No Authorization header | Actual error; no fabricated identity or permission order |
| Bucket revocation | Remove allowed caller's bucket grant | Read becomes forbidden while project-granted fixture read still succeeds |
| Read-only grant | Add Object Viewer for denied caller | Read succeeds, create remains forbidden |
| Reader revocation | Remove Object Viewer | Read becomes forbidden again |
| Foreign principal | Separate project; no target Storage grant, then bucket Object Viewer, then revoke | Denied → allowed → denied; runner implemented, cloud evidence pending peer authorization |

Every failed write is checked against a fixture-admin inventory, the original generation/etag, and original bytes. Only fixture-owned objects and the one temporary bucket are removed. Temporary IAM grants are attached to that bucket and disappear with it, including on failures. Project IAM is never changed by these scenarios. A fixture read with no explicit bucket binding demonstrates inherited access under the documented project setup; this is not evidence for every inheritance path or policy-check order.

Grant/revocation polling uses read operations, with a seven-minute deadline and five-second interval. Unexpected status codes fail immediately; unresolved 200/403 transitions are inconclusive. Polls do not create resources or change the initial object. Google documents that [IAM changes can take seven minutes or longer](https://docs.cloud.google.com/iam/docs/access-change-propagation); the deadline bounds a trial rather than claiming a consistency guarantee. Bucket policy changes preserve unrelated bindings and the concurrency etag. [Bucket policy reads omit inherited project policies](https://docs.cloud.google.com/storage/docs/access-control/iam).

Run the recorded same-project cases with the commands in [validation.md](validation.md), selecting `tests/conformance/test_storage_authorization.py`. Real observations distinguish authenticated permission failures (403) from anonymous failures (401) and preserve the upload error's JSON body with its `text/html` content type. Expiry/refresh and service-agent cases follow the identity and asynchronous providers (#6/#12/#13); advanced policy discovery remains #15/#18.

## Optional cross-project input

The separate `storage-cross-project` scenario needs an explicitly authorized peer project and service account. Add `GCP_TEST_PEER_PROJECT_ID`, `GCP_TEST_PEER_SERVICE_ACCOUNT`, and an absolute `GCP_TEST_PEER_CREDENTIALS_FILE` to the selected profile. The peer must differ from the resource project. Its impersonated ADC targets that foreign service account, while its quota project remains the approved primary sandbox; grant it Service Usage Consumer there, without any Storage role. Enable IAM credentials and grant the setup user impersonation on the peer service account. The peer project needs no billing account or Storage resources.

All bucket/object resources still belong to the primary sandbox and stay under its existing limits. Local mode uses the synthetic `peer` identity from `synthetic-peer`; the later provider must model that distinct project. Missing live peer configuration is unverified before fixture creation, not a skipped success. This scenario establishes foreign-principal authorization at a target bucket, not multi-bucket data-store isolation, which belongs to #5/#8.
