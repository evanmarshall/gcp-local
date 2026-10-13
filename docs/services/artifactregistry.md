# Artifact Registry

Artifact Registry v1 repositories, their operations, and per-repository IAM over REST. Metadata only: no image push or pull. The scenario `artifactregistry-repository-lifecycle` is recorded against GCP.

## Use

Select `-services ...,artifactregistry`. Routes are `/v1/projects/{project}/locations/{location}/repositories` on port 4566.

## Methods

| Method | Notes |
| --- | --- |
| `repositories.create` | `repositoryId` query. Returns an operation that starts not done. Formats `DOCKER`, `NPM`, `MAVEN`, `PYTHON`, `GO` |
| `repositories.get` | 404 `Requested entity was not found.` for a missing repository the caller may read |
| `repositories.list` | One location, in name order; `orderBy` `name` or `name desc`; `pageSize` up to 1000 with `pageToken` |
| `repositories.patch` | `description`, `labels`, `cleanupPolicyDryRun` by `updateMask`. Unknown mask paths are ignored. Without a mask every field is replaced, so a body whose `format` differs is 400 |
| `repositories.delete` | Returns an operation with `google.protobuf.Empty` |
| `repositories.getIamPolicy`, `repositories.setIamPolicy` | A policy with a condition is 400 `Can't set conditional policy on this resource` |
| `projects.locations.operations.get` | Create and delete operations report `done` after 500 ms. A missing operation is 403 `Permission denied on operation ... (or it may not exist)` |

Repositories are keyed by project and location: the same ID in `us-central1` and `us-east1` is two repositories with their own names, `registryUri`, policies, and resets. Locations are regions (`us-east1`) or `us`, `europe`, `asia`; any region-shaped name is accepted.

Request bodies are decoded strictly: an unknown field is GCP's 400 (`Unknown name "x" at 'repository'`), and output-only fields are dropped.

## IAM

Pinned roles: `roles/artifactregistry.admin`, `.repoAdmin`, `.reader`. A repository-level reader can read but not update. Create and list are checked on the project; repository methods on the repository, or the project when it doesn't exist, so a caller without access gets 403, not 404. Operations get needs `artifactregistry.repositories.get` on the project (modeled locally; no role names an operations permission).

A 403 names the resource and carries ErrorInfo `IAM_PERMISSION_DENIED`, domain `artifactregistry.googleapis.com`, as GCP's does. GCP also appends a troubleshooter link; gcp-local doesn't.

IAM Conditions see no resource attributes: GCP supplies no `resource.service`, `resource.type`, or `resource.name` to a project-level condition on `repositories.get` (recorded in `logging-conditional-access`), so a condition that reads them grants nothing. Time conditions apply.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `artifactregistry-repository-lifecycle` | 2026-10-13 | Create operation not done, then polled; missing operation 403; per-method field sets; one ID in two locations; list order, pages, `name desc`, bad token and negative size refusals; denials with ErrorInfo; empty policy; refused conditional policy; reader grant; masked, unknown-mask, and maskless patches; 409 duplicate; 400 unknown format, unknown field, and mistyped field; delete operation; 404 after delete |

`satisfiesPzi` and `vulnerabilityScanningConfig` are stand-ins; only their presence is recorded. Operation IDs start with `artifactregistry-` (GCP's are UUIDs): Memorystore serves the same operations path, and the prefix routes it.

```sh
sh scripts/check/recordings.sh
```

`TestLocalArtifactRegistryRepository` passes ([Terraform](../contributing/terraform-compatibility.md)).

## Gaps

These return 501 naming the field:

- `mode` `REMOTE_REPOSITORY` or `VIRTUAL_REPOSITORY`, and their configs.
- `cleanupPolicies`, `kmsKeyName`, `dockerConfig`, `mavenConfig`, `disallowUnspecifiedMode`, `vulnerabilityScanningConfig.enablementConfig`, on create or in an update mask.
- Formats `APT`, `YUM`, `GOOGET`, `KFP`, `GENERIC`.
- List `filter`, `orderBy` other than `name`, and the `-` location.
- `setIamPolicy` `auditConfigs` and `updateMask` paths other than `bindings` and `etag`.

Not implemented: image push or pull, the Docker token flow, packages, versions, tags, files, operations list (GCP serves none), and vulnerability scanning. Error messages other than those recorded are local.
