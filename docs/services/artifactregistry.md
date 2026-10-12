# Artifact Registry

Artifact Registry v1 repositories and per-repository IAM over REST. Metadata only: no image push or pull. Modeled locally. The scenario `artifactregistry-repository-lifecycle` is recorded against GCP.

## Use

Select `-services ...,artifactregistry`. Routes are `/v1/projects/{project}/locations/{location}/repositories` on port 4566.

## Methods

| Method | Notes |
| --- | --- |
| `repositories.create` | `repositoryId` query; returns a completed operation. Formats `DOCKER`, `NPM`, `MAVEN`, `PYTHON`, `GO`; others are 400 |
| `repositories.get`, `repositories.list` | List is per location |
| `repositories.patch` | `description` and `labels`; honors `updateMask` |
| `repositories.delete` | Returns a completed operation with `google.protobuf.Empty` |
| `repositories.getIamPolicy`, `repositories.setIamPolicy` | An empty policy returns only `etag` |

`mode` (default `STANDARD_REPOSITORY`) and `cleanupPolicyDryRun` are stored and returned.

## IAM

Pinned roles: `roles/artifactregistry.admin`, `.repoAdmin`, `.reader`. A repository-level reader can read but not update.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `artifactregistry-repository-lifecycle` | 2026-10-12 | Create and delete operations, per-method field sets, empty policy, denied read 403, reader cannot update, masked PATCH, 409 duplicate, 400 unknown format, 404 after delete |

`satisfiesPzi` and `vulnerabilityScanningConfig` are stand-ins; only their presence is recorded.

```sh
sh scripts/check/recordings.sh
```

`TestAccArtifactRegistryRepository` passes ([Terraform](../contributing/terraform-compatibility.md)).

## Gaps

- No image push or pull, Docker token flow, or package storage.
- No cleanup policies, remote or virtual modes, KMS encryption, or vulnerability scanning.
