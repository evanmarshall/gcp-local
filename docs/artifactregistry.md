# Artifact Registry (admin control plane)

Modeled surface for the Artifact Registry v1 API: repositories and
per-repository IAM. The endpoint lives at
`/v1/projects/{project}/locations/{location}/repositories` and the provider
is selected with `--services=...,artifactregistry`.

## What is modeled

- Repositories: create (repositoryId/repository_id query, LRO envelope),
  get, list (per location), update (description, labels), delete, and
  `getIamPolicy`/`setIamPolicy`.
- Formats: DOCKER, NPM, MAVEN, PYTHON, GO; unknown formats are refused.
- Repository mode (STANDARD_REPOSITORY by default) and cleanupPolicyDryRun
  round-trip so the pinned Terraform case stays stable.
- Predefined roles pinned: `roles/artifactregistry.admin`,
  `roles/artifactregistry.repoAdmin`, `roles/artifactregistry.reader`.

## Recorded scenario

`artifactregistry-repository-lifecycle` was recorded and revalidated against
real GCP on 2026-10-11, and the local provider matches it in comparison mode
(`sh scripts/check_rest_recordings.sh`). One empty labeled DOCKER repository in
us-central1; the allowed identity receives `roles/artifactregistry.reader` on
the repository only. Observed and compared:

- create and delete return completed operations (`name`, `metadata`, `done`,
  typed `response`); delete's response is `google.protobuf.Empty`
- field sets differ by method: the create response has no times or
  `registryUri`, PATCH adds the times but not `registryUri`, GET has all of
  them; `cleanupPolicyDryRun` is omitted while false
- an empty repository policy reads back as only `etag`
- denied read 403; the reader can read but not update; PATCH with
  `updateMask=description` leaves labels untouched
- duplicate create 409; an unknown format 400; 404 after delete

The recording fixed five local divergences: bare create results, 204 deletes,
missing fields, an empty policy rendered with bindings, and PATCH replacing
unmasked labels. `satisfiesPzi` and `vulnerabilityScanningConfig` values are
local stand-ins; only their presence is recorded.

## Explicit gaps

- Image push/pull (the container data plane, GCR/token flow) is out of scope;
  repositories store metadata only.
- Cleanup policies, remote/upstream/virtual modes beyond STANDARD, KMS
  encryption settings, and vulnerability scanning are not modeled.
