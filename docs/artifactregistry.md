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

## Explicit gaps

- No cloud recording covers these methods yet; nothing claims recorded
  conformance (tracked under the recording backlog issues).
- Image push/pull (the container data plane, GCR/token flow) is out of scope;
  repositories store metadata only.
- Cleanup policies, remote/upstream/virtual modes beyond STANDARD, KMS
  encryption settings, and vulnerability scanning are not modeled.

