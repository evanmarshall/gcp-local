# Real-GCP sandbox

Recording and revalidating scenarios needs a dedicated, billed GCP project and three service-account identities. Offline and local tests need none of this. Running anything live also needs the owner's explicit authorization and cost scope; filling in `.env` is not that authorization.

## Profile

Copy [.env.example](../.env.example) to the ignored `.env`. It holds names and file paths, never tokens, passwords, or key contents. The runner reads it only when passed with `--profile` or `--gcp-profile`; it is never sourced as shell, and `$(...)` or `${...}` are not expanded. Duplicate fields are rejected.

| Variable | Value |
| --- | --- |
| `GCP_PROJECT_ID` | New project ID, 6–30 lowercase letters, digits, or hyphens, starting with a letter |
| `GCP_PROJECT_NUMBER` | From `gcloud projects describe PROJECT_ID --format='value(projectNumber)'` |
| `GCP_BILLING_ACCOUNT_ID` | `XXXXXX-XXXXXX-XXXXXX` |
| `GCP_SETUP_ACCOUNT` | Your Google account; it creates the project and links billing |
| `GCP_ORGANIZATION_ID` or `GCP_FOLDER_ID` | Optional parent; set at most one |
| `GCP_REGION` | Default `us-central1` |
| `GCP_TEST_LIVE_SCOPE` | Free-text record of the services and limits the owner approved |
| `GCP_TEST_BUDGET_USD` | Optional; not used yet and never a spending cap |
| `GCP_TEST_*`, `GCP_TEST_FIXTURE_*`, `GCP_TEST_DENIED_*` | Service account email and ADC file path for the allowed caller, the setup/cleanup identity, and the denied caller |
| `GCP_TEST_MAX_BUCKETS`, `_OBJECTS`, `_OBJECT_BYTES` | Storage runner bounds: 1, 10, 1 MiB. Can be lowered, not raised |
| `GCP_TEST_PEER_*` | Optional second project for cross-project isolation scenarios |

## Credentials

Each identity uses keyless impersonated ADC with an `authorized_user` source, standard Google endpoints, and `quota_project_id` set to the sandbox. Files must be mode `0600` and live outside Git and moo. JSON service-account keys are rejected.

```sh
gcloud auth login
gcloud auth application-default login --impersonate-service-account=SERVICE_ACCOUNT_EMAIL
```

The signing-in user needs Service Account Token Creator on each account. Grants:

- **Fixture**: setup and cleanup permissions for the scenario (Storage Admin for Storage).
- **Allowed**: only the scenario's grants.
- **Denied**: no service grants. Check inherited grants from parents too.
- **All three**: Service Usage Consumer, for quota charging.

Check the profile offline, then optionally refresh all three tokens (no resources created):

```sh
python -m gcp_validation.sandbox doctor --profile /absolute/path/to/.env
python -m gcp_validation.sandbox doctor --profile /absolute/path/to/.env --allow-live
```

## Setup plan

`scripts/plan_sandbox.py` prints the gcloud commands for APIs, identities, grants, and GitHub workload identity federation. It makes no network calls and doesn't create the project, link billing, or create keys.

```sh
python scripts/plan_sandbox.py \
  --project YOUR_SANDBOX --project-number YOUR_PROJECT_NUMBER \
  --setup-account YOU@example.com --repository OWNER/REPO \
  --repository-id YOUR_REPO_ID --owner-id YOUR_OWNER_ID > /tmp/sandbox-plan.json
```

Get the repository and owner IDs with `gh api repos/OWNER/REPO --jq '{id, owner_id: .owner.id}'`. Run each resource's `check` command before its create command; an existing pool or provider must match `expected`, not be overwritten.

## CI workflow

The `local-github/manual-storage` WIF provider admits only this repository's immutable IDs, `refs/heads/main`, environment `gcp-conformance`, `workflow_dispatch`, and `cloud-conformance.yml`. Its principal set gets Workload Identity User on each account and Service Usage Consumer on the project, nothing else.

Create the GitHub `gcp-conformance` environment, restricted to branch `main`, with these variables (no secrets): `GCP_PROJECT_ID`, `GCP_PROJECT_NUMBER`, `GCP_REGION`, `GCP_TEST_WIF_PROVIDER`, `GCP_TEST_SERVICE_ACCOUNT`, `GCP_TEST_FIXTURE_SERVICE_ACCOUNT`, `GCP_TEST_DENIED_SERVICE_ACCOUNT`.

```sh
gh workflow run cloud-conformance.yml --ref main -f mode=revalidate
# mode=record produces candidates only; CI never promotes or commits them.
```

- Runs `test_storage.py` and `test_storage_authorization.py` within the one-bucket limits.
- Only manual dispatch triggers it. Concurrent jobs queue; don't record from a host at the same time.
- It retries manifest cleanup after failures and keeps reports, candidates, and manifests as artifacts for 14 days. After a killed runner, download its manifest and run the [cleanup command](validation.md#interrupted-live-runs).
