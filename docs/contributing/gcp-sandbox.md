# Real-GCP sandbox

Recording and revalidating scenarios needs a dedicated, billed GCP project and three service-account identities. Offline and local tests need none of this. Running anything live also needs the owner's explicit authorization and cost scope; filling in `.env` is not that authorization.

## Profile

Copy [gcp-sandbox.env.example](gcp-sandbox.env.example) to the ignored `.env` at the repository root. It holds names and file paths, never tokens, passwords, or key contents. The runner reads it only when passed with `--profile` or `--gcp-profile`; it is never sourced as shell, and `$(...)` or `${...}` are not expanded. Duplicate fields are rejected.

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

### Standing groups

`storage-iam-groups` grants through two Google groups in the sandbox organization. The owner maintains them; scenarios never change membership, and service accounts can't administer groups.

| Group | Members |
| --- | --- |
| `gcp-local-allowed@stitchwink.com` | The allowed identity |
| `gcp-local-nested@stitchwink.com` | The group `gcp-local-allowed@stitchwink.com` |

The denied identity is in neither. A sandbox in another organization needs the same groups, or the scenario fails with "Group ... does not exist." `tools/storage-conformance` declares the same groups in its local directory for replay.

Check the profile offline, then optionally refresh all three tokens (no resources created):

```sh
python -m gcp_validation.sandbox doctor --profile /absolute/path/to/.env
python -m gcp_validation.sandbox doctor --profile /absolute/path/to/.env --allow-live
```

## Setup plan

`scripts/live/plan_sandbox.py` prints the gcloud commands for APIs, identities, grants, and GitHub workload identity federation. It makes no network calls and doesn't create the project, link billing, or create keys.

```sh
python scripts/live/plan_sandbox.py \
  --project YOUR_SANDBOX --project-number YOUR_PROJECT_NUMBER \
  --setup-account YOU@example.com --repository OWNER/REPO \
  --repository-id YOUR_REPO_ID --owner-id YOUR_OWNER_ID > /tmp/sandbox-plan.json
```

Get the repository and owner IDs with `gh api repos/OWNER/REPO --jq '{id, owner_id: .owner.id}'`. Run each resource's `check` command before its create command; an existing pool or provider must match `expected`, not be overwritten.

## CI workflow

The `local-github/manual-storage` WIF provider admits only this repository's immutable IDs, `refs/heads/main`, and environment `gcp-conformance`, and only two workflows: `cloud-conformance.yml` on `workflow_dispatch`, and `gcp-drift.yml` on `workflow_dispatch` or `schedule`. Its principal set gets Workload Identity User on each account and Service Usage Consumer on the project, nothing else.

Create the GitHub `gcp-conformance` environment, restricted to branch `main`, with these variables (no secrets): `GCP_PROJECT_ID`, `GCP_PROJECT_NUMBER`, `GCP_REGION`, `GCP_TEST_WIF_PROVIDER`, `GCP_TEST_SERVICE_ACCOUNT`, `GCP_TEST_FIXTURE_SERVICE_ACCOUNT`, `GCP_TEST_DENIED_SERVICE_ACCOUNT`.

```sh
gh workflow run cloud-conformance.yml --ref main -f mode=revalidate
# mode=record produces candidates only; CI never promotes or commits them.
```

- Runs `test_storage.py` and `test_storage_authorization.py` within the one-bucket limits.
- Only manual dispatch triggers it. Concurrent jobs queue; don't record from a host at the same time.
- It retries manifest cleanup after failures and keeps reports, candidates, and manifests as artifacts for 14 days. After a killed runner, download its manifest and run the [cleanup command](validation.md#interrupted-live-runs).

## Weekly drift job

`gcp-drift.yml` runs every Monday and on dispatch. It revalidates each entry in `.github/drift/plan.json` under its committed scope in `.github/drift/`. Those scopes set `scheduled_runs: true`, which authorizes `revalidate` only. Each scoped entry names its scenario's scope `loader`; `tests/validation/harness/test_drift.py` runs it on the committed scope, so changing a scenario's limits fails CI until its drift scope matches. A failing run opens or comments on the `GCP drift detected` issue and keeps reports and manifests as artifacts for 30 days.

```sh
gh workflow run gcp-drift.yml --ref main
python scripts/live/drift.py --check   # offline: every planned test and scope exists
```

The fixture account keeps standing setup roles for every planned service, since the job can't add grants: `storage.admin`, `serviceusage.serviceUsageConsumer`, `artifactregistry.admin`, `logging.admin`, `aiplatform.user`, `bigquery.admin`, `datastore.user`, `cloudtasks.admin`, `cloudkms.admin`, `iam.serviceAccountAdmin`, `secretmanager.admin`, and `pubsub.admin`. Recording anything else adds its role for the run and removes it after: `orgpolicy-project-viewer` needs `roles/orgpolicy.policyViewer` on the project for the allowed identity, and `iam-custom-role-lifecycle` and `iam-deny-policy-enforcement` need `roles/iam.roleAdmin` on the project for the fixture. The deny policy scenarios need the fixture to hold `roles/iam.denyAdmin`, which GCP grants only on an organization; the owner granted it on the sandbox organization with an expiry condition, and the scenarios attach policies to the sandbox project only. The Organization Policy scenarios need `orgpolicy.googleapis.com` enabled on the sandbox (enabled 2026-10-13). Never grant the fixture project-level `iam.serviceAccountUser` or the allowed account standing `bigquery.jobUser`: the Cloud Tasks, Cloud Scheduler, and BigQuery denial checks need them absent. A run costs well under $1.
