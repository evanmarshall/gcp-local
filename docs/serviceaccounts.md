# IAM service accounts

IAM v1 service accounts, per-account IAM policy, and synthesized keys over REST. Modeled locally. The scenario `serviceaccount-lifecycle-authorization` is recorded against GCP. For local identity and token minting, see [local identity](local-identity.md).

## Use

Select `-services ...,iam`. Routes are `/v1/projects/{project}/serviceAccounts` on port 4566. Accounts are addressed by email.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Accounts | `create`, `get`, `list`, `patch`, `delete` | `{accountId, serviceAccount}` create body; `patch` takes `{serviceAccount, updateMask}` and returns `name` plus masked fields |
| Accounts | `getIamPolicy` (POST), `setIamPolicy` | |
| Keys | `create`, `get`, `list`, `delete` | `privateKeyData` is synthesized and signs nothing |

## IAM

Pinned roles: `roles/iam.serviceAccountAdmin`, `.serviceAccountViewer`, `.serviceAccountUser`, `.serviceAccountTokenCreator`. An account-level `serviceAccountUser` grant can read the account but not update it.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `serviceaccount-lifecycle-authorization` | 2026-10-11 | Create and get fields (`uniqueId` equals `oauth2ClientId`), denied read 403, user grant reads but cannot update, POST `getIamPolicy`, PATCH envelope, 409 duplicate, 400 invalid ID, delete then 404 |

```sh
sh scripts/check_rest_recordings.sh
```

`TestAccServiceAccount` passes ([Terraform](terraform-compatibility.md)).

To re-record, get an approved `.gcp-local/serviceaccounts-scope.json` (`LIMITS` in `gcp_validation/serviceaccounts.py`) and a temporary `roles/iam.serviceAccountAdmin` on the fixture identity:

```sh
.venv-tools/bin/python -m pytest tests/conformance/serviceaccounts -q \
  --gcp-mode record --gcp-profile /absolute/path/to/.env \
  --gcp-live-scope-file /absolute/path/to/.gcp-local/serviceaccounts-scope.json \
  --gcp-allow-live --gcp-artifacts .artifacts/serviceaccounts-record
```

After an interrupted run, `python -m gcp_validation.serviceaccounts cleanup --profile ... --scope ... --manifest ... --allow-live` deletes only the account in the manifest.

## Gaps

- Keys are not recorded; recording them would mint real credentials.
- No undelete, disable or enable, `signJwt`/`signBlob`, `projects/-` or `uniqueId` addressing, or workload identity federation.
