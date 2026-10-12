# IAM service accounts

IAM v1 service accounts, per-account IAM policy, local RSA keys, and IAM Credentials `signBlob` over REST, and most of them over gRPC `google.iam.admin.v1.IAM`. Modeled locally. The scenario `serviceaccount-lifecycle-authorization` is recorded against GCP. For local identity and token minting, see [local identity](local-identity.md).

## Use

Select `-services ...,iam`. Routes are `/v1/projects/{project}/serviceAccounts` on port 4566. Accounts are addressed by email.

gRPC `google.iam.admin.v1.IAM` on port 4567 serves every method below except key `create` and `signBlob`, through the REST provider methods, so permissions and errors are the same ([clients](clients.md)). The Go `cloud.google.com/go/iam/admin/apiv1` 1.11.0 and Python `google-cloud-iam` 2.26.0 clients are tested.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Accounts | `create`, `get`, `list`, `patch`, `delete` | `{accountId, serviceAccount}` create body; `patch` takes `{serviceAccount, updateMask}` and returns `name` plus masked fields |
| Accounts | `getIamPolicy` (POST), `setIamPolicy` | |
| Keys | `create`, `get`, `list`, `delete` | `create` returns a 2048-bit RSA `TYPE_GOOGLE_CREDENTIALS_FILE` key file whose signatures verify Storage signed URLs; `delete` stops them verifying |
| IAM Credentials | `signBlob` | `POST /v1/projects/-/serviceAccounts/EMAIL:signBlob`; signs with the account's managed key, also for accounts declared in the IAM configuration. Needs `iam.serviceAccounts.signBlob` (Token Creator), even on itself |

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

- gRPC refuses with `UNIMPLEMENTED`: `CreateServiceAccountKey` (the key file's `token_uri` names the HTTP origin, which a gRPC request doesn't carry; create keys over REST), the deprecated IAM `SignBlob` and `SignJwt` (REST `signBlob` is the IAM Credentials API), `UpdateServiceAccount` (PUT), `TestIamPermissions`, and every role and other method.

- Keys and `signBlob` are modeled locally, not recorded; recording keys would mint real credentials.
- A key file's `token_uri` names this emulator, which does not accept JWT-bearer grants: a key signs but cannot mint access tokens.
- No `signJwt`, `generateAccessToken`, or `signBlob` delegation chains.
- No undelete, disable or enable, `projects/-` or `uniqueId` addressing outside `signBlob`, or workload identity federation.
