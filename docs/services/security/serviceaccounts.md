# IAM service accounts

IAM v1 service accounts, per-account IAM policy, local RSA keys, and IAM Credentials impersonation (`generateAccessToken`, `generateIdToken`, `signJwt`, `signBlob`) over REST and gRPC. The scenarios `serviceaccount-lifecycle-authorization` and `iamcredentials-impersonation` are recorded against GCP. For local identity and token minting, see [local identity](../../guides/local-identity.md).

## Use

Select `-services ...,iam`. Routes are `/v1/projects/{project}/serviceAccounts` on port 4566. Accounts are addressed by email.

gRPC on port 4567 serves `google.iam.admin.v1.IAM` (every IAM v1 method below except key `create`) and `google.iam.credentials.v1.IAMCredentials` (all four), through the REST provider methods, so permissions and errors are the same ([clients](../../contributing/clients.md)). Tested clients are in [coverage](../../coverage.md#client-compatibility).

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Accounts | `create`, `get`, `list`, `patch`, `delete` | `{accountId, serviceAccount}` create body; `patch` takes `{serviceAccount, updateMask}` and returns `name` plus masked fields; `list` pages by unique id (`pageSize` default 20, at most 100) |
| Accounts | `getIamPolicy` (POST), `setIamPolicy` | Conditional bindings with policy version 3 |
| Keys | `create`, `get`, `list`, `delete` | `create` returns a 2048-bit RSA `TYPE_GOOGLE_CREDENTIALS_FILE` key file whose signatures verify Storage signed URLs; `delete` stops them verifying. `list` returns the account's system-managed key and its user-managed keys by name, filtered by `keyTypes`. `get` returns `publicKeyData` for `publicKeyType` `TYPE_X509_PEM_FILE` (a self-signed certificate) or `TYPE_RAW_PUBLIC_KEY` |
| IAM Credentials | `generateAccessToken` | Local access token bound to the account ([below](#impersonation)) |
| IAM Credentials | `generateIdToken` | One-hour local ID token: `iss`, `sub`, `aud`, `azp`, `iat`, `exp`, plus `email` and `email_verified` with `includeEmail` |
| IAM Credentials | `signJwt` | Signs the claim set unchanged with the account's managed key; header `{"alg":"RS256","kid":KEY_ID,"typ":"JWT"}`; an `exp` more than 12 hours ahead is 400 |
| IAM Credentials | `signBlob` | Signs with the account's managed key; the signature verifies Storage signed URLs |

## Impersonation

IAM Credentials methods take `projects/-/serviceAccounts/EMAIL` and act on any account the provider created or the [IAM configuration](../../architecture/iam.md#bootstrap-config) declares. A missing account is 404.

| Caller needs | Permission (`roles/iam.serviceAccountTokenCreator` has all) |
| --- | --- |
| On the target, without `delegates` | The method's: `getAccessToken`, `getOpenIdToken`, `signJwt`, `signBlob` (`iam.serviceAccounts.*`) |
| On each delegate (caller on the first, each delegate on the next) | `iam.serviceAccounts.implicitDelegation`. Only the method's permission on a delegate is 403, as recorded |
| Last delegate on the target | The method's permission |

- `delegates` entries are `projects/-/serviceAccounts/EMAIL` or a bare email; both are recorded as accepted.
- `generateAccessToken` needs a non-empty `scope` from the [scope allowlist](../../guides/local-identity.md#oauth-scopes) (400 otherwise). `lifetime` defaults to `3600s`; `0s` and anything over `3600s` are 400. An account listed in the identity configuration's `lifetime_extension_accounts` may ask for up to `43200s`; it stands in for the organization policy `constraints/iam.allowServiceAccountCredentialLifetimeExtension`.
- The token is a local token for `serviceAccount:EMAIL`: every gcp-local service accepts it, `/v1/introspect` names the account, and it dies at `expireTime` or core restart. It needs the identity service (`-identity-config`); without it the method returns `UNIMPLEMENTED`.

```sh
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  http://127.0.0.1:4566/v1/projects/-/serviceAccounts/app@synthetic-project.iam.gserviceaccount.com:generateAccessToken \
  -d '{"scope":["https://www.googleapis.com/auth/cloud-platform"],"lifetime":"600s"}'
```

Point clients' IAM Credentials endpoint at `http://127.0.0.1:4566` ([clients](../../contributing/clients.md)).

## IAM

Pinned roles: `roles/iam.serviceAccountAdmin`, `.serviceAccountViewer`, `.serviceAccountUser`, `.serviceAccountTokenCreator`, `.serviceAccountOpenIdTokenCreator`. An account-level `serviceAccountUser` grant can read the account but not update it.

- A missing account is 404 `Unknown service account` to every caller, as recorded.
- A 403 on an account or key method reads `Permission 'PERMISSION' denied on resource '//iam.googleapis.com/projects/-/serviceAccounts/UNIQUE_ID' (or it may not exist).` with an `ErrorInfo` (`IAM_PERMISSION_DENIED`, domain `iam.googleapis.com`, metadata `permission` and `resource`). GCP appends a per-call Troubleshooter link; gcp-local doesn't. IAM Credentials keeps its own denial text.
- IAM Conditions on an account see no `resource.*` attributes: on GCP a condition on the account's `resource.type` and `resource.name` granted nothing while one on `request.time` granted (recorded).

Bodies are decoded strictly: an unknown or mistyped field, including a top-level `displayName`, is GCP's 400.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `serviceaccount-lifecycle-authorization` | 2026-10-13 | Create and get fields (`uniqueId` equals `oauth2ClientId`), denied read 403 with message and details, 404 for a missing account to denied and admin callers, unknown, top-level, and mistyped body fields, conditional grants by resource attributes and by `request.time`, user grant reads but cannot update, POST `getIamPolicy`, PATCH envelope, 409 duplicate, 400 invalid ID, key types listed (one system-managed key) and filtered, a refused key body, list order and paging, page refusals, delete then 404 |
| `iamcredentials-impersonation` | 2026-10-12 | Direct and one-hop delegated `generateAccessToken` with one-hour expiry; 403 for a delegate held with only `getOpenIdToken` (ID and access tokens); bare-email delegate; 403 without Token Creator, direct and delegated; 404 missing account; lifetime `600s` and `3600s` succeed, `3601s`, `43200s`, `0s` are 400; missing scope 400; the minted token reads the target and is denied another account; ID-token claims with and without `includeEmail`, one-hour lifetime; missing audience 400; `signJwt` header and unchanged payload; `exp` over 12 hours 400; delegated `signBlob` |

```sh
sh scripts/check/recordings.sh
```

`TestLocalServiceAccount` passes ([Terraform](../../contributing/terraform-compatibility.md)).

To re-record, get an approved `.gcp-local/serviceaccounts-scope.json` (`LIMITS` in `gcp_validation/services/security/serviceaccounts.py`); the fixture's standing `roles/iam.serviceAccountAdmin` covers the run. It creates two accounts, grants only on them (Key Admin for the fixture, User and conditional Viewer for the allowed caller), and deletes both:

```sh
.venv-tools/bin/python -m pytest tests/conformance/security/iam -q \
  --gcp-mode record --gcp-profile /absolute/path/to/.env \
  --gcp-live-scope-file /absolute/path/to/.gcp-local/serviceaccounts-scope.json \
  --gcp-allow-live --gcp-artifacts .artifacts/serviceaccounts-record
```

`iamcredentials-impersonation` runs the same way on `tests/conformance/security/iam/test_credentials.py` with a scope for `LIMITS` in `gcp_validation/services/security/iamcredentials.py`. It creates two accounts and grants only on them, so deleting them removes every grant; the fixture needs only `roles/iam.serviceAccountAdmin`. It costs nothing.

After an interrupted run, `python -m gcp_validation.services.security.serviceaccounts cleanup --profile ... --scope ... --manifest ... --allow-live` deletes only the accounts in the manifest.

## Gaps

- gRPC `google.iam.admin.v1.IAM` refuses with `UNIMPLEMENTED`: `CreateServiceAccountKey` (the key file's `token_uri` names the HTTP origin, which a gRPC request doesn't carry; create keys over REST), the deprecated IAM `SignBlob` and `SignJwt` (REST `signBlob` is the IAM Credentials API), `UpdateServiceAccount` (PUT), `TestIamPermissions`, and every other method except the [role methods](iam-roles.md).

- User-managed key create, get, and delete are modeled locally, not recorded: the sandbox organization enforces `iam.managed.disableServiceAccountKeyCreation`. Organization policy isn't enforced here, so local key creation succeeds. `TYPE_PKCS12_FILE` and `KEY_ALG_RSA_1024` are `UNIMPLEMENTED`. System-managed key validity (two years) is not recorded.
- A key file's `token_uri` names this emulator, which does not accept JWT-bearer grants: a key signs but cannot mint access tokens.
- IAM Credentials names accounts and delegates by email only: unique IDs and Google-managed accounts return `UNIMPLEMENTED`. Error message text is not recorded.
- That the last delegate needs the method's own permission on the target, rather than `implicitDelegation`, is modeled, not isolated by the recording. A scope outside the allowlist is 400 locally; GCP's answer isn't recorded.
- A `generateIdToken` token without `includeEmail` doesn't authenticate [Cloud Run](../cloudrun.md) invocation, which needs the email.
- Terraform's `impersonate_service_account` can't be pointed at gcp-local: provider 8.5.0 calls `https://iamcredentials.UNIVERSE_DOMAIN`. Google's Go `impersonate` package hardcodes `iamcredentials.googleapis.com` and needs a redirecting HTTP client ([clients](../../contributing/clients.md)).
- No undelete, disable or enable, `uniqueId` addressing, or workload identity federation.
- A `getIamPolicy` `requestedPolicyVersion` below 3 still returns conditional bindings.
