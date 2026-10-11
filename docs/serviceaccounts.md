# IAM service accounts (control plane)

Modeled surface for the IAM v1 service-account API: accounts, per-account
IAM, and synthesized key records. The endpoint lives at
`/v1/projects/{project}/serviceAccounts` and the provider is selected with
`--services=...,iam`.

## What is modeled

- Accounts: create (`{accountId, serviceAccount}` envelope), get, list,
  update, delete, and `getIamPolicy`/`setIamPolicy`.
- Keys: create returns a synthesized `privateKeyData`; list and delete by
  key id. Key material is local only and signs nothing; token minting stays
  with the identity service.
- Predefined roles pinned: `roles/iam.serviceAccountAdmin`,
  `roles/iam.serviceAccountViewer` (alongside the existing
  `iam.serviceAccountUser` and `iam.serviceAccountTokenCreator`).
- Terraform: the pinned `TestAccServiceAccount` case drives
  `google_service_account` through `iam_custom_endpoint`.

## Recorded scenario

`serviceaccount-lifecycle-authorization` was recorded and revalidated against
real GCP on 2026-10-11, and the local provider matches it in comparison mode
(`sh scripts/check_rest_recordings.sh` in Moo/Linux; CI runs it in the
`pubsub-backend` job). The fixture identity creates one account; the allowed
identity receives `roles/iam.serviceAccountUser` on that account only; the
denied identity has no grant. Observed and compared:

- create/get fields: `name` is `projects/PROJECT_ID/serviceAccounts/EMAIL`,
  `uniqueId` is 21 digits and equals `oauth2ClientId`, an `etag` is present,
  and `disabled` is omitted while false; accounts are addressed by email
- denied read is 403; after the account-level grant propagates the allowed
  caller can read but not update (403)
- `getIamPolicy` is a POST; an account with no bindings returns only `etag`
- PATCH takes `{serviceAccount, updateMask}` and returns only `name` and the
  masked fields
- duplicate create is 409 `ALREADY_EXISTS`; an invalid id is 400
- delete returns 200 `{}`, then 404 `NOT_FOUND`

The recording fixed six local divergences: ID-keyed names and paths, the
`disabled` field, missing `etag`, PATCH ignoring the real envelope (it would
have blanked the display name), GET-only `getIamPolicy`, and 204 deletes.

To re-record, use an approved `.gcp-local/serviceaccounts-scope.json`
(`LIMITS` in `gcp_validation/serviceaccounts.py`) and a temporary
project-level `roles/iam.serviceAccountAdmin` grant on the fixture identity:

```sh
.venv-tools/bin/python -m pytest tests/conformance/serviceaccounts -q \
  --gcp-mode record --gcp-profile /absolute/path/to/.env \
  --gcp-live-scope-file /absolute/path/to/.gcp-local/serviceaccounts-scope.json \
  --gcp-allow-live --gcp-artifacts .artifacts/serviceaccounts-record
```

Interrupted runs: `.venv-tools/bin/python -m gcp_validation.serviceaccounts cleanup --profile ... --scope ... --manifest ... --allow-live`
deletes only the account named in the owner-only manifest under
`~/.local/state/gcp-local/PROJECT/serviceaccounts/`.

## Explicit gaps

- Keys are not recorded: creating one would mint real credential material,
  so the scenario's scope allows zero keys. Local keys are synthesized and
  sign nothing; use the identity service for ID-token minting.
- Undelete, disable/enable, short-lived credentials (signJwt/signBlob),
  `projects/-` and uniqueId addressing, and workload identity federation are
  not modeled.
