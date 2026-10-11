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

## Explicit gaps

- No cloud recording covers these methods yet; nothing claims recorded
  conformance (tracked under the recording backlog issues).
- Keys do not sign JWTs or verify signatures; use the identity service for
  ID-token minting.
- Undelete, short-lived credentials (signJwt/signBlob), and workloads
  identity federation are not modeled.

