# Project IAM policy (Resource Manager v1)

The resourcemanager provider serves `POST /v1/projects/{project}:getIamPolicy`
and `:setIamPolicy`, backed by the shared IAM engine. The pinned Terraform case
`TestAccProjectIamPolicy` drives `google_project_iam_policy` through
`resource_manager_custom_endpoint`.

## Recorded scenario

`project-iam-policy-binding` was recorded and revalidated against real GCP on
2026-10-11, and the local provider matches it in comparison mode
(`sh scripts/check_rest_recordings.sh` in Moo/Linux; CI runs it in the
`pubsub-backend` job). The fixture identity reads the project policy, adds one
unconditional `roles/iam.serviceAccountViewer` binding for the allowed
identity, and removes it again. Every pre-existing binding is checked before
and after; cleanup removes only that role/member pair and fails if any prior
binding is missing. Observed and compared:

- policy fields are `bindings`, `etag`, and `version` (1)
- the denied identity, and the allowed identity before any grant, get 403
- a write changes the etag and preserves every other binding
- a write with the pre-change etag is 409 `ABORTED`

The recording fixed one local divergence: stale-etag writes returned 500
instead of 409 `ABORTED` (the service-account policy surface had the same bug).
Binding contents are not compared, because the sandbox and local projects hold
different grants.

Re-recording needs an approved `.gcp-local/projectiam-scope.json` (`LIMITS` in
`gcp_validation/projectiam.py`) and a temporary
`roles/resourcemanager.projectIamAdmin` grant on the fixture identity. That role
can change any project binding, so back up the policy first and remove the
grant afterwards:

```sh
.venv-tools/bin/python -m pytest tests/conformance/projectiam -q \
  --gcp-mode record --gcp-profile /absolute/path/to/.env \
  --gcp-live-scope-file /absolute/path/to/.gcp-local/projectiam-scope.json \
  --gcp-allow-live --gcp-artifacts .artifacts/projectiam-record
```

## Gaps

- `google_project_iam_binding`/`google_project_iam_member`, conditional
  bindings, audit configs, and policy versions above 1 are not recorded.
- Project service enablement is not modeled.
