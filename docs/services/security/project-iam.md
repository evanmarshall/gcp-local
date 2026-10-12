# Project IAM policy

Resource Manager v1 `projects.getIamPolicy` and `projects.setIamPolicy` over REST, backed by the shared IAM engine. The scenario `project-iam-policy-binding` is recorded against GCP.

## Use

Select `-services ...,resourcemanager`. Routes are `POST /v1/projects/{project}:getIamPolicy` and `:setIamPolicy` on port 4566. Changes take effect for every provider immediately. Policy evaluation is described in [IAM](../../architecture/iam.md).

## Methods

| Method | Notes |
| --- | --- |
| `projects.getIamPolicy` | Returns `bindings`, `etag`, `version` 1 |
| `projects.setIamPolicy` | Replaces the policy; a stale `etag` is 409 `ABORTED` |

## IAM

`resourcemanager.projects.getIamPolicy` and `.setIamPolicy`. Pinned role: `roles/resourcemanager.projectIamAdmin`.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `project-iam-policy-binding` | 2026-10-12 | Policy fields, 403 before grant, write changes the etag and keeps other bindings, stale etag 409 |

Binding contents are not compared; sandbox and local projects hold different grants.

```sh
sh scripts/check/recordings.sh
```

`TestLocalProjectIamPolicy` passes ([Terraform](../../contributing/terraform-compatibility.md)).

To re-record, get an approved `.gcp-local/projectiam-scope.json` (`LIMITS` in `gcp_validation/services/security/projectiam.py`) and a temporary `roles/resourcemanager.projectIamAdmin` on the fixture identity. That role can change any binding: back up the policy first and remove the grant afterwards.

```sh
.venv-tools/bin/python -m pytest tests/conformance/security/resourcemanager -q \
  --gcp-mode record --gcp-profile /absolute/path/to/.env \
  --gcp-live-scope-file /absolute/path/to/.gcp-local/projectiam-scope.json \
  --gcp-allow-live --gcp-artifacts .artifacts/projectiam-record
```

## Gaps

- Conditional bindings, audit configs, and policy versions above 1 are not recorded.
- No project create or service enablement.
