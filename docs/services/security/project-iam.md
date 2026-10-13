# Project IAM policy

Resource Manager v1 `projects.getIamPolicy`, `projects.setIamPolicy`, and `projects.testIamPermissions` over REST, backed by the shared IAM engine. The scenarios `project-iam-policy-binding` and `project-iam-policy-versions` are recorded against GCP. Folders, organizations, and the v3 project methods are in [Resource Manager](resourcemanager.md).

## Use

Select `-services ...,resourcemanager`. Routes are `POST /v1/projects/{project}:getIamPolicy` and `:setIamPolicy` on port 4566. Changes take effect for every provider immediately. Policy evaluation is described in [IAM](../../architecture/iam.md).

## Methods

| Method | Notes |
| --- | --- |
| `projects.getIamPolicy` | Returns `bindings`, `auditConfigs`, `etag`, and `version` at `options.requestedPolicyVersion` ([versions](../../architecture/iam.md#policy-versions-and-audit-configs)) |
| `projects.setIamPolicy` | Replaces the fields `updateMask` names: `bindings`, `auditConfigs`, `version`, `etag`; the default is `bindings,etag`. A named field the policy omits is cleared, so `bindings` without bindings removes every binding. A stale `etag` is 409 `ABORTED` whether or not the mask names it. A policy with a condition takes only version 3 writes |
| `projects.testIamPermissions` | The tested permissions the caller holds; needs no permission; not recorded on v1 |

## IAM

`resourcemanager.projects.getIamPolicy` and `.setIamPolicy`. Pinned role: `roles/resourcemanager.projectIamAdmin`.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `project-iam-policy-binding` | 2026-10-12 | Policy fields, 403 before grant, write changes the etag and keeps other bindings, stale etag 409 |
| `project-iam-policy-versions` | 2026-10-13 | Requested versions 0 to 3, 7, and -1 on a conditional policy; version-1 writes refused while it holds a condition; `auditConfigs` set, kept, and cleared under each mask; stale etag outside the mask; a `version` mask |

Binding contents are not compared; sandbox and local projects hold different grants.

```sh
sh scripts/check/recordings.sh
```

`TestLocalProjectIamPolicy` passes ([Terraform](../../contributing/terraform-compatibility.md)).

To re-record, get approved scopes (`LIMITS` in `gcp_validation/services/security/projectiam.py` and `projectiam_versions.py`) and a temporary `roles/resourcemanager.projectIamAdmin` on the fixture identity. That role can change any binding: back up the policy first and remove the grant afterwards. `project-iam-policy-versions` refuses to start while the project policy holds conditions or audit configs, and removes only its own binding and audit config.

```sh
.venv-tools/bin/python -m pytest tests/conformance/security/resourcemanager -q \
  --gcp-mode record --gcp-profile /absolute/path/to/.env \
  --gcp-live-scope-file /absolute/path/to/.gcp-local/projectiam-scope.json \
  --gcp-allow-live --gcp-artifacts .artifacts/projectiam-record
```

## Gaps

- Audit configs are stored and returned, not enforced: no audit logs are written.
- Conditional project bindings are recorded only for versions and writes, not for what they grant.
- No project create or service enablement.
