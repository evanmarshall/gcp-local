# IAM roles

IAM v1 custom roles for projects and organizations, and reads of the pinned predefined roles, over REST and gRPC `google.iam.admin.v1.IAM`. Bindings to `projects/P/roles/R` and `organizations/O/roles/R` grant through the [IAM engine](../../architecture/iam.md#custom-roles). Project roles are recorded against GCP (`iam-custom-role-lifecycle`); organization roles are modeled locally and not recorded.

## Use

Select `-services ...,iam`. Routes are on port 4566:

```sh
curl -X POST http://127.0.0.1:4566/v1/projects/PROJECT/roles \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"roleId":"appReader","role":{"title":"App reader","includedPermissions":["storage.objects.get"],"stage":"GA"}}'
```

Then grant `projects/PROJECT/roles/appReader` in any allow policy inside that project. `google_project_iam_custom_role` works with Terraform (`iam_custom_endpoint`).

## Methods

| Method | Route | Permission |
| --- | --- | --- |
| `projects.roles` and `organizations.roles` `create` | `POST /v1/{parent}/roles` | `iam.roles.create` |
| `get`, `list` | `GET /v1/{parent}/roles[/{role}]` | `iam.roles.get`, `iam.roles.list` |
| `patch` | `PATCH /v1/{parent}/roles/{role}` | `iam.roles.update` |
| `delete`, `undelete` | `DELETE /v1/{parent}/roles/{role}`, `POST ...:undelete` | `iam.roles.delete`, `iam.roles.undelete` |
| `roles.get`, `roles.list` | `GET /v1/roles[/{role}]` | None; any authenticated caller. `roles.list?parent=projects/P` lists custom roles |

Permissions are checked on the parent project or organization. gRPC serves every method except `ListRoles` without a parent (predefined roles), which returns `UNIMPLEMENTED`.

## Semantics

- `create` takes `{roleId, role}`. Role IDs match `[a-zA-Z0-9_.]{3,64}`. The response sorts `includedPermissions` and omits an `ALPHA` stage, as GCP does.
- `includedPermissions` are checked against GCP's `queryTestablePermissions` for a project, recorded in `internal/iam/roles/testable-permissions.json` (13,790 permissions): unknown or not applicable to a project is 400 "Permission P is not valid."; `NOT_SUPPORTED` is 400 "Permission P is not supported in custom roles."; `TESTING` and permissions of disabled APIs are accepted.
- `patch` applies `updateMask` (`title`, `description`, `includedPermissions`, `stage`). Without a mask every field is replaced. A stale `etag` is 409 `ABORTED`.
- `delete` is soft: the role stops granting at once, reads back with `deleted: true`, appears in `list` only with `showDeleted=true`, and can't be updated, deleted again, or recreated (400 `FAILED_PRECONDITION`).
- `undelete` restores it within 7 days. After 7 days the role is gone (404) and its ID stays reserved until 44 days after the delete. Reusing the ID first removes the old role's bindings.
- `list` takes `view` (`BASIC` omits permissions, `FULL`), `pageSize` (default 300, max 1,000), and `pageToken`, in name order. `get` takes no `view`.
- `setIamPolicy` on any resource: a custom role must exist in the resource's own project or organization ("Role (NAME) does not exist in the resource's hierarchy."); a role in a project that doesn't exist is 403; a deleted role keeps its existing members but takes no new ones ("Cannot add new grants to deleted role"); a malformed name is 400.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `iam-custom-role-lifecycle` | 2026-10-12 | Project role create, get, list (`BASIC`, `FULL`, `showDeleted`), 409 duplicate, 403 for an ungranted caller, 400 for `view` on get, unknown, unsupported, and above-project permissions, bad ID and stage; the role granted on a service account allows a read, delete denies it, undelete allows it again, a permission patch denies it; deleted-role 400s for delete, patch, and recreate; stale etag 409; policies with a deleted, missing, other-project, and malformed role |

To re-record, approve a `customroles` scope (`LIMITS` in `gcp_validation/services/security/customroles.py`) and grant the fixture identity `roles/iam.roleAdmin` for the run. Every run uses a new role ID, because deleted IDs stay reserved for weeks.

```sh
.venv-tools/bin/python -m pytest tests/conformance/security/iam/test_custom_roles.py -q \
  --gcp-mode record --gcp-profile /absolute/path/to/.env \
  --gcp-live-scope-file /absolute/path/to/custom_roles.json \
  --gcp-allow-live --gcp-artifacts .artifacts/customroles-record
```

`TestLocalProjectIamCustomRole` passes ([Terraform](../../contributing/terraform-compatibility.md)).

## Gaps

- Organization roles are not recorded. A permission that isn't testable on a project (such as `resourcemanager.projects.list`) is refused in an organization role with `UNIMPLEMENTED`, because organization-level validity isn't recorded.
- `roles.get` and `roles.list` serve only the pinned predefined roles; any other `roles/...` name is `UNIMPLEMENTED`, not 404.
- Not implemented: `queryGrantableRoles`, `queryTestablePermissions`, `queryAuditableServices`, role tag bindings, and the 300-roles-per-project quota.
- Bindings of a role past its undelete window stay in policies, inert, until the ID is reused or the project is reset; GCP removes them within 30 days.
