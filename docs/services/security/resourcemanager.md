# Resource Manager

Resource Manager v3 organizations, folders, and projects over REST and gRPC (`google.cloud.resourcemanager.v3.Folders`, `Organizations`, `Projects`), plus v1 project reads. Folders are created, renamed, moved, soft-deleted, and undeleted here; organizations and projects come from the [bootstrap config](../../architecture/iam.md#bootstrap-config), and projects can move. Bindings on an organization or folder grant on everything under it. What callers without organization roles observe is recorded against GCP (`resourcemanager-hierarchy-visibility`); every success path that needs organization roles is modeled locally and not recorded. Project IAM v1 is in [project IAM](project-iam.md).

## Use

Select `-services ...,resourcemanager` and declare organizations, folders, and projects under `resources` in `-iam-config`. A folder created through the API gets a random 12-digit ID.

```sh
curl -s -X POST -H "Authorization: Bearer $TOKEN" http://127.0.0.1:4566/v3/folders \
  -d '{"parent": "organizations/123456789012", "displayName": "Team A"}'
# {"name": "operations/operation-...", "done": false, ...}; poll GET /v3/operations/operation-...
```

## Methods

29 methods: 24 v3 and 5 v1. Every v3 method is also served over gRPC; `google.longrunning.Operations/GetOperation` serves `operations/*` names.

| Method | Permission | Notes |
| --- | --- | --- |
| `organizations.get` | `resourcemanager.organizations.get` | |
| `organizations.search` | `resourcemanager.organizations.get` on each | Returns the organizations the caller may get; a `query` is a gap |
| `organizations.getIamPolicy`, `setIamPolicy` | `resourcemanager.organizations.getIamPolicy`, `.setIamPolicy` | |
| `folders.create` | `resourcemanager.folders.create` on the parent | Operation |
| `folders.get` | `resourcemanager.folders.get` | Either state |
| `folders.list` | `resourcemanager.folders.list` on the parent | ACTIVE folders unless `showDeleted=true` |
| `folders.search` | `resourcemanager.folders.get` on each | Exact `displayName`, `parent`, `state` terms joined by `AND` |
| `folders.patch` | `resourcemanager.folders.update` | Operation; only `display_name`; an absent `updateMask` means it |
| `folders.move` | `resourcemanager.folders.move` on the folder and the destination | Operation |
| `folders.delete` | `resourcemanager.folders.delete` | Operation; `DELETE_REQUESTED`, kept until undeleted |
| `folders.undelete` | `resourcemanager.folders.undelete` | Operation |
| `folders.getIamPolicy`, `setIamPolicy` | `resourcemanager.folders.getIamPolicy`, `.setIamPolicy` | |
| `projects.get` (v3 and v1) | `resourcemanager.projects.get` | By ID or number; v1 reports `parent` as `{type, id}` |
| `projects.list` | `resourcemanager.projects.list` on the parent | |
| `projects.search` | `resourcemanager.projects.get` on each | Exact `parent`, `id`, `projectId`, `state` terms |
| `projects.move` | `resourcemanager.projects.move` on the project and the destination | Operation |
| `projects.getIamPolicy`, `setIamPolicy` (v3) | `resourcemanager.projects.getIamPolicy`, `.setIamPolicy` | |
| `testIamPermissions` on organizations, folders, projects (v3), projects (v1) | none | The tested permissions the caller holds; a node that doesn't exist holds none |
| `operations.get` (v3 and v1) | the caller started it | Another caller's or an unknown operation is 400 |

`getIamPolicy` and `setIamPolicy` on folders and organizations follow [project IAM](project-iam.md): requested versions, `updateMask`, and `auditConfigs` (stored, not enforced). Pinned roles: `roles/resourcemanager.organizationAdmin`, `organizationViewer`, `folderAdmin`, `folderCreator`, `folderEditor`, `folderIamAdmin`, `folderMover`, `folderViewer`, `projectMover`, `projectIamAdmin`.

## Hierarchy

- The IAM engine walks every resource through its project, folders, and organization ([IAM](../../architecture/iam.md#evaluation)). A project or folder that moves changes its ancestry at once and keeps its own policy.
- Mutations return operations with `done: false`; they report done 250 ms later. Business-rule failures finish the operation with `FAILED_PRECONDITION`: a display name an ACTIVE sibling has ("Requested entity already exists"), more than 10 folder levels ("Folder reached maximum depth."), and deleting a folder with an ACTIVE folder or a project under it.
- Moving a folder under itself or a descendant, and changing a folder in `DELETE_REQUESTED`, is a synchronous `FAILED_PRECONDITION`.
- Request bodies decode strictly: an unknown field is GCP's 400; `tags` is a gap unless empty.

## Errors

Recorded (`resourcemanager-hierarchy-visibility`):

- A denial on a named node is 403 "Permission 'P' denied on resource '//cloudresourcemanager.googleapis.com/R' (or it may not exist)." with ErrorInfo `IAM_PERMISSION_DENIED`, domain `cloudresourcemanager.googleapis.com`, and metadata `permission` and `resource`. A node that doesn't exist gets the same 403. GCP appends a troubleshooter link; gcp-local doesn't.
- `projects.list` denied, `projects.move` denied, `folders.move` of an unknown folder, and `folders.create` or `folders.list` without a parent are 403 "The caller does not have permission" without details.
- A bad page token, an unknown operation, and a malformed move destination are 400 "Request contains an invalid argument."; a negative page size is 400 "field [pageSize] has issue [invalid page size]"; `projects.list` without an organization or folder parent is 400 "field [parent] has issue [invalid parent name]".

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `resourcemanager-hierarchy-visibility` | 2026-10-13 | Project get (v3, v1) with its organization parent; project search; permission tests; the denial shapes of organization get and IAM, folder list and create, every folder method on `folders/1`, project list and move; empty organization and folder searches; page and operation refusals |

The scenario creates and changes nothing, costs nothing, needs no temporary grant, and runs in the weekly drift job. Its create and move requests run only after `testIamPermissions` shows the caller lacks the permissions they need.

```sh
.venv-tools/bin/python -m pytest tests/conformance/security/resourcemanager/hierarchy -q \
  --gcp-mode record --gcp-profile=/absolute/path/to/.env \
  --gcp-live-scope-file=/absolute/path/to/scopes/security/resourcemanager/hierarchy/hierarchy.json \
  --gcp-allow-live --gcp-artifacts=.artifacts/hierarchy-record
```

`TestLocalFolderAndFolderIam` passes ([Terraform](../../contributing/terraform-compatibility.md)).

## Gaps

- Not recorded: every success path that needs organization roles, which the sandbox doesn't have: folder create, get, list, search, rename, move, delete, and undelete; folder and organization IAM reads and writes; organization get and search results; project move; inheritance from organizations and folders; and the business-rule failures above. They are modeled locally and tested in `internal/providers/security/resourcemanager`.
- `folders.move` checks the folder (its current parent included) and the destination; GCP documents the current and the new parent.
- Not implemented: project create, update, delete, and undelete (`UNIMPLEMENTED`); folder tags and capabilities; organization domains (`displayName`, `directoryCustomerId`, `domain:` queries); project labels; wildcard and other search operators; purging a deleted folder after 30 days; deny policies attached to organizations and folders.
- Organizations and folders aren't project-scoped: a service reset doesn't touch them.
- A Terraform `google_project` move isn't tested: project create isn't modeled.
