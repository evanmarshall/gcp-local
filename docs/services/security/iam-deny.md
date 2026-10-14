# IAM deny policies

IAM v2 deny policies on organizations, folders, and projects, over REST and gRPC (`google.iam.v2.Policies`). The [IAM engine](../../architecture/iam.md#deny-policies) checks every deny policy on a request's ancestry, and a matching rule refuses the permission whatever the allow bindings grant. Policies declared in the [IAM config](../../architecture/iam.md#bootstrap-config) and policies written through the API are the same records. The API surface, its refusals, and enforcement over a custom-role grant (directly, with an exemption, and through a group) are recorded against GCP.

## Use

Select `-services ...,iam`. Declare policies under `deny_policies` in `-iam-config`, in the API's JSON form, named on a declared organization, folder, or project:

```json
"deny_policies": [
  {"name": "policies/cloudresourcemanager.googleapis.com%2Fprojects%2Fmy-project/denypolicies/no-keys",
   "rules": [{"denyRule": {
     "deniedPrincipals": ["principalSet://goog/group/contractors@example.com"],
     "deniedPermissions": ["iam.googleapis.com/serviceAccountKeys.create"]}}]}
]
```

Writing policies needs `roles/iam.denyAdmin`; GCP grants it only on an organization, gcp-local on any node.

```sh
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:4566/v2/policies/cloudresourcemanager.googleapis.com%2Fprojects%2Fmy-project/denypolicies?policyId=no-keys" \
  -d '{"rules": [{"denyRule": {"deniedPrincipals": ["principalSet://goog/public:all"], "deniedPermissions": ["iam.googleapis.com/serviceAccountKeys.create"]}}]}'
```

## Methods

6 methods. The attachment point is URL-encoded in one path segment; a project is named by ID or number, and responses name it by number.

| Method | Permission | Notes |
| --- | --- | --- |
| `policies.listPolicies` | `iam.denypolicies.list` | The node's policies in creation order, without rules. `pageSize` is ignored, as GCP ignores it; there is never a next page |
| `policies.get` | `iam.denypolicies.get` | 404 `POLICY_NOT_FOUND` |
| `policies.createPolicy` | `iam.denypolicies.create` | `policyId` required (400 without it, or outside `[a-z][a-z0-9-.]`, 3 to 63 characters); 409 `POLICY_ALREADY_EXISTS` |
| `policies.update` | `iam.denypolicies.update` | Replaces display name, annotations, and rules. A stale `etag` is 409 `ABORTED` `INVALID_ETAG`; no etag writes unconditionally |
| `policies.delete` | `iam.denypolicies.delete` | Optional `etag` |
| `policies.operations.get` | `iam.denypolicies.get` | |

Writes answer with a finished operation as GCP does: `name`, `metadata` (`PolicyOperationMetadata`), and `response`, without `done`; `operations.get` returns `done: true` and the response without metadata. A deleted policy's response has `deleteTime` and no `uid`. Denials are 403 "Permission iam.googleapis.com/denypolicies.list denied on resource cloudresourcemanager.googleapis.com/projects/P". A kind other than `denypolicies` is 400. `managingAuthority` is `UNIMPLEMENTED`.

## Rules

| Field | Accepted | Refused (400, as GCP) |
| --- | --- | --- |
| Principals | `principal://goog/subject/EMAIL`, `principal://iam.googleapis.com/projects/-/serviceAccounts/EMAIL`, `principalSet://goog/group/EMAIL` of a [directory](../../architecture/iam.md#groups-and-domains) group, `principalSet://goog/public:all` (denied list only) | Allow-policy forms (`user:`, `serviceAccount:`, `domain:`), unknown forms, a group outside the directory, `public:all` as an exception |
| Permissions | `SERVICE.googleapis.com/RESOURCE.VERB` and `SERVICE.googleapis.com/RESOURCE.*` | v1 names, `SERVICE.googleapis.com/*`, names that aren't permissions |
| Denial condition | CEL tag functions (`matchTag`, `matchTagId`, `hasTagKey`, `hasTagKeyId`) | Anything else, with `INVALID_CONDITION` |

- GCP accepts a policy without rules and rules without principals or permissions; they deny nothing.
- Limits per attachment point: 500 policies, 500 rules, 500 groups, and 2,500 principals across its policies, counted as distinct identifiers. Exceeding one is 429 `RESOURCE_EXHAUSTED` `QUOTA_EXCEEDED` naming the usage.
- Not modeled: workforce and workload identity pool principals, `principalSet://goog/cloudIdentityCustomerId/`, `deleted:` principals, and other wildcard forms (all `UNIMPLEMENTED`); whether a user or service account named by a principal exists (GCP refuses a missing one as "not found"); GCP's list of permissions supported in deny policies (any testable permission is accepted).
- Resources carry no tags, so `matchTag` is false: a rule conditioned on it never applies, and its negation always does.

## Evaluation

Deny policies on every ancestor (project, folders, organization) are checked before allow bindings. A rule applies when it covers the permission (not in its `exceptionPermissions`), names the caller directly or through a group, doesn't exempt the caller, and its condition is true or can't be evaluated. An applying rule refuses the permission; services report it as they report a missing permission, and those whose message ends "(or it may not exist)." end it "due to an IAM deny policy." instead (recorded for IAM service accounts). The decision record's check names the rule (`denied_by`: "rule 0 of policies/.../denypolicies/ID") and lists every deny policy considered.

GCP applied each recorded change by the next read; gcp-local applies it at once.

## Evidence

- `iam-deny-policy-lifecycle` (2026-10-14): the denied identity's 403; create, the operation, get by ID and number, list without rules, duplicate create, stale-etag update and delete, update to the accepted rule shapes, the refusals above (permissions, principals, condition, 501 rules, policy ID, kind), delete, 404 after delete.
- `iam-deny-policy-enforcement` (2026-10-14): a project custom role granting `iam.serviceAccounts.get` and `getIamPolicy` on a service account; a deny rule on `get` for the grantee refuses it with "due to an IAM deny policy" while `getIamPolicy` still works; an exemption restores it; a rule on `principalSet://goog/group/` of a group holding the grantee refuses it again; delete restores it.
- `TestLocalIamDenyPolicy`: `google_iam_deny_policy` create, update in place, import, destroy ([Terraform](../../contributing/terraform-compatibility.md)).
- Not recorded: folder and organization attachment points (the sandbox fixture may attach only to its project), gRPC.
