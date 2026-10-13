# Organization Policy

Organization Policy v2 policies on organizations, folders, and projects, over REST and gRPC (`google.cloud.orgpolicy.v2.OrgPolicy`), inherited down the hierarchy and enforced for 6 constraints at the mutation, after IAM allows the request. Policies declared in the [IAM config](../../architecture/iam.md#bootstrap-config) and policies written through the API are the same records. What a project's callers see of the sandbox organization's policies, and the refusals of 3 of the constraints, are recorded against GCP. Policy writes are modeled locally: the sandbox grants no organization roles.

## Use

Select `-services ...,orgpolicy`. Declare policies under `org_policies` in `-iam-config`, in the API's JSON form, on a declared organization, folder, or project:

```json
"org_policies": [
  {"name": "organizations/123456789012/policies/iam.allowedPolicyMemberDomains",
   "spec": {"rules": [{"values": {"allowedValues": ["C0123abcd"]}}]}},
  {"name": "projects/my-project/policies/gcp.resourceLocations",
   "spec": {"rules": [{"values": {"allowedValues": ["in:us-locations"]}}]},
   "dryRunSpec": {"rules": [{"values": {"allowedValues": ["us-east1"]}}]}}
]
```

`iam.allowedPolicyMemberDomains` admits the members of the directory's `customer_id` ([groups and domains](../../architecture/iam.md#groups-and-domains)). Writing policies needs `roles/orgpolicy.policyAdmin`; GCP grants it only on an organization, gcp-local on any node.

```sh
curl -s -X POST -H "Authorization: Bearer $TOKEN" http://127.0.0.1:4566/v2/projects/my-project/policies \
  -d '{"name": "projects/my-project/policies/storage.uniformBucketLevelAccess", "spec": {"rules": [{"enforce": true}]}}'
```

## Methods

21 methods: 7 on each of `projects`, `folders`, and `organizations`. Custom constraints (`organizations.customConstraints.*`) are `UNIMPLEMENTED`.

| Method | Permission | Notes |
| --- | --- | --- |
| `constraints.list` | `orgpolicy.constraints.list` on the parent | The 6 enforced constraints, as GCP defines them, named under the parent as the request names it; `pageSize` and `pageToken` are ignored, as GCP ignores them |
| `policies.list` | `orgpolicy.policies.list` on the parent | Policies set on the node itself; page options ignored |
| `policies.get` | `orgpolicy.policy.get` | 404 `POLICY_NOT_FOUND` when the node sets none |
| `policies.getEffectivePolicy` | `orgpolicy.policy.get` | The merged hierarchy without etags or conditions; an unset constraint reports its default |
| `policies.create` | `orgpolicy.policies.create` on the parent | 409 `ALREADY_EXISTS` for a second policy |
| `policies.patch` | `orgpolicy.policies.update` | `updateMask` of `spec` and `dryRunSpec`, with or without a `policy.` prefix (Terraform sends one); none replaces both. A stale `etag` is 409 `ABORTED` |
| `policies.delete` | `orgpolicy.policies.delete` | Optional `etag` |

A project is named by ID or number; responses name it by number. Pinned roles: `roles/orgpolicy.policyAdmin`, `roles/orgpolicy.policyViewer`; `roles/storage.admin` also carries `orgpolicy.policy.get`.

## Enforced constraints

| Constraint | Checked at | Refusal | Evidence |
| --- | --- | --- | --- |
| `iam.allowedPolicyMemberDomains` | Every `setIamPolicy`, for the members it adds | Storage: 412 `conditionNotMet` "One or more users named in the policy do not belong to a permitted customer." IAM: 400 `FAILED_PRECONDITION` with that message and a `PreconditionFailure` (`subject` `orgpolicy:projects/-/serviceAccounts/UNIQUE_ID?configvalue=MEMBER`). Other services return the IAM form naming their resource | Recorded for Storage buckets and service accounts |
| `storage.uniformBucketLevelAccess` | Bucket create and patch | 412 `conditionNotMet` "Request violates constraint 'constraints/storage.uniformBucketLevelAccess'" for a disabled setting; an omitted setting becomes enabled | Recorded |
| `iam.managed.disableServiceAccountKeyCreation` | Service account key create | 400 `FAILED_PRECONDITION` "Operation denied by org policy on resource 'projects/-/serviceAccounts/UNIQUE_ID': [...]" with ErrorInfo `CUSTOM_ORG_POLICY_VIOLATION` | Recorded |
| `iam.disableServiceAccountKeyCreation` | Service account key create | 400 `FAILED_PRECONDITION` "Key creation is not allowed on this service account." | Modeled |
| `storage.publicAccessPrevention` | Buckets whose prevention is `inherited` | Public members refused with the enforced-prevention 412; existing public grants grant nothing | Modeled |
| `gcp.resourceLocations` | Storage bucket create | 412 `conditionNotMet` "Request violates constraint 'constraints/gcp.resourceLocations'" | Modeled |

Members of the directory customer: users of a declared domain, declared groups, `domain:` of a declared domain, and service accounts of indexed projects (Google-managed ones by project number). `allUsers`, `allAuthenticatedUsers`, and everyone else belong to none. Members a policy already has are not checked, so the constraint isn't retroactive.

A policy for any other constraint is refused with `UNIMPLEMENTED`, through the API and in the IAM config, and so is `getEffectivePolicy` for one: storing it would claim enforcement that never happens.

## Evaluation

- **Hierarchy.** From the organization down: a node without a policy inherits; `reset` restores the constraint default; `inheritFromParent` (list constraints) merges the node's rules with its parent's; any other spec replaces them. A denied value or `denyAll` refuses, `allowAll` or an allowed value admits, allowed values that don't match refuse.
- **Values.** `gcp.resourceLocations` compares without case and expands `in:us-locations`, `in:eu-locations`, `in:asia-locations`, and `in:REGION-locations`; a multi-region value doesn't cover its regions. `iam.allowedPolicyMemberDomains` takes customer IDs, with or without `is:`.
- **Conditions** compile against the tag functions (`resource.matchTag`, `matchTagId`, `hasTagKey`, `hasTagKeyId`). Tags aren't modeled, so each is false and only negated tag conditions hold.
- **Dry run.** A `dryRunSpec` is evaluated beside the spec (falling back to a node's spec where it has none) and only traced: it never refuses.
- **Precedence.** IAM first: a caller IAM denies gets the IAM denial, not the constraint (recorded for key creation and bucket policies). A bucket's own enforced public access prevention refuses before domain-restricted sharing (`storage-public-access-enforced`). A key create body with an unknown field is refused before the key creation constraint (`serviceaccount-lifecycle-authorization`). No other cross-family order is modeled.
- **Traces.** Every check adds a signal to the request's [decision record](../../architecture/iam.md#decision-records) (`family` `org-policy`, `verdict` `allow`, `violation`, `dry-run allow`, or `dry-run violation`), apart from the IAM checks. Storage and IAM trace their checks; `iam.allowedPolicyMemberDomains` on other services' policies is enforced without a signal.

## VPC Service Controls

Not enforced. A perimeter declared under `vpcsc_perimeters` in the IAM config adds an `inside-perimeter` signal to the decision record of each request on its resources, and nothing is refused. Enforcing would need access levels, ingress and egress rules, and each request's network origin, which local requests don't carry. GCP refuses a crossing with 403 "Request is prohibited by organization's policy." and a `vpcServiceControlsUniqueIdentifier`. Not recorded: the sandbox has no access policy, which is an organization-level change.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `orgpolicy-effective-visibility` | 2026-10-13 | The sandbox project's effective policy for the 6 constraints (by ID and number), 404 for a policy the project doesn't set, and the 403s of every list, write, and organization and folder read for a caller with only `orgpolicy.policy.get` |
| `orgpolicy-project-viewer` | 2026-10-13 | With `roles/orgpolicy.policyViewer` granted for the run: the 6 constraint definitions in order, ignored page options, and an empty policy list |
| `orgpolicy-constraint-enforcement` | 2026-10-13 | A bucket create with uniform access disabled refused, an omitted setting enabled, `allUsers` and `allAuthenticatedUsers` refused on the bucket's policy (403 for the denied caller), uniform access disable refused; `allUsers` refused on a service account's policy, key creation 403 for the denied caller and refused by the managed constraint |

`TestLocalOrgPolicyProject` sets a project `gcp.resourceLocations` policy with `google_org_policy_policy`, has it refuse a bucket, widens it, and imports it ([Terraform](../../contributing/terraform-compatibility.md)).

## Gaps

- Not recorded: every policy write, `folders` and `organizations` success paths, and the refusals of `storage.publicAccessPrevention`, `gcp.resourceLocations`, and the legacy key creation constraint. The sandbox organization doesn't enforce the first two, and writes need organization roles.
- `gcp.resourceLocations` is checked only on Storage bucket creates: a key ring, repository, queue, or instance in a disallowed location is created locally, while GCP refuses it.
- Not modeled: custom and other managed constraints, `alternate` specs, rule `parameters`, principal-set values of `iam.allowedPolicyMemberDomains`, `under:` values, other value groups, tags, and `getEffectivePolicy` of constraints outside the 6 (GCP returns their defaults).
- Bucket metadata keeps `publicAccessPrevention: inherited` under an enforcing organization; GCP's report is not recorded.
