# IAM engine

`internal/iam` authorizes every provider request. Providers register a permission plan per method, resolve the request's target and features, and call `Authorize` before any mutation. Engine unit tests cover the evaluator; only recorded scenarios establish that a service's IAM matches GCP ([coverage](../coverage.md)).

## Permission plans

Each provider registers its plans with `Engine.Register` when it starts (Storage and Pub/Sub keep them in `permissions.go`).

- A plan has baseline permissions plus permissions added by named features. Example: Storage object insert requires the `object_exists` feature, and replacement adds `storage.objects.delete` to `storage.objects.create`.
- An omitted feature is a coverage error, not an assumption.
- `Baseline` can authorize internal state resolution but reports `complete: false`. Providers must call `Authorize` before mutation.
- Extra checks can't remove baseline checks. Request validation and service-specific error precedence stay in the provider.
- `sh scripts/go.sh run ./tools/permission-plans -service secretmanager` exports registered plans as JSON. Secret Manager is the only service it supports.

## Evaluation

- The engine walks the target (or its validated anchor, such as a bucket) through the resource index to project, folder, and organization, and unions allow bindings.
- Caller project and resource project are separate. A role in the caller's project doesn't grant access to a foreign resource.
- Missing parents, cycles, conflicting ownership, and unverified target shapes fail closed.
- Members: exact `user:` and `serviceAccount:`, `allUsers`, `allAuthenticatedUsers`, and the [directory's](#groups-and-domains) `group:`, `principalSet://goog/group/`, and `domain:`. Absent credentials never become an administrator; unknown or expired tokens stay unauthenticated.
- OAuth scopes are checked before policy: when none of the token's scopes covers every permission the plan resolves to, the call is 403 `ACCESS_TOKEN_SCOPE_INSUFFICIENT`. `cloud-platform` covers every plan; the [scope table](../guides/local-identity.md#oauth-scopes) lists the rest. Scopes outside the allowlist cover nothing.
- Deny rules run before allow bindings, and principal access boundaries run before both ([below](#conditions-deny-rules-and-boundaries)).
- `MarkUnsupported` stores known advanced configuration as a fail-closed marker; a matching allow can't bypass it.
- Not supported: custom roles, roles without a committed snapshot, and `principal://` or `principalSet://` members other than groups (workforce and workload identity pools, `cloudIdentityCustomerId`), which are refused with `UNIMPLEMENTED`.

Policies are stored once per owning service and scope, returned as copies, and replaced only with a matching etag. A service reset clears that service's resource policies for the scope and keeps project and organization policy.

A resource that leaves the index takes its policy with it, whichever provider deleted it, so a resource recreated with the same name starts with an empty policy, as in GCP. Deleting a service account also rewrites every grant to it as `deleted:serviceAccount:EMAIL?uid=UNIQUE_ID`; an account recreated with the same email has a new unique ID and inherits nothing ([GCP](https://cloud.google.com/iam/docs/service-accounts-delete-undelete)). Policies accept that form back, so read-modify-write keeps working. Undelete isn't modeled.

## Conditions, deny rules, and boundaries

Only allow-binding conditions on Storage are recorded against GCP (`storage-iam-conditions`). Deny rules and principal access boundaries are enforced locally and not recorded.

**Conditions (CEL).** Expressions see `resource.name`, `resource.type`, `resource.service`, and `request.time`. Providers supply the `resource.*` values through `iam.Target.Attributes`; only Storage and Cloud SQL do. An attribute the provider didn't supply is absent, so reading it is an evaluation error, not `""`. An allow binding grants only when its condition returns `true`; `false`, an error, or a non-boolean grants nothing. So `!resource.name.startsWith("x")` grants nothing on a service without `resource.name`. GCP documents the same outcome: a condition part that reads an attribute the resource doesn't provide "is never interpreted as granting access" ([attribute reference](https://cloud.google.com/iam/docs/conditions-attribute-reference)).

**Deny rules** (`denyRules` on a v3 policy) are checked at every node of the target's ancestry, each rule independently:

- A rule applies when it lists the permission (not in `exceptionPermissions`), names the caller in `deniedPrincipals`, doesn't name the caller in its own `exceptionPrincipals`, and its `denialCondition` is absent, `true`, or can't be evaluated. GCP: "If the condition evaluates to `true` or cannot be evaluated, the deny rule applies" ([deny overview](https://cloud.google.com/iam/docs/deny-overview)). An exception exempts the caller from that rule only.
- Any applying rule denies, regardless of allow bindings. An anonymous caller gets 401, others 403.
- Principals: `principalSet://goog/public:all` (every caller, including anonymous), `principal://goog/subject/EMAIL`, `principal://iam.googleapis.com/projects/-/serviceAccounts/EMAIL`, `principalSet://goog/group/EMAIL` for a [directory](#groups-and-domains) group, and the local-only `user:EMAIL` / `serviceAccount:EMAIL` / `group:EMAIL`. A group outside the directory is refused at write time with 400 "Group EMAIL does not exist.", so a stored rule never names a group it can't resolve. Anything else, such as `domain:` or `principalSet://goog/cloudIdentityCustomerId/`, is refused with `UNIMPLEMENTED`.
- Permissions: v1 (`storage.objects.get`) or v2 (`storage.googleapis.com/objects.get`) names, with a trailing `*` as a prefix match. A name outside the pinned roles is refused with `UNIMPLEMENTED`.
- Deny rules are stored on the resource's own policy. GCP attaches separate deny policies to organizations, folders, and projects; that API isn't implemented.

**Principal access boundaries** (`principalAccessBoundary` on a project, folder, or organization policy; refused elsewhere with `INVALID_ARGUMENT`):

- A boundary binds the principal set of the node it's stored on. A service account `NAME@PROJECT.iam.gserviceaccount.com` is enrolled in the sets of its project and that project's folders and organization.
- An enrolled caller may reach only targets inside the union of its boundaries' `boundCloudResources` (by resource name or `projects/NUMBER`) and their descendants. Anything else is 403 before allow or deny evaluation.
- Not enrolled: users (Workspace domains aren't modeled), Google-managed service agents, workload and workforce pools, and anonymous callers. Boundary conditions and enforcement versions aren't modeled.

Not recorded: deny rules and boundaries need organization-level setup (`roles/iam.denyAdmin` and principal access boundary admin are grantable only at an organization), which the sandbox doesn't allow ([#33](https://github.com/evanmarshall/gcp-local/issues/33), [#18](https://github.com/evanmarshall/gcp-local/issues/18)). Conditions on Cloud SQL are not recorded.

## Groups and domains

`group:` and `domain:` members resolve against a local directory declared in the [bootstrap config](#bootstrap-config). Nothing is fetched from Google.

```json
"directory": {
  "domains": ["example.com"],
  "groups": {
    "team@example.com": ["user:alice@example.com", "group:leads@example.com"],
    "leads@example.com": ["serviceAccount:app@synthetic-project.iam.gserviceaccount.com"]
  }
}
```

- `group:EMAIL` and `principalSet://goog/group/EMAIL` cover the group's members and, transitively, the members of nested groups. Group members are `user:`, `serviceAccount:`, or `group:` naming another declared group. Nesting cycles are allowed.
- `domain:NAME` covers every `user:` whose email is in a declared domain. Service accounts are never domain members.
- Emails and domains compare without case. Policies read back members as written.
- Groups work in allow bindings (with or without conditions), deny rules, and deny exceptions. The decision record names the matching group or domain member.
- A policy naming a group or domain outside the directory is refused with GCP's 400 errors: "Group EMAIL does not exist.", "Domain NAME does not exist.", a `group:` naming a service account, and empty or malformed references (recorded: `storage-iam-directory`). The same applies to `-iam-config` policies, which fail the load.
- The directory is fixed at startup: there is no reload, and declaring it after any policy is stored fails. Restart the core to change it.

Not recorded: a grant or deny through an existing group or domain. The sandbox organization has no Google groups, and its callers are service accounts, which no domain covers. Not modeled: Workspace users outside groups (`group:` naming a user is "does not exist", not GCP's "is of type user"), `deleted:group:`, dynamic groups, and principal access boundary enrollment by group or domain.

## Role snapshots

`internal/iam/roles/` holds 66 predefined roles read from GCP with `roles.get` (full view), last on 2026-10-12 UTC. `manifest.json` records source and hashes; the engine verifies them at load and rejects launch stages other than GA and BETA. Builds and tests never fetch roles.

To propose a refresh (read-only; needs an authorized account and quota project):

```sh
python3 scripts/live/record_iam_roles.py --allow-live \
  --account YOUR_ACCOUNT --quota-project YOUR_SANDBOX \
  --output .artifacts/roles-candidate \
  --service storage --service pubsub  # repeat for every committed set
```

Review permission changes before replacing committed files, and revalidate affected recordings. [Permission discovery](../contributing/permission-discovery.md) minimizes a method's permissions from these snapshots.

## Bootstrap config

`-iam-config` loads an explicit owner-only (`0600`) JSON file:

- `resources`: organization, folder, and project references with `service`, `kind`, `parent`, `number`, and `scope`. A project can be the root.
- `policies`: allow policies keyed by resource name.
- `unsupported`: optional fail-closed feature markers.
- `service_accounts`: optional local account email, project, and numeric unique ID, used for [Pub/Sub push](../services/messaging/pubsub.md) tokens.
- `directory`: optional [groups and domains](#groups-and-domains).

Providers publish their own resources after backend success. A failed load discards the whole file.

```json
{
  "resources": [{
    "name": "//cloudresourcemanager.googleapis.com/projects/synthetic-project",
    "service": "resourcemanager", "kind": "project", "parent": "", "number": "123456",
    "scope": {"project": "synthetic-project", "location": "global"}
  }],
  "policies": {
    "//cloudresourcemanager.googleapis.com/projects/synthetic-project": {
      "version": 1,
      "bindings": [{"role": "roles/storage.admin", "members": [
        "serviceAccount:app@synthetic-project.iam.gserviceaccount.com"
      ]}]
    }
  }
}
```

## Decision records

Each evaluation gets a random decision ID, separate from the trace ID. The record lists principal, method, target, every check, considered policies, and the deciding binding; it never stores credentials. The last 1,000 stay in memory. Unsupported and incomplete evaluations are distinct from allow and deny.

Providers return the ID in `X-Gcp-Local-Decision-Id`. Look one up:

```sh
bin/gcp-local why-denied -endpoint http://127.0.0.1:4566 DECISION_ID
```

The command accepts only a numeric loopback endpoint and ignores proxies and redirects.
