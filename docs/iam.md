# IAM engine

`internal/iam` authorizes every provider request. Providers register a permission plan per method, resolve the request's target and features, and call `Authorize` before any mutation. Engine unit tests cover the evaluator; only recorded scenarios establish that a service's IAM matches GCP ([coverage](coverage.md)).

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
- Members: exact `user:` and `serviceAccount:`, `allUsers`, `allAuthenticatedUsers`. Absent credentials never become an administrator; unknown or expired tokens stay unauthenticated.
- OAuth scopes: `cloud-platform` covers every plan. `devstorage.full_control` and `pubsub` cover plans whose permissions are all in that service. Other scopes are refused.
- IAM conditions (CEL) see `resource.name`, `resource.type`, `resource.service`, and `request.time`. Providers pass resource attributes through `iam.Target.Attributes`; only Storage and Cloud SQL do, so conditions elsewhere fail closed. Storage conditions are recorded (`storage-iam-conditions`).
- Deny rules (deny overrides allow) and principal access boundary policies are modeled locally and not recorded. Recording deny policies needs `roles/iam.denyAdmin`, which is grantable only at an organization.
- `MarkUnsupported` stores known advanced configuration as a fail-closed marker; a matching allow can't bypass it.
- Not supported: groups, custom roles, roles without a committed snapshot.

Policies are stored once per owning service and scope, returned as copies, and replaced only with a matching etag. A service reset clears that service's resource policies for the scope and keeps project and organization policy.

## Role snapshots

`internal/iam/roles/` holds 62 predefined roles read from GCP with `roles.get` (full view), last on 2026-10-12 UTC. `manifest.json` records source and hashes; the engine verifies them at load and rejects launch stages other than GA and BETA. Builds and tests never fetch roles.

To propose a refresh (read-only; needs an authorized account and quota project):

```sh
python3 scripts/record_iam_roles.py --allow-live \
  --account YOUR_ACCOUNT --quota-project YOUR_SANDBOX \
  --output .artifacts/roles-candidate \
  --service storage --service pubsub  # repeat for every committed set
```

Review permission changes before replacing committed files, and revalidate affected recordings. [Permission discovery](permission-discovery.md) minimizes a method's permissions from these snapshots.

## Bootstrap config

`-iam-config` loads an explicit owner-only (`0600`) JSON file:

- `resources`: organization, folder, and project references with `service`, `kind`, `parent`, `number`, and `scope`. A project can be the root.
- `policies`: allow policies keyed by resource name.
- `unsupported`: optional fail-closed feature markers.
- `service_accounts`: optional local account email, project, and numeric unique ID, used for [Pub/Sub push](pubsub.md) tokens.

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
