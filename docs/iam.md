# Baseline IAM evaluator

The evaluator supplies the permission layer for Storage and the initial [Pub/Sub provider](pubsub.md). Its unit tests exercise inheritance, cross-project grants, revocation, composed checks, and diagnostics. The [Storage provider](storage.md) also passes recorded authorization, cross-project, and replacement scenarios through actual backend operations. Pub/Sub has local tests but no cloud-recorded permission baseline yet.

## Permission plans and policies

Storage registers each supported method's baseline permissions in `internal/providers/storage/permissions.go`. Object insertion additionally requires an explicitly resolved `object_exists` feature; replacement adds `storage.objects.delete` to `storage.objects.create`. An omitted feature is a coverage error, not an assumption that an object is new. Baseline preflight can authorize internal state resolution, but reports `complete: false`; providers must complete `Authorize` before mutation. Additional checks cannot remove baseline checks. Request-shape validation and service-specific error precedence remain provider responsibilities.

The shared resource index supplies ownership and ancestry. Evaluation walks the target resource or its validated bucket anchor through project, folder, and organization. Storage objects remain in the backend rather than being duplicated into the index. A caller's project is separate from the resource project: a role granted in the caller's project does not grant access to a foreign bucket, while an explicit binding for that foreign caller can. Missing parents, cycles, conflicting project ownership, and unverified target shapes fail closed.

The first policy set supports unconditional version-1 allow bindings for exact user/service-account members, `allUsers`, and `allAuthenticatedUsers`. No absent credential becomes an administrator; unknown/expired credentials remain unauthenticated. A public binding is an explicit grant and still needs the provider's public-access controls; the Pub/Sub slice requires named callers. Cloud-platform covers both current providers; Storage full-control and Pub/Sub scopes are restricted to their respective permission plans. Other narrower-scope enforcement is explicitly unverified and fails rather than being treated as full access.

The engine stores each policy once, with its owning service/scope, returns copies, and checks supplied etags on replacement. Storage API handlers own authorization and wire-format validation around policy operations. Service reset removes that service's resource policies/coverage markers for the selected scope while retaining project/organization policy configuration. Persistence remains unsupported.

IAM conditions are cloud-recorded for Storage: `storage-iam-conditions` (2026-10-11) covers a version-3 conditional `objectViewer` binding scoped by `resource.name.startsWith(...)`, the 400 for a conditional binding in a version-1 policy and for an uncompilable expression, version-1 reads of a conditional policy (200), prefix-scoped allow/deny, and an expired `request.time < timestamp(...)` grant. Conditions see `resource.name`, `resource.type`, `resource.service`, and `request.time` (a timestamp); providers supply the resource attributes through `iam.Target.Attributes`, and today only Storage does, so conditions elsewhere fail closed. Deny rules (v3, deny-overrides-allow) and principal access boundary policies remain local-modeled and unrecorded: real deny policies are a separate IAM v2 resource that needs `roles/iam.denyAdmin`, which can only be granted at the organization, so recording them needs an owner decision. Groups, custom roles, and unpinned roles remain unsupported. `MarkUnsupported` retains known advanced configuration as a fail-closed marker; even an otherwise matching allow cannot bypass it. The absence of such a local marker models absence in this local configuration, not proof that a real project's policies are absent.

## Role snapshots

All 50 committed predefined roles were read from GCP on 2026-10-11 with `roles.get` (full view), replacing earlier Cloud Run, Compute, Secret Manager, Cloud Tasks, Network Management, KMS, Firestore, BigQuery, Bigtable, Spanner, Artifact Registry, service-account, and Resource Manager files that had been hand-authored with invented contents. The ten Storage, Pub/Sub, and identity roles recorded on 2026-10-05 were byte-identical on re-read. `internal/iam/roles/manifest.json` records the source and hashes, verified when the evaluator loads; GA and BETA roles load, other launch stages are rejected. Role permissions are data; they do not expand the registered method/request coverage. Ordinary builds/tests never fetch roles or credentials. The recorder defaults to Storage; repeat `--service` to record several sets into one manifest.

To propose a later read-only refresh under an authorized account/quota project:

```sh
python3 scripts/record_iam_roles.py --allow-live \
  --account YOUR_ACCOUNT --quota-project YOUR_SANDBOX \
  --output .artifacts/roles-candidate \
  --service storage --service pubsub  # repeat for every committed set
```

Inspect the candidate and its permission changes before replacing committed files and updating affected conformance evidence. The recorder never changes project IAM or grants permissions.

## Bootstrap and diagnostics

`-iam-config` loads an explicit owner-only JSON file. It contains `resources` (canonical resource-manager organization/folder/project references with `service`, `kind`, `parent`, and `scope`), `policies` keyed by resource name, and optional `unsupported` feature markers. A project may be an explicit local hierarchy root. Optional `service_accounts` explicitly declares local account email, project, and numeric unique ID for [push token authorization](pubsub.md); duplicate IDs and mismatched ownership are rejected. This is synthetic bootstrap data, not a cloud import or service-account credential. Other service resources are published by their providers after backend success. A failed load discards the entire new model. Example single-project bootstrap:

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

Each evaluation records a random decision ID distinct from the client-supplied trace ID. Records include principal state/subject, method, target, every check, considered policies, and the deciding role/binding. No bearer or subject credentials are stored. The most recent 1,000 records remain in memory; unsupported/incomplete evaluations are distinguishable from allow and deny.

The runtime serves `GET /_local/iam/decisions/ID` when IAM is configured. Retrieve one with `bin/gcp-local why-denied -endpoint http://127.0.0.1:4566 ID`. The command accepts only a numeric loopback HTTP endpoint and never follows redirects or uses ambient proxies. Storage returns `X-Gcp-Local-Decision-Id` with supported API responses. Without an implemented provider, an unimplemented API request does not fabricate an IAM decision.
