# Baseline IAM evaluator

The evaluator supplies the permission layer for the first Storage provider. Its unit tests exercise inheritance, cross-project grants, revocation, composed checks, and diagnostics. The [Storage provider](storage.md) also passes the recorded authorization, cross-project, and replacement scenarios through actual backend operations.

## Permission plans and policies

Storage registers each supported method's baseline permissions in `internal/providers/storage/permissions.go`. Object insertion additionally requires an explicitly resolved `object_exists` feature; replacement adds `storage.objects.delete` to `storage.objects.create`. An omitted feature is a coverage error, not an assumption that an object is new. Baseline preflight can authorize internal state resolution, but reports `complete: false`; providers must complete `Authorize` before mutation. Additional checks cannot remove baseline checks. Request-shape validation and service-specific error precedence remain provider responsibilities.

The shared resource index supplies ownership and ancestry. Evaluation walks the target resource or its validated bucket anchor through project, folder, and organization. Storage objects remain in the backend rather than being duplicated into the index. A caller's project is separate from the resource project: a role granted in the caller's project does not grant access to a foreign bucket, while an explicit binding for that foreign caller can. Missing parents, cycles, conflicting project ownership, and unverified target shapes fail closed.

The first policy set supports unconditional version-1 allow bindings for exact user/service-account members, `allUsers`, and `allAuthenticatedUsers`. No absent credential becomes an administrator; unknown/expired credentials remain unauthenticated. A public binding is an explicit grant and still needs the provider's public-access controls. Cloud-platform and Storage full-control OAuth scope sets are supported; narrower-scope enforcement is explicitly unverified and fails rather than being treated as full access.

The engine stores each policy once, with its owning service/scope, returns copies, and checks supplied etags on replacement. Storage API handlers own authorization and wire-format validation around policy operations. Service reset removes that service's resource policies/coverage markers for the selected scope while retaining project/organization policy configuration. Persistence remains unsupported.

Conditions, groups, custom roles, unpinned roles, and advanced deny/PAB/org-policy/VPC-SC evaluation are unsupported. `MarkUnsupported` retains known advanced configuration as a fail-closed marker; even an otherwise matching allow cannot bypass it. The absence of such a local marker models absence in this local configuration, not proof that a real project's policies are absent.

## Role snapshots

Four complete predefined roles were read from GCP on 2026-10-05: Storage Admin, Object User, Object Viewer, and Object Creator. `internal/iam/roles/manifest.json` records their source and hashes, verified when the evaluator loads. Role permissions are data; they do not expand the registered method/request coverage. Ordinary builds/tests never fetch roles or credentials.

To propose a later read-only refresh under an authorized account/quota project:

```sh
python3 scripts/record_iam_roles.py --allow-live \
  --account YOUR_ACCOUNT --quota-project YOUR_SANDBOX \
  --output .artifacts/roles-candidate
```

Inspect the candidate and its permission changes before replacing committed files and updating affected conformance evidence. The recorder never changes project IAM or grants permissions.

## Bootstrap and diagnostics

`-iam-config` loads an explicit owner-only JSON file. It contains `resources` (canonical resource-manager organization/folder/project references with `service`, `kind`, `parent`, and `scope`), `policies` keyed by resource name, and optional `unsupported` feature markers. A project may be an explicit local hierarchy root. Service resources cannot be bootstrapped through this path; their providers publish them after backend success. A failed load discards the entire new model. Example single-project bootstrap:

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
