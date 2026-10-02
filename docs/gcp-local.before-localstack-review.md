# gcp-local

Oct 2, 2026 · @Evan Marshall

## Summary

gcp-local is a local GCP emulator whose core is identity, IAM enforcement, and a networking control plane. Data-plane services are delegated to existing emulators behind an authz proxy. The goal is that a workload, a test suite, or `terraform apply` succeeds or fails locally exactly as it would against real GCP, including on permissions and reachability.

The closest existing project, [localgcp](https://github.com/slokam-ai/localgcp), is data-plane only. Its README lists IAM/auth enforcement as not included (all requests are accepted), and its Cloud Run is record CRUD with no container execution or revisions. It has no Compute or VPC APIs. Its services are still useful as backends behind our proxy.

The defining bet: a method→permission table built and continuously verified by probing real GCP. Without that oracle, an IAM emulator produces false confidence, which is worse than having none.

## Goals and non-goals

Goals:

- Catch IAM bugs locally. A call by principal P on resource R is allowed or denied exactly as GCP would decide, with the same error shape and missing-permission name.
- Catch networking bugs locally. "Can workload A reach B:port?" is answered the way GCP's Connectivity Tests would answer it, from the same config.
- Run the Terraform google provider against local for the supported resources. Plan, apply, and destroy behave like real GCP, including validation errors and long-running operations.
- Use unmodified client libraries and workloads. Identity comes from standard ADC and metadata-server paths, with no code changes.
- Measure fidelity. Every emulated method has a recorded real-GCP oracle, and drift fails CI.

Non-goals for v1:

- Data-plane reimplementation. Pub/Sub, GCS, Spanner, Firestore, etc. come from existing emulators.
- Packet-level network simulation. Reachability is analytical first. Real packets are a later option.
- Performance, quota, and billing fidelity.
- Full API surface coverage. Coverage is driven by what real workloads and Terraform modules touch.

## Identity and token minting

Every request reaching gcp-local carries a known principal. Callers get that identity through the same mechanisms they use against real GCP, so no code changes are needed.

| Caller | Mechanism | How it resolves locally |
| --- | --- | --- |
| Developer / test runner | ADC file of type `external_account` | `token_url` points at the local STS; subject token names the principal (e.g. `user:dev@local`) |
| Workload "running as" an SA | Metadata server via `GCE_METADATA_HOST` | Per-workload metadata endpoint returns tokens for the SA the workload is configured with (Cloud Run service account, VM SA) |
| Impersonation | `iamcredentials.generateAccessToken`, `signJwt`, `generateIdToken` | Gated on `iam.serviceAccounts.getAccessToken` / `signJwt` / `getOpenIdToken` against the target SA's policy |
| Terraform | ADC or `impersonate_service_account` in the provider | Same paths as above; endpoint overrides via `*_custom_endpoint` |
| SA keys | JSON key file | Supported but subject to the `iam.disableServiceAccountKeyCreation` org policy, as in prod |

Tokens are opaque handles (`gl.<random>`) mapped server-side to principal, delegation chain, scopes, and expiry. Short default lifetimes (1h, configurable down to seconds) surface refresh bugs. ID tokens are real signed JWTs from a local JWKS, so services verifying audience and issuer can be tested.

Traffic interception has two modes:

- **Endpoint override** (default). Use `*_EMULATOR_HOST`, `api_endpoint`, and provider custom endpoints. Explicit, with no TLS tricks.
- **Transparent** (opt-in). Local DNS resolves `*.googleapis.com` to the gateway, which serves certs from a local CA that's trusted in the container image or dev machine. Needed for code and tools that can't be pointed at a custom endpoint.

## IAM evaluation engine

The engine answers one question: given principal P, permission set S, resource R, and request context C, is the call allowed? If not, it names which permission was missing and why. It evaluates checks in GCP's order, and the first denial wins:

1. **Service enablement.** Is the API enabled on R's project? Fails with `SERVICE_DISABLED`, as in prod.
2. **VPC Service Controls.** Is R inside a perimeter, and is the caller or source outside it without a matching ingress/egress rule? Fails with a VPC-SC violation, not a plain 403.
3. **Org policy constraints.** These are evaluated on create/update (e.g. `compute.vmExternalIpAccess`, `iam.allowedPolicyMemberDomains`, `gcp.resourceLocations`).
4. **Deny policies.** Attached at org/folder/project. Matching denied permissions win regardless of allows.
5. **Principal access boundary policies.** These limit which resources a principal is even eligible to access.
6. **Allow policies.** The union of bindings from R up through its ancestors (resource → project → folders → org). Roles are expanded to permissions, and IAM Conditions are evaluated in CEL against `request.time`, `resource.name`, `resource.type`, and other attributes.

Data model:

- **Resource hierarchy.** Org, folders, projects, then resources. Each resource has its canonical full name (`//storage.googleapis.com/projects/_/buckets/b`) and a parent pointer.
- **Roles.** Predefined roles snapshotted from the real `roles.list` and `roles.get` (with `view=FULL`), versioned and refreshed weekly. Custom roles are stored at org or project level. Roles that are deprecated or in launch stages behave as they do in prod.
- **Principals.** Users, groups (with local membership), service accounts (including Google-managed service agents), `allUsers`/`allAuthenticatedUsers`, and workload/workforce identity pool principals.

Realism toggles:

- **Propagation delay.** Off by default. When on, policy changes take a random 0–N seconds to apply per check site, which reproduces grant-then-use races in deploy scripts.
- **Strict mode.** Fail on any method missing from the permission table, instead of allow-with-warning.

Every decision is logged as a structured audit record: principal, method, resource, permissions checked, result, and deciding policy. This doubles as a Policy Troubleshooter: `gcp-local why-denied <request-id>`.

## Method→permission table and probing harness

The permission table maps each API method and request shape to the full set of permissions GCP checks, and to the resource each one is checked on. It is built and re-verified by probing a real sandbox org. Google's docs are only a seed.

### Why request shape matters

One method can check different permissions depending on its arguments. `compute.instances.insert` always needs `compute.instances.create`. It also needs `compute.disks.create` for a new boot disk, `compute.subnetworks.use` on the subnet, and `compute.subnetworks.useExternalIp` if there's an access config. With an attached SA it needs `iam.serviceAccounts.actAs` on that SA, and with a custom image it needs `compute.images.useReadOnly` on the image's project. The table is keyed by method plus a set of **shape features** extracted from the request, not by method alone.

### Table schema

```yaml
method: compute.instances.insert          # API method id (discovery or proto full name)
api_version: v1
shape:                                     # predicate over the request; most specific match wins
  networkInterfaces[].accessConfigs: present
  serviceAccounts: present
checks:
  - permission: compute.instances.create
    on: "projects/{project}/zones/{zone}/instances/{name}"
  - permission: compute.subnetworks.use
    on: "{networkInterfaces[].subnetwork}"
  - permission: compute.subnetworks.useExternalIp
    on: "{networkInterfaces[].subnetwork}"
  - permission: iam.serviceAccounts.actAs
    on: "{serviceAccounts[].email}"
  - permission: compute.disks.create
    on: "projects/{project}/zones/{zone}/disks/{disks[].initializeParams.diskName}"
error_on_deny: {code: 403, reason: forbidden, message_template: "Required '{permission}' permission for '{resource}'"}
provenance: {probed_at: 2026-10-02, harness_run: r-0412, sandbox_org: gl-probe}
```

### Probing algorithm

For each (method, shape) pair:

1. **Fixture.** Terraform creates the minimal prerequisite resources in a fresh probe project: network, subnet, SA, image, etc. Fixtures are per-method modules, reused across shapes.
2. **Zero-permission principal.** Mint a probe SA holding no roles anywhere. It gets permissions only through a custom role bound by the harness.
3. **Loop.** Call the method as the probe SA. On 403, parse the missing permission and resource from the error. Add that permission to the custom role on that resource and wait for propagation (poll `testIamPermissions` until true, with a 10-minute cap). Retry.
4. **Terminate** on success, or on a non-IAM error (recorded as a validation oracle too).
5. **Minimize.** Remove each discovered permission one at a time and confirm the call then fails. This drops permissions that were coincidental, such as ones satisfied by an earlier fixture step.
6. **Record** the minimal set, which resource each was checked on, the exact error body for each single-missing case, and the response body of the success. Write it all to the table with provenance.
7. **Teardown.** Delete the probe project, or recycle it from a warm pool.

Seeding and shape discovery:

- Seed shapes from the Terraform provider's acceptance-test configs and from request logs of real workloads run in record mode.
- Seed expected permissions from Google's per-method permission docs and from `queryTestablePermissions`. This narrows the search and flags undocumented checks when the probe disagrees.
- Error messages aren't uniform across APIs. Some name the permission, some only the resource, and some return 404 instead of 403 to hide existence. Per-API parsers handle each. When a parse fails, fall back to granting candidate permissions from the seed set in bisection.

### Operations

| Concern | Approach |
| --- | --- |
| Cost | Probe projects in a dedicated billing account with hard budget alerts; most control-plane calls are free; resources deleted within the run |
| Propagation latency | Run probes in parallel across a pool of \~50 projects; a full table rebuild is a nightly job, targeted re-probes on change |
| Drift detection | Nightly: re-run a sampled 10% of entries plus all entries for APIs with new discovery revisions; any diff opens a PR against the table |
| Coverage tracking | Every method observed in record mode without a table entry is reported; strict mode fails on it |
| Safety | Probe org isolated from production orgs; probe SAs can't hold permissions outside the probe folder (enforced by a deny policy at the org) |

## Control-plane resource graph

The control plane is one typed resource graph behind generated API servers. Terraform must get the same answers from it that it gets from GCP: same IDs, same validation errors, same operation lifecycle.

| API | v1 resources | Notes |
| --- | --- | --- |
| Resource Manager v3 | organizations, folders, projects, tag keys/values/bindings | Hierarchy feeds IAM inheritance and org policy |
| IAM v1 / v2 | roles, service accounts, SA keys, deny policies, workload identity pools | Plus `getIamPolicy`/`setIamPolicy`/`testIamPermissions` on every resource type that supports them |
| IAM Credentials v1 | generateAccessToken, signJwt, generateIdToken | Backs impersonation |
| Service Usage v1 | services enable/disable | Drives `SERVICE_DISABLED` |
| Org Policy v2 | policies, constraints | Boolean and list constraints first; custom constraints later |
| Access Context Manager v1 | access policies, perimeters, access levels | VPC-SC |
| Compute v1 (networking) | networks, subnetworks (+ secondary ranges), firewalls, network and hierarchical firewall policies, routes, routers + NAT, peerings, addresses, global addresses (PSA ranges), forwarding rules / PSC endpoints, service attachments | REST/discovery-only API |
| Service Networking v1 | connections (private service access) | Peering to producer VPCs for Cloud SQL, Memorystore |
| Cloud Run v2 | services, revisions, jobs | Records SA identity, ingress setting, VPC egress config; execution via local containers |
| Cloud SQL Admin v1 | instances (IP config only, plus a backing Postgres container) | Private IP via PSA range |

Implementation rules:

- **Generate, don't hand-write, the API surface.** gRPC services come from the [googleapis](https://github.com/googleapis/googleapis) protos. Compute and other REST-only APIs come from discovery docs. Handlers implement only semantics.
- **Long-running operations** follow real shapes: zonal, regional, and global `Operation` resources for Compute and `google.longrunning` elsewhere. They default to completing after a short delay rather than instantly, so callers that skip polling break locally too.
- **Fingerprints and etags** are enforced on update. A stale fingerprint fails as in prod, which catches concurrent-modification bugs.
- **Validation** is modeled on real errors captured by the harness: CIDR overlap across subnets and peered networks, secondary range conflicts, region mismatch, delete-with-dependents (`resourceInUseByAnotherResource`), name format, and the 63-char limit.
- **selfLinks and IDs** use real formats (numeric IDs plus `https://www.googleapis.com/compute/v1/...` selfLinks), since the provider parses them.
- **Default resources.** A new project gets the `default` network with auto-mode subnets and the four default firewall rules, unless the `compute.skipDefaultNetworkCreation` org policy is set, as in prod.

## Network reachability engine

v1 computes reachability analytically from the control-plane graph, in the same style as GCP's Connectivity Tests. A query takes source, destination, protocol, and port and returns a verdict with the hop-by-hop trace and the deciding rule at each hop.

```
gcp-local net check --from run/api@us-central1 --to sql/main:5432
✗ DROPPED at egress: Cloud Run 'api' has no VPC egress configured;
  10.40.0.3 (PSA range) is not reachable via the internet route
```

The evaluation pipeline for one flow:

1. **Resolve endpoints.** Map each workload to its network attachment: VM NIC, Cloud Run direct VPC egress subnet or Serverless VPC Access connector range, GKE pod/node ranges, PSC endpoint, or producer-side IP for PSA services.
2. **Egress firewall at source.** Evaluate in this order: hierarchical firewall policies (org, then folder, with `goto_next`), then the network firewall policy and VPC firewall rules in the configured enforcement order, by priority. Targets match on network tags, target SAs, or secure tags. The implied allow-all egress rule applies last.
3. **Route selection.** Longest prefix match, then priority. Covers subnet routes, static routes, peering-imported routes (subject to export/import custom-route flags), dynamic routes from Cloud Router (simplified: declared advertised prefixes), and the default internet route.
4. **Path checks.** Peering is **not transitive**: a route learned from peer B is never re-exported to peer C. Internet egress from an instance without an external IP requires Cloud NAT on that region and subnet. Google APIs from internal IPs require Private Google Access on the subnet, or a PSC endpoint for APIs. VPC-SC perimeters apply to Google API access.
5. **Ingress firewall at destination.** Same policy order as egress. The implied deny-all ingress rule applies last. GFE and health-check ranges (`35.191.0.0/16`, `130.211.0.0/22`) are modeled for load-balancer flows.
6. **Destination listening.** Optional: if the destination is a running local container, confirm the port is bound.

Assertions in tests:

```yaml
reachability:
  - from: run/api
    to: sql/main:5432
    expect: reachable
  - from: vm/bastion
    to: internet:443
    expect: dropped          # no NAT on purpose
```

These run against config alone, so they work in CI on a Terraform plan, before anything is deployed.

Packet-level mode, later and opt-in: subnets map to Linux bridges in network namespaces, compiled firewall rules map to nftables, and Cloud Run services and VMs run as containers attached to those namespaces. Its value is end-to-end tests with real traffic. Its cost is subtle divergence from GCP's distributed firewall (connection tracking, implied rules, metadata server at 169.254.169.254). Every packet-level verdict is cross-checked against the analytical engine, and a disagreement is a bug in one of them.

## Data-plane authz proxy

Data-plane services run on existing emulators. gcp-local sits in front of them as an authenticating, authorizing proxy, so IAM enforcement comes without reimplementation.

Per request, the proxy:

1. Authenticates the bearer token to a principal.
2. Decodes the method. For gRPC this comes from the path. For REST it comes from route templates generated from discovery docs, plus the request body where shape features need it.
3. Resolves the permission set from the table and evaluates it with the IAM engine.
4. On deny, returns the GCP-shaped error itself. On allow, strips the auth header and forwards to the backend.
5. Rewrites resources the backend doesn't know about. For example, the bucket's IAM policy lives in gcp-local, not in fake-gcs-server, so `storage.buckets.getIamPolicy` is served by gcp-local.

| Service | Backend | Gaps the proxy covers |
| --- | --- | --- |
| Pub/Sub | Official emulator | IAM on topics/subscriptions; push subscription OIDC tokens minted by gcp-local |
| Cloud Storage | fake-gcs-server | Bucket/object IAM, uniform bucket-level access, signed URL verification against the signing SA |
| Firestore | Official emulator | IAM (not security rules) for server-side clients |
| Spanner / Bigtable | Official emulators | IAM at instance/database/table level |
| BigQuery | goccy/bigquery-emulator or LocalBQ | Dataset/table IAM; `bigquery.jobs.create` on the job project vs. data access on the dataset project |
| Secret Manager, KMS, Cloud Tasks | localgcp services | IAM; Cloud Tasks OIDC tokens for HTTP targets |
| Cloud SQL / Memorystore | Postgres / Redis containers | `cloudsql.instances.connect` via a local Cloud SQL Auth Proxy shim; IAM DB auth later |

Backends run in a single docker-compose project managed by `gcp-local up`. Their ports are never exposed to the host, so the proxy is the only path to them.

## Conformance and fidelity

Every behavior gcp-local claims to emulate has a real-GCP recording it is tested against. Any divergence fails CI.

- **Differential suite.** A corpus of scenarios is run against both real GCP (a sandbox org) and gcp-local. Each scenario is a Terraform config, a sequence of API calls as named principals, and expected outcomes. The suite compares status codes, error reasons, missing-permission names, and normalized response bodies (IDs, timestamps, and etags masked).
- **Record mode.** `gcp-local record` proxies to real GCP and logs request/response pairs. Real workloads and Terraform runs then become scenarios with no hand-writing. This is also how new shapes reach the probing harness.
- **Reachability conformance.** For each network topology fixture, run GCP Connectivity Tests in the sandbox and assert the same verdict and deciding rule from the local engine.
- **Fidelity score.** Published per API: methods covered, shapes covered, last verified date, and known divergences. Users see exactly how much to trust each area.
- **Cadence.** Run the full suite nightly. Run targeted re-runs when a discovery doc revision changes or a predefined role's permission list changes (diffed from the weekly `roles.list` snapshot).

Known-divergence policy: if gcp-local can't match GCP on a behavior, it fails closed and loud (`UNIMPLEMENTED` with a pointer to the gap). It never silently allows.

## Risks and open questions

The largest risk is false confidence: a local allow that prod denies. Every design choice above leans toward failing closed for that reason.

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Permission table incomplete or stale | Tests pass, prod 403s | Strict mode in CI; nightly drift probes; coverage report per API |
| Undocumented check order (VPC-SC vs. IAM vs. org policy) | Wrong error surfaced, debugging misled | Capture the order from harness runs that combine failures; don't assume |
| Error messages don't name the permission for some APIs | Harness can't discover the set automatically | Per-API parsers; bisection over seed permissions |
| Probe cost and propagation latency | Slow, expensive table builds | Warm project pool, parallelism, sampled nightly re-probes |
| Compute API breadth | Control plane never "done" | Coverage driven by record mode and Terraform modules actually in use |
| Google changes behavior silently | Fidelity decays | Discovery-revision and role-snapshot diffs trigger re-probes |

Open questions:

- [ ] License and distribution of the permission table: open, or the paid part?
- [ ] GKE in scope? It adds Kubernetes RBAC plus Workload Identity, which is a large surface.
- [ ] Groups: local-only membership, or sync from Cloud Identity for realistic group-based grants?
- [ ] Ship packet-level mode at all, or stop at analytical reachability plus real containers on a flat network?
- [ ] Terraform provider version range to certify against.
