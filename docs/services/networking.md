# Compute networking and Connectivity Tests

VPC networks, subnetworks, firewall rules, routes, routers with Cloud NAT, peering, VM instances as topology endpoints, and Network Management Connectivity Tests, in one in-memory provider. Nothing runs: instances exist only so the analyzer can trace paths. Four scenarios are recorded against GCP; analyzer outcomes outside them are local models.

## Use

Select `-services ...,compute`. Compute routes are `/compute/v1/projects/{project}/...`; Connectivity Tests are `/v1/projects/{project}/locations/global/connectivityTests` on port 4566. Subnetworks and routers are regional: a name exists once per region, and get, list, and delete see only the request's region. Networks, firewalls, and routes are global. Projects are isolated.

`POST /_local/networking/reachability` runs the same analyzer and returns `unknown`, `dropped`, or `unsupported`.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Networks | `insert`, `get`, `list`, `delete`, `addPeering`, `removePeering` | `addPeering` accepts `networkPeering` or the legacy `name`/`peerNetwork` body; a peering is `INACTIVE` until both sides exist |
| Subnetworks | `insert`, `get`, `list`, `delete`, `getIamPolicy`, `setIamPolicy` | Overlapping range: operation finishes `DONE` with `INVALID_USAGE`. Policies take conditions and read at `optionsRequestedPolicyVersion` ([versions](../architecture/iam.md#policy-versions-and-audit-configs)); the deprecated top-level `bindings` and `etag` of `setIamPolicy` return 501 |
| Firewalls | `insert`, `get`, `list`, `update`, `delete` | Tags and service accounts can't be combined; `EGRESS` rules take no source tags or accounts (400, Compute's messages). An `INGRESS` rule without sources stores `0.0.0.0/0` |
| Routes | `insert`, `get`, `list`, `delete` | The default route is implied, not listed |
| Routers | `insert`, `get`, `list`, `delete` | NAT with `AUTO_ONLY` addresses |
| Instances | `insert`, `get`, `list`, `delete` | One interface, no access config; gets the lowest free host address or a requested `networkIP`. Network tags, labels, and one user-managed service account; running as the account needs `iam.serviceAccounts.actAs` on it, else the operation fails with `SERVICE_ACCOUNT_ACCESS_DENIED` |
| Operations | `get`, `list`, `wait`, `delete` on `globalOperations`, `regionOperations`, and `zoneOperations` | All operations are `DONE`, so `wait` returns at once; `delete` is 204 and a later `get` 404. Each collection holds only its own region's or zone's operations |
| Connectivity Tests | `create`, `get`, `list`, `delete`, `rerun` | Create returns a finished operation; the result is computed on create and rerun only |

## Lists

Every Compute list takes `filter`, `orderBy`, `maxResults`, and `pageToken` (recorded):

- Order is by name. `orderBy` takes `name` or `creationTimestamp desc` (newest first); `creationTimestamp`, `name desc`, or another field is 400. `orderBy` with a `filter` is 400 ("Specifying both a list filter and sort order is not currently supported").
- `maxResults` is 0 to 500; 0 and anything above 500 mean 500. A negative or non-number is 400. `nextPageToken` resumes; an unknown token is 400.
- `filter` takes one comparison, or parenthesized comparisons joined by spaces or `AND`: `name = x`, `name != "x"`, regular expressions `name eq x.*` and `name ne x.*` (whole-field RE2), and on instances `labels.KEY = v` and `labels.KEY != v`. An instance without the label matches neither. Every other form (`OR`, `>`, `:`, wildcards, other fields, mixing `eq` with `=`) returns 501.

Connectivity Tests lists take `pageSize` and `pageToken` (up to 1000 per page); `filter` and `orderBy` return 501.

Deleting a network still in use returns an operation `DONE` with `RESOURCE_IN_USE_BY_ANOTHER_RESOURCE`; the network stays.

## Request bodies

Every body field is read, refused, or ignored, never dropped:

- Modeled fields are read with Compute's coercions (recorded): a string for a list is one item, an object for a list is ignored, `"yes"` is `true` and `"no"` is `false`, `"900"` is a number, protocol `6` is `tcp`.
- A value Compute's parser refuses returns its 400 (`Invalid value at 'resource.priority' (TYPE_INT32), "high"`, with `INVALID_ARGUMENT` and a `BadRequest` detail).
- A Compute field that isn't modeled returns 501 unless it holds its default (`logConfig.enable: false`, `stackType: IPV4_ONLY`). Instance `metadata`, `scheduling`, and access configs are such fields.
- Names that aren't Compute fields are ignored, as Compute ignores them.

Compute errors use Compute's body: `errors[]` entries with `domain` and `reason`, and no `status`.

## Analyzer

Every trace starts with `START_FROM_INSTANCE` or `START_FROM_PRIVATE_NETWORK`, then `APPLY_EGRESS_FIREWALL_RULE` (highest-priority matching rule, deny wins ties, else the implied `default-allow-egress`), then `APPLY_ROUTE`.

| Flow | Result | Evidence |
| --- | --- | --- |
| Instance to instance, same network, ingress allowed | `REACHABLE`: `ARRIVE_AT_INSTANCE`, `APPLY_INGRESS_FIREWALL_RULE`, `DELIVER` | Recorded |
| Instance to instance, no ingress rule allows the port | `UNREACHABLE`, `DROP` `FIREWALL_RULE` | Recorded |
| Instance to instance in another network, no peering | `UNREACHABLE`, `DROP` `PRIVATE_TRAFFIC_TO_INTERNET` | Recorded |
| Same, after peering both ways with subnet route exchange | `REACHABLE` | Recorded |
| Instance to `8.8.8.8` without NAT | `UNREACHABLE`, `DROP` `PRIVATE_GOOGLE_ACCESS_DISALLOWED` | Recorded |
| Instance to `8.8.8.8` with NAT on `ALL_SUBNETWORKS_ALL_IP_RANGES` | `REACHABLE` | Recorded |
| IP address to an address in the same network | `UNDETERMINED`, `ABORT` `UNKNOWN_IP` | Recorded |
| IP address to a private address in another network, peered or not | `UNREACHABLE`, `DROP` `PRIVATE_TRAFFIC_TO_INTERNET` | Recorded |
| Ingress rule with `targetServiceAccounts` or `targetTags`: applies only to instances running as the account or carrying the tag | `REACHABLE` to a target, `DROP` `FIREWALL_RULE` (implied deny) to any other instance | Recorded |
| Ingress rule with `sourceServiceAccounts` or `sourceTags`: admits only sources running as the account or carrying the tag | Same | Recorded |
| Egress deny rule with `targetServiceAccounts` | `DROP` `FIREWALL_RULE` from the account's instances only | Recorded |
| Egress deny rule | `UNREACHABLE`, `DROP` `FIREWALL_RULE` | Local model |

IP-address endpoints never return `REACHABLE`. These return 501 from Connectivity Tests and `unsupported` from the local endpoint:

- Source address outside every subnetwork, or a destination matched by a static route (next hops are not modeled).
- Public destinations from IP sources; from instances, any internet address but `8.8.8.8`, Private Google Access, or NAT with other source selectors.
- IP source with an instance destination, a missing instance, or an IP owned by an instance.
- One-sided peering, peering without subnet route exchange, or overlapping ranges across networks.
- A rule scoped to tags or service accounts that could decide an IP-address flow, or a scoped-source rule facing a source in another network.
- Cross-project endpoints or peering.

## IAM

Permissions follow the documented `compute.*` and `networkmanagement.connectivitytests.*` rows. A request on an existing resource is authorized at the resource, so its own policy applies (subnetworks have one); otherwise at the project. IAM Conditions see `resource.name` and `resource.service` for Compute resources and no `resource.type` (recorded for subnetworks). `operations.wait` needs the operation's `get` permission. `instances.insert` also needs `compute.disks.create` and `compute.subnetworks.use`, plus `iam.serviceAccounts.actAs` on the instance's service account. Pinned roles: `roles/compute.admin`, `.networkAdmin`, `.networkViewer`, `.viewer`, `roles/networkmanagement.admin`, `.viewer`. Compute denials use the Compute error body; Network Management denials use `PERMISSION_DENIED`.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `vpc-firewall-connectivity` | 2026-10-12 | Network, subnetwork, firewall, and peering field sets; async overlap and in-use errors; Connectivity Tests for IP endpoints |
| `vm-connectivity-nat-peering` | 2026-10-12 | Instance and router inserts; instance-to-instance tests with firewall, peering, and Cloud NAT |
| `vm-firewall-service-accounts` | 2026-10-12 | Firewall rules scoped by service account and tag, as targets and sources, ingress and egress; contradictory and mistyped bodies; `actAs` on an instance account; list order; a subnetwork read from another region |
| `compute-lists-operations-iam` | 2026-10-13 | Firewall and instance list filters, order, paging, and their refusals; global, regional, and zonal operations list, wait, and delete; subnetwork policies at versions 1, 2, 3, and none; which condition attributes grant a subnetwork read |

```sh
sh scripts/go.sh test -race ./internal/providers/compute
sh scripts/check/recordings.sh   # every recording
```

`TestLocalComputeNetworkAndSubnetwork` passes ([Terraform](../contributing/terraform-compatibility.md)).

## Gaps

- Instance and subnetwork documents, step details, and instance addresses are not recorded.
- Instances have no external IPs, IPv6, alias ranges, metadata, second interfaces or disks, or start/stop/update. The project's default compute account (`default`) and accounts the emulator doesn't declare return 501.
- Manually allocated NAT addresses return 501.
- Not analyzed: internal load balancers, custom route next hops, PSA, PSC, Cloud SQL, Memorystore, and hierarchical or network firewall policies.
- Network Management `operations.get` is not routed.
- Shared VPC returns 501: the `projects.*Xpn*` methods, and any network or subnetwork reference into another project (instances, subnetworks, firewalls, routes, routers, NAT).
- Hierarchical, network, and regional firewall policies (`firewallPolicies`) return 501, as does every other unrouted Compute v1 path.
- `returnPartialSuccess` is accepted; nothing fails partially.
- State is in memory.
