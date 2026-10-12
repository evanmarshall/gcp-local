# Compute networking and Connectivity Tests

VPC networks, subnetworks, firewall rules, routes, routers with Cloud NAT, peering, VM instances as topology endpoints, and Network Management Connectivity Tests, in one in-memory provider. Nothing runs: instances exist only so the analyzer can trace paths. Two scenarios are recorded against GCP; analyzer outcomes outside them are local models.

## Use

Select `-services ...,compute`. Compute routes are `/compute/v1/projects/{project}/...`; Connectivity Tests are `/v1/projects/{project}/locations/global/connectivityTests` on port 4566. Regions are attributes, not namespaces, so network references resolve across regions. Projects are isolated.

`POST /_local/networking/reachability` runs the same analyzer and returns `unknown`, `dropped`, or `unsupported`.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Networks | `insert`, `get`, `list`, `delete`, `addPeering`, `removePeering` | `addPeering` accepts `networkPeering` or the legacy `name`/`peerNetwork` body; a peering is `INACTIVE` until both sides exist |
| Subnetworks | `insert`, `get`, `list`, `delete`, `getIamPolicy`, `setIamPolicy` | Overlapping range: operation finishes `DONE` with `INVALID_USAGE` |
| Firewalls | `insert`, `get`, `list`, `update`, `delete` | |
| Routes | `insert`, `get`, `list`, `delete` | The default route is implied, not listed |
| Routers | `insert`, `get`, `list`, `delete` | NAT with `AUTO_ONLY` addresses |
| Instances | `insert`, `get`, `list`, `delete` | One interface, no access config; gets the lowest free host address or a requested `networkIP` |
| Operations | `globalOperations.get`, `.list`, `regionOperations.get`, `zoneOperations.get` | All operations are `DONE` |
| Connectivity Tests | `create`, `get`, `list`, `delete`, `rerun` | Create returns a finished operation; the result is computed on create and rerun only |

Deleting a network still in use returns an operation `DONE` with `RESOURCE_IN_USE_BY_ANOTHER_RESOURCE`; the network stays.

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
| Egress deny rule | `UNREACHABLE`, `DROP` `FIREWALL_RULE` | Local model |

IP-address endpoints never return `REACHABLE`. These return 501 from Connectivity Tests and `unsupported` from the local endpoint:

- Source address outside every subnetwork, or a destination matched by a static route (next hops are not modeled).
- Public destinations from IP sources; from instances, any internet address but `8.8.8.8`, Private Google Access, or NAT with other source selectors.
- IP source with an instance destination, a missing instance, or an IP owned by an instance.
- One-sided peering, peering without subnet route exchange, or overlapping ranges across networks.
- Firewall rules with source or target tags that could decide the flow.
- Cross-project endpoints or peering.

## IAM

Permissions follow the documented `compute.*` and `networkmanagement.connectivitytests.*` rows. `instances.insert` also needs `compute.disks.create` and `compute.subnetworks.use`. Pinned roles: `roles/compute.admin`, `.networkAdmin`, `.viewer`, `roles/networkmanagement.admin`, `.viewer`. Compute denials use the Compute error body; Network Management denials use `PERMISSION_DENIED`.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `vpc-firewall-connectivity` | 2026-10-11 | Network, subnetwork, firewall, and peering field sets; async overlap and in-use errors; Connectivity Tests for IP endpoints |
| `vm-connectivity-nat-peering` | 2026-10-12 | Instance and router inserts; instance-to-instance tests with firewall, peering, and Cloud NAT |

```sh
sh scripts/go.sh test -race ./internal/providers/compute
sh scripts/check/recordings.sh   # both recordings
```

`TestAccComputeNetworkAndSubnetwork` passes ([Terraform](../contributing/terraform-compatibility.md)).

## Gaps

- Instance and subnetwork documents, step details, and instance addresses are not recorded.
- Instances have no external IPs, IPv6, alias ranges, tags, service accounts, second interfaces, or start/stop/update.
- Manually allocated NAT addresses return 501.
- Not analyzed: internal load balancers, custom route next hops, PSA, PSC, Cloud SQL, Memorystore, and hierarchical or network firewall policies.
- Network Management `operations.get` is not routed.
- State is in memory.
