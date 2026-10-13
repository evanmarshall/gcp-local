# Bigtable

Bigtable admin v2 over REST and gRPC (instances with one cluster, tables, column families, IAM) and the v2 data API over gRPC, with IAM checked before any backend call. Tables and rows run in Google's in-process emulator, `bttest` v1.58.0 ([pin](../../../third_party/bigtable-bttest/manifest.json)); the provider owns instances and clusters. The admin scenario is recorded against GCP; the data plane is modeled locally.

## Use

Select `-services ...,bigtable`. Admin routes are `/v2/projects/{project}/...` on port 4566; admin (`google.bigtable.admin.v2.BigtableInstanceAdmin`, `BigtableTableAdmin`) and the data API are gRPC on 4567. gRPC admin runs through the REST provider methods, so permissions and errors are the same. Never set `BIGTABLE_EMULATOR_HOST`; it selects anonymous credentials, which the gateway refuses. The gateway is plaintext loopback, so the per-RPC credential must not require TLS:

```go
conn, _ := grpc.NewClient("127.0.0.1:4567",
	grpc.WithTransportCredentials(insecure.NewCredentials()),
	grpc.WithPerRPCCredentials(localToken)) // RequireTransportSecurity() == false
client, _ := bigtable.NewClientWithConfig(ctx, project, instance,
	bigtable.ClientConfig{MetricsProvider: bigtable.NoopMetricsProvider{}},
	option.WithGRPCConn(conn))
```

The Go client (v1.58.0) is tested, data and admin. The client matrix also drives the Node and Python clients over the TLS port ([clients](../../contributing/clients.md#grpc-over-tls)).

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Instances | `create`, `get`, `list`, `update`, `delete`, `getIamPolicy`, `setIamPolicy` | Exactly one cluster. Create returns a running operation that finishes after 100 ms. `update` is `PartialUpdateInstance`: it changes only the `updateMask` fields (`displayName`, `labels`, `type`) and returns a done operation; no mask is GCP's 501. `list` is by name and ignores `pageToken`, as GCP does |
| Clusters | `get`, `list` | The instance's one cluster |
| Tables | `create`, `get`, `list`, `delete`, `modifyColumnFamilies`, `getIamPolicy`, `setIamPolicy` | Table IDs up to 50 characters. `list` is by name, paged by `pageSize`/`pageToken` |
| Tables | `update` | `updateMask=deletionProtection` only, returning a done operation; protection blocks table delete, family drop, and instance delete |
| Operations | `get`, `list`, `cancel`, `delete` | `/v2/operations/projects/P/instances/I[/tables/T]/locations/Z/operations/ID`, over REST and gRPC (`google.longrunning.Operations`) |
| Data (gRPC) | `ReadRows`, `SampleRowKeys`, `MutateRow`, `MutateRows`, `CheckAndMutateRow`, `ReadModifyWriteRow`, `PingAndWarm` | Only the `default` app profile; 2 MiB request limit |

## IAM

Permissions follow [Bigtable access control](https://docs.cloud.google.com/bigtable/docs/access-control). Pinned roles: `roles/bigtable.admin`, `roles/bigtable.user`, `roles/bigtable.viewer`.

| Call | Permission |
| --- | --- |
| `ReadRows` | `bigtable.tables.readRows` |
| `SampleRowKeys` | `bigtable.tables.sampleRowKeys` |
| `MutateRow`, `MutateRows` | `bigtable.tables.mutateRows` |
| `CheckAndMutateRow`, `ReadModifyWriteRow` | `bigtable.tables.mutateRows` and `bigtable.tables.readRows` |
| `PingAndWarm` | `bigtable.instances.ping` |

- `roles/bigtable.user` and `roles/bigtable.admin` hold every data permission; `roles/bigtable.viewer` holds none.
- Instance bindings apply to its tables. A missing table returns 404 only to callers with the permission on its instance.
- An admin call racing a delete of its table or instance reads 404, never 501. A denial reads "Access denied. Missing IAM permission: P." with no details, and a call without credentials gets GCP's 401 with `CREDENTIALS_MISSING` naming the RPC (both recorded).
- IAM Conditions see `resource.type` (`bigtableadmin.googleapis.com/Instance` or `/Table`), `resource.name` (`projects/P/instances/I[/tables/T]`), and `resource.service` of the authorized resource; a missing table is authorized at its instance, so an instance condition sees the instance (not recorded). Recorded: an instance-only viewer grant reads the instance and is denied its table.
- Operation permissions are not documented or recorded: reads need the resource's `get`, cancel and delete its `update`, and list `bigtable.instances.list`.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `bigtable-instance-table-admin` | 2026-10-13 | Two instance creates as running operations polled to done, instance and cluster reads, instance list order and a bad page token, partial updates by mask and the empty-mask 501, denial and missing-credential messages, a conditional instance-only viewer grant (instance read, table denied), table and family create, table list order, paging, and page refusals, unknown body name 400, unknown family drop 404, deletion protection operations and refused delete, duplicate conflicts, deletes |

```sh
sh scripts/check/recordings.sh   # the recording
sh scripts/check/go.sh                # provider and Go data-plane tests
```

## Gaps

- The data plane, table IAM, and operation `list`, `cancel`, and `delete` are not recorded. `edition` and `knowledgeCatalogRegion` values are not recorded. The data plane's denial message is not recorded.
- No REST data routes. Authorized views, materialized views, `ExecuteQuery`, `PrepareQuery`, change streams, and `OpenTable` return `UNIMPLEMENTED`.
- Not routed: app profiles, backups, snapshots, consistency tokens. Over gRPC these return `UNIMPLEMENTED` "No provider implements ...", as do `UpdateInstance`, `TestIamPermissions`, and materialized-view, logical-view, backup, and authorized-view policies.
- Refused by name (`UNIMPLEMENTED` "Bigtable coverage gap: …"): instance `tags`, cluster `clusterConfig` (autoscaling), `encryptionConfig` (CMEK), and `nodeScalingFactor`; more than one cluster; update masks other than `displayName`, `labels`, `type`; table `changeStreamConfig`, `automatedBackupPolicy`, `tieredStorageConfig`, `rowKeySchema`; table views other than the schema view; named app profiles on data calls.
- One cluster per instance; no replication. Instance type carries no quota behavior.
- Missing, duplicate, and protected table errors, missing and duplicate instances, and an unknown family drop use GCP's recorded messages; other table errors are bttest's.
- State is in memory; reset or restart discards it.
