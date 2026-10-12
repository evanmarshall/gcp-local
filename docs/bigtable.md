# Bigtable

Bigtable admin v2 over REST (instances with one cluster, tables, column families, IAM) and the v2 data API over gRPC, with IAM checked before any backend call. Tables and rows run in Google's in-process emulator, `bttest` v1.58.0 ([pin](../third_party/bigtable-bttest/manifest.json)); the provider owns instances and clusters. The admin scenario is recorded against GCP; the data plane is modeled locally.

## Use

Select `-services ...,bigtable`. Admin routes are `/v2/projects/{project}/...` on port 4566; the data API is gRPC on 4567. Never set `BIGTABLE_EMULATOR_HOST`; it selects anonymous credentials, which the gateway refuses. The gateway is plaintext loopback, so the per-RPC credential must not require TLS:

```go
conn, _ := grpc.NewClient("127.0.0.1:4567",
	grpc.WithTransportCredentials(insecure.NewCredentials()),
	grpc.WithPerRPCCredentials(localToken)) // RequireTransportSecurity() == false
client, _ := bigtable.NewClientWithConfig(ctx, project, instance,
	bigtable.ClientConfig{MetricsProvider: bigtable.NoopMetricsProvider{}},
	option.WithGRPCConn(conn))
```

The Go client (v1.58.0) is tested. The Python client is not.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Instances | `create`, `get`, `list`, `update`, `delete`, `getIamPolicy`, `setIamPolicy` | Exactly one cluster; create returns a completed operation; update changes display name and labels |
| Clusters | `get`, `list` | The instance's one cluster |
| Tables | `create`, `get`, `list`, `delete`, `modifyColumnFamilies`, `getIamPolicy`, `setIamPolicy` | Table IDs up to 50 characters |
| Tables | `update` | `updateMask=deletionProtection` only; protection blocks table delete, family drop, and instance delete |
| Data (gRPC) | `ReadRows`, `SampleRowKeys`, `MutateRow`, `MutateRows`, `CheckAndMutateRow`, `ReadModifyWriteRow`, `PingAndWarm` | App profiles accepted and ignored; 2 MiB request limit |

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

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `bigtable-instance-table-admin` | 2026-10-11 | Instance create, instance and cluster reads, viewer denied then allowed by an instance grant, empty policy, table and family create, duplicate conflicts, deletes |

```sh
sh scripts/check_rest_recordings.sh   # the recording
sh scripts/check_go.sh                # provider and Go data-plane tests
```

## Gaps

- The data plane, table IAM, table list contents, and `tables.update` operations are not recorded. `edition` and `knowledgeCatalogRegion` values are not recorded.
- No REST data routes. Authorized views, materialized views, `ExecuteQuery`, `PrepareQuery`, change streams, and `OpenTable` return `UNIMPLEMENTED`.
- Not routed: operation polling, app profiles, backups, snapshots, consistency tokens.
- One cluster per instance; no replication. Instance type carries no quota behavior.
- Error messages are bttest's. Dropping an unknown family returns 500.
- State is in memory; reset or restart discards it.
