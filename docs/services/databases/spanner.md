# Spanner

Spanner admin v1 over REST and gRPC and the `google.spanner.v1.Spanner` data plane over gRPC and REST, with IAM checked before the call reaches Google's official emulator (1.5.58, [pin](../../../toolchains/spanner.json)). Admin and REST data-plane scenarios are recorded against GCP. The emulator is Linux only; `-services spanner` refuses to start on macOS.

## Use

Install on Linux, moo, or use the [packaged image](../../guides/quickstart.md):

```sh
python3 scripts/install/spanner.py   # linux/amd64 or linux/arm64; no Docker or gcloud
bin/gcp-local -services spanner -identity-config ... -iam-config ... \
  -spanner-emulator "$PWD/.tools/spanner/emulator_main" \
  -spanner-libraries "$PWD/.tools/spanner/lib"
```

The emulator needs glibc 2.35 or later; the installer supplies its own `libstdc++`. Never set `SPANNER_EMULATOR_HOST`; it selects anonymous credentials, which the gateway refuses. The gRPC gateway is plaintext loopback, so the per-RPC credential must not require TLS:

```go
conn, _ := grpc.NewClient("127.0.0.1:4567",
	grpc.WithTransportCredentials(insecure.NewCredentials()),
	grpc.WithPerRPCCredentials(localToken)) // RequireTransportSecurity() == false
client, _ := spanner.NewClientWithConfig(ctx, "projects/p/instances/i/databases/d",
	spanner.ClientConfig{DisableNativeMetrics: true}, option.WithGRPCConn(conn))
```

Admin over gRPC (`google.spanner.admin.instance.v1.InstanceAdmin`, `google.spanner.admin.database.v1.DatabaseAdmin`) runs through the REST provider methods, so permissions, decorated fields, and errors are the same. Clients poll operations on the same connection: `google.longrunning.Operations` `GetOperation` serves Spanner instance and database operation names.

The Go client (v1.95.1) is tested, data and admin. The client matrix also drives the Node and Python clients over the TLS port ([clients](../../contributing/clients.md#grpc-over-tls)).

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Instance configs | `get`, `list` | Only `emulator-config` is listed |
| Instances | `create`, `get`, `list`, `delete`, `getIamPolicy`, `setIamPolicy` | `update` returns the emulator's 501 |
| Databases | `create`, `get`, `list`, `update`, `delete`, `getDdl`, `updateDdl`, `getIamPolicy`, `setIamPolicy` | |
| Operations | Instance and database `operations.get` | The emulator's operations; they complete immediately |
| Backups | `list` | Empty: no backup can be created locally |
| Sessions (gRPC and REST) | `CreateSession`, `BatchCreateSessions`, `GetSession`, `ListSessions`, `DeleteSession` | `spanner.sessions.*` |
| Queries | `ExecuteSql`, `ExecuteBatchDml`, `Read`; `ExecuteStreamingSql`, `StreamingRead` (gRPC only) | |
| Transactions | `BeginTransaction`, `Commit`, `Rollback` | |
| Partitions | `PartitionQuery`, `PartitionRead` | |

The provider fills service-managed fields the emulator omits (`instanceType`, `edition`, `resourceLocation`, database `createTime`, `versionRetentionPeriod`, `earliestVersionTime`, `encryptionInfo`). Non-default `edition`, `instanceType`, `defaultBackupScheduleType`, any `autoscalingConfig`, and a database `encryptionConfig` (CMEK, which the emulator ignores) return 501.

- Bodies are decoded strictly: an unknown or mistyped field is GCP's 400 with a `BadRequest` violation. A `setIamPolicy` body is a `SetIamPolicyRequest`; `updateMask` returns 501.
- Lists (`instanceConfigs`, `instances`, `databases`, `backups`) are in name order and paged by the provider. `pageSize` of 0 or less is the maximum (1000); a token the provider didn't issue is GCP's 400 "Invalid List... request"; `filter` returns 501.
- A 403 is GCP's "Operation denied by [IAM permission '...' on resource '...']." with an `iam.googleapis.com` ErrorInfo.

## IAM

Pinned roles: `roles/spanner.admin`, `roles/spanner.databaseAdmin`, `roles/spanner.viewer`. Data-plane permissions come from the [v1 REST reference](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases.sessions), checked on the session's database:

| Call | Permission |
| --- | --- |
| `ExecuteSql`, `ExecuteStreamingSql` | `spanner.databases.select` |
| `Read`, `StreamingRead` | `spanner.databases.read` |
| `ExecuteBatchDml`, `Commit` | `spanner.databases.write` |
| `BeginTransaction` read-write, `Rollback` | `spanner.databases.beginOrRollbackReadWriteTransaction` |
| `BeginTransaction` read-only | `spanner.databases.beginReadOnlyTransaction` |
| `PartitionQuery`, `PartitionRead` | `spanner.databases.partitionQuery`, `.partitionRead` |

A request that begins a transaction inline also needs the `BeginTransaction` permission (local model). `roles/spanner.viewer` has no data permission except `beginReadOnlyTransaction`. Instance and database policies are inherited by child resources.

IAM Conditions see `resource.type` (`spanner.googleapis.com/Instance` or `/Database`), `resource.name` (`projects/P/instances/I[/databases/D]`), and `resource.service` (`spanner.googleapis.com`). A request is authorized on the addressed resource, or its nearest existing ancestor, under the lock deletes take, so a request racing a delete reads 404.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `spanner-instance-database-admin` | 2026-10-13 | Instance and database create, DDL update and canonical form, operations, 409 duplicate, delete then 404; denial messages and ErrorInfo, unknown body field, database list order and paging, page-token refusal, negative page size, empty backups list, a databaseAdmin grant on the instance conditioned on a database's `resource.type`, `.name`, and `.service` |
| `spanner-data-plane-rest` | 2026-10-13 | Session create and delete, mutation commit, read, DML in a transaction, query, denied session and query |

```sh
sh scripts/check/backend/spanner.sh          # provider and Go client against the real emulator
sh scripts/check/recordings.sh   # both recordings
```

`TestLocalSpannerInstanceAndDatabase` passes ([Terraform](../../contributing/terraform-compatibility.md)).

## Gaps

- `UNIMPLEMENTED` "Spanner coverage gap": backup `create`, `copy`, `get`, `patch`, `delete`, `databases.restore`, and instance and database `operations.list` and `cancel`.
- Not served: instance partitions, database roles, `testIamPermissions`, REST `:executeStreamingSql` and `:streamingRead`. Over gRPC these return `UNIMPLEMENTED` "No provider implements ...", as do backup and backup-schedule IAM policies and `ListOperations`, `CancelOperation`, and `DeleteOperation` on Spanner operation names.
- `UNIMPLEMENTED`: partitioned DML, Data Boost, `creator_role`, single-use read-write `ExecuteSql`/`Read`, `BatchWrite`.
- Requests are limited to 2 MiB. Query and lock behavior is the emulator's, including one read-write transaction at a time.
- State is in memory; reset or restart discards it.
