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
| Operations | Instance and database `operations.get` | Operations complete immediately |
| Backups | `list` | Always empty |
| Sessions (gRPC and REST) | `CreateSession`, `BatchCreateSessions`, `GetSession`, `ListSessions`, `DeleteSession` | `spanner.sessions.*` |
| Queries | `ExecuteSql`, `ExecuteBatchDml`, `Read`; `ExecuteStreamingSql`, `StreamingRead` (gRPC only) | |
| Transactions | `BeginTransaction`, `Commit`, `Rollback` | |
| Partitions | `PartitionQuery`, `PartitionRead` | |

The provider fills service-managed fields the emulator omits (`instanceType`, `edition`, `resourceLocation`, database `createTime`, `versionRetentionPeriod`, `earliestVersionTime`, `encryptionInfo`). Non-default `edition`, `instanceType`, `defaultBackupScheduleType`, and any `autoscalingConfig` return 501.

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

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `spanner-instance-database-admin` | 2026-10-12 | Instance and database create, DDL update and canonical form, operations, 409 duplicate, delete then 404 |
| `spanner-data-plane-rest` | 2026-10-12 | Session create and delete, mutation commit, read, DML in a transaction, query, denied session and query |

```sh
sh scripts/check/backend/spanner.sh          # provider and Go client against the real emulator
sh scripts/check/recordings.sh   # both recordings
```

`TestAccSpannerInstanceAndDatabase` passes ([Terraform](../../contributing/terraform-compatibility.md)).

## Gaps

- Not served: backup create and restore, instance partitions, database roles, operation list and cancel, `testIamPermissions`, REST `:executeStreamingSql` and `:streamingRead`. Over gRPC these return `UNIMPLEMENTED` "No provider implements ...", as do backup and backup-schedule IAM policies and `ListOperations`, `CancelOperation`, and `DeleteOperation` on Spanner operation names.
- `UNIMPLEMENTED`: partitioned DML, Data Boost, `creator_role`, single-use read-write `ExecuteSql`/`Read`, `BatchWrite`.
- Requests are limited to 2 MiB. Query and lock behavior is the emulator's, including one read-write transaction at a time.
- State is in memory; reset or restart discards it.
