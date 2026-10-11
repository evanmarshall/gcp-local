# Spanner (admin control plane)

Modeled surface for the Spanner admin v1 API: instances, databases, DDL, and
per-resource IAM. The endpoint lives at `/v1/projects/{project}/...` and the
provider is selected with `--services=...,spanner`.

## Recorded behavior

The `spanner-instance-database-admin` recording
(`tests/conformance/recordings/`, produced by `gcp_validation/spanner.py`)
pins these observations, and the provider is implemented to match them:

- `instances.create` returns a done long-running operation
  (`name`, `metadata` typed `CreateInstanceMetadata`, `done`, `response` typed
  `Instance`). Instance documents carry exactly `config`
  (`projects/P/instanceConfigs/C`), `createTime`, `defaultBackupScheduleType`,
  `displayName`, `edition`, `instanceType`, `labels`, `name`,
  `processingUnits`, `replicaComputeCapacity`, `resourceLocation`, `state`,
  and `updateTime`; `nodeCount` is omitted below 1000 processing units, as
  proto3 JSON omits zero. Duplicate creates are 409 `ALREADY_EXISTS`.
- `POST ...:getIamPolicy` with `{}` returns an empty policy as only `etag`.
  An instance-level `roles/spanner.viewer` grant allows reading that instance
  but not creating databases (403).
- `databases.create` accepts `CREATE DATABASE \`id\`` (ids are 2-30 chars,
  `[a-z][a-z0-9_-]*[a-z0-9]`) and returns a done operation typed
  `CreateDatabaseMetadata`/`Database`. Database GET carries exactly
  `createTime`, `databaseDialect` (`GOOGLE_STANDARD_SQL`),
  `earliestVersionTime`, `encryptionInfo`, `name`, `state` (`READY`), and
  `versionRetentionPeriod`.
- `GET .../ddl` returns Spanner's canonical table form, for example
  `CREATE TABLE Items (\n  Id STRING(36) NOT NULL,\n  Name STRING(64),\n) PRIMARY KEY(Id)`.
  `PATCH .../ddl` returns a done operation typed
  `UpdateDatabaseDdlMetadata` with a `google.protobuf.Empty` response.
- Database and instance deletes return 200 `{}`; later reads are 404.

The values of `defaultBackupScheduleType`, `edition`, `instanceType`,
`resourceLocation`, `replicaComputeCapacity`, `earliestVersionTime`,
`versionRetentionPeriod`, `encryptionInfo`, and `resourceLocationId` are local
defaults: the recording compares only their presence.

## Also modeled (unrecorded)

- Instance configs: get and list for the modeled regional configs
  (`regional-us-central1`, `regional-us-east1`, `regional-europe-west1`).
- Instances: list, update, and delete (refused while databases exist, 400
  FAILED_PRECONDITION; GCP deletes the databases with the instance).
- Databases: list and database-level IAM.
- Backups: list returns an empty set; backups are otherwise not modeled.
- Predefined roles pinned: `roles/spanner.admin`,
  `roles/spanner.databaseAdmin`, `roles/spanner.viewer`.
- Terraform: the pinned `TestAccSpannerInstanceAndDatabase` acceptance case
  drives `google_spanner_instance` and `google_spanner_database` through
  `spanner_custom_endpoint` (see [terraform-compatibility.md](terraform-compatibility.md)).

## Explicit gaps

- DDL covers only `CREATE TABLE` with `Name TYPE [NOT NULL]` columns and a
  primary key, and `ALTER TABLE ... ADD COLUMN`. Every other statement
  (indexes, drops, interleaving, defaults, options, constraints) is refused
  with 501 `UNIMPLEMENTED`; a refused batch changes nothing.
- Operations complete synchronously and are returned `done`; operation
  get/list/cancel routes are not modeled.
- Database field updates (`PATCH .../databases/{db}`) return
  `UNIMPLEMENTED`.
- Query/session data-plane is out of scope: no SQL execution, no sessions.
- Multi-region instance configs, backups (create/restore), instance
  partitions, database roles, and autoscaling are not modeled; unknown
  configs are refused.
