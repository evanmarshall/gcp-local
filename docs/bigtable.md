# Bigtable (admin control plane)

Modeled surface for the Bigtable admin v2 API: instances with one cluster,
tables with column families, and per-resource IAM. The endpoint lives at
`/v2/projects/{project}/...` and the provider is selected with
`--services=...,bigtable`.

## Backend

Tables and column families are served by Google's Bigtable emulator, the
`cloud.google.com/go/bigtable/bttest` package at **v1.58.0** (the same code the
`cbtemulator` binary serves). See the [provenance pin](../third_party/bigtable-bttest/manifest.json)
and `go.sum`. The provider starts one emulator per project lazily, inside the
backend manager, after authorization. It listens on an in-memory `bufconn`
listener: there is no network port and no credential. The provider talks to it
with the table-admin gRPC client, translating REST JSON with `protojson` in both
directions. Reset stops the project's emulator, so its tables are discarded,
then clears that project's instances, policies, and index entries. Other
projects keep their own emulators.

Ownership:

| Data | Owner |
| --- | --- |
| Instances, the one cluster, create-operation documents | Provider (bttest has no instance/cluster admin) |
| Table schema: column families, GC rules, granularity, deletion protection | bttest |
| Canonical instance/table names and IAM ancestry | Shared resource index, published after backend success |
| Instance and table IAM policies | IAM engine |
| Table update operation names | Provider (bttest returns a fixed `projects/my-project/operations/1234`) |

Every request authorizes before any backend call; a denied request neither
starts nor calls the emulator. Table methods authorize against the published
table, or against its instance when the table is not published, so a missing
table is reported as `404` only to callers holding the permission. The
emulator's data API (`ReadRows`, `MutateRow`, ...) is registered on its private
listener but no gateway route reaches it.

## What is modeled

- Instances: create takes `instanceId`, `instance`, and a `clusters` map of
  cluster id to `{location, serveNodes, defaultStorageType}` (exactly one
  cluster). It returns a completed long-running operation named
  `operations/projects/P/instances/I/locations/<zone>/operations/<n>` with
  `CreateInstanceMetadata` and an `Instance` response (provider-owned). Get returns
  `createTime`, `displayName`, `edition`, `knowledgeCatalogRegion`, `labels`,
  `name`, `state`, `type`. List and update (display name, labels) are modeled.
  Delete returns `200 {}` and removes the instance's tables; a duplicate
  create is `409 ALREADY_EXISTS`.
- Clusters: list and get for the requested cluster id, zone, node count, and
  storage type.
- Tables (bttest): create (ids `[_a-zA-Z0-9][-_.a-zA-Z0-9]*`, up to 50
  characters, validated by the provider), get, list (names only, sorted),
  delete (`200 {}`), `:modifyColumnFamilies` with create/update/drop, and
  `PATCH` with `updateMask=deletionProtection` (a completed operation). Tables
  render `name`, `columnFamilies`, and `granularity` in the proto JSON mapping;
  a family without a GC rule renders as `{}`. Request bodies with unknown
  fields are `400 INVALID_ARGUMENT`. Deletion protection blocks table deletes,
  family drops, and the instance delete (`400 FAILED_PRECONDITION`).
- IAM: `POST :getIamPolicy`/`:setIamPolicy` on instances and tables. An empty
  policy reads back as only `etag`. Instance bindings apply to the instance and
  its tables; deleting a resource drops its policy.
- Predefined roles pinned: `roles/bigtable.admin`, `roles/bigtable.user`,
  `roles/bigtable.viewer`.

## Recorded conformance

`bigtable-instance-table-admin` records instance creation, instance and
cluster reads, denied and instance-granted viewer access, the empty instance
policy, table and column-family creation, duplicate table/instance conflicts,
and deletes. The table steps of that recording are now served by bttest and
match without response patches; `sh scripts/check_rest_recordings.sh` compares
them. Provider unit tests cover the same shapes, that denied requests never
reach the emulator, reset clearing tables, and project isolation.

## Explicit gaps

- Only the recorded field names are evidence. The `edition` default
  (`ENTERPRISE`) and the `knowledgeCatalogRegion` value (the cluster's region)
  are not recorded values.
- Instance creation completes immediately; operation polling
  (`GET /v2/operations/...`) is not routed.
- App profiles are not modeled. Row data-plane (read/write/scan) is not
  exposed, although bttest implements it; adding it needs routing and
  permission plans first. Backups, snapshots, authorized views, and
  consistency tokens are not routed.
- Error messages are bttest's (for example `table "projects/..." already
  exists`); only the codes and statuses are recorded. Dropping an unknown
  family returns bttest's untyped error, which surfaces as `500`.
- `tables.update` operation names and table list contents are not recorded.
- Replication across multiple clusters is not modeled: one cluster per
  instance, so cluster-level reads return that cluster only.
- Instance type semantics (PRODUCTION/DEVELOPMENT) are stored but carry no
  quota behavior. Table IAM and table-level policies are not recorded.
