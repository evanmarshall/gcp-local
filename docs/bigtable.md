# Bigtable (admin control plane)

Modeled surface for the Bigtable admin v2 API: instances with one cluster,
tables with column families, and per-resource IAM. The endpoint lives at
`/v2/projects/{project}/...` and the provider is selected with
`--services=...,bigtable`.

## What is modeled

- Instances: create takes `instanceId`, `instance`, and a `clusters` map of
  cluster id to `{location, serveNodes, defaultStorageType}` (exactly one
  cluster). It returns a completed long-running operation named
  `operations/projects/P/instances/I/locations/<zone>/operations/<n>` with
  `CreateInstanceMetadata` and an `Instance` response. Get returns
  `createTime`, `displayName`, `edition`, `knowledgeCatalogRegion`, `labels`,
  `name`, `state`, `type`. List and update (display name, labels) are modeled.
  Delete returns `200 {}` and removes the instance's tables; a duplicate
  create is `409 ALREADY_EXISTS`.
- Clusters: list and get for the requested cluster id, zone, node count, and
  storage type.
- Tables: create (ids `[_a-zA-Z0-9][-_.a-zA-Z0-9]*`, up to 50 characters),
  get, list, delete (`200 {}`), and `:modifyColumnFamilies` with
  create/update/drop. Tables render `name`, `columnFamilies`, and
  `granularity`; a family without a GC rule renders as `{}`.
- IAM: `POST :getIamPolicy`/`:setIamPolicy` on instances and tables. An empty
  policy reads back as only `etag`. Instance bindings apply to the instance and
  its tables; deleting a resource drops its policy.
- Predefined roles pinned: `roles/bigtable.admin`, `roles/bigtable.user`,
  `roles/bigtable.viewer`.

## Recorded conformance

`bigtable-instance-table-admin` records instance creation, instance and
cluster reads, denied and instance-granted viewer access, the empty instance
policy, table and column-family creation, duplicate table/instance conflicts,
and deletes. Unit tests cover the same shapes locally.

## Explicit gaps

- Only the recorded field names are evidence. The `edition` default
  (`ENTERPRISE`) and the `knowledgeCatalogRegion` value (the cluster's region)
  are not recorded values.
- Instance creation completes immediately; operation polling
  (`GET /v2/operations/...`) is not routed.
- App profiles are not modeled. Row data-plane (read/write/scan) is out of
  scope; the storage backend does not store Bigtable rows.
- Replication across multiple clusters is not modeled: one cluster per
  instance, so cluster-level reads return that cluster only.
- Instance type semantics (PRODUCTION/DEVELOPMENT) are stored but carry no
  quota behavior. Table IAM and table-level policies are not recorded.
