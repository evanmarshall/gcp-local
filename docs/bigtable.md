# Bigtable (admin control plane)

Modeled surface for the Bigtable admin v2 API: instances with one cluster,
tables with column families, and per-resource IAM. The endpoint lives at
`/v2/projects/{project}/...` and the provider is selected with
`--services=...,bigtable`.

## What is modeled

- Instances: create (single cluster), get, list, update (display name, labels),
  delete (refused while tables exist, 400 FAILED_PRECONDITION).
- Clusters: list and get for the instance's modeled cluster.
- Tables: create with `columnFamilies` and `granularity`, get, list, delete,
  and `:modifyColumnFamilies` with create/update/drop modifications.
- IAM: `getIamPolicy`/`setIamPolicy` on instances and tables, authorized
  against the parent instance or project anchor.
- Predefined roles pinned: `roles/bigtable.admin`, `roles/bigtable.user`,
  `roles/bigtable.viewer`.

## Explicit gaps

- No cloud recording covers these methods yet; nothing claims recorded
  conformance (tracked under the recording backlog issues).
- App profiles are not modeled. Row data-plane (read/write/scan) is out of
  scope; the storage backend does not store Bigtable rows.
- Replication across multiple clusters is not modeled: one cluster per
  instance, so cluster-level reads return that cluster only.
- Instance type semantics (PRODUCTION/DEVELOPMENT) are stored but carry no
  quota behavior.
