# Spanner (admin control plane)

Modeled surface for the Spanner admin v1 API: instances, databases, and
per-resource IAM. The endpoint lives at `/v1/projects/{project}/...` and the
provider is selected with `--services=...,spanner`.

## What is modeled

- Instance configs: get and list for the modeled regional configs
  (`regional-us-central1`, `regional-us-east1`, `regional-europe-west1`).
- Instances: create (`{instance, instanceId}` envelope with an LRO response),
  get, list, update, delete (refused while databases exist, 400
  FAILED_PRECONDITION), and `getIamPolicy`/`setIamPolicy`.
- Databases: create from a `CREATE DATABASE` statement, get, list, DDL
  application on `PATCH .../databases/{db}/ddl`, delete, and IAM.
- Backups: list returns an empty set; backups are otherwise not modeled.
- Create and DDL responses use the real long-running-operation envelope
  (`{"done": true, "response": ...}`).
- Predefined roles pinned: `roles/spanner.admin`,
  `roles/spanner.databaseAdmin`, `roles/spanner.viewer`.
- Terraform: the pinned `TestAccSpannerInstanceAndDatabase` acceptance case
  drives `google_spanner_instance` and `google_spanner_database` through
  `spanner_custom_endpoint` (see [terraform-compatibility.md](terraform-compatibility.md)).

## Explicit gaps

- No cloud recording covers these methods yet; nothing claims recorded
  conformance (tracked under the recording backlog issues).
- Query/session data-plane is out of scope: no SQL execution, no sessions.
- Multi-region instance configs, backups (create/restore), instance
  partitions, and autoscaling are not modeled; unknown configs are refused.
- `database_role` and DDL table schema enforcement are not modeled; DDL is
  recorded verbatim.
EOF