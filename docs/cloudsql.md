# Cloud SQL for PostgreSQL

Cloud SQL Admin v1beta4 over REST at `/sql/v1beta4/projects/{project}/...`. Each instance is a real PostgreSQL 16 or 15 server; databases and users are real objects in it. Connect with a password on a published port or through an IAM-checked connector. The admin scenario `cloudsql-postgres-instance-admin` is recorded against GCP; the connection paths are tested locally only. Linux only.

v1beta4 is served instead of v1 because v1 shares `/v1/projects/...` paths with Spanner. The pinned Terraform provider and gcloud use v1beta4.

## Use

```sh
python3 scripts/install_postgres.py   # Linux; installs .tools/postgres/16 and .tools/postgres/15
bin/gcp-local -services cloudsql -identity-config ... -iam-config ... \
  -cloudsql-postgres "$PWD/.tools/postgres" \
  -data-ports 4568-4583
```

PostgreSQL refuses to run as root; when gcp-local is root, pass `-cloudsql-run-as uid:gid` for an unprivileged user that can read the install.

### Connect from the host

The packaged environment publishes every public-IP instance (`ipv4Enabled: true`, the default) on a `127.0.0.1` port from `GCP_LOCAL_DATA_PORTS` (4568-4583 by default). `instances.get` returns it as `ipAddresses[].port`, a gcp-local extension:

```sh
PGPASSWORD=root-password psql 'host=127.0.0.1 port=4568 user=postgres dbname=postgres'
```

- `postgres` logs in only if `rootPassword` was set on insert. Other users come from `users.insert`.
- No IAM check per connection, as with Cloud SQL public IP. Authorized networks are not checked.
- TLS is declined: use `sslmode=disable` or `prefer`.
- A full range returns 429. Widen it with `up --data-ports FIRST-LAST` (at most 64). Outside the package, publishing needs `-data-ports` (and optionally `-data-listen`).

### Connector

A loopback listener bound at startup to one instance and one principal, like a Cloud SQL Auth Proxy:

```sh
-cloudsql-connector 'my-project:us-central1:my-db,serviceAccount:app@my-project.iam.gserviceaccount.com,127.0.0.1:5432'
psql 'host=127.0.0.1 port=5432 user=postgres dbname=postgres sslmode=disable'
```

Each new connection checks `cloudsql.instances.connect` for the bound principal (refusal: `FATAL 28000` with a decision ID for `why-denied`), then the instance and region, then forwards to PostgreSQL, which still checks the password. Open sessions are not rechecked after an IAM change. The packaged environment opens no connector.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Instances | `insert`, `get`, `list`, `patch`, `delete` | `POSTGRES_16` or `POSTGRES_15`; `settings.tier` required. `patch` changes tier, `userLabels`, and `deletionProtectionEnabled`; stale `settingsVersion` returns 412. Protected delete returns 400 |
| Databases | `insert`, `get`, `list`, `delete` | Optional `charset` and `collation` |
| Users | `insert`, `list`, `delete` | `BUILT_IN` only |
| Operations | `get`, `list` | Filter by `instance`; 1,000 kept per project |

Every operation returns `DONE`; GCP returns `PENDING` first. Instances report `state: RUNNABLE` and one `127.0.0.1` address, `PRIMARY`, or `PRIVATE` when `ipv4Enabled` is false. A `PRIVATE` address is not reachable from the host.

## IAM

Permissions follow the [Cloud SQL access-control table](https://cloud.google.com/sql/docs/postgres/iam-permissions): `cloudsql.instances.*`, `cloudsql.databases.*`, `cloudsql.users.*`; operations need `cloudsql.instances.get`; the connector needs `cloudsql.instances.connect`. Cloud SQL has no instance-level policy; grants come from the project. Denials return 403 `notAuthorized`. Roles: `roles/cloudsql.admin`, `.editor`, `.viewer`, `.client`.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `cloudsql-postgres-instance-admin` | 2026-10-12 | Operation envelopes, instance and database fields, deletion protection, default `postgres` database and user, denied read and database create, 409 duplicate, 404 after delete |

Service-managed fields such as `gceZone`, `serverCaCert`, and `serviceAccountEmailAddress` have stand-in values; only their presence is recorded.

```sh
sh scripts/check_rest_recordings.sh   # the recording
sh scripts/check_cloudsql.sh          # real PostgreSQL: connector, published port, isolation, reset
```

`scripts/smoke_environment.py` also connects from the host in the packaged environment.

## Gaps

- The connector and published-port paths are not recorded.
- 501 for other engines, replicas, `settings.databaseFlags`, other patch fields, list `filter`/`pageToken`, and IAM database users.
- No TLS, Auth Proxy certificates, `connectSettings`, `generateEphemeralCert`, authorized networks, or private-IP networking.
- No Cloud Run `/cloudsql` socket.
- Durability settings are off. Data is deleted with the instance, on reset, and on restart.
