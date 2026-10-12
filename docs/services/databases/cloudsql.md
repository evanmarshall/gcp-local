# Cloud SQL for PostgreSQL and MySQL

Cloud SQL Admin v1beta4 over REST at `/sql/v1beta4/projects/{project}/...`. Each instance is a real server: PostgreSQL 16 or 15, or MySQL 8.4 or 8.0. Databases and users are real objects in it. Connect with a password on a published port or through an IAM-checked connector. The admin scenarios `cloudsql-postgres-instance-admin` and `cloudsql-mysql-instance-admin` are recorded against GCP; the connection paths are tested locally only. Linux only.

v1beta4 is served instead of v1 because v1 shares `/v1/projects/...` paths with Spanner. The pinned Terraform provider and gcloud use v1beta4.

## Use

```sh
python3 scripts/install/postgres.py   # Linux; installs .tools/postgres/16 and .tools/postgres/15
python3 scripts/install/mysql.py      # Linux; installs .tools/mysql/8.4 and .tools/mysql/8.0
bin/gcp-local -services cloudsql -identity-config ... -iam-config ... \
  -cloudsql-postgres "$PWD/.tools/postgres" -cloudsql-mysql "$PWD/.tools/mysql" \
  -data-ports 4568-4583
```

Either install root may be omitted; versions that aren't installed return 501. PostgreSQL and MySQL refuse to run as root; when gcp-local is root, pass `-cloudsql-run-as uid:gid` for an unprivileged user that can read the installs.

`install_mysql.py` downloads Oracle's generic Linux (glibc 2.28) MySQL Community Server tarballs (about 850 MB each), checks their SHA-256 and published MD5, and keeps only `mysqld`, its private libraries, and its data files (about 75 MB per version). It drops the binaries' debug sections, which the loader never reads, and checks the result against a pin. `mysqld` also needs `libaio.so.1` and `libnuma.so.1` from the host: `apt-get install libaio1 libnuma1` on Debian 12 and Ubuntu 22.04. Ubuntu 24.04 ships only `libaio.so.1t64` (package `libaio1t64`); link `libaio.so.1` to it.

### Connect from the host

The packaged environment publishes every public-IP instance (`ipv4Enabled: true`, the default) on a `127.0.0.1` port from `GCP_LOCAL_DATA_PORTS` (4568-4583 by default). `instances.get` returns it as `ipAddresses[].port`, a gcp-local extension:

```sh
PGPASSWORD=root-password psql 'host=127.0.0.1 port=4568 user=postgres dbname=postgres'
mysql -h 127.0.0.1 -P 4569 -u root -p --ssl-mode=DISABLED
```

- `postgres` (PostgreSQL) and `root@%` (MySQL) log in only if `rootPassword` was set on insert. Other users come from `users.insert`.
- No IAM check per connection, as with Cloud SQL public IP. Authorized networks are not checked.
- TLS is declined. PostgreSQL: use `sslmode=disable` or `prefer`. MySQL: the server advertises no TLS; a client that requires it fails.
- MySQL users authenticate with `mysql_native_password`, on 8.4 too. Clients that dropped that plugin (MySQL 9 clients) can't log in.
- A full range returns 429. Widen it with `up --data-ports FIRST-LAST` (at most 64). Outside the package, publishing needs `-data-ports` (and optionally `-data-listen`).

### Connector

A loopback listener bound at startup to one instance and one principal, like a Cloud SQL Auth Proxy:

```sh
-cloudsql-connector 'my-project:us-central1:my-db,serviceAccount:app@my-project.iam.gserviceaccount.com,127.0.0.1:5432'
psql 'host=127.0.0.1 port=5432 user=postgres dbname=postgres sslmode=disable'
```

Each new connection checks `cloudsql.instances.connect` for the bound principal, then the instance and region, then forwards to the server, which still checks the password. Refusals carry a decision ID for `why-denied`: PostgreSQL `FATAL 28000`, MySQL error 1045 (SQLSTATE 28000) before the server greeting. MySQL connections are authorized before the server is dialed; the client's handshake is read for its user before it reaches the server. Open sessions are not rechecked after an IAM change. The packaged environment opens no connector.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Instances | `insert`, `get`, `list`, `patch`, `delete` | `POSTGRES_16`, `POSTGRES_15`, `MYSQL_8_4`, or `MYSQL_8_0`; `settings.tier` required. `patch` changes tier, `userLabels`, and `deletionProtectionEnabled`; stale `settingsVersion` returns 412. Protected delete returns 400 |
| Databases | `insert`, `get`, `list`, `delete` | Optional `charset` and `collation`. MySQL defaults to `utf8mb4` and `utf8mb4_0900_ai_ci`; unknown or mismatched names return 400. MySQL lists the system schemas; deleting one returns 400. A duplicate MySQL database returns 400, as recorded |
| Users | `insert`, `list`, `delete` | `BUILT_IN` only. MySQL users have a `host` (default `%`) and `delete` takes `host`; users get the `cloudsqlsuperuser` role. A user without a password can't log in |
| Operations | `get`, `list` | Filter by `instance`; 1,000 kept per project |

Every operation returns `DONE`; GCP returns `PENDING` first. Instances report `state: RUNNABLE` and one `127.0.0.1` address, `PRIMARY`, or `PRIVATE` when `ipv4Enabled` is false. A `PRIVATE` address is not reachable from the host.

On MySQL, `cloudsqlsuperuser` holds every static privilege except `SUPER`, `FILE`, and `SHUTDOWN`, and no dynamic administrative privilege, so its members can't alter the internal administrative account (`SYSTEM_USER`). The real role's grants are not recorded.

## IAM

Permissions follow the [Cloud SQL access-control table](https://cloud.google.com/sql/docs/postgres/iam-permissions), the same for both engines: `cloudsql.instances.*`, `cloudsql.databases.*`, `cloudsql.users.*`; operations need `cloudsql.instances.get`; the connector needs `cloudsql.instances.connect`. Cloud SQL has no instance-level policy; grants come from the project. Denials return 403 `notAuthorized`. Roles: `roles/cloudsql.admin`, `.editor`, `.viewer`, `.client`.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `cloudsql-postgres-instance-admin` | 2026-10-12 | Operation envelopes, instance and database fields, deletion protection, default `postgres` database and user, denied read and database create, 409 duplicate, 404 after delete |
| `cloudsql-mysql-instance-admin` | 2026-10-12 | `MYSQL_8_0` instance fields (no `OUTGOING` address), the four system schemas and `root@%`, denied read and database create, a database with `utf8mb4_unicode_ci` and one with the defaults, 400 duplicate database, user fields and a user with host `%`, 404 after delete |

Service-managed fields such as `gceZone`, `serverCaCert`, and `serviceAccountEmailAddress`, and MySQL users' `etag`, `iamStatus`, and `passwordPolicy`, have stand-in values; only their presence is recorded.

```sh
sh scripts/check/recordings.sh       # the recordings
sh scripts/check/backend/cloudsql.sh          # real PostgreSQL and MySQL: connector, published port, isolation, reset
```

`scripts/smoke/environment.py` also connects to both engines from the host in the packaged environment.

## Gaps

- The connector and published-port paths are not recorded. `MYSQL_8_4` is not recorded.
- 501 for other engines, replicas, `settings.databaseFlags`, other patch fields, list `filter`/`pageToken`, IAM database users, and `users.insert` `passwordPolicy` and `dualPasswordType`.
- No TLS, Auth Proxy certificates, `connectSettings`, `generateEphemeralCert`, authorized networks, or private-IP networking.
- Every MySQL connection arrives over the server's local socket, so a user whose `host` excludes `localhost` can't log in.
- A connector bound to a MySQL instance that doesn't exist yet answers in the PostgreSQL protocol.
- No Cloud Run `/cloudsql` socket.
- Durability settings are off. Data is deleted with the instance, on reset, and on restart.
