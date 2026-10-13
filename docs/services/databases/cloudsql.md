# Cloud SQL for PostgreSQL and MySQL

Cloud SQL Admin v1beta4 over REST at `/sql/v1beta4/projects/{project}/...`. Each instance is a real server: PostgreSQL 16 or 15, or MySQL 8.4 or 8.0. Databases, users, and backups are real objects in it. Connect with a password on a published port, which enforces the instance's TLS mode and authorized networks, or through an IAM-checked connector. Five scenarios are recorded against GCP ([Verification](#verification)); the connection paths are tested locally only. Linux only.

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

`install_mysql.py` downloads Oracle's generic Linux (glibc 2.28) MySQL Community Server tarballs (about 850 MB each), checks their SHA-256 and published MD5, and keeps only `mysqld`, its private libraries, the clone plugin backups use, and its data files (about 75 MB per version). An install from before the clone plugin serves everything except MySQL backups; rerun the installer. It drops the binaries' debug sections, which the loader never reads, and checks the result against a pin. `mysqld` also needs `libaio.so.1` and `libnuma.so.1` from the host: `apt-get install libaio1 libnuma1` on Debian 12 and Ubuntu 22.04. Ubuntu 24.04 ships only `libaio.so.1t64` (package `libaio1t64`); link `libaio.so.1` to it.

### Connect from the host

The packaged environment publishes every public-IP instance (`ipv4Enabled: true`, the default) on a `127.0.0.1` port from `GCP_LOCAL_DATA_PORTS` (4568-4583 by default). `instances.get` returns it as `ipAddresses[].port`, a gcp-local extension:

```sh
PGPASSWORD=root-password psql 'host=127.0.0.1 port=4568 user=postgres dbname=postgres'
mysql -h 127.0.0.1 -P 4569 -u root -p --ssl-mode=DISABLED
```

- `postgres` (PostgreSQL) and `root@%` (MySQL) log in only if `rootPassword` was set on insert. Other users come from `users.insert`.
- No IAM check per connection, as with Cloud SQL public IP.
- MySQL users authenticate with `mysql_native_password`, on 8.4 too. Clients that dropped that plugin (MySQL 9 clients) can't log in.
- A full range returns 429. Widen it with `up --data-ports FIRST-LAST` (at most 64). Outside the package, publishing needs `-data-ports` (and optionally `-data-listen`).

### TLS

The published port terminates TLS with the instance's own server certificate, signed by its server CA (`serverCaCert` on `instances.get` and `connectSettings`), and forwards cleartext to the server's private socket. `settings.ipConfiguration.sslMode` applies to every new connection:

| `sslMode` | Cleartext | TLS without a client certificate | TLS with an `sslCerts` certificate |
| --- | --- | --- | --- |
| `ALLOW_UNENCRYPTED_AND_ENCRYPTED` (default) | Accepted | Accepted | Accepted |
| `ENCRYPTED_ONLY` | PostgreSQL `FATAL 28000`; MySQL error 3159 | Accepted | Accepted |
| `TRUSTED_CLIENT_CERTIFICATE_REQUIRED` | Refused | TLS handshake fails | Accepted |

```sh
curl -s -H "Authorization: Bearer $TOKEN" $API/instances/my-db/connectSettings | jq -r .serverCaCert.cert > server-ca.pem
curl -s -H "Authorization: Bearer $TOKEN" -d '{"commonName":"app"}' $API/instances/my-db/sslCerts > cert.json
jq -r .clientCert.certInfo.cert cert.json > client-cert.pem; jq -r .clientCert.certPrivateKey cert.json > client-key.pem
psql "host=127.0.0.1 port=4568 user=postgres sslmode=verify-full sslrootcert=server-ca.pem sslcert=client-cert.pem sslkey=client-key.pem"
mysql -h 127.0.0.1 -P 4569 -u root -p --ssl-mode=VERIFY_CA --ssl-ca=server-ca.pem --ssl-cert=client-cert.pem --ssl-key=client-key.pem
```

- The server certificate names `127.0.0.1` and `localhost`, so `sslmode=verify-full` works locally. GCP's names `project:instance` only.
- Server certificates use ECDSA P-256 keys; client certificates use RSA 2048 in PKCS #1, as GCP returns (recorded). A deleted `sslCerts` entry stops working on the next connection.
- `requireSsl` alone sets `sslMode` to `TRUSTED_CLIENT_CERTIFICATE_REQUIRED` (`true`) or `ALLOW_UNENCRYPTED_AND_ENCRYPTED` (`false`), as recorded. Both given must agree (`true` only with `TRUSTED_CLIENT_CERTIFICATE_REQUIRED`), or the patch returns GCP's 400.
- PostgreSQL 17's direct TLS (`sslnegotiation=direct`) is not accepted. The connector still declines TLS, as the Auth Proxy's local socket does.

### Authorized networks

`settings.ipConfiguration.authorizedNetworks` is checked against each published-port connection's source address before the server is dialed. A refused connection gets PostgreSQL `FATAL 28000` or MySQL error 1130 naming the address; GCP's firewall drops it instead, so a client there times out.

- Host connections come from `127.0.0.1` when gcp-local runs on the host. In the packaged container they come from the Docker network's gateway (often `172.17.0.1` or `192.168.65.1`): authorize that address, or `0.0.0.0/0`.
- An empty list admits every source. GCP admits no direct public-IP connection then; the local published port stays usable.
- A replacement list returns new entries first, in request order, then the entries kept from before (recorded).
- Entries are IPv4 CIDR ranges with no host bits set, or one address. Expired entries (`expirationTime`) admit nothing. The connector ignores the list, as the Auth Proxy does.

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
| Instances | `insert`, `get`, `list`, `patch`, `update`, `delete`, `restoreBackup` | `POSTGRES_16`, `POSTGRES_15`, `MYSQL_8_4`, or `MYSQL_8_0`; `settings.tier` required (400 `Missing parameter: Tier.`, checked first). `patch` merges the fields it names; `databaseFlags` and `authorizedNetworks` replace the whole list (recorded). `update` (PUT) also clears the labels, flags, and networks it omits (not recorded). Region, version, and edition can only be restated. Stale `settingsVersion` returns 412. Protected delete returns 400. `list` takes `maxResults` (default 500, at most 1,000) and opaque `pageToken`, ordered by name, and has no `kind` |
| Backup runs | `insert`, `get`, `list`, `delete` | See [Backups](#backups) |
| SSL certs | `insert`, `get`, `list`, `delete` | Client certificates for `TRUSTED_CLIENT_CERTIFICATE_REQUIRED`; `insert` returns the private key once. A duplicate `commonName` returns 400 `Invalid request: Common name already exists: NAME.`, a missing fingerprint 404 `sslCertificateDoesNotExist` (recorded). Operations are `UPDATE` |
| Connect | `connectSettings` | `serverCaCert`, addresses, version, region, `serverCaMode` `GOOGLE_MANAGED_INTERNAL_CA` (recorded) |
| Databases | `insert`, `get`, `list`, `delete` | Optional `charset` and `collation`. MySQL defaults to `utf8mb4` and `utf8mb4_0900_ai_ci`; unknown or mismatched names return 400. MySQL lists the system schemas; deleting one returns 400. A duplicate MySQL database returns 400, as recorded |
| Users | `insert`, `list`, `delete` | `BUILT_IN` only. MySQL users have a `host` (default `%`) and `delete` takes `host`; users get the `cloudsqlsuperuser` role. A user without a password can't log in |
| Operations | `get`, `list`, `cancel` | `list` filters by `instance`, newest first, with `maxResults` and `pageToken`; 1,000 kept per project. `cancel` refuses every served operation type with GCP's 400 (recorded for `CREATE`); `cancel` needs `cloudsql.instances.update` (`roles/cloudsql.viewer` is denied, recorded; the exact permission is inferred) |

Operations report `PENDING` for 500 ms after the work ends, then `DONE`, as GCP reports `PENDING` first; the work itself is finished when the response is sent. Work the server fails (a server that won't start, a failed backup or restore) ends its operation `DONE` with an `INTERNAL_ERROR` naming the cause, never a 500, which clients retry. Instances report `state: RUNNABLE` and one `127.0.0.1` address, `PRIMARY`, or `PRIVATE` when `ipv4Enabled` is false. A `PRIVATE` address is not reachable from the host.

### Backups

`settings.backupConfiguration` is stored and returned as requested, with GCP's defaults (recorded): disabled, 7 retained backups, 7 days of logs, and a start time gcp-local assigns. PostgreSQL point-in-time recovery also reports `replicationLogArchivingEnabled` and `transactionalLogStorageState: CLOUD_STORAGE` (recorded).

| Call | Local behavior |
| --- | --- |
| `backupRuns.insert` | An online physical backup into the instance's private directory: PostgreSQL's base backup (`pg_backup_start`, the data directory, the WAL it needs) or a MySQL clone. Sessions continue. `BACKUP_VOLUME` operation with `backupContext` |
| Automated backups | With `enabled: true`, one `AUTOMATED` run at `startTime` (UTC) each day; the oldest beyond `retainedBackups` are deleted |
| `backupRuns.list`, `get` | Newest first, `ON_DEMAND` or `AUTOMATED`, `status: SUCCESSFUL`, `backupKind: SNAPSHOT` |
| `backupRuns.delete` | Deletes the files; `get` then reports the run `DELETED` (recorded: a deleted run stays readable, and `list` omits it). `DELETE_BACKUP` operation |
| `instances.restoreBackup` | Stops the target, replaces its whole data directory (databases and users) with the backup of the same or another instance in the project, and starts it. Open sessions end. `RESTORE_VOLUME` operation |

- A restore needs the same `databaseVersion`. Another project's backup, `backup` and `backupdrBackup` names, and `restoreInstanceSettings` are gaps.
- Point-in-time recovery is stored but no point-in-time restore exists (`instances.clone` is not served). GCP's backup taken at creation is not.
- Backups are deleted with the instance, on reset, and on restart.

### High availability

`availabilityType: REGIONAL` is stored and returned, with `secondaryGceZone` and `failoverReplica` as GCP reports them (recorded); shared-core tiers are accepted, as GCP accepted one. One local server serves the instance and there is no standby, so `instances.failover` returns 501.

### Database flags

`settings.databaseFlags` is stored and passed to the server at startup; changing it restarts the server, ending open sessions. `patch` and `update` replace the whole list (recorded).

| Engine | Modeled flags |
| --- | --- |
| PostgreSQL | `max_connections`, `log_min_duration_statement`, `log_statement`, `log_connections`, `log_disconnections`, `log_lock_waits`, `log_checkpoints`, `log_duration`, `autovacuum`, `log_temp_files`, `work_mem`, `maintenance_work_mem`, `idle_in_transaction_session_timeout`, `lock_timeout`, `max_locks_per_transaction` |
| MySQL | `max_connections`, `slow_query_log`, `long_query_time`, `general_log`, `log_output`, `wait_timeout`, `interactive_timeout`, `max_allowed_packet`, `innodb_lock_wait_timeout`, `local_infile`, `log_bin_trust_function_creators`, `event_scheduler` |

Values are checked against Cloud SQL's documented types and ranges. A flag Cloud SQL documents but gcp-local doesn't model (for example `cloudsql.iam_authentication`) returns 501 naming it; a name Cloud SQL doesn't document returns GCP's 400.

On MySQL, `cloudsqlsuperuser` holds every static privilege except `SUPER`, `FILE`, and `SHUTDOWN`, and no dynamic administrative privilege, so its members can't alter the internal administrative account (`SYSTEM_USER`). The real role's grants are not recorded.

## IAM

Permissions follow the [Cloud SQL access-control table](https://cloud.google.com/sql/docs/postgres/iam-permissions), the same for both engines: `cloudsql.instances.*` (`restoreBackup` needs `cloudsql.instances.restoreBackup`, `failover` `cloudsql.instances.failover`), `cloudsql.databases.*`, `cloudsql.users.*`, `cloudsql.backupRuns.*`, `cloudsql.sslCerts.*`; operations `get` and `list` and `connectSettings` need `cloudsql.instances.get`; the connector needs `cloudsql.instances.connect`. Automated backups are the service's own work and check no caller. Cloud SQL has no instance-level policy; grants come from the project. Denials return 403 `The client is not authorized to make this request.` with `errors[].reason` `notAuthorized` and no `status`, as recorded.

IAM Conditions see `resource.name` `projects/P/instances/I`, `resource.type` `sqladmin.googleapis.com/Instance`, and `resource.service` `sqladmin.googleapis.com` for an existing instance (recorded). A missing instance, `insert`, and `list` carry no attributes, so a condition grants nothing there; GCP denied both in the recording.

Errors use the v1beta4 shape: `errors[]` with `domain` `global` and a `reason` (`notAuthorized`, `instanceAlreadyExists`, `instanceDoesNotExist`, `badRequest`, `invalid`, `required`), no `status`, no `details`. A non-numeric or negative `maxResults` is the transcoder's 400 with `status` and a `BadRequest`. Names outside the schema are dropped, as GCP does. Roles: `roles/cloudsql.admin`, `.editor`, `.viewer`, `.client`.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `cloudsql-postgres-instance-admin` | 2026-10-13 | Operation envelopes (`PENDING` first), instance and database fields, deletion protection, default `postgres` database and user, denied read and database create with their messages and reasons, 409 duplicate, an unknown field dropped (400 for the missing tier), `maxResults` paging, an invalid page token, a negative `maxResults`, operations newest first across pages, cancel of a finished and of a malformed operation, a `roles/cloudsql.viewer` grant conditioned on `resource.name`, `resource.type`, and `resource.service` (allowed on the instance and its operation; denied on cancel, another name, and list), 404 after delete |
| `cloudsql-mysql-instance-admin` | 2026-10-13 | `MYSQL_8_0` instance fields (no `OUTGOING` address), the four system schemas and `root@%`, denied read and database create, a database with `utf8mb4_unicode_ci` and one with the defaults, 400 duplicate database, user fields and a user with host `%`, 404 after delete |
| `cloudsql-postgres-backups` | 2026-10-13 | `backupConfiguration` round trip with point-in-time recovery, location, and retention, and the defaults of an instance without one; refused start time, binary logging on PostgreSQL, log retention days, and PITR without backups; a patch that merges one field; an on-demand run's operation, list, and read; a missing run; restore in place and into a second instance; restore of a missing run and without a context; deletion, the `DELETED` run, and its absence from `list` |
| `cloudsql-mysql-backups-flags` | 2026-10-13 | `MYSQL_8_0` with backups, binary logging, and three flags; binary logging without backups refused; an unknown flag (404) and two bad values (400); a flags patch that replaces the list; an on-demand run, restore in place, deletion |
| `cloudsql-postgres-connectivity` | 2026-10-13 | `REGIONAL` on `db-custom-1-3840` with `ENCRYPTED_ONLY`, an authorized network, and two flags; the edition/tier, enum, and network refusals; unknown, out-of-range, non-numeric, and duplicate flags; a conflicting `sslMode`/`requireSsl` pair; `connectSettings`; `requireSsl` alone in both directions; `TRUSTED_CLIENT_CERTIFICATE_REQUIRED`; the order of replaced networks; `sslCerts` insert, list, read, duplicate, and delete |

Service-managed fields such as `gceZone`, `serverCaCert`'s certificate, `serviceAccountEmailAddress`, the disk and replication settings, and MySQL users' `etag`, `iamStatus`, and `passwordPolicy` have stand-in values; only their presence is recorded.

```sh
sh scripts/check/recordings.sh       # the recordings
sh scripts/check/backend/cloudsql.sh          # real PostgreSQL and MySQL: connector, published port, TLS, networks, flags, backup and restore
```

`scripts/smoke/environment.py` also connects to both engines from the host in the packaged environment.

## Gaps

- The connector and published-port paths, TLS enforcement, and authorized networks are tested locally only; GCP can't be reached from the recorder. `MYSQL_8_4` is not recorded.
- 501 naming the field for every request field that isn't modeled: other engines, replicas, `activationPolicy` other than `ALWAYS`, `pricingPlan` `PACKAGE`, database flags gcp-local doesn't model, `serverCaMode` other than `GOOGLE_MANAGED_INTERNAL_CA`, `serverCertificateRotationMode`, `pscConfig`, `maintenanceWindow`, `diskEncryptionConfiguration` (CMEK), `finalBackupConfig` and `enableFinalBackup`, `restoreInstanceSettings`, list `filter`, the `location` query parameter, IAM database users, and `users.insert` `passwordPolicy` and `dualPasswordType`. An empty or `false` value of such a field is accepted.
- `instances.failover`, `clone` (point-in-time restore), `restart`, `resetSslConfig`, `listServerCas`, `rotateServerCa`, `generateEphemeralCert`, and `flags.list` are not served. Failover returns 501; the others are unrouted.
- No operation can be cancelled; `operations.wait` doesn't exist in v1beta4.
- No Auth Proxy certificates or private-IP networking.
- Every MySQL connection arrives over the server's local socket, so a user whose `host` excludes `localhost` can't log in.
- A connector bound to a MySQL instance that doesn't exist yet answers in the PostgreSQL protocol.
- No Cloud Run `/cloudsql` socket.
- Durability settings are off. Data and backups are deleted with the instance, on reset, and on restart.
