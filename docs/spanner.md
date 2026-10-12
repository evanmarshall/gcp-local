# Spanner

The Spanner provider serves the admin v1 REST surface at `/v1/projects/{project}/...` and the `google.spanner.v1.Spanner` data plane over gRPC (select it with `-services=...,spanner`). Its backend is Google's official **Cloud Spanner emulator 1.5.58** (`emulator_main`), pinned in [toolchains/spanner.json](../toolchains/spanner.json). The provider is the authentication and authorization gate: it authorizes each call with the shared IAM engine before any backend start or call, then forwards it over gRPC to the emulator and renders the response as proto3 JSON.

## Ownership

| Data | Owner |
| --- | --- |
| Instances, databases, schemas (DDL), long-running operations, instance configs | Emulator, one process per project scope |
| Sessions, transactions, rows | Emulator |
| Instance and database IAM policies | IAM engine (`getIamPolicy`/`setIamPolicy` never reach the emulator; it does not enforce IAM) |
| Instance/database references used for policy inheritance | Shared index, published after backend success and removed after confirmed deletion (an instance delete removes its databases' references and policies) |
| Database `createTime` | Provider, recorded when it observes a successful create |
| Service-managed defaults listed below | Provider |

Each project gets its own lazily started emulator on a random loopback port, with a private temporary directory and an environment of only `PATH`, `HOME`/`TMPDIR` (that directory), and `LD_LIBRARY_PATH`; no credentials, ADC, or proxies reach it. Reset stops that project's emulator (discarding its in-memory state) and then clears the project's Spanner policies, references, and recorded create times; other projects are untouched. A create or delete with an uncertain outcome is reconciled by reading the emulator; if that read fails, the scope refuses further Spanner calls until reset. Nothing persists across restarts.

## Why gRPC rather than the REST gateway

The image also ships `gateway_main`, a grpc-gateway front end. The provider calls the emulator's gRPC admin APIs instead and transcodes REST following the `google.api.http` annotations. That keeps one child process per project with a gRPC readiness probe, passes the emulator's gRPC status codes straight into the gateway's GCP error envelope (`{"error": {"code", "status", "message"}}`), and lets the provider control proto3 JSON rendering (unset fields omitted, as GCP does). Path-bound names always override names in a request body, so a body cannot redirect a call to another project or resource.

## Recorded behavior and divergences

The `spanner-instance-database-admin` recording (real GCP, 2026-10-11) passes in comparison mode. Observed against the emulator:

- Instance create, database create, and DDL update return `done: true` operations with the recorded metadata/response types; `GET .../operations/{id}` routes exist and are authorized with `spanner.instanceOperations.get` / `spanner.databaseOperations.get`. Operation names are emulator-assigned (`.../operations/_auto0`).
- The emulator's canonical DDL matches the recording exactly, duplicate creates return 409, deletes return 200 `{}`, and later reads 404.
- The emulator omits several service-managed fields that GCP returns. The provider fills them only when the emulator leaves them unset; the recording compares only their presence:
  - Instance: `instanceType` (`PROVISIONED`), `edition` (`STANDARD`), `defaultBackupScheduleType` (`AUTOMATIC`), `resourceLocation` and `replicaComputeCapacity` (location derived from the config id, e.g. `regional-us-central1` → `us-central1`, with the instance's processing units).
  - Database: `createTime` (provider-observed), `versionRetentionPeriod` (`1h`, or the `version_retention_period` option read from the emulator's DDL), and `earliestVersionTime` (the later of `createTime` and now minus retention). Create responses add `resourceLocationId`; reads and lists add `encryptionInfo` (`GOOGLE_DEFAULT_ENCRYPTION`), matching the recorded field sets.
- Because the emulator silently drops them, non-default `edition`, `instanceType`, `defaultBackupScheduleType`, and any `autoscalingConfig` are refused with 501 `UNIMPLEMENTED` before the backend is called.

The emulator accepts any instance config name on create but lists and serves only `emulator-config`; `instanceConfigs.get` for other names returns the emulator's response. Instance updates return the emulator's 501 (`Cloud Spanner Emulator does not support updating instances`). Database field updates are forwarded and return what the emulator returns. Backups cannot be created locally; `backups.list` confirms the instance exists and returns an empty collection rather than the emulator's 501.

Predefined roles pinned: `roles/spanner.admin`, `roles/spanner.databaseAdmin`, `roles/spanner.viewer`. The Terraform `TestAccSpannerInstanceAndDatabase` case drives `google_spanner_instance` (with `force_destroy`) and `google_spanner_database` through `spanner_custom_endpoint` (see [terraform-compatibility.md](terraform-compatibility.md)).

Not exposed: backups/restores, instance partitions, database roles, operation list/cancel, and `testIamPermissions`.

## Data plane

Applications can read and write rows with the official client libraries over gRPC (port 4567 locally) using local ADC credentials. Modeled locally, not recorded.

| gRPC method | REST route (unary only) | Permission on the session's database |
| --- | --- | --- |
| `CreateSession`, `BatchCreateSessions` | `sessions`, `sessions:batchCreate` | `spanner.sessions.create` |
| `GetSession`, `ListSessions`, `DeleteSession` | `sessions/{s}`, `sessions` | `spanner.sessions.get` / `.list` / `.delete` |
| `ExecuteSql`, `ExecuteStreamingSql` | `:executeSql` | `spanner.databases.select` |
| `ExecuteBatchDml` | `:executeBatchDml` | `spanner.databases.write` |
| `Read`, `StreamingRead` | `:read` | `spanner.databases.read` |
| `BeginTransaction` | `:beginTransaction` | read-write: `spanner.databases.beginOrRollbackReadWriteTransaction`; read-only: `spanner.databases.beginReadOnlyTransaction` |
| `Commit` | `:commit` | `spanner.databases.write` |
| `Rollback` | `:rollback` | `spanner.databases.beginOrRollbackReadWriteTransaction` |
| `PartitionQuery`, `PartitionRead` | `:partitionQuery`, `:partitionRead` | `spanner.databases.partitionQuery` / `.partitionRead` |

Permissions come from the per-method "Authorization requires" rows of [the v1 REST reference](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases.sessions). A request that begins a transaction inline (`transaction.begin`, or a single-use read-write `Commit`) also needs the matching `BeginTransaction` permission; that addition is a local model, not documented per method. `roles/spanner.viewer` holds no data permission except `beginReadOnlyTransaction`; `roles/spanner.databaseAdmin` and `roles/spanner.admin` hold all of them.

Authorization runs once per call, before the emulator starts or receives anything. Streaming calls authorize the single request, then relay the emulator's stream; client cancellation and scope reset cancel the backend call. The emulator owns sessions and transactions, one emulator per project.

Go client example (the gateway listens on plaintext loopback, so the per-RPC credential must not require TLS):

```go
conn, _ := grpc.NewClient("127.0.0.1:4567",
	grpc.WithTransportCredentials(insecure.NewCredentials()),
	grpc.WithPerRPCCredentials(localToken)) // RequireTransportSecurity() == false
client, _ := spanner.NewClientWithConfig(ctx, "projects/p/instances/i/databases/d",
	spanner.ClientConfig{DisableNativeMetrics: true}, option.WithGRPCConn(conn))
```

The Python client is not verified; the channel setup from [Pub/Sub](pubsub.md#run-the-provider) is the expected starting point. Never set `SPANNER_EMULATOR_HOST`: it selects anonymous credentials, which the gateway refuses.

Limits:

- `ExecuteStreamingSql` and `StreamingRead` are gRPC only; the REST `:executeStreamingSql` and `:streamingRead` routes are not served.
- Partitioned DML, Data Boost, fine-grained access control (`creator_role`), single-use read-write `ExecuteSql`/`Read`, and `BatchWrite` return `UNIMPLEMENTED`.
- Request messages are limited to 2 MiB.
- Query, transaction, and lock behavior is the emulator's, including its one-read-write-transaction-at-a-time limit.
- The `spanner-data-plane-rest` scenario (`tests/conformance/spannerdata`) is not recorded; local runs report it unverified.

## Platforms and installation

The emulator is distributed for Linux only. Supported paths are Linux CI, Moo task machines, and the [packaged Docker image](quickstart.md); **macOS hosts are unsupported** for this backend (`gcp-local -services spanner` refuses to start there). Unit tests use a fake driver and run anywhere.

```sh
python3 scripts/install_spanner.py   # linux/amd64 or linux/arm64
```

The installer needs neither Docker nor gcloud. It fetches the per-architecture manifest of `gcr.io/cloud-spanner-emulator/emulator:1.5.58` by digest anonymously from the registry v2 API, verifies the manifest and layer blob digests, extracts only `emulator_main` and the image's `libstdc++.so.6`, and checks each file's pinned SHA-256 into `.tools/spanner/`. Redirected blob downloads never carry the registry token, and ambient proxies are ignored.

`emulator_main` requires `GLIBC_2.35` and `GLIBCXX_3.4.32`. Debian 12's system libstdc++ (GCC 12) stops at `GLIBCXX_3.4.30`, so the provider sets `LD_LIBRARY_PATH` to the installed `lib/` directory; glibc (2.35+), `libm`, and `libgcc_s` come from the host. On the arm64 Moo machine (Debian 12.15), `ldd` resolves all four with that path, the emulator listened after about 0.02 s, and its RSS was about 29 MiB when idle. The packaged image (Ubuntu 22.04 Temurin base, glibc 2.35) carries the files in `/opt/spanner`.

```sh
bin/gcp-local -services spanner -identity-config ... -iam-config ... \
  -spanner-emulator "$PWD/.tools/spanner/emulator_main" \
  -spanner-libraries "$PWD/.tools/spanner/lib"
```

At startup the provider verifies both files against the pins compiled for the running architecture. The conformance fixture reads `GCP_LOCAL_SPANNER_EMULATOR` and `GCP_LOCAL_SPANNER_LIBRARIES` (the check scripts default them to `.tools/spanner`).

## Verification

```sh
sh scripts/check_spanner.sh          # real emulator, loopback-only runner
sh scripts/check_rest_recordings.sh  # includes the Spanner recording
sh scripts/check_terraform.sh
```

`check_spanner.sh` drives the provider against the real emulator: a denied call starts no backend, the recorded DDL canonical form, project isolation, and reset discarding emulator state while another project's survives. Its data-plane test runs the official Go client (`cloud.google.com/go/spanner` v1.95.1) with local-STS tokens over the gRPC gateway: mutations, DML, batch DML, point reads, streaming queries, read-only and partitioned transactions, rollback, cancellation, and viewer/anonymous denial before the emulator starts. Go unit tests with a fake driver cover authorization before any backend call, reset, project isolation, path binding over body names, reference publication, and reconciliation of uncertain creates.
