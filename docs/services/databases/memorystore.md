# Memorystore for Redis

Redis v1 REST control plane at `/v1/projects/{project}/locations/{region}/...`, with each instance backed by a real Redis 7.2.16 server built from the [pinned source](../../../toolchains/redis.json). IAM is checked before any server starts. The control plane is recorded against GCP; Redis connections are tested locally. Linux only.

## Use

```sh
python3 scripts/install/redis.py   # Linux amd64/arm64; needs a C compiler and make
bin/gcp-local -services redis -identity-config ... -iam-config ... \
  -redis-server "$PWD/.tools/redis/bin/redis-server"
```

### Endpoint and AUTH semantics

Connect to the `host` and `port` from `instances.get`. `host` is always `127.0.0.1`, and `port` is not 6379:

```sh
redis-cli -h 127.0.0.1 -p 4568 -a "$AUTH_STRING" PING   # string from getAuthString
```

- The packaged environment publishes each instance on a host port from `GCP_LOCAL_DATA_PORTS` (4568-4583 by default). A full range returns 429; widen it with `up --data-ports FIRST-LAST` (at most 64). Without `-data-ports`, `port` is the server's own random loopback port.
- `authEnabled: true` requires the AUTH string (`NOAUTH` without it, `WRONGPASS` if wrong). `authEnabled: false` needs no password.
- Commands Memorystore [blocks](https://docs.cloud.google.com/memorystore/docs/redis/supported-commands), such as `CONFIG`, `CLIENT`, `COMMAND`, `ACL`, and `SHUTDOWN`, return `NOPERM`. Clients that send `CLIENT SETINFO` on connect see that refusal, as in production.
- Delete and reset close the port and its open sessions. Idle connections through the packaged entry point close after one hour.

## Methods

| Method | Permission | Notes |
| --- | --- | --- |
| `instances.create` | `redis.instances.create` | Returns an operation once the server answers `PING` |
| `instances.get`, `instances.list` | `redis.instances.get`, `.list` | `pageSize` (default and maximum 1,000) and opaque `pageToken`, ordered by name; an invalid token is 400 |
| `instances.patch` | `redis.instances.update` | `displayName`, `labels`, `memorySizeGb`; memory change keeps data |
| `instances.delete` | `redis.instances.delete` | Discards the keyspace |
| `instances.getAuthString` | `redis.instances.getAuthString` | `{}` when AUTH is off |
| `operations.get`, `operations.list` | `redis.operations.get`, `.list` | Creation order, newest 1,000 kept |
| `operations.cancel`, `operations.delete` | `redis.operations.cancel`, `.delete` | Cancel marks `cancelRequested`; the finished work still completes. Not recorded |

Instances report `tier: BASIC`, `redisVersion: REDIS_7_2`, `state: READY`, and `connectMode: DIRECT_PEERING`. Operations report `done: false` for 500 ms, then `done: true` with the response; GCP takes minutes. The instance is `READY` as soon as the operation is returned.

## IAM

Permissions follow [Memorystore access control](https://docs.cloud.google.com/memorystore/docs/redis/access-control). There are no instance-level policies; every check targets the project. A denial is 403 `Permission 'P' denied on 'R'`, naming the addressed instance, operation, or location, with no `details`, as GCP returns. Roles: `roles/redis.admin`, `.editor`, `.viewer`; only Admin has `redis.instances.getAuthString`. IAM is not checked per Redis connection, as in GCP: the AUTH string is the only connection control.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `memorystore-redis-instance-admin` | 2026-10-12 | Create, get, list, patch, delete operations and metadata; instance fields; AUTH string shape; denied read, getAuthString, and delete with their messages and empty `details`; invalid page token (400 with a `BadRequest` on `pageToken`) and negative page size; 409 duplicate; 400 patch without mask; 404 after delete |

Service-managed fields such as `maintenanceVersion`, `nodes`, and `reservedIpRange` have stand-in values; only their presence is recorded.

```sh
sh scripts/check/recordings.sh   # the recording
sh scripts/check/backend/redis.sh             # real redis-server: AUTH, NOPERM, patch, published ports, reset
```

`scripts/smoke/environment.py` also connects from the host in the packaged environment.

## Gaps

- Redis connections, AUTH, and published ports are not recorded.
- An unknown Instance field is 400 with a `BadRequest` violation; a mistyped one is 400 naming its path.
- 501 for `STANDARD_HA`, other Redis versions, TLS, `PRIVATE_SERVICE_ACCESS`, `redisConfigs`, replicas, persistence, maintenance policy, CMEK, reserved IP ranges, tags, `operations.list` filters, and the `-` location wildcard.
- Not routed: clusters, backups, import/export, failover, upgrade.
- Network reachability is not modeled: anything on host loopback can connect. `authorizedNetwork` is stored, not checked.
- An extra ACL user, `gcp-local-admin`, exists on each server.
- No pinned Terraform case. Data is in memory only.
