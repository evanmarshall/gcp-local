# Scoped state and backend lifecycle

This layer follows LocalStack's scoped stores, lazy service loading, and reset/stop hooks. It supplies the first Storage catalog and a reusable child-process driver; it does not implement Storage API methods or replace fake-gcs-server. The real integration remains #8.

## State ownership

| Data | Authoritative owner | When shared references change |
| --- | --- | --- |
| Bucket membership in a project and bucket placement | Storage provider catalog, scoped by project + `global` | Publish the bucket reference after successful backend creation; remove it after confirmed deletion |
| Object bytes and object wire metadata | Storage backend (#8) | Do not copy objects into the shared index for ordinary object operations |
| Bucket IAM and other provider-only controls | Storage provider / IAM integration (#7/#8) | Add supported fields to their owning store when their semantics are implemented |
| Canonical resource name, service, kind, project/location, parent reference | Shared resource index | Provider publication/deletion/reset only; no backend databases or duplicate policy payloads |
| Process status, readiness, lifecycle failure category | Backend manager | Startup/probe/reset/shutdown transitions |

The same split applies to emulator-backed providers. Pub/Sub and Firestore each run one official emulator JVM per project scope. That emulator owns topics/subscriptions or documents. The provider owns what the emulator cannot: Pub/Sub push configuration, and Firestore collection-group field/TTL configuration. Reset stops the JVM, which discards backend data, before clearing provider state. See [pubsub.md](pubsub.md) and [firestore.md](firestore.md). Memorystore runs one real `redis-server` per instance inside a per-project/region fleet that the manager starts lazily and stops on reset; the provider owns instance metadata and AUTH strings, and each server owns its keyspace. See [memorystore.md](memorystore.md).

`state.Store` requires an explicit project and location and serializes mutations per scope. Providers supply the service-specific location validator. Storage is global: a bucket's physical region is a placement attribute, not another namespace for the same name. The shared index enforces global bucket-name ownership across projects. Regional/zonal providers can use distinct location keys; store tests verify that project and location resets do not cross those boundaries. Store callbacks perform short in-memory work and must not retain mutable references or re-enter their store. Catalog getters return value copies.

## Lifecycle and reset

Register a backend service definition with a scope validator, an inert driver factory, and an `AfterReset` hook. `Require` or `Preload` starts one instance per service/project/location and waits for readiness. Concurrent starts share that instance; pending callers can cancel while waiting. A preload failure is a startup error: its caller shuts down the manager to stop any earlier successful preloads.

Provider mutations use `Manager.Use` after authorization. It serializes mutations against reset/shutdown for that instance, passes a context cancelled at shutdown, and prevents an in-flight metadata publication from escaping reset. Callbacks must observe that context and must not re-enter the same instance. Different scopes use independent gates.

Pub/Sub pulls and streams use `Manager.Session`, which releases the mutation gate after startup so a publisher can supply the message they await. This path requires a concurrency-safe backend client and cannot publish provider/index metadata. Reset cancels and drains sessions before stopping the backend or clearing state; an unresponsive session leaves reset failed with metadata retained. Listener shutdown first prevents/cancels new and existing sessions, then gives ordinary mutations their drain period. Storage continues using the original serialized path.

Reset proceeds in order: stop the backend, remove/reset its owned assets, then clear provider metadata and that service's index entries for the selected scope. The Storage catalog supplies the final hook. If stopping or asset cleanup fails, the manager reports `failed`, retains metadata/index entries, and refuses subsequent dispatch until an explicit reset succeeds. A failed state hook also leaves the instance failed; hooks must be idempotent for retry. No other project or service is reset.

A readiness failure stops the partially started process using an independent cleanup deadline, even if the requesting client cancelled. Scratch assets remain available for diagnosis until explicit reset. An unexpected process exit is detected on the next required-use readiness probe. Cancelling a caller's probe does not mark an otherwise healthy backend failed. Diagnostics reports the last observed status without starting a backend or performing network probes.

The core drains HTTP/gRPC calls before shutting down managed backends. The manager cancels pending startup/operations and stops independent instances concurrently under the remaining deadline. The process driver uses an absolute command, an explicit environment, a process group, SIGTERM, and SIGKILL at the deadline. It also cleans remaining workers when the leader exits. It does not inherit host credentials/proxies or forward raw child stdout/stderr into diagnostics. Readiness HTTP probes accept numeric loopback addresses only and never follow redirects. Backend adapters remain responsible for listening on private endpoints and resetting only their own assets; the process driver is not a container isolation layer.

## Partial mutations and persistence limits

Publishing the Storage catalog before backend success is forbidden. If a backend creates a bucket but catalog/index publication fails, the Storage provider must compensate by deleting that newly created bucket, or mark the scope failed and reconcile the exact owned resource before serving more mutations. If a backend mutation times out with an unknown outcome, retain the last confirmed metadata and reconcile via a backend read; do not invent success or blindly clear state. These create/delete reconciliation paths belong to the actual backend adapter in #8, where response ambiguity can be tested against its API. The catalog rejects conflicting ownership without publishing a false entry.

All current provider/index state is process-local. `Snapshot` and `Restore` return `ErrPersistenceUnsupported`; diagnostics explicitly reports both capabilities false. Keeping backend files on stop does not constitute a restorable emulator snapshot. A future restore must cover provider stores, index references, and backend assets together. No metadata-only restore or silent partial success is offered.

## Validation

Race-enabled tests cover concurrent lazy start, project/location isolation, global bucket ownership, reset during active mutation, failed asset cleanup with retained metadata, retry, startup failure, readiness timeout, unexpected exit, forced termination, cancelled callers, core shutdown, and explicit persistence gaps. Lifecycle tests launch a controlled real HTTP child process with owned temporary files. CI also runs the runtime/state/backend suite in a network namespace with only loopback; no GCP account or service resource is needed.
