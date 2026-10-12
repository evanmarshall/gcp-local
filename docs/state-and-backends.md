# Scoped state and backend lifecycle

Providers keep state in `internal/state` stores scoped by project and location, and run backends through the `internal/backend` manager, one instance per service, project, and location. Every field has one authoritative owner.

## Ownership

| Data | Owner | When it changes |
| --- | --- | --- |
| Provider metadata (bucket catalog, push config, instance metadata, IAM-only fields) | Provider store | After authorization, inside `Manager.Use` |
| Object bytes, messages, documents, rows, database contents | Backend | Backend call inside `Manager.Use` |
| Canonical name, service, kind, project/location, parent | Shared resource index (`state.Index`) | Published after backend success; removed after confirmed deletion or reset |
| Process status and readiness | Backend manager | Start, probe, reset, shutdown |

The index exists for IAM ancestry and cross-service references. It never copies backend data or policy payloads.

## Stores

- `state.Store` requires an explicit project and location and serializes mutations per scope. Each provider supplies its location validator.
- Storage is global: a bucket's region is an attribute, not a namespace. The index enforces global bucket-name ownership across projects.
- Store callbacks do short in-memory work, never retain mutable references, and never re-enter their store. Getters return copies.

## Backend lifecycle

1. Register a definition with a scope validator, a driver factory, and an `AfterReset` hook.
2. `Require` or `Preload` starts the instance and waits for readiness. Concurrent starts share one instance.
3. Mutations run in `Manager.Use` after authorization. It serializes against reset and shutdown for that instance and passes a context cancelled at shutdown. Don't re-enter the same instance from the callback.
4. Long-lived reads (Pub/Sub pull and streaming pull) use `Manager.Session`, which releases the mutation gate after startup. Sessions can't publish metadata. Reset cancels and drains them first.

Reset order for one scope: stop the backend, remove its owned assets, then clear provider metadata and that service's index entries. If stop or asset cleanup fails, the instance is `failed`, metadata is retained, and dispatch is refused until a reset succeeds. Hooks must be idempotent. No other project or service is touched.

The process driver runs an absolute command with an explicit environment in its own process group, sends SIGTERM, then SIGKILL at the deadline. It doesn't inherit host credentials or proxies. Readiness probes accept numeric loopback addresses only and don't follow redirects. It is not a container isolation layer.

## Partial failures

- Never publish metadata before backend success.
- If the backend succeeds and publication fails, delete the new backend resource or mark the scope failed and reconcile before more mutations.
- If a backend call times out with an unknown outcome, keep the last confirmed metadata and reconcile with a backend read. Don't invent success or clear state.

## Limits

- All state is process-local. `Snapshot` and `Restore` return `ErrPersistenceUnsupported`.
- Diagnostics reports the last observed status; it never starts a backend or probes the network.
- Per-service backend details: [Pub/Sub](pubsub.md), [Firestore](firestore.md), [Memorystore](memorystore.md), [Cloud SQL](cloudsql.md), [Spanner](spanner.md).
