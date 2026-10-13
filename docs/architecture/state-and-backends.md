# Scoped state and backend lifecycle

Providers keep state in `internal/state` stores scoped by project and location, and run backends through the `internal/backend` manager, one instance per service, project, and location. Every field has one authoritative owner.

## Ownership

| Data | Owner | When it changes |
| --- | --- | --- |
| Provider metadata (bucket catalog, push config, instance metadata, IAM-only fields) | Provider store | After authorization, inside `Manager.Use` |
| Object bytes, messages, documents, rows, database contents | Backend | Backend call inside `Manager.Use` |
| Canonical name, service, kind, project/location, parent | Shared resource index (`state.Index`) | Published after backend success; removed after confirmed deletion or reset |
| Process status and readiness | Backend manager | Start, probe, reset, shutdown |

The index exists for IAM ancestry and cross-service references. It never copies backend data or policy payloads. The Resource Manager provider owns the organization, folder, and project entries; a move changes only the entry's parent (`Index.Reparent`), so no removal hook drops its policy.

## Stores

- `state.Store` requires an explicit project and location and serializes mutations per scope. Each provider supplies its location validator.
- Storage is global: a bucket's region is an attribute, not a namespace. The index enforces global bucket-name ownership across projects.
- The resource hierarchy is global: organizations and folders belong to no project. Resource Manager serializes it under one provider lock instead of a scoped store, and resolves, authorizes, changes, and renders under that lock.
- Store callbacks do short in-memory work, never retain mutable references, and never re-enter their store. Getters return copies.
- A handler looks up, changes, and renders a record inside one `With` callback. Only rendered wire documents or copies leave it; no helper returns a pointer into the store. Wire documents clone record maps and slices.
- A patch validates its whole mask and body before the callback and applies every field inside it, so a rejected patch changes nothing.
- A metadata-only provider (no backend) updates its index entry inside the same callback as the record, and never drops an index error. Create rolls back if the index refuses it.
- A request racing a delete gets `NOT_FOUND`, not the engine's "unverified target" refusal. A metadata-only provider checks the target's existence and calls `Authorize` inside one `With` callback (the engine never re-enters a store). A provider that publishes its index after a backend call resolves the target again when `provider.Unverified` reports that refusal, then authorizes once more.
- Each provider's `concurrency_test.go` hammers create, get, update, and delete of one resource from parallel clients with `providertest.Hammer`; `-race` fails it when state escapes the lock.

## Provider helpers

New and changed provider code uses the helpers in `internal/providers/provider`. Cloud Scheduler uses `Nearest`, `DecodeProto`, `Page`, and the mask helpers; Cloud Tasks uses `Nearest`, `Gate.Check`, `DecodeProto` with `Unmodeled`, `Page`, and the mask helpers; Cloud Run uses `Nearest`, `DecodeProto` with `Unmodeled`, `Page`, and newest-first `Operations` with metadata; Memorystore uses `Gate`, `DecodeJSON`, `Page`, and `Operations`; Cloud SQL uses `Nearest` with `Attributes`, `DecodeJSON`, and `Page`; Bigtable uses `Nearest` (admin and data), `DecodeProto`, `Page`, and named `Operations`; BigQuery uses `Nearest`, `DecodeJSON` with `IgnoreUnknown`, and `Page`, and renders its own v2 error bodies; Spanner admin and Pub/Sub use `Nearest`, `DecodeProto`, and `Page`; Firestore uses `Gate`, `DecodeProto`, and `Operations`; Cloud KMS uses `Nearest` with `Gate.Attributes`, `DecodeProto`, `DecodePolicy`, and `Page`; Secret Manager and IAM service accounts use `Gate.Check`, `DecodeProto`, `DecodePolicy`, and `Page`.

| Helper | Use |
| --- | --- |
| `Gate{Engine, Index, Message, Domain, OmitResource, Attributes}` | One per provider. `Message` is the service's recorded 403 format (`LacksPermission`, `PermissionDeniedOn`, `PermissionDeniedOnResource`, `PermissionDeniedResourceMayNotExist`, `PermissionDeniedUnnamed`, `MissingPermission`, or the provider's own), or a fixed message without verbs (`NotAuthorized`). `Domain` adds ErrorInfo `IAM_PERMISSION_DENIED` with `permission`/`resource` metadata; `OmitResource` drops `resource`. Leave `Domain` empty unless a recorded denial has details. `Attributes(target, resource)` gives `Nearest`'s call its IAM Conditions `resource.*` values from the chosen target and the name the permission is checked on (a KMS version checked at its key). |
| `Nearest(gate, store, scope, call, resource, candidates...)` | Authorize at the first indexed candidate (most specific first, project last) under the scope lock. Returns the decision, the target, and the refusal to return as is: 401, 403, or a wrapped engine refusal. |
| `gate.Check(call, target, resource)` | Authorize one fixed target the same way. |
| `WithDecision(response, decision)` | Stamp the decision ID header on the executed response. |
| `DecodeProto(body, root, message, gap, unmodeled...)` | Decode a body a proto message describes. Unknown name or wrong type: 400 with a BadRequest violation in GCP's transcoder wording. A listed unmodeled field set to a non-default value: `gap`. |
| `Unmodeled(message, modeled...)` | The `DecodeProto` gap list derived from the descriptor: every input field (not `OUTPUT_ONLY`) that no modeled path covers or leads to. A field a later proto adds becomes a gap. Name output fields as modeled when the descriptor doesn't mark them (Cloud Tasks v2beta3). |
| `MaskPaths(mask, update, output)`, `ApplyMask(current, update, paths)` | Resolve an update mask (JSON or proto names; top-level `output` fields refused) and copy the masked fields; an unset masked field is cleared. |
| `DecodeJSON(body, root, &value, gap, Fields{Unmodeled, OutputOnly})` | Decode into a Go struct. List every schema field the struct lacks: unmodeled ones are `gap` unless null or default, output-only ones are dropped, any other name is GCP's 400, or ignored with `IgnoreUnknown` (Cloud SQL and BigQuery, recorded). |
| `DecodePolicy(body, gap)` | Decode a `setIamPolicy` body strictly; `auditConfigs` and an `updateMask` beyond `bindings,etag` are `gap`. |
| `Page(items, query, Paging{Default, Max, InvalidToken, NegativeSize, NegativeDefault, FullPageToken}, gap, handled...)` | One page of already ordered items and the next token. Opaque tokens; a bad token or negative size returns the service's recorded refusal (`NegativeDefault`: a negative size is the default, Cloud Run); `FullPageToken` issues a token after every full page, even the last (Cloud Tasks tasks); `filter`/`orderBy` is a gap unless named in `handled`. |
| `ListBody(field, items, next)` | The list response, omitting empty fields. |
| `NewOperations(validate, latency, metadata)` | A scoped `google.longrunning` store: `Start(scope, Launch{Target, Verb, Metadata, Immediate})`, `Finish`, `Get`, `List`, `Cancel`, `Delete`, `Wait`, `Reset`. `done` is false until `Finish` and `latency` have both passed; `Immediate` skips the latency for a method GCP answers already done (Bigtable). `Metadata` is a payload the renderer sees as `Operation.Metadata`, with the finished response as `Operation.Response` (Cloud Run renders its `Service`). `Named` replaces `OperationName` (Bigtable names operations after their resource); `NewestFirst` lists the newest first (Cloud Run). |
| `ErrorInfo`, `FieldViolation`, `BadRequest`, `ResourceInfo` | Status errors with details; the gateway renders them in the REST `details` array and gRPC carries them natively. |
| `CredentialsMissing(service, rpc)` | GCP's 401 for a call without credentials, with ErrorInfo `CREDENTIALS_MISSING` naming the host and RPC (recorded for Bigtable and BigQuery). `Gate` returns a plain 401; a provider with the recording swaps it for an anonymous caller. |

Authorize and execute:

```go
decision, target, err := provider.Nearest(p.gate, p.scopes, scope, call, jobName(project, location, id),
	jobResource(project, location, id), provider.ProjectResource(project))
if err != nil {
	return nil, err // 401, 403, or a wrapped engine refusal
}
call.Resource = target.Name
response, err := p.execute(call, request)
if err != nil {
	return nil, err
}
return provider.WithDecision(response, decision), nil
```

`Nearest` holds the scope lock while it picks and checks the target, so every index change of the service's resources must hold the same lock (`store.With(scope, ...)`). A request racing a delete then authorizes at the parent and reads 404, never the engine's 501 for an unindexed target.

Run a mutation as an operation: `Start` before the work, `Finish` with the response (`"@type"` included) after it, and `Delete` the operation if the request fails synchronously. Serve `operations.get`/`list`/`cancel`/`delete` from the store.

Limits:

- Recorded denials of Cloud Scheduler, Cloud Tasks, Cloud Run, Memorystore, Firestore, Bigtable, and BigQuery carry no `details`, and their wording differs (`cloudscheduler-job-lifecycle`, `cloudtasks-dispatch-retry`, `run-service-lifecycle`, `memorystore-redis-instance-admin`, `firestore-document-lifecycle`, `bigtable-instance-table-admin`, `bigquery-api-surface`). Pub/Sub's, Spanner's, Secret Manager's, and IAM's carry ErrorInfo domain `iam.googleapis.com`, Cloud KMS's `cloudkms.googleapis.com`; Secret Manager's and Cloud KMS's metadata name only the permission. GCP's per-request `error_info_id`, `troubleshooter_url`, and Troubleshooter sentence are not modeled; the security scenarios drop them before comparing (`gcp_validation/services/security/refusals.py`). Record a denial before setting `Domain`.
- Cloud Run, Cloud Tasks, and Cloud Scheduler set no `Attributes`: GCP gives Cloud Run's service policies no resource attributes (`run-service-lifecycle`), and the other two are not recorded.
- `DecodeProto` words a mistyped wrapper (`Int64Value`) and an array for a non-repeated field as GCP does (recorded by Cloud KMS and IAM).
- Page-token and page-size refusals differ per service; set `Paging` from the service's recording. A refusal that quotes the request (Pub/Sub's, Spanner's, and Secret Manager's name the token) uses a sentinel `InvalidToken` the provider replaces. Spanner treats a negative size as the maximum; Secret Manager as unset.
- `Operations` renders `google.longrunning` operations only. Cloud SQL's `sql#operation` (a `status` of `PENDING` or `DONE`, UUID names, no location) keeps its own records, `PENDING` for 500 ms after the work ends.
- Page tokens aren't bound to the other list parameters.
- Storage's JSON API doesn't fit `Gate`, `Nearest`, or `Page`. Its recorded denials are wire bodies with an `errors[]` list, actor-specific wording, and `text/html` on uploads; `authorize` in the Storage provider renders them. Its list tokens resume after the last entry's name and it pages by `maxResults`; `Page`'s offset tokens would shift when entries change between pages (`storage-list-paging`). It uses `ListBody` and `DecodeJSON` (folders).

## Backend lifecycle

1. Register a definition with a scope validator, a driver factory, and an `AfterReset` hook.
2. The first `Use` starts the instance and waits for readiness. Concurrent starts share one instance. Startup runs under the manager lifetime and start timeout, so a caller that gives up mid-start gets its cancellation error and the backend still comes up.
3. Mutations run in `Manager.Use` after authorization. It serializes against reset and shutdown for that instance and passes a context cancelled at shutdown. Don't re-enter the same instance from the callback.
4. Long-lived reads (Pub/Sub pull and streaming pull) use `Manager.Session`, which releases the mutation gate after startup. Sessions can't publish metadata. Reset cancels and drains them first.

Reset order for one scope: stop the backend, remove its owned assets, then clear provider metadata and that service's index entries. If stop or asset cleanup fails, the instance is `failed`, metadata is retained, and dispatch is refused until a reset succeeds. Hooks must be idempotent. No other project or service is touched.

The process driver runs an absolute command with an explicit environment in its own process group, sends SIGTERM, then SIGKILL at the deadline. It doesn't inherit host credentials or proxies. Readiness probes accept numeric loopback addresses only and don't follow redirects. It is not a container isolation layer.

## Partial failures

- Never publish metadata before backend success.
- If the backend succeeds and publication fails, delete the new backend resource or mark the scope failed and reconcile before more mutations.
- If a backend call times out with an unknown outcome, keep the last confirmed metadata and reconcile with a backend read. Don't invent success or clear state.

## Limits

- All state is process-local and lost on restart.
- Diagnostics reports the last observed status; it never starts a backend or probes the network.
- Per-service backend details: [Pub/Sub](../services/messaging/pubsub.md), [Firestore](../services/databases/firestore.md), [Memorystore](../services/databases/memorystore.md), [Cloud SQL](../services/databases/cloudsql.md), [Spanner](../services/databases/spanner.md).
