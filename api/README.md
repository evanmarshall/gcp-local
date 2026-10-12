# Transport contracts

Storage input: the complete v1 discovery document from Google's [discovery artifact repository](https://github.com/googleapis/discovery-artifact-manager), with commit, revision, and SHA-256 in `discovery/manifest.json` and its Apache-2.0 license alongside it. It is data for generation, never a list of supported operations.

Memorystore for Redis input: the complete Redis v1 discovery document from the same repository commit, with its own source, revision, and SHA-256 in `discovery/redis-v1.manifest.json` (same Apache-2.0 license). `tools/genapi` verifies the checksum and generates routes only for an explicit method allowlist (instances create/get/list/patch/delete/getAuthString, operations get/list); see [Memorystore](../docs/memorystore.md).
Cloud SQL input: the complete Cloud SQL Admin v1beta4 discovery document from the same discovery artifact commit, with its source, revision, and SHA-256 in `discovery/sqladmin-v1beta4.manifest.json`. The generator emits only an allowlisted method slice and fails if a listed method is missing. v1beta4 is used because the v1 paths (`/v1/projects/{project}/instances`) collide with the Spanner admin routes on the path-only gateway; see [Cloud SQL](../docs/cloudsql.md).

Pub/Sub input: the protoc-generated wire types and descriptors from `cloud.google.com/go/pubsub/v2/apiv1/pubsubpb`, pinned to v2.7.0 in `go.mod` and verified by `go.sum`. These are Google's published bindings for [`google/pubsub/v1/pubsub.proto`](https://github.com/googleapis/googleapis/blob/master/google/pubsub/v1/pubsub.proto). We reuse the published bindings directly instead of maintaining a second fork of their messages/compiler outputs. Their source and license are in the Go module cache; changing them requires a deliberate dependency update.

Spanner and Bigtable data-plane input: the protoc descriptors of `cloud.google.com/go/spanner/apiv1/spannerpb` (v1.95.1) and `cloud.google.com/go/bigtable/apiv2/bigtablepb` (v1.58.0) from `go.mod`. `tools/genapi` emits typed stubs for an explicit method allowlist only (`spanner_dispatch.go`, `bigtable_dispatch.go`); unary and server-streaming methods take their resource selector from the first `google.api.http` path variable, and a client-streaming method fails generation. Server-streaming stubs call `GRPCDispatch.ServerStream` with the single decoded request.

Firestore gRPC input: the published `cloud.google.com/go/firestore/apiv1/firestorepb` bindings, pinned to v1.22.0 in `go.mod`. `firestore_dispatch.go` stubs every `google.firestore.v1.Firestore` method; resource selectors merge each method's root and nested HTTP bindings (`A` and `A/*/**`) into `A/**`, where a trailing `**` matches zero or more segments. Firestore REST routes use `{path=**}` variables that span segments; the gateway unescapes each segment separately and rejects escaped slashes and dot segments.

Generate gcp-local's separate outputs with the pinned Go toolchain:

```sh
sh scripts/go.sh run ./tools/genapi
sh scripts/go.sh run ./tools/genapi -check
```

`tools/genapi` reads the committed discovery document and checksum, plus the compiled protobuf descriptors from the pinned module. It writes Storage route descriptors and typed Pub/Sub server forwarding stubs into `api/generated`. Resource field selectors come from protobuf HTTP annotations; StreamingPull has an explicit initial-subscription selector because it has no HTTP binding. A new unknown streaming shape fails generation. Output is sorted/formatted deterministically; the CI runtime check fails if regeneration differs. Generation needs no GCP account or live discovery request. The first Go dependency installation requires the module proxy, after which it runs offline from the module cache.

Handwritten transport validation and dispatch live in `internal/gateway`; provider semantics will live outside the generated tree. Storage currently decodes JSON objects and escaped route variables, and leaves service field validation to providers. Pub/Sub uses the upstream protobuf codecs. Adding a route or wire type grants neither provider support nor IAM support.

The shared `google.iam.v1.IAMPolicy` gRPC bindings are also generated from the pinned `cloud.google.com/go/iam/apiv1/iampb` descriptors. Their resource selector is unconstrained in the wire definition; the registered Pub/Sub handler validates topic/subscription names and rejects other resource kinds. Get/Set policy are implemented for that slice; TestIamPermissions remains unimplemented. A future service sharing this RPC must add explicit resource-owner dispatch rather than competing registrations or an implicit administrative fallback.
