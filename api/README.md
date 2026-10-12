# Transport contracts

Storage input: the complete v1 discovery document from Google's [discovery artifact repository](https://github.com/googleapis/discovery-artifact-manager), with commit, revision, and SHA-256 in `discovery/manifest.json` and its Apache-2.0 license alongside it. It is data for generation, never a list of supported operations.

Memorystore for Redis input: the complete Redis v1 discovery document from the same repository commit, with its own source, revision, and SHA-256 in `discovery/redis-v1.manifest.json` (same Apache-2.0 license). `tools/genapi` verifies the checksum and generates routes only for an explicit method allowlist (instances create/get/list/patch/delete/getAuthString, operations get/list); see [Memorystore](../docs/memorystore.md).

Pub/Sub input: the protoc-generated wire types and descriptors from `cloud.google.com/go/pubsub/v2/apiv1/pubsubpb`, pinned to v2.7.0 in `go.mod` and verified by `go.sum`. These are Google's published bindings for [`google/pubsub/v1/pubsub.proto`](https://github.com/googleapis/googleapis/blob/master/google/pubsub/v1/pubsub.proto). We reuse the published bindings directly instead of maintaining a second fork of their messages/compiler outputs. Their source and license are in the Go module cache; changing them requires a deliberate dependency update.

Generate gcp-local's separate outputs with the pinned Go toolchain:

```sh
sh scripts/go.sh run ./tools/genapi
sh scripts/go.sh run ./tools/genapi -check
```

`tools/genapi` reads the committed discovery document and checksum, plus the compiled protobuf descriptors from the pinned module. It writes Storage route descriptors and typed Pub/Sub server forwarding stubs into `api/generated`. Resource field selectors come from protobuf HTTP annotations; StreamingPull has an explicit initial-subscription selector because it has no HTTP binding. A new unknown streaming shape fails generation. Output is sorted/formatted deterministically; the CI runtime check fails if regeneration differs. Generation needs no GCP account or live discovery request. The first Go dependency installation requires the module proxy, after which it runs offline from the module cache.

Handwritten transport validation and dispatch live in `internal/gateway`; provider semantics will live outside the generated tree. Storage currently decodes JSON objects and escaped route variables, and leaves service field validation to providers. Pub/Sub uses the upstream protobuf codecs. Adding a route or wire type grants neither provider support nor IAM support.

The shared `google.iam.v1.IAMPolicy` gRPC bindings are also generated from the pinned `cloud.google.com/go/iam/apiv1/iampb` descriptors. Their resource selector is unconstrained in the wire definition; the registered Pub/Sub handler validates topic/subscription names and rejects other resource kinds. Get/Set policy are implemented for that slice; TestIamPermissions remains unimplemented. A future service sharing this RPC must add explicit resource-owner dispatch rather than competing registrations or an implicit administrative fallback.
