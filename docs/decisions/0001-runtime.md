# Decision: Go runtime, Python conformance

The emulator process is Go. Conformance runners and repository tooling are Python with pytest.

## Why Go

- Maintained [gRPC with streaming and cancellation](https://github.com/grpc/grpc-go), [generated protobuf bindings](https://protobuf.dev/reference/go/go-generated/), and Google's [CEL implementation](https://github.com/google/cel-go) for IAM conditions.
- The standard library covers HTTP, JSON, contexts, and child processes, so the gateway needs no web framework.
- The Terraform google provider's acceptance suite is Go.

## Alternatives

- **Python** matches LocalStack and the test runner, but LocalStack's AWS runtime isn't reusable for GCP, and Go reaches the gRPC and CEL toolchain without a bridge.
- **Java** has the same libraries but would add a JVM to the core for no current need.
- **Rust** stays an option for a measured bottleneck. No performance claim is made.

## Rules that follow

- Go is pinned with per-platform checksums in `toolchains/go.json` and installed into `.tools/go` per worktree. `scripts/go.sh` sets `GOTOOLCHAIN=local`.
- Commit `go.mod` and `go.sum`. Add a dependency only when a provider needs it.
- One process owns dispatch, identity, IAM, provider state, and lifecycle. Backends are separate processes or embedded libraries. There is no general workflow engine.
