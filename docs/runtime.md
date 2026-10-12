# Core runtime

`bin/gcp-local` is the single core process: gateway, identity, IAM, providers, and backend lifecycle ([design](design.md)). It builds with a Go toolchain pinned inside the checkout.

## Build and run

```sh
python3 scripts/install_go.py
sh scripts/check_go.sh
bin/gcp-local -listen 127.0.0.1:4566 -grpc-listen 127.0.0.1:4567
curl --fail http://127.0.0.1:4566/_local/health
```

- `install_go.py` installs the checksum-pinned Go from `toolchains/go.json` into `.tools/go` (macOS arm64, Linux arm64/amd64). `scripts/go.sh` runs it with `GOTOOLCHAIN=local`. Don't use an ambient Go.
- Race tests need a C compiler: Xcode Command Line Tools or `build-essential`.
- `check_go.sh` runs gofmt, `genapi -check`, vet, race tests, a build, and a process start/SIGTERM smoke.
- Focused loop: `sh scripts/go.sh fmt ./...` and `sh scripts/go.sh test -race ./internal/runtime ./cmd/gcp-local`.
- Release builds set the version with `-ldflags '-X main.version=VERSION'`; the default is `dev`.

## Flags

- With no `-services`, no provider is registered and every API method returns `UNIMPLEMENTED` (HTTP 501). `-services` takes comma-separated keys; `SERVICE_FLAG` in `gcp_local/bootstrap.py` is the full list. `bin/gcp-local -h` lists every flag.
- `-grpc-tls-listen 127.0.0.1:4584 -grpc-tls-dir DIR` adds a TLS listener for the same gRPC server. On first start the core creates `DIR` (mode 0700) with `ca.pem`, `server.pem`, and `server-key.pem` (mode 0600); later starts reuse them ([gRPC over TLS](clients.md#grpc-over-tls)).
- `-identity-config` and `-iam-config` take explicit owner-only files ([local identity](local-identity.md), [IAM](iam.md)).
- Emulator-backed providers need absolute backend paths (`-pubsub-java`, `-pubsub-jar`, `-firestore-jar`, `-spanner-emulator`, `-redis-server`, `-cloudsql-postgres`). The [packaged environment](quickstart.md) sets all of them.

## Behavior

- `/_local/health` reports readiness, registered providers, and backend states. `/_local/diagnostics` reports Go and platform versions. Neither calls a backend.
- SIGINT or SIGTERM drains HTTP and gRPC calls, then stops backends. `-shutdown-timeout` (default 10s) forces close and exits nonzero when it expires. A bind failure exits nonzero.
- Logs are JSON on stdout and omit credentials.
- Listeners default to loopback. Binding another interface exposes the process there.
