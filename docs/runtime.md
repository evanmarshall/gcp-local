# Core runtime

The core process serves health/diagnostics and the [REST/gRPC dispatch layer](gateway.md). It has no service providers; GCP API requests return an explicit `501 UNIMPLEMENTED` response. This is the process foundation for #4–#8, not an application-compatible Storage emulator.

## Build and run

Python 3.11+ installs the checksum-pinned Go toolchain into this checkout on macOS arm64 or Linux arm64/amd64. No global Go installation or cloud credentials are needed. Race-enabled tests require a C compiler: Xcode Command Line Tools on macOS, or `build-essential` on Debian/Ubuntu.

```sh
python3 scripts/install_go.py
sh scripts/check_go.sh
bin/gcp-local -listen 127.0.0.1:4566 -grpc-listen 127.0.0.1:4567
```

From another terminal:

```sh
curl --fail http://127.0.0.1:4566/_local/health
curl --fail http://127.0.0.1:4566/_local/diagnostics
```

Health reports process readiness, registered providers, and observed backend states. The default process has no service providers or backend instances. Diagnostics reports Go/platform versions and never forwards to a backend. SIGINT or SIGTERM closes both listeners, drains active HTTP requests and gRPC calls, and exits successfully. `-shutdown-timeout` defaults to ten seconds; expiration forces connections closed and reports a nonzero exit. A bind failure on either listener also exits nonzero. Logs are JSON on stdout and omit request credentials. The default listener is loopback; binding another interface explicitly exposes the local process on that interface.

For focused development, run `sh scripts/go.sh fmt ./...` and `sh scripts/go.sh test -race ./internal/runtime ./cmd/gcp-local`. `check_go.sh` checks formatting, vets, runs race-enabled tests, builds, and exercises actual process startup/SIGTERM. CI runs it on Linux amd64; moo development runs it on Linux arm64. Toolchain and dependency choices are recorded in [the runtime decision](decisions/0001-runtime.md). The binary defaults to version `dev`; release builds can set `-ldflags '-X main.version=VERSION'`.

## Moo

In the task worktree, use the machine setup in [moo.md](moo.md), then:

```sh
moo run task-name -- 'cd /srv/app && python3 scripts/install_go.py && sh scripts/check_go.sh'
```

For an existing baseline created before the runtime, install `build-essential` inside the task machine first (`moo run task-name -- 'apt-get update && apt-get install -y --no-install-recommends build-essential'`). Build outputs and the Go toolchain stay inside the guest. Tests use ephemeral loopback ports. To use the existing moo HTTP forwarding interactively, start `bin/gcp-local -listen 0.0.0.0:8080` inside that machine and use its configured host port. Moo currently forwards HTTP only; gRPC transport tests use loopback inside the guest. The core owns [scoped backend lifecycle hooks](state-and-backends.md); concrete backend adapters and the packaged `up` workflow arrive in subsequent issues.
