# gcp-local

A local GCP emulator following LocalStack's architecture: generated API contracts, service providers, reusable emulator backends, and cloud-recorded conformance tests. GCP identity, IAM enforcement, and analytical networking are core design goals.

**Status:** offline validation runners, manual keyless cloud CI, and four real-GCP baselines covering Storage lifecycle, same- and cross-project authorization, and selected upstream Python SDK assertions. The Go core has health/diagnostics and shared REST/gRPC dispatch, scoped stores, backend lifecycle hooks, and local ADC/metadata identity; service API providers are still pending. Local comparison is tested against a controlled protocol server; no supported-service release is available yet. The validation prerequisites for runtime implementation are complete.

- [Build and run the core process](docs/runtime.md)
- [Configure local ADC and workload identity](docs/local-identity.md)
- [Design and architecture diagram](docs/design.md)
- [Direct comparison with archived LocalStack](docs/localstack-comparison.md)
- [Implementation roadmap](docs/roadmap.md)
- [Validation runners and evidence](docs/validation.md)
- [GCP sandbox configuration](docs/gcp-sandbox.md)
- [Portable agent setup for application repositories](docs/agent-setup.md)
- [GitHub issues](https://github.com/evanmarshall/gcp-local/issues)
- [Contributing and local setup](CONTRIBUTING.md)
- [Isolated agent development with moo](docs/moo.md)
- [Agent instructions](AGENTS.md)

The canonical design now lives in this repository. The original pre-review outline is retained in `docs/gcp-local.before-localstack-review.md` as historical context.

The [pinned upstream SDK slice](docs/sdk-compatibility.md) and Linux isolation launcher validate client behavior before provider integration.
