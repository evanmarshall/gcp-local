# gcp-local

A local GCP emulator following LocalStack's architecture: generated API contracts, service providers, reusable emulator backends, and cloud-recorded conformance tests. GCP identity, IAM enforcement, and analytical networking are core design goals.

**Status:** authenticated [Cloud Storage](docs/storage.md) runs with local ADC and basic IAM. Seven real-GCP recordings and three adapted upstream Terraform acceptance cases pass against the actual backend. The [packaged quickstart](docs/quickstart.md) starts, resets, and tears down a private environment without production credentials. An initial [Pub/Sub provider](docs/pubsub.md) passes local SDK, IAM, streaming, authenticated push, and lifecycle tests, and one recorded pull/authorization scenario now matches real GCP behavior; packaged setup remains pending. Other service providers remain pending.

- [Start local Storage](docs/quickstart.md)
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

The [pinned upstream SDK slice](docs/sdk-compatibility.md) and Linux isolation launcher compare the provider with recorded cloud behavior.
