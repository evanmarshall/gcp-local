# gcp-local

A local GCP emulator following LocalStack's architecture: generated API contracts, service providers, reusable emulator backends, and cloud-recorded conformance tests. GCP identity, IAM enforcement, and analytical networking are core design goals.

**Status:** validation runners and one reviewed, revalidated real-GCP Storage lifecycle baseline. Local comparison is tested against a controlled protocol server. No emulator runtime or supported-service release is available yet.

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
