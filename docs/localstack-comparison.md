# LocalStack mapping

Each gcp-local abstraction maps to a LocalStack source analogue or to a documented GCP requirement. A new abstraction needs one or the other; cite it in review.

Reference: [localstack/localstack](https://github.com/localstack/localstack) at `8b9a79f05846835cf4dff63ab7eefdde9df83783`, the archived `main` HEAD (2026-03-23). An optional checkout lives at `../localstack`; don't edit or vendor it. Links below point at that revision.

| Area | LocalStack source | gcp-local equivalent |
| --- | --- | --- |
| Gateway | `LocalstackAwsGateway` builds handler chains around a request context and dispatches to services. [Gateway][gateway] | `internal/gateway`: REST and gRPC entry points with method, principal, project, location, and trace context |
| Generated API | ASF generates service API stubs; parsing and serialization are separate from providers. [ASF][asf] | `tools/genapi` generates `api/generated/` from discovery documents and protobuf descriptors |
| Backend reuse | Providers fall back to Moto or forward HTTP; Kinesis forwards to its backend and adds policy handlers. [Providers][providers], [Kinesis][kinesis] | Providers wrap official emulators, fake-gcs-server, bttest, PostgreSQL, MySQL, and Redis, and add only missing behavior |
| Scoped state | `BaseStore` and `AccountRegionBundle` scope state per account and region; the state API visits stores and asset directories. [Stores][stores], [State][state] | `internal/state` stores scoped by project and location, plus a shared resource index |
| Lifecycle | `ServiceLoader` loads services on demand; Kinesis stops its backend before reset and starts it lazily. [Loader][loader], [Kinesis][kinesis] | `internal/backend` manager: lazy start, readiness, reset, shutdown |
| Async work | SNS delivery publishers and dispatcher; Lambda Docker executor. [SNS][sns], [Lambda][lambda] | Provider-owned workers: Pub/Sub push, Cloud Tasks dispatch, Cloud Run containers |
| Recorded tests | Snapshot tests record AWS responses and compare local runs; transformers keep reference relationships; validation metadata tracks cloud runs. [Parity][parity], [Validation][validation] | `gcp_validation` pytest plugin and `tests/conformance/recordings/` |
| Terraform | An allowlist of Terraform AWS provider acceptance tests. [Terraform][terraform] | Pinned google provider cases in `tests/terraform/` |
| Database ports | External service ports for databases such as PostgreSQL and Redis. [Ports][ports] | `-data-ports` listeners for Cloud SQL and Memorystore |

## Where gcp-local departs

These come from GCP requirements, not from LocalStack:

- **Strict IAM.** LocalStack's default gateway injects missing auth headers, and its IAM policy simulator is not a cross-service enforcement engine. [Auth handler][auth], [Simulator][simulator]. gcp-local authorizes every request and fails closed.
- **Per-SDK credential paths.** Python Pub/Sub switches to anonymous credentials when `PUBSUB_EMULATOR_HOST` is set ([client source](https://github.com/googleapis/python-pubsub/blob/main/google/cloud/pubsub_v1/publisher/client.py)), so IAM tests use endpoint overrides with real credentials.
- **State-dependent permissions.** Replacing a Storage object needs `storage.objects.delete` as well as `create` ([Storage permissions](https://docs.cloud.google.com/storage/docs/access-control/iam-permissions)), so permission plans take resolved state features.
- **Analytical networking.** Reachability is computed from configuration, as GCP's [Connectivity Tests](https://docs.cloud.google.com/network-intelligence-center/docs/connectivity-tests/concepts/overview#considerations_and_constraints) do.

[gateway]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/localstack-core/localstack/aws/app.py#L16
[asf]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/docs/localstack-concepts/README.md#aws-server-framework-asf
[providers]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/localstack-core/localstack/services/providers.py#L95
[kinesis]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/localstack-core/localstack/services/kinesis/provider.py#L75
[stores]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/localstack-core/localstack/services/stores.py#L164
[state]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/localstack-core/localstack/state/core.py#L21
[loader]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/localstack-core/localstack/aws/handlers/service_plugin.py#L21
[sns]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/localstack-core/localstack/services/sns/publisher.py#L1181
[lambda]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/localstack-core/localstack/services/lambda_/invocation/docker_runtime_executor.py#L274
[auth]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/localstack-core/localstack/aws/handlers/auth.py#L22
[simulator]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/localstack-core/localstack/services/iam/resources/policy_simulator.py#L32
[ports]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/docs/localstack-concepts/README.md#external-service-ports
[parity]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/docs/testing/parity-testing/README.md
[validation]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/localstack-core/localstack/testing/pytest/validation_tracking.py#L74
[terraform]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/docs/testing/terraform-tests/README.md
