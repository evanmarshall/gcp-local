# LocalStack and gcp-local architecture comparison

The archived LocalStack implementation is a suitable baseline for gcp-local. Its gateway, generated API contracts, service providers, scoped stores, reusable backends, and recorded-cloud tests already cover most of the proposed structure. The recommended changes make those boundaries explicit and adapt them to GCP identity and networking. They do not require a new orchestration framework.

This is a source-level comparison, not a runtime benchmark or a cloud conformance result.

## Reference checkout

- Repository: [localstack/localstack](https://github.com/localstack/localstack).
- Local checkout: optional `../localstack`, a sibling of this repository.
- Revision: `8b9a79f05846835cf4dff63ab7eefdde9df83783`, the archived `main` HEAD; commit dated March 23, 2026, titled “Update the project README (#13901)”.
- Checkout type: shallow Git clone with the complete working tree at that revision. Source files are unmodified.
- Compared design: [design.md](design.md), imported from the reviewed outline; the pre-review version is preserved in [gcp-local.before-localstack-review.md](gcp-local.before-localstack-review.md).

The comparison applies to this public source snapshot. References to Pro features or directories in its documentation do not establish that those implementations are present. Findings below use files that exist in the checkout.

## Direct comparison

| Area | What the archived source does | Original gcp-local outline | Decision applied to the outline |
| --- | --- | --- | --- |
| API entry point | `LocalstackAwsGateway` builds request, exception, response, and finalization chains around a request context. It loads and dispatches service implementations. [Gateway source][gateway] | Generated API servers plus an authorization proxy; the common dispatch and provider boundary was implicit. | Keep a shared gateway and ordinary service providers. Add GCP REST/gRPC transports and principal/project/location context. |
| API generation | ASF generates service API stubs and uses separate parsing/serialization to invoke provider methods. [ASF architecture][asf] | Generate from googleapis protos and discovery documents. | Keep this choice. Translate the source of API definitions, not the AWS wire implementation. |
| Emulator reuse | Providers use Moto fallback or HTTP forwarding. DynamoDB and Kinesis register HTTP fallbacks; Kinesis adds handlers for policy metadata while forwarding backend operations. [Provider registration][providers], [Kinesis provider][kinesis] | Existing emulators behind an authorization proxy. | Keep backend reuse. Providers override only missing or incompatible behavior. This is directly analogous to LocalStack's approach. |
| State ownership | `BaseStore` and `AccountRegionBundle` scope service state. External backend state also exists; the state API can visit both stores and asset directories. [Stores][stores], [state API][state] | One typed control-plane graph, with separate emulator data. | Use service stores plus a shared hierarchy/policy/network index. Each field has one owner; graph metadata is updated at defined provider lifecycle transitions. |
| Service lifecycle | `ServiceLoader` loads services on demand. Kinesis shuts down backend processes before reset/load/stop and starts them lazily through its manager. [Loader][loader], [Kinesis provider][kinesis] | One Compose project without a provider lifecycle contract. | Add lazy startup or preload, readiness, shutdown, reset, and explicit persistence support. Keep one core process plus required backends. |
| Asynchronous behavior | SNS has delivery publishers and a thread-pool dispatcher. Lambda has a dedicated Docker runtime executor. [SNS dispatcher][sns], [Lambda executor][lambda] | Cloud Run containers and Pub/Sub/Tasks tokens were mentioned, but delivery/runtime ownership was not explicit. | Use provider-owned workers and runtime helpers. Internal calls carry the service identity and use shared authorization. No universal workflow engine is needed. |
| Authorization | The default gateway includes a missing-auth-header injector. The IAM provider's basic simulator has limited explicit evaluation logic; it is not a comprehensive cross-service enforcement engine. [Auth handler][auth], [simulator][simulator] | GCP permission parity is a central requirement. | Preserve strict authorization as a deliberate GCP addition. Adopt the extension points without claiming the archive supplies the requested enforcement. This does not assert that every archived service ignores every policy. |
| Database endpoints | The architecture describes external service ports for resources such as PostgreSQL and Redis. [External ports][ports] | All backend ports private, with the proxy described as the only path. | Keep API backends private, but define explicit SQL/Redis endpoints or connectors. Their authorization differs from an HTTP/gRPC API request. |
| Conformance tests | Tests first record AWS responses, then compare local runs with those snapshots. Transformers preserve repeated-reference relationships. Validation metadata records successful cloud runs. [Parity tests][parity], [validation tracking][validation] | Real-cloud probing, record mode, nightly differential runs. | Make committed recordings the normal local CI oracle. Run cloud revalidation separately. Keep permission probing as incremental tooling for targeted scenarios. |
| Terraform | The testing guide describes an allowlisted selection of the Terraform AWS provider's tests. [Terraform testing][terraform] | Broad google-provider compatibility and acceptance configs used to seed probes. | Pin a google-provider version and certify an acceptance-test allowlist. Test refresh and a clean second plan as well as apply/destroy. |
| Release confidence | The test taxonomy prioritizes fast, stable acceptance scenarios involving multiple services, with broader integration coverage separately. [Test types][test-types] | A broad v1 resource list and extensive fidelity machinery. | Preserve the service roadmap, deliver verified workflows incrementally, and avoid making a complete permission-table rebuild a prerequisite for each useful service. |

## GCP adaptations that remain necessary

These changes follow from the stated GCP behavior, rather than a preference to replace LocalStack's architecture.

**Credential handling must be verified per SDK.** Python Pub/Sub switches to anonymous credentials when `PUBSUB_EMULATOR_HOST` is set. Endpoint configuration therefore needs a tested path that preserves credentials for IAM scenarios. The outline now scopes the no-code-change claim to verified SDK/transport/identity combinations. [Python client](https://github.com/googleapis/python-pubsub/blob/main/google/cloud/pubsub_v1/publisher/client.py)

**Providers need state-dependent authorization checks.** Replacing a Storage object requires create and delete permissions, whereas adding a new object does not require delete. Keep a simple common table and let normal provider code contribute checks using resolved state. The illustrative Compute entry now composes applicable checks and is explicitly marked unprobed. No permission-rule language needs to be designed before implementing providers. [Storage permissions](https://docs.cloud.google.com/storage/docs/access-control/iam-permissions)

**Authorization decisions and visible error precedence are different contracts.** Google describes IAM policy types as evaluated simultaneously. The design now treats the IAM list as evaluation concerns and records service-specific error behavior rather than asserting one universal first-denial order. [IAM evaluation](https://docs.cloud.google.com/iam/docs/policy-types#policy_evaluation)

**Network analysis needs a stated boundary.** The comparison found no reason to make packet simulation a prerequisite for the core emulator. Keep analytical reachability as the outline intended, with explicit assertions and unknown/unsupported results. A Terraform plan also needs prior state, resolved references, and defaults before it becomes a usable topology. Google itself distinguishes configuration analysis from actual packet delivery. [Connectivity Tests](https://docs.cloud.google.com/network-intelligence-center/docs/connectivity-tests/concepts/overview#considerations_and_constraints)

The outline also corrects concrete API details: Cloud SQL has its own operation shape; route selection has category precedence; advertised routes are not learned routes; principal access boundaries require IAM v3; and an org deny policy does not govern resources in other organizations. Sources are linked at the relevant changes in the outline.

## Changes made

The updated outline adds the LocalStack baseline table and a Mermaid diagram, provider/state/lifecycle responsibilities, transport-aware backend forwarding, and a local snapshot-testing workflow. It preserves the existing emulator choices, generated API strategy, IAM goal, service roadmap, analytical networking, and optional future packet mode.

The probe algorithm now restores initial state between experiments, observes operation completion, distinguishes check targets from grant locations, and records inconclusive runs. Strict authorization now has one consistent default. Known functional gaps are tracked separately from unsupported authorization decisions, so useful baseline behavior can ship without claiming full parity.

The revision adds no implementation dependencies, language decision, distributed infrastructure, or mandatory general-purpose transaction engine. The working project contained no emulator implementation to modify, so these are design-document changes.

## Suggested first implementation checkpoint

Implement the gateway, provider registry, one scoped store, and one existing backend integration. Run its supported operations through authenticated SDK calls and a small Terraform configuration, then compare with committed real-GCP recordings. Add a second service and a real cross-service action before generalizing provider interfaces further. Extend IAM and networking coverage as concrete scenarios demand it; the overall resource roadmap remains intact.

## Verification performed

Inspected the pinned gateway, auth handler, provider registrations, backend forwarding, Kinesis lifecycle, scoped stores, state hooks, SNS delivery, Lambda executor, IAM simulator, and testing documentation/code. Checked source-link targets against the local checkout, reviewed the outline diff, and verified the original backup and clean reference checkout. No LocalStack runtime, AWS/GCP conformance suite, or cloud resource creation was run. The Mermaid source is included in the outline; its rendered appearance was not previewed.

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
[test-types]: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/docs/testing/test-types/README.md#acceptance-tests
