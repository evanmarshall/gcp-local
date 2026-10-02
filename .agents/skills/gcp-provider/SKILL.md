---
name: gcp-provider
description: "Implement or fix gcp-local API providers, emulator integrations, service state, and background execution using the established LocalStack architecture."
---

# Gcp Provider

Read the assigned issue and relevant sections of [the design](../../../docs/design.md). Use [the LocalStack comparison](../../../docs/localstack-comparison.md) to locate an existing implementation pattern before introducing a new abstraction.

Identify the generated API contract, provider method, state owner, and backend behavior involved. Implement only the missing semantics; preserve backend behavior that already matches the required scenario. Generated files are outputs, so changes to their structure belong in generation inputs or tooling.

Carry method, principal, resource/project/location, and trace context through the request. Compose baseline and request/state-dependent permission checks before any side effect or forwarding. Unsupported authorization must produce an explicit coverage failure rather than a successful backend request. A publicly accessible resource still requires the service's anonymous-access semantics to be evaluated.

Use service-scoped state and a shared hierarchy/network index. Declare the authoritative owner of each new field, when index changes become visible, and how failed backend mutations are cleaned up or reconciled. Cover cross-project and relevant regional/global isolation.

Provider workers own operation completion, delivery, and workload lifecycle. Calls between services use the acting service-agent identity and shared authorization; they must not acquire an implicit administrator identity. Preserve cancellation, streaming, and error details for the transports claimed as supported.

Reuse the provider registry and backend start/readiness/stop/reset hooks. If persistence cannot restore the backend, expose that limitation. Keep analytical network results separate from observed container connectivity.

Verify a complete supported behavior and a meaningful failure case, then update coverage notes. Use the conformance skill when recording or asserting real-GCP parity. Report the issue criteria met, checks run, and any remaining unsupported behavior. Do not create a general workflow engine or rewrite a backend merely to make providers look uniform.
