---
name: gcp-provider
description: "Implement or fix gcp-local API providers, emulator integrations, service state, and background execution using the established LocalStack architecture."
---

# Gcp Provider

Read the issue and [the design](../../../docs/design.md). Find the existing pattern in [the LocalStack mapping](../../../docs/localstack-comparison.md) before adding an abstraction.

## Steps

1. Identify the generated contract, provider method, state owner, and backend behavior. Implement only the missing semantics; keep backend behavior that already matches the recording. Fix generated output in `tools/genapi` or its inputs.
2. Register a permission plan. Compose baseline and request/state feature checks and call `Authorize` before any side effect or backend call. Unsupported authorization fails closed. Public resources still go through the service's anonymous-access rules ([IAM](../../../docs/iam.md)).
3. Store state in a project/location-scoped store. Declare each new field's single owner, when index changes publish (after backend success), and how a failed backend mutation is compensated ([state and backends](../../../docs/state-and-backends.md)). Test cross-project and location isolation. Look up, change, and render records inside one store callback; validate a whole patch before applying any of it. Add the resource to the provider's `concurrency_test.go` (`providertest.Hammer`, run under `-race`).
4. Put background work (operations, delivery, workloads) in provider workers. Cross-service calls act as the service agent through the IAM engine, never as an implicit administrator. Preserve cancellation, streaming, and error details.
5. Use the backend manager's start, readiness, reset, and shutdown hooks (`Manager.Use`). Persistence is unsupported. Keep analytical network results separate from observed container connectivity.

## Client compatibility

- Check every query parameter and body field of a served method against Google's API reference. Each is supported, accepted with its documented default, or refused with an explicit gap. Note in the code which reference a choice follows.
- A refusal must not make a client hang. Clients retry 5xx; where the main client libraries retry a service's 5xx without a limit (Storage), refuse with a non-retryable 4xx.
- Never drop or rewrite a GCP response field (`mediaLink`, `selfLink`, operation names) on the assumption that clients derive it. Rewrite it to the emulator's public origin, and test with a client that reads it.
- Claim a client works only with a row in the [client compatibility table](../../../docs/coverage.md#client-compatibility) backed by a test that drives that client.
- When a change makes a [client matrix](../../../docs/clients.md) pair pass, `sh scripts/check_clients.sh` fails until you set it to `pass` in `tests/clients/matrix.json` and move its line in `docs/coverage.md`.

## Finish

Test one complete supported behavior and one meaningful denial or failure. Every refusal the change adds (an `UNIMPLEMENTED` coverage gap or a fail-closed fallback) gets a test that asserts the refusal and its message, so removing it fails a test even when another check returns the same code. Update [coverage](../../../docs/coverage.md) with the gcp-docs skill. Use gcp-conformance for any real-GCP parity claim. Report the criteria met, checks run, and what remains unsupported. Don't build a workflow engine or rewrite a backend for uniformity.
