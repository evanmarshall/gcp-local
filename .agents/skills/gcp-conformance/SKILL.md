---
name: gcp-conformance
description: "Create or review gcp-local conformance fixtures, permission probes, Terraform compatibility tests, and fidelity claims against real GCP behavior."
---

# Gcp Conformance

Start from a concrete scenario and existing recordings. Read [validation](../../../docs/contributing/validation.md); for probes, [permission discovery](../../../docs/contributing/permission-discovery.md); for Terraform, [Terraform compatibility](../../../docs/contributing/terraform-compatibility.md).

## Scenarios

- A recording proves only the requests it sends. Build each scenario from what real clients send: run the client against the conformance server with `-trace` (method, path, query parameter names, status) and record those exact request shapes, including the options they set by default (for example the Go Storage SDK's `projection=full`).
- Define request features, initial state, named callers and service agents, grants, completion criteria, cleanup, and expected outcomes. Record SDK, provider, and backend versions.
- Normalize variable IDs but keep repeated references. Assert etags and lifecycle transitions in the test rather than normalizing them away.
- Redact credentials and sensitive payloads before anything is written.
- Change expectations only through an authorized real-cloud run. Never edit a recording to match the emulator.
- Mark unrecorded scenarios unverified. Never fabricate provenance.

## Live runs

Recording is encouraged: a scenario that only runs locally is unverified. The owner pre-authorizes live runs that meet all of these:

- Only the sandbox project and its peer project from the [sandbox guide](../../../docs/contributing/gcp-sandbox.md), through the recorder with `--gcp-profile` and a live scope file. Never production, never ambient credentials, never organization-level changes.
- Estimated total cost under $10, including anything that keeps billing after the run (KMS key versions until destruction, instances that outlive cleanup). Estimate before running; report the estimate and what was created.
- Temporary grants and enabled APIs are listed, and grants are removed after the run. Cleanup covers every resource, including after a failed run; verify nothing is left.

Above $10, or anything else live, ask first. Record, review the candidate for sensitive data, promote, revalidate, then compare locally.

## Permission probes

- Separate check targets from grant locations, and caller permissions from service-agent permissions.
- Restore the initial state for each trial, wait for propagation, and run the real operation. Poll async work to completion.
- `ALREADY_EXISTS`, unrelated validation errors, and propagation timeouts are inconclusive. One minimal set is not proof of every path.

## Terraform and networking

Pin the provider and the selected acceptance tests. Cover create, read, update, delete, refresh, polling, and an unchanged second plan. Keep unknown and unsupported reachability results; a local socket connection is not a GCP reachability oracle.

## Report

State the source evidence, the local checks run, whether any live run happened, and the exact scope of the claim.
