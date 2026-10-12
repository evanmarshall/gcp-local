---
name: gcp-conformance
description: "Create or review gcp-local conformance fixtures, permission probes, Terraform compatibility tests, and fidelity claims against real GCP behavior."
---

# Gcp Conformance

Start from a concrete scenario and existing recordings. Read [validation](../../../docs/validation.md); for probes, [permission discovery](../../../docs/permission-discovery.md); for Terraform, [Terraform compatibility](../../../docs/terraform-compatibility.md).

## Scenarios

- Define request features, initial state, named callers and service agents, grants, completion criteria, cleanup, and expected outcomes. Record SDK, provider, and backend versions.
- Normalize variable IDs but keep repeated references. Assert etags and lifecycle transitions in the test rather than normalizing them away.
- Redact credentials and sensitive payloads before anything is written.
- Change expectations only through an authorized real-cloud run. Never edit a recording to match the emulator.
- Mark unrecorded scenarios unverified. Never fabricate provenance.

## Live runs

Confirm the task authorizes cloud execution and names a sandbox and cost scope. If not, build the local test or recorder and report the missing verification. Never touch production or ambient credentials. Cleanup must cover every resource, including after a failed run.

## Permission probes

- Separate check targets from grant locations, and caller permissions from service-agent permissions.
- Restore the initial state for each trial, wait for propagation, and run the real operation. Poll async work to completion.
- `ALREADY_EXISTS`, unrelated validation errors, and propagation timeouts are inconclusive. One minimal set is not proof of every path.

## Terraform and networking

Pin the provider and the selected acceptance tests. Cover create, read, update, delete, refresh, polling, and an unchanged second plan. Keep unknown and unsupported reachability results; a local socket connection is not a GCP reachability oracle.

## Report

State the source evidence, the local checks run, whether any live run happened, and the exact scope of the claim.
