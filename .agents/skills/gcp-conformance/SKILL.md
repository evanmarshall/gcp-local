---
name: gcp-conformance
description: "Create or review gcp-local conformance fixtures, permission probes, Terraform compatibility tests, and fidelity claims against real GCP behavior."
---

# Gcp Conformance

Read the relevant behavior in [the design](../../../docs/design.md) and the testing rows of [the LocalStack comparison](../../../docs/localstack-comparison.md). Start from a concrete scenario and existing fixtures; ordinary development runs against local emulation and committed recordings.

Describe request features, initial state, named callers and service agents, required grants, completion criteria, cleanup, and expected outcomes. Record SDK/provider/backend versions and the last successful real-cloud verification date. Distinguish implemented behavior, verified IAM coverage, unknown results, and known divergences.

For live recording, first establish that the current task authorizes cloud execution and supplies a dedicated sandbox and cost scope. If those are absent, implement the local test/recorder and report the missing verification; do not contact production or silently use ambient credentials. Ensure teardown and failed-run cleanup cover every resource created.

For permission probes, separate check targets from grant attachment points and caller permissions from service-agent permissions. Restore equivalent initial state for each trial, wait for grant/revocation propagation, and check the actual operation. Poll asynchronous work to completion. `ALREADY_EXISTS`, unrelated validation errors, and propagation timeouts do not establish permission requirements. Record inconclusive trials separately; one successful permission set is not proof of all possible paths.

Normalize variable IDs while preserving repeated references and relationships. Test etag conflicts and lifecycle transitions explicitly rather than masking their entire behavior. Redact credentials and sensitive payloads before writing recordings. Regenerate cloud expectations from authorized real-cloud runs; never edit them to bless emulator output.

For Terraform, pin the provider version and selected acceptance tests. Exercise create/read/update/delete, refresh, polling, and an unchanged second plan. For network analysis, preserve unknown/unsupported values and do not treat a successful local socket connection as a GCP reachability oracle.

Report the source evidence, local checks, whether any live-cloud execution occurred, and the exact scope of the fidelity claim. Mark new unrecorded scenarios as unverified rather than fabricating provenance.
