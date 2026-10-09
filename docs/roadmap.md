# Implementation roadmap

The [GitHub roadmap tracker](https://github.com/evanmarshall/gcp-local/issues/1) and linked issues are the source of truth for status. This table records the validation-first sequencing; completed work and scope changes are tracked on GitHub.

Build on LocalStack's existing patterns and prove useful workflows before broadening coverage. The Go runtime follows the [recorded implementation decision](decisions/0001-runtime.md).

## Milestones

- **M0: Validation foundation.** Reusable offline runners, explicit sandbox credentials, reviewed Storage/IAM baselines, and portable agent setup.
- **M1: Authenticated Storage.** A reproducible local Storage workflow with SDK identity, basic IAM, recorded conformance, and a selected Terraform acceptance suite.
- **M2: Cloud Run and Pub/Sub.** Run a container under its assigned identity and process Pub/Sub messages with verified service-to-service authorization.
- **M3: IAM and networking fidelity.** Expand permission probes, networking resources and analysis, advanced policies, and service coverage from recorded scenarios.

The runtime entry gate (#20 + #9 + #24) is complete. The Go core, generated REST/gRPC dispatch, scoped state, local ADC/STS/metadata, basic IAM, and actual Storage backend are implemented. Seven cloud-recorded Storage scenarios pass without egress, and three adapted upstream Terraform cases cover create/read/update/refresh/import/destroy and unchanged plans. The [packaged workflow](quickstart.md) adds private startup, reset, teardown, and application-agent readiness checks. M1 remains limited to the documented Storage and credential slices; it is not broad GCP or Terraform certification.

Pub/Sub (#12) completes its initial [local provider](pubsub.md), including authenticated SDK calls, resource IAM, streaming/cancellation, authenticated loopback HTTP push, and official-emulator lifecycle tests. Cloud-recorded pull and authorization behavior now has a committed, revalidated baseline ([#29](https://github.com/evanmarshall/gcp-local/issues/29), 2026-10-09 UTC) that the local provider matches in comparison mode; the recording stays bounded by the [recorder plan](pubsub-validation.md), and the [packaged workflow](quickstart.md) now starts Storage and Pub/Sub together. Cloud Run (#13) and the combined workflow (#14) follow. Broader live-cloud execution needs its own authorized scope; current authorization remains bounded Storage testing. [Moo setup](moo.md) and [runtime capability probes](moo-runtime.md) are available; the measured guest's Docker bridge limitation keeps container/network acceptance on host Docker or Linux CI.

## Issues and dependencies

| Issue | Outcome | Milestone | Priority | Depends on |
| --- | --- | --- | --- | --- |
| [#20](https://github.com/evanmarshall/gcp-local/issues/20) | Reusable validation runners and reporting | M0 | P0 | #2 |
| [#21](https://github.com/evanmarshall/gcp-local/issues/21) | Dedicated GCP sandbox and explicit ADC/WIF credentials | M0 | P0 | #2 |
| [#22](https://github.com/evanmarshall/gcp-local/issues/22) | Portable agent setup skill for application repos | M0 | P0 | #20, #21 configuration contract |
| [#23](https://github.com/evanmarshall/gcp-local/issues/23) | Version-pinned upstream SDK compatibility suites | M1 | P1 | #20, #8, #9 |
| [#24](https://github.com/evanmarshall/gcp-local/issues/24) | Recorded authorization and isolation baselines | M0 | P0 | #21, #9 |
| [#2](https://github.com/evanmarshall/gcp-local/issues/2) | Bootstrap repository guidance, skills, and fast-forward workflow | M1 | P0 | None |
| [#3](https://github.com/evanmarshall/gcp-local/issues/3) | Select the emulator runtime and establish a runnable skeleton | M1 | P0 | #2, #20, #9, #24 |
| [#4](https://github.com/evanmarshall/gcp-local/issues/4) | Add generated API contracts, request context, and provider dispatch | M1 | P0 | [#3](https://github.com/evanmarshall/gcp-local/issues/3) |
| [#5](https://github.com/evanmarshall/gcp-local/issues/5) | Implement scoped provider state and backend lifecycle hooks | M1 | P0 | [#4](https://github.com/evanmarshall/gcp-local/issues/4) |
| [#6](https://github.com/evanmarshall/gcp-local/issues/6) | Implement local ADC, STS, and workload metadata identity | M1 | P0 | [#4](https://github.com/evanmarshall/gcp-local/issues/4) |
| [#7](https://github.com/evanmarshall/gcp-local/issues/7) | Implement basic IAM inheritance, permission checks, and decision traces | M1 | P0 | [#5](https://github.com/evanmarshall/gcp-local/issues/5), [#6](https://github.com/evanmarshall/gcp-local/issues/6), #24 |
| [#8](https://github.com/evanmarshall/gcp-local/issues/8) | Integrate Cloud Storage backend with verified IAM behavior | M1 | P0 | [#5](https://github.com/evanmarshall/gcp-local/issues/5), [#7](https://github.com/evanmarshall/gcp-local/issues/7) |
| [#9](https://github.com/evanmarshall/gcp-local/issues/9) | Record Storage behavior and integrate the scenario adapter | M0 | P0 | #20, #21 |
| [#10](https://github.com/evanmarshall/gcp-local/issues/10) | Certify the first google-provider Terraform acceptance workflow | M1 | P1 | [#8](https://github.com/evanmarshall/gcp-local/issues/8), [#9](https://github.com/evanmarshall/gcp-local/issues/9), #20 |
| [#11](https://github.com/evanmarshall/gcp-local/issues/11) | Package a reproducible gcp-local up workflow | M1 | P1 | [#5](https://github.com/evanmarshall/gcp-local/issues/5), [#8](https://github.com/evanmarshall/gcp-local/issues/8), #22 |
| [#12](https://github.com/evanmarshall/gcp-local/issues/12) | Integrate Pub/Sub with authenticated publishing and delivery | M2 | P0 | [#5](https://github.com/evanmarshall/gcp-local/issues/5), [#7](https://github.com/evanmarshall/gcp-local/issues/7), [#9](https://github.com/evanmarshall/gcp-local/issues/9) |
| [#13](https://github.com/evanmarshall/gcp-local/issues/13) | Run Cloud Run services locally under their configured identity | M2 | P0 | [#5](https://github.com/evanmarshall/gcp-local/issues/5), [#6](https://github.com/evanmarshall/gcp-local/issues/6), [#7](https://github.com/evanmarshall/gcp-local/issues/7), [#9](https://github.com/evanmarshall/gcp-local/issues/9) |
| [#29](https://github.com/evanmarshall/gcp-local/issues/29) | Record bounded Pub/Sub pull and IAM conformance | M2 | P0 | #9, #12, #21; separate cloud approval |
| [#14](https://github.com/evanmarshall/gcp-local/issues/14) | Verify a Terraform to Pub/Sub to Cloud Run to Storage workflow | M2 | P1 | [#10](https://github.com/evanmarshall/gcp-local/issues/10), [#11](https://github.com/evanmarshall/gcp-local/issues/11), [#12](https://github.com/evanmarshall/gcp-local/issues/12), [#13](https://github.com/evanmarshall/gcp-local/issues/13) |
| [#15](https://github.com/evanmarshall/gcp-local/issues/15) | Add targeted real-GCP permission discovery and drift checks | M3 | P1 | [#7](https://github.com/evanmarshall/gcp-local/issues/7), [#9](https://github.com/evanmarshall/gcp-local/issues/9), [#10](https://github.com/evanmarshall/gcp-local/issues/10) |
| [#16](https://github.com/evanmarshall/gcp-local/issues/16) | Implement the first networking control-plane resources | M3 | P1 | [#5](https://github.com/evanmarshall/gcp-local/issues/5), [#10](https://github.com/evanmarshall/gcp-local/issues/10) |
| [#17](https://github.com/evanmarshall/gcp-local/issues/17) | Add analytical reachability assertions with unknown results | M3 | P1 | [#16](https://github.com/evanmarshall/gcp-local/issues/16), [#9](https://github.com/evanmarshall/gcp-local/issues/9) |
| [#18](https://github.com/evanmarshall/gcp-local/issues/18) | Expand IAM conditions, deny, PAB, org policy, and VPC-SC coverage | M3 | P2 | [#7](https://github.com/evanmarshall/gcp-local/issues/7), [#15](https://github.com/evanmarshall/gcp-local/issues/15), [#16](https://github.com/evanmarshall/gcp-local/issues/16) |
| [#19](https://github.com/evanmarshall/gcp-local/issues/19) | Expand reusable service backends through verified provider slices | M3 | P2 | [#5](https://github.com/evanmarshall/gcp-local/issues/5), [#9](https://github.com/evanmarshall/gcp-local/issues/9), [#14](https://github.com/evanmarshall/gcp-local/issues/14) |

P0 marks the milestone's critical path; P1 is follow-on implementation, and P2 is expansion. Milestones do not imply that every advanced policy or service must ship before the first useful release. The advanced-policy and additional-backend issues are epics to split as workflows are selected.
