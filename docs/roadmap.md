# Implementation roadmap

The [GitHub roadmap tracker](https://github.com/evanmarshall/gcp-local/issues/1) and linked issues are the source of truth for status. This table records the validation-first sequencing; completed work and scope changes are tracked on GitHub.

Build on LocalStack's existing patterns and prove useful workflows before broadening coverage. The emulator runtime language is selected in the first implementation decision.

## Milestones

- **M0: Validation foundation.** Reusable offline runners, explicit sandbox credentials, reviewed Storage/IAM baselines, and portable agent setup.
- **M1: Authenticated Storage.** A reproducible local Storage workflow with SDK identity, basic IAM, recorded conformance, and a selected Terraform acceptance suite.
- **M2: Cloud Run and Pub/Sub.** Run a container under its assigned identity and process Pub/Sub messages with verified service-to-service authorization.
- **M3: IAM and networking fidelity.** Expand permission probes, networking resources and analysis, advanced policies, and service coverage from recorded scenarios.

The runtime entry gate is #20 + #9 + #24. Runner implementation needs no cloud account; real recordings require the sandbox from #21. The runner foundation, first Storage baseline, and portable agent setup are merged through PRs #26–#28, and local sandbox credentials are verified. The broader authorization/isolation baseline (#24) remains ahead of runtime implementation. Agent setup can now use the explicit credential-profile contract while CI authentication work continues. [Moo setup](moo.md) is available; backend/protocol checks remain in [#25](https://github.com/evanmarshall/gcp-local/issues/25).

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
| [#14](https://github.com/evanmarshall/gcp-local/issues/14) | Verify a Terraform to Pub/Sub to Cloud Run to Storage workflow | M2 | P1 | [#10](https://github.com/evanmarshall/gcp-local/issues/10), [#11](https://github.com/evanmarshall/gcp-local/issues/11), [#12](https://github.com/evanmarshall/gcp-local/issues/12), [#13](https://github.com/evanmarshall/gcp-local/issues/13) |
| [#15](https://github.com/evanmarshall/gcp-local/issues/15) | Add targeted real-GCP permission discovery and drift checks | M3 | P1 | [#7](https://github.com/evanmarshall/gcp-local/issues/7), [#9](https://github.com/evanmarshall/gcp-local/issues/9), [#10](https://github.com/evanmarshall/gcp-local/issues/10) |
| [#16](https://github.com/evanmarshall/gcp-local/issues/16) | Implement the first networking control-plane resources | M3 | P1 | [#5](https://github.com/evanmarshall/gcp-local/issues/5), [#10](https://github.com/evanmarshall/gcp-local/issues/10) |
| [#17](https://github.com/evanmarshall/gcp-local/issues/17) | Add analytical reachability assertions with unknown results | M3 | P1 | [#16](https://github.com/evanmarshall/gcp-local/issues/16), [#9](https://github.com/evanmarshall/gcp-local/issues/9) |
| [#18](https://github.com/evanmarshall/gcp-local/issues/18) | Expand IAM conditions, deny, PAB, org policy, and VPC-SC coverage | M3 | P2 | [#7](https://github.com/evanmarshall/gcp-local/issues/7), [#15](https://github.com/evanmarshall/gcp-local/issues/15), [#16](https://github.com/evanmarshall/gcp-local/issues/16) |
| [#19](https://github.com/evanmarshall/gcp-local/issues/19) | Expand reusable service backends through verified provider slices | M3 | P2 | [#5](https://github.com/evanmarshall/gcp-local/issues/5), [#9](https://github.com/evanmarshall/gcp-local/issues/9), [#14](https://github.com/evanmarshall/gcp-local/issues/14) |

P0 marks the milestone's critical path; P1 is follow-on implementation, and P2 is expansion. Milestones do not imply that every advanced policy or service must ship before the first useful release. The advanced-policy and additional-backend issues are epics to split as workflows are selected.
