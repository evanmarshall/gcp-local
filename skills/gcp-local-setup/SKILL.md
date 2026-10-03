---
name: gcp-local-setup
description: Prepare an existing GCP application repository for gcp-local validation by inspecting SDKs, services, test commands, and explicit local or sandbox profiles. Use for application onboarding, not emulator provider implementation.
---

# GCP application setup

Help an agent prepare this application for isolated validation while preserving its existing tools and instructions. The current gcp-local release has validation tooling and a reviewed Storage baseline, but no emulator runtime or supported local services. Do not claim a working local environment or invent a `gcp-local up` command.

## Inspect the application

Run the bundled helper from its installed location, substituting the application path:

```sh
python3 .agents/skills/gcp-local-setup/scripts/inspect_repo.py --repo .
```

The helper reads selected source/manifests, recognizes common Python/Node/Go/Java/.NET GCP SDK references and Terraform resource types, and reports candidate test commands and endpoint variable references. It does not execute application code, read credential files, or contact GCP. Treat its findings as a starting point; inspect unfamiliar dependencies and omitted files when the report is truncated.

For a saved report, add `--output .gcp-local/compatibility.json`. For readiness diagnostics, add `--doctor`; exit code 2 currently means local emulation is unavailable, not that inspection failed. Missing tools are reported separately for the lane where they are needed.

## Prepare the appropriate profile

Read [the profile guide](references/profiles.md) when configuring execution. Keep existing offline tests usable, keep synthetic local settings separate from live settings, and preserve the application's existing AGENTS.md, test runner, and credentials. The adjacent templates contain no real identity or credential data.

Inspect candidate test commands before executing them; discovery is not a network-isolation guarantee. Use existing authorization for the task. Live work needs an explicitly designated sandbox and allowed resource scope; request only missing inputs. A local feature gap must remain unsupported or unverified rather than falling back to real GCP.

Report the detected services, existing test entry points, missing prerequisites, and which setup steps were actually completed. Distinguish successful inspection/profile preparation from a working emulator or verified SDK/IAM compatibility. Runtime startup and supported-service checks will arrive through gcp-local issue #11; this bundle deliberately contains no startup wrapper.

## Portability and updates

All scripts and references needed for inspection travel with this folder. Agents without automatic skill discovery can read this SKILL.md directly. `.install.json` records the source revision and hashes of copied files. The source installer's default mode previews an update and refuses to overwrite local modifications; reconcile those changes explicitly before installing a new version.
