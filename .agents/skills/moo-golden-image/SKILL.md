---
name: moo-golden-image
description: "Install or repair the moo host prerequisites and provision a credential-free baseline for gcp-local development."
---

# Provision the gcp-local baseline

Use [the moo guide](../../../docs/moo.md) and the repository scripts. Install the host tool only when setup is part of the user's task. An existing healthy installation is reused; a failed doctor check needs diagnosis, not automatic replacement of active machines.

```sh
sh scripts/setup_moo.sh
```

This checks or installs the reviewed moo release, creates the repository-scoped `base` machine, runs the guest provisioner, checks the tooling, and saves the baseline. The image contains Git, Python, and pinned contributor dependencies. It does not choose the emulator language or configure live GCP.

Change `moo.toml`, `scripts/provision_moo.sh`, and the relevant dependency lockfiles when requirements change. Recipe identity includes the provisioner and tooling requirements. Existing named machines do not automatically acquire a new recipe: provision a distinct replacement such as `sh scripts/setup_moo.sh base-v2`, validate it, then tell contributors which baseline to fork. Do not destroy an existing baseline to force an update.

Never seed real credentials or application data into a shared baseline. Keep setup idempotent, run guest commands through moo, and keep source/Git edits on the host. After the setup commit, run `moo save base` (or the replacement name) to associate the tested state with that revision.

Report the actual checks and remaining gaps. Docker backend and gRPC conformance require the later runtime; a healthy VM does not establish those claims.

Adapted from [moo v0.3.1](https://github.com/heyito/moo/blob/6ca60ffd23564c740c39d8f21d612bbe6cb63ae9/skills/moo-golden-image/SKILL.md); [MIT notice](../../../third_party/moo/LICENSE).
