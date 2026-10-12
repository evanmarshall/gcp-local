---
name: moo-golden-image
description: "Install or repair the moo host prerequisites and provision a credential-free baseline for gcp-local development."
---

# Provision the gcp-local baseline

Install the host tool only when setup is part of the task. Reuse a healthy installation; a failed `moo doctor` needs diagnosis, not replacement of active machines. Details are in [the moo guide](../../../docs/contributing/moo.md).

```sh
sh scripts/moo/setup.sh
```

This checks or installs the reviewed moo release, creates the `base` machine, runs the provisioner and tooling tests, and saves the baseline.

- **Keep the baseline complete.** When a check gains a dependency (a package, installer, toolchain, or requirements file), add it to `scripts/moo/provision.sh` and `moo.toml`'s lockfiles in the same change. Never install it only in your own clone. Every Linux check must run in a fresh fork of the baseline without setup.
- When requirements change, edit `moo.toml`, `scripts/moo/provision.sh`, and the lockfiles. Existing machines don't pick up a new recipe: provision a new name (`sh scripts/moo/setup.sh base-v4`), validate a fresh fork of it, point the fork commands in `docs/contributing/moo.md` and the moo-code skill at it, then drop the old baseline with its snapshots.
- Never put real credentials or application data in a baseline. Keep setup idempotent and Git edits on the host.
- After the setup commit, `moo save base` (or the new name).
- The guest's Docker default bridge doesn't work; a healthy VM doesn't establish container or network claims.

Report the checks actually run and remaining gaps.

Adapted from [moo v0.3.1](https://github.com/heyito/moo/blob/6ca60ffd23564c740c39d8f21d612bbe6cb63ae9/skills/moo-golden-image/SKILL.md); [MIT notice](../../../third_party/moo/LICENSE).
