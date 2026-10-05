# Development with moo

Moo supplies a Linux machine per development task on an Apple Silicon Mac. Use it with a separate Git worktree for parallel work. Normal repository checks and Linux CI remain usable without moo; this is developer tooling, not the emulator runtime.

## Setup

```sh
sh scripts/setup_moo.sh
```

The setup reuses an installed moo after `moo doctor` passes. If missing, the installer downloads the upstream **v0.3.1** installer, verifies its SHA-256, and selects that release. Homebrew must already be installed; upstream installs its required VM/filesystem dependencies. An unhealthy existing installation fails instead of being silently replaced.

The provisioned `base` machine belongs to this repository. Fresh provisioning installs Python 3.11, Git, a C compiler for race tests, iproute2 for isolated runners, the pinned Go toolchain, and the dependencies in `requirements-tools.txt` and `requirements-sdk.txt`; no GCP authentication or services are configured. Existing machine snapshots retain their prior tools until reprovisioned or updated; see [the runtime guide](runtime.md) for the task-machine upgrade. The dated Debian image tag narrows rebuild drift, but registry tags and Debian package repositories are not immutable. The reviewed upstream source is [6ca60ffd23564c740c39d8f21d612bbe6cb63ae9](https://github.com/heyito/moo/tree/6ca60ffd23564c740c39d8f21d612bbe6cb63ae9). An existing host binary can differ from that release; record what was tested rather than claiming its version was checked by `moo doctor`.

The provisioner runs the tooling tests before saving. Rerunning setup reprovisions the named machine; save any valuable changes first. To change the base image or recipe, provision a new name, for example `sh scripts/setup_moo.sh base-v2`. Existing machines keep their original base image.

## Work on an issue

From the host repository:

```sh
git worktree add ../gcp-local-issue-20 -b issue/20
cd ../gcp-local-issue-20
moo new issue-20 from base
moo run issue-20 -- 'cd /srv/app && python3 -m unittest discover -s tests/tooling -v'
```

Edit files on the host. Moo syncs that worktree into `/srv/app`; tracked and nonignored untracked files are included. Ignored files, including credentials and environment files, are not copied. Do not seed credentials into the shared baseline: anything written inside a machine can be captured in a snapshot.

Run these checks on the host, where the real Git metadata lives:

```sh
uv run --with 'PyYAML==6.0.3' python scripts/check_repo.py
python3 scripts/check_history.py HEAD
```

After committing, save the task machine with `moo save issue-20`. Save valuable state before switching commits. `moo new` can restore a saved state and discard unsaved runtime changes. Use the [fast-forward integration workflow](../CONTRIBUTING.md#fast-forward-only-integration) to advance main, then save the machine at the integrated commit. Never run two tasks in one checkout or one machine.

When finished, remove only the task's machine and worktree:

```sh
moo drop issue-20 --force
git worktree remove ../gcp-local-issue-20
```

Snapshots survive `moo drop`; add `--snapshots` only when intentionally discarding that task's saved states.

## Network and verification

Port 8080 is available for a development smoke server, not a shipped emulator endpoint. Services must bind `0.0.0.0` to use host forwarding. `moo ls` shows each machine's host port. Private VM loopback does not imply blocked internet access or isolation of remote GCP projects.

The initial integration smoke check forks two temporary worktrees/machines, exercises independent files and the same guest HTTP port, checks ignored credential-file exclusion, and restores a saved state. It removes only its temporary machines and worktrees:

```sh
python3 scripts/smoke_moo.py
```

Run it after committing the setup, because temporary worktrees start at `HEAD`. The shared baseline must exist first. Follow-up [runtime probes](moo-runtime.md) now verify official Pub/Sub gRPC separately inside the isolated guest and through host forwarding, plus native arm64 Docker/Compose execution with no container network. The measured guest lacks the NAT support needed by Docker's default bridge, so the packaged multi-network workflow and Cloud Run network acceptance still belong on host Docker or ordinary Linux CI. The TCP half-close limitation documented upstream did not reproduce in the measured probe; it is not assumed away for other binaries or workloads.

## Agent skills

The repository includes adapted [moo-code](../.agents/skills/moo-code/SKILL.md) and [moo-golden-image](../.agents/skills/moo-golden-image/SKILL.md) skills. They use the upstream workflow with repository-specific setup, credential handling, host Git checks, and fast-forward-only integration. Upstream attribution is retained in [third_party/moo/LICENSE](../third_party/moo/LICENSE).
