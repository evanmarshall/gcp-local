# Development with moo

Moo gives each development task its own Linux VM on an Apple Silicon Mac. Pair each machine with its own Git worktree. Moo is optional: host checks and Linux CI don't need it.

## Setup

```sh
sh scripts/setup_moo.sh
```

- Reuses an installed moo if `moo doctor` passes. Otherwise downloads the upstream v0.3.1 installer, checks its SHA-256, and installs it. Needs Homebrew. An unhealthy install fails instead of being replaced.
- Provisions a `base` machine with Python 3.11, Git, a C compiler, iproute2, the pinned Go toolchain, and `requirements-tools.txt` plus `requirements-sdk.txt`. No GCP credentials or services. The provisioner runs the tooling tests before saving.
- Rerunning setup reprovisions the named machine. To change the recipe, provision a new name (`sh scripts/setup_moo.sh base-v2`); existing machines keep their image.
- Reviewed upstream source: [6ca60ff](https://github.com/heyito/moo/tree/6ca60ffd23564c740c39d8f21d612bbe6cb63ae9). `moo doctor` doesn't attest the installed binary's version.

## Work on an issue

```sh
git worktree add ../gcp-local-issue-20 -b issue/20
cd ../gcp-local-issue-20
moo new issue-20 from base
moo run issue-20 -- 'cd /srv/app && python3 -m unittest discover -s tests/tooling -v'
moo run issue-20 -- 'cd /srv/app && python3 scripts/install_go.py && sh scripts/check_go.sh'
```

- Edit on the host. Moo syncs tracked and nonignored untracked files to `/srv/app`; a later sync can overwrite guest edits. Ignored files, including `.env` and credentials, are not copied.
- Moo doesn't copy `.git`. Run `scripts/check_repo.py` and `scripts/check_history.py HEAD` on the host.
- Machines made before a dependency changed don't pick it up; install it in the guest. Older baselines may need `apt-get install -y build-essential` for race tests.
- Save with `moo save issue-20` after committing and before switching commits. `moo new` can discard unsaved runtime state.
- Never run two tasks in one checkout or one machine. Never put credentials in the baseline; snapshots capture everything in the machine.

Clean up only this task:

```sh
moo drop issue-20 --force
git worktree remove ../gcp-local-issue-20
```

Snapshots survive `moo drop` unless you add `--snapshots`.

## Network

- Guest port 8080 forwards to a host port (`moo ls`). Bind `0.0.0.0` inside the guest, for example `bin/gcp-local -listen 0.0.0.0:8080`.
- VM loopback is not isolation from the internet or from GCP.
- Docker's default bridge doesn't work in the guest: the kernel lacks NAT/MASQUERADE support. Run Cloud Run container tests, the packaged multi-network environment, and `smoke_cloudrun.py` on host Docker or Linux CI. Don't weaken provider network or identity checks to work around this.

Measured on 2026-10-05 (Linux 6.12.91 aarch64, Debian 12, 2 CPUs, 2 GiB):

| Capability | Result |
| --- | --- |
| Official Pub/Sub emulator inside the guest, loopback only | Passed: publish, pull, ack, streaming, half-close, cancellation, restart state loss |
| Same, through host forwarding to guest port 8080 | Passed |
| Docker with `network_mode: none` | Passed: core and Storage under Compose |
| Docker default bridge | Failed |

## Smoke checks

```sh
python3 scripts/smoke_moo.py
```

Forks two temporary worktrees and machines, checks file independence, port forwarding, ignored-credential exclusion, and snapshot restore, then removes only what it created. Run it after committing; the `base` machine must exist.

Pub/Sub protocol probe (needs Java 17 and the Pub/Sub JAR in the guest; guest port 8080 free). Writes `.artifacts/moo-runtime/protocol.json`:

```sh
python3 scripts/smoke_moo_protocols.py --machine TASK_NAME
```

Network-none Docker diagnostic, in a disposable task machine only:

```sh
moo run TASK_NAME -- 'apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io'
moo run TASK_NAME -- 'cd /srv/app && python3 scripts/install_moo_compose.py'
# Start a private dockerd with its own socket, data root, and
# --bridge=none --iptables=false --ip6tables=false; pull the Dockerfile's base image.
moo run TASK_NAME -- 'cd /srv/app && python3 scripts/smoke_moo_docker.py --docker-host unix:///run/gcp-local-docker.sock'
```

Stop that daemon before saving the machine. This diagnostic doesn't establish bridge-network isolation.

## Skills

[moo-code](../.agents/skills/moo-code/SKILL.md) and [moo-golden-image](../.agents/skills/moo-golden-image/SKILL.md) are adapted from upstream moo ([MIT notice](../third_party/moo/LICENSE)).
