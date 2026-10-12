# Contributing

Pick an issue from the [backlog](https://github.com/evanmarshall/gcp-local/issues) ([roadmap](https://github.com/evanmarshall/gcp-local/issues/1)), read [the design](../docs/design.md), and change only what the acceptance criteria need. The runtime is Go ([runtime guide](../docs/architecture/runtime.md)); tooling and conformance runners are Python. Agent rules are in [AGENTS.md](../AGENTS.md).

## Local setup

Needs Python 3.11 or newer. No GCP credentials, Docker, or LocalStack checkout.

```sh
sh scripts/dev/setup_git.sh
python3 -m venv .venv-tools
.venv-tools/bin/python -m pip install -r requirements/tools.txt -r requirements/sdk.txt
python3 scripts/install/go.py
python3 scripts/install/quality_tools.py
uv run --with 'PyYAML==6.0.3' python scripts/check/repo.py   # or .venv-tools/bin/python scripts/check/repo.py
python3 -m unittest discover -s tests/tooling -v
python3 scripts/check/history.py HEAD
sh scripts/check/go.sh
.venv-tools/bin/python scripts/check/quality.py
.venv-tools/bin/python -m pytest
```

`check/quality.py` fails on any finding not in the `quality/` baselines ([gcp-code-quality](../.agents/skills/gcp-code-quality/SKILL.md)). Never add new findings to a baseline. `pytest` runs the offline harness ([validation](../docs/contributing/validation.md)). Real-cloud inputs go in the ignored `.env` ([sandbox](../docs/contributing/gcp-sandbox.md)); tests never load it.

## Checks

Run the checks for every area you change; CI runs all of them. "Linux" means CI or a [moo](../docs/contributing/moo.md) task machine, as root or with network-namespace permission (`scripts/run/isolated.py`). Run Python scripts with `.venv-tools/bin/python`.

| Command | Verifies | Runs on | Install first |
| --- | --- | --- | --- |
| `scripts/check/repo.py` | Links, YAML, skill metadata, `docs/coverage.md`, this page naming every script and requirements file, the [layout rules](../.agents/skills/repo-layout/SKILL.md) (`check/layout.py`), and that every recording has a scenario the `recordings` CI job replays | Any | `requirements/tools.txt` |
| `scripts/check/history.py HEAD` | Linear history | Host (needs `.git`) | None |
| `sh scripts/check/go.sh` | gofmt, `genapi -check`, vet, race tests, built-binary smoke (`smoke/runtime.py`) | Any | `install/go.py` |
| `scripts/check/quality.py` | Go/Python lint and dead code against `quality/` | Any | `install/go.py`, `install/quality_tools.py`, `requirements/tools.txt` |
| `python -m pytest` | Offline validation harness | Any | `requirements/sdk.txt`, `requirements/tools.txt` |
| `sh scripts/check/recordings.sh` | Replays every GCP recording against the local providers; fails unless each one is verified (`run/recordings.py`). CI job `recordings` | Linux | Java 17, every backend, `requirements/services.txt` |
| `sh scripts/check/storage.sh` | Storage with production ADC and the SDK smoke | Linux | `requirements/sdk.txt` |
| `sh scripts/check/backend/pubsub_backend.sh` | Official Pub/Sub emulator protocol and lifecycle | Linux | Java 17, `install/emulator_jar.py pubsub` |
| `sh scripts/check/backend/pubsub.sh` | Pub/Sub provider IAM, streams, and the Python SDK | Linux | Java 17, `install/emulator_jar.py pubsub`, `requirements/services.txt` |
| `sh scripts/check/backend/firestore.sh` | Firestore provider and the Python SDK | Linux | Java 17, `install/emulator_jar.py firestore`, `requirements/services.txt` |
| `sh scripts/check/backend/spanner.sh` | Spanner provider against the official emulator | Linux | `install/spanner.py` |
| `sh scripts/check/backend/redis.sh` | Memorystore against real Redis | Linux | `install/redis.py` |
| `sh scripts/check/backend/cloudsql.sh` | Cloud SQL against real PostgreSQL and MySQL | Linux; non-root, or root with `.tools/postgres` and `.tools/mysql` traversable by `nobody`; `libaio.so.1` and `libnuma.so.1` | `install/postgres.py`, `install/mysql.py` |
| `sh scripts/check/sdk/vertexai.sh` | Vertex AI with the google-genai SDK | Linux | `requirements/services.txt` |
| `sh scripts/check/sdk/logging.sh` | Cloud Logging with the Python SDK | Linux | `requirements/services.txt` |
| `sh scripts/check/sdk/cloudkms.sh` | Cloud KMS with the Python SDK over REST and gRPC, and the Go client over gRPC | Linux | `requirements/services.txt` |
| `sh scripts/check/sdk/secretmanager.sh` | Secret Manager with the Python SDK over gRPC | Linux | `requirements/services.txt` |
| `sh scripts/check/sdk/cloudtasks.sh` | Cloud Tasks with the Python SDK over gRPC, including dispatch | Linux | `requirements/services.txt` |
| `sh scripts/check/clients.sh` | Go, Node.js, Python, gcloud, and rclone clients per service against `tests/clients/matrix.json` ([client matrix](../docs/contributing/clients.md)) | Linux | Java 17, `install/emulator_jar.py pubsub` and `firestore`, `install/spanner.py`, `install/client_tools.py`, `npm ci` in `tests/clients/node`, `tests/clients/python/requirements.txt` |
| `sh scripts/check/sdk/cloudrun.sh` | Cloud Run with the Python SDK over gRPC | Linux | `requirements/services.txt` |
| `sh scripts/check/terraform.sh` | Pinned Terraform cases (3 extracted from upstream, the rest written locally) and an RPO negative control; never calls GCP | Linux | Java 17, every backend, `install/terraform.py` |
| `scripts/smoke/isolation.py` | `run/isolated.py` blocks egress and kills hung descendants | Linux, root | None |
| `scripts/smoke/environment.py` | Packaged environment: every service under IAM, reset, teardown | Docker | `requirements/pubsub.txt` |
| `scripts/smoke/workflow.py [--packaged [--cloud-run-docker]]` | Terraform Pub/Sub push to Cloud Run to Storage workflow; `--cloud-run-docker` runs a user-built image and checks per-environment container cleanup | Linux with Java 17, or Docker with `--packaged` | `requirements/pubsub.txt` |
| `scripts/smoke/package.py` | The built wheel's `gcp-local` CLI against an image | Docker | `requirements/pubsub.txt` |
| `scripts/smoke/cloudrun.py` | Cloud Run deploying a real container | Linux with Docker | None |

"Every backend" means `install/emulator_jar.py pubsub`, `install/emulator_jar.py firestore`, `install/spanner.py`, `install/redis.py`, `install/postgres.py`, and `install/mysql.py`. There is no single runner; CI splits the Linux checks across jobs.

## Scripts and requirements

Every other file in `scripts/` supports a check or a documented workflow:

| Script | Purpose |
| --- | --- |
| `go.sh` | Run the pinned Go toolchain in `.tools/go` |
| `install/go.py`, `install/quality_tools.py`, `install/terraform.py`, `install/client_tools.py` | Install pinned tools under `.tools/` (`install/client_tools.py`: Node.js, gcloud, rclone for the client matrix) |
| `install/emulator_jar.py`, `install/spanner.py`, `install/redis.py`, `install/postgres.py`, `install/mysql.py` | Install pinned backends under `.tools/`; the Dockerfile runs them too |
| `run/backend_env.sh` | Sourced by check scripts to export the `.tools/` backend paths |
| `run/isolated.py` | Run a server and a test command in a loopback-only Linux namespace |
| `run/pubsub.py`, `run/terraform.py`, `run/clients.py`, `run/probe_pubsub_backend.py`, `smoke/storage.py`, `smoke/runtime.py` | Called by the check scripts |
| `run/client_matrix.py` | Loads `tests/clients/matrix.json` and resolves client versions for `run/clients.py` and `check/repo.py` |
| `smoke/support.py` | Helpers shared by the smoke scripts |
| `dev/generate_terraform_tests.py` | Regenerate, or `--check`, the adapted Terraform cases ([audit](../tests/terraform/audit.md)) |
| `dev/local_env.py`, `dev/create_local_identity.py` | Run a packaged environment; write a local ADC bundle ([quickstart](../docs/guides/quickstart.md), [local identity](../docs/guides/local-identity.md)) |
| `dev/install_agent_skill.py` | Install the portable skill into an application ([agent setup](../docs/guides/agent-setup.md)) |
| `live/plan_sandbox.py`, `live/plan_pubsub.py`, `live/record_iam_roles.py` | Live-cloud planning and role recording ([sandbox](../docs/contributing/gcp-sandbox.md), [Pub/Sub](../docs/services/messaging/pubsub.md), [IAM](../docs/architecture/iam.md)) |
| `live/prepare_ci_profile.py`, `live/cleanup_ci.py` | Used only by the cloud-conformance and drift workflows |
| `live/drift.py` | Runs the weekly drift plan ([drift job](../docs/contributing/gcp-sandbox.md#weekly-drift-job)) |
| `live/batch.py` | Records or revalidates many scenarios in parallel ([validation](../docs/contributing/validation.md)) |
| `dev/setup_git.sh` | Configure fast-forward defaults and the pre-push hook |
| `moo/setup.sh`, `moo/install.sh`, `moo/provision.sh`, `moo/smoke.py` | Moo baseline setup and its smoke ([moo guide](../docs/contributing/moo.md)) |
| `moo/smoke_protocols.py`, `moo/protocol_guest.py`, `moo/smoke_docker.py`, `moo/install_compose.py` | Moo runtime diagnostics ([moo guide](../docs/contributing/moo.md#smoke-checks)) |

Python dependencies are fully pinned, transitives included:

| File | Installs | Used by |
| --- | --- | --- |
| `requirements/tools.txt` | PyYAML, ruff, vulture | `check/repo.py`, `check/quality.py` |
| `requirements/cloud.txt` | pytest harness and google-auth | Live-cloud workflow; included by `requirements/sdk.txt` |
| `requirements/sdk.txt` | Storage SDK, plus `requirements/cloud.txt` | `pytest`, Storage checks, smoke scripts |
| `requirements/pubsub.txt` | Pub/Sub SDK, plus `requirements/sdk.txt` | Pub/Sub, workflow, packaged, and release smoke |
| `tests/clients/python/requirements.txt` | Every Python client in the client matrix, plus `requirements/services.txt` | `check/clients.sh` |
| `requirements/services.txt` | Firestore, Logging, KMS, Secret Manager, Cloud Tasks, Cloud Run, and google-genai SDKs, plus `requirements/pubsub.txt` | `check/backend/pubsub.sh`, `check/backend/firestore.sh`, `check/sdk/logging.sh`, `check/sdk/vertexai.sh`, `check/sdk/cloudkms.sh`, `check/sdk/secretmanager.sh`, `check/sdk/cloudtasks.sh`, `check/sdk/cloudrun.sh` |

- `requirements/sdk.txt` is a recording contract file. Changing it invalidates the Storage SDK recording.
- `requirements/pubsub.txt` stays separate because the release workflow and application docs install only the Pub/Sub SDK.

## Agent setup

[AGENTS.md](../AGENTS.md) holds the agent rules; contributor skills are in `.agents/skills/`. Codex discovers both ([AGENTS.md](https://learn.chatgpt.com/docs/agent-configuration/agents-md), [skills](https://learn.chatgpt.com/docs/build-skills#where-codex-loads-local-skills)); restart the session to pick up changes. `skills/gcp-local-setup/` is the portable bundle for application repositories, not contributor guidance.

## Commit messages

One or two summary sentences, then short bullets if needed. No conventional-commit prefix. `scripts/dev/setup_git.sh` sets `.github/gitmessage` as the template.

```text
Add authenticated Storage requests.

- Resolve the caller through local ADC.
- Verify allowed and denied uploads with recorded fixtures.
```

## Fast-forward-only integration

Advance `main` only by fast-forward, and only when integration is authorized:

```sh
git fetch origin
git switch main
git merge --ff-only origin/main
git merge --ff-only feature/my-change
# Run the applicable checks at this exact revision.
git push origin main
```

- If `main` moved, the merge or push stops. Update the feature branch on the new base and rerun checks.
- Never force-push `main`. Don't use GitHub's merge buttons; [rebase merging rewrites SHAs](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/about-merge-methods-on-github#rebasing-and-merging-your-commits).
- Safeguards: `pull.ff=only` and `merge.ff=only`, a pre-push hook (rejects merge commits, deleting `main`, and non-fast-forward `main` updates), and the CI history check. GitHub branch protection isn't available on this private repository's plan, so all of these can be bypassed.

## Verification

- Test behavior changes, including denial and cross-project isolation where relevant.
- Real-cloud recording needs an explicitly authorized sandbox and is separate from normal tests.
- Docs changes need `check/repo.py`. Don't claim coverage you didn't run.
- Permission to work locally doesn't authorize a cloud probe or publishing anything.
