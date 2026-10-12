# Contributing

Start with an issue in the [backlog](https://github.com/evanmarshall/gcp-local/issues) and the [design](docs/design.md). Keep changes focused on the stated acceptance criteria. The runtime uses Go; Python supplies repository tooling and conformance runners. [The runtime guide](docs/runtime.md) covers the pinned toolchain, build, tests, and process lifecycle.

## Local setup

After cloning, install the Git conventions for this clone:

```sh
sh scripts/setup_git.sh
```

Install the tooling environment with Python 3.11 or newer, then run the host checks:

```sh
python3 -m venv .venv-tools
.venv-tools/bin/python -m pip install -r requirements-tools.txt -r requirements-sdk.txt
python3 scripts/install_go.py
python3 scripts/install_quality_tools.py
uv run --with 'PyYAML==6.0.3' python scripts/check_repo.py
python3 -m unittest discover -s tests/tooling -v
python3 scripts/check_history.py HEAD
sh scripts/check_go.sh
.venv-tools/bin/python scripts/check_quality.py
.venv-tools/bin/python -m pytest
```

`.venv-tools/bin/python scripts/check_repo.py` works without `uv`. None of these need GCP credentials, Docker, or the sibling LocalStack checkout. [The quality gate](.agents/skills/gcp-code-quality/SKILL.md) blocks findings beyond the baselines in `quality/`. `pytest` runs the offline validation harness; [the validation guide](docs/validation.md) covers local and live modes. Real-cloud inputs go in the ignored `.env` ([sandbox guide](docs/gcp-sandbox.md)); tests never load it.

## Checks

Run the checks for every area you change; CI runs all of them. "Linux" means CI or a [moo](docs/moo.md) task machine. Those checks use `scripts/run_isolated.py` network namespaces, so run them as root or with namespace permission. Run Python scripts with `.venv-tools/bin/python`.

| Command | Verifies | Runs on | Install first |
| --- | --- | --- | --- |
| `scripts/check_repo.py` | Links, YAML, skill metadata, `docs/coverage.md`, and that this page names every script and requirements file | Any | `requirements-tools.txt` |
| `scripts/check_history.py HEAD` | Linear history | Host (needs `.git`) | None |
| `sh scripts/check_go.sh` | gofmt, `genapi -check`, vet, race tests, built-binary smoke (`smoke_runtime.py`) | Any | `install_go.py` |
| `scripts/check_quality.py` | Go/Python lint and dead code against `quality/` | Any | `install_go.py`, `install_quality_tools.py`, `requirements-tools.txt` |
| `python -m pytest` | Offline validation harness | Any | `requirements-sdk.txt`, `requirements-tools.txt` |
| `sh scripts/check_storage.sh` | Storage with production ADC, the SDK smoke, and its recordings | Linux | Java 17, every backend, `requirements-sdk.txt` |
| `sh scripts/check_rest_recordings.sh` | Other REST providers against their recordings | Linux | Same as Storage |
| `sh scripts/check_pubsub_backend.sh` | Official Pub/Sub emulator protocol and lifecycle | Linux | Java 17, `install_emulator_jar.py pubsub` |
| `sh scripts/check_pubsub.sh` | Pub/Sub provider IAM, streams, and the Python SDK | Linux | Java 17, `install_emulator_jar.py pubsub`, `requirements-services.txt` |
| `sh scripts/check_firestore.sh` | Firestore provider and the Python SDK | Linux | Java 17, `install_emulator_jar.py firestore`, `requirements-services.txt` |
| `sh scripts/check_spanner.sh` | Spanner provider against the official emulator | Linux | `install_spanner.py` |
| `sh scripts/check_redis.sh` | Memorystore against real Redis | Linux | `install_redis.py` |
| `sh scripts/check_cloudsql.sh` | Cloud SQL against real PostgreSQL | Linux; non-root, or root with `.tools/postgres` traversable by `nobody` | `install_postgres.py` |
| `sh scripts/check_vertexai.sh` | Vertex AI with the google-genai SDK | Linux | `requirements-services.txt` |
| `sh scripts/check_logging.sh` | Cloud Logging with the Python SDK | Linux | `requirements-services.txt` |
| `sh scripts/check_terraform.sh` | Pinned upstream Terraform cases and an RPO negative control; never calls GCP | Linux | Java 17, every backend, `install_terraform.py` |
| `scripts/smoke_isolation.py` | `run_isolated.py` blocks egress and kills hung descendants | Linux, root | None |
| `scripts/smoke_environment.py` | Packaged environment: every service under IAM, reset, teardown | Docker | `requirements-pubsub.txt` |
| `scripts/smoke_workflow.py [--packaged]` | Terraform Pub/Sub push to Cloud Run to Storage workflow | Linux with Java 17, or Docker with `--packaged` | `requirements-pubsub.txt` |
| `scripts/smoke_package.py` | The built wheel's `gcp-local` CLI against an image | Docker | `requirements-pubsub.txt` |
| `scripts/smoke_cloudrun.py` | Cloud Run deploying a real container | Linux with Docker | None |

"Every backend" means `install_emulator_jar.py pubsub`, `install_emulator_jar.py firestore`, `install_spanner.py`, `install_redis.py`, and `install_postgres.py`. There is no single runner: the Linux checks need root and installed backends, and CI splits them across jobs.

## Scripts and requirements

Every other file in `scripts/` supports a check or a documented workflow:

| Script | Purpose |
| --- | --- |
| `go.sh` | Run the pinned Go toolchain in `.tools/go` |
| `install_go.py`, `install_quality_tools.py`, `install_terraform.py` | Install pinned tools under `.tools/` |
| `install_emulator_jar.py`, `install_spanner.py`, `install_redis.py`, `install_postgres.py` | Install pinned backends under `.tools/`; the Dockerfile runs them too |
| `backend_env.sh` | Sourced by check scripts to export the `.tools/` backend paths |
| `run_isolated.py` | Run a server and a test command in a loopback-only Linux namespace |
| `run_pubsub.py`, `run_terraform.py`, `probe_pubsub_backend.py`, `smoke_storage.py`, `smoke_runtime.py` | Called by the check scripts |
| `smoke_support.py` | Helpers shared by the smoke scripts |
| `generate_terraform_tests.py` | Regenerate, or `--check`, the adapted Terraform cases ([audit](tests/terraform/audit.md)) |
| `local_env.py`, `create_local_identity.py` | Run a packaged environment; write a local ADC bundle ([quickstart](docs/quickstart.md), [local identity](docs/local-identity.md)) |
| `install_agent_skill.py` | Install the portable skill into an application ([agent setup](docs/agent-setup.md)) |
| `plan_sandbox.py`, `plan_pubsub.py`, `record_iam_roles.py` | Live-cloud planning and role recording ([sandbox](docs/gcp-sandbox.md), [Pub/Sub validation](docs/pubsub-validation.md), [IAM](docs/iam.md)) |
| `prepare_ci_profile.py`, `cleanup_ci.py` | Used only by the cloud-conformance workflow |
| `setup_git.sh` | Configure fast-forward defaults and the pre-push hook |
| `setup_moo.sh`, `install_moo.sh`, `provision_moo.sh`, `smoke_moo.py` | Moo baseline setup and its smoke ([moo guide](docs/moo.md)) |
| `smoke_moo_protocols.py`, `moo_protocol_guest.py`, `smoke_moo_docker.py`, `install_moo_compose.py` | Moo runtime diagnostics ([moo runtime](docs/moo-runtime.md)) |

Python dependencies are fully pinned, transitives included:

| File | Installs | Used by |
| --- | --- | --- |
| `requirements-tools.txt` | PyYAML, ruff, vulture | `check_repo.py`, `check_quality.py` |
| `requirements-cloud.txt` | pytest harness and google-auth | Live-cloud workflow; included by `requirements-sdk.txt` |
| `requirements-sdk.txt` | Storage SDK, plus `requirements-cloud.txt` | `pytest`, Storage checks, smoke scripts |
| `requirements-pubsub.txt` | Pub/Sub SDK, plus `requirements-sdk.txt` | Pub/Sub, workflow, packaged, and release smoke |
| `requirements-services.txt` | Firestore, Logging, and google-genai SDKs, plus `requirements-pubsub.txt` | `check_pubsub.sh`, `check_firestore.sh`, `check_logging.sh`, `check_vertexai.sh` |

- `requirements-sdk.txt` is a recording contract file. Changing it invalidates the Storage SDK recording.
- `requirements-pubsub.txt` stays separate because the release workflow and application docs install only the Pub/Sub SDK.

## Agent setup

[AGENTS.md](AGENTS.md) holds project rules for agents and lists the contributor skills in `.agents/skills/`. Codex discovers both ([AGENTS.md guidance](https://learn.chatgpt.com/docs/agent-configuration/agents-md), [skill discovery](https://learn.chatgpt.com/docs/build-skills#where-codex-loads-local-skills)); restart the session if new guidance is not picked up. `skills/gcp-local-setup/` is the portable bundle for application repositories, not contributor guidance.

## Isolated development with moo

On Apple Silicon Macs, run `sh scripts/setup_moo.sh` to check/install moo and provision this repository's baseline. The [moo guide](docs/moo.md) covers separate worktrees/machines, guest tests, host Git checks, snapshots, and cleanup. Adapted `moo-code` and `moo-golden-image` skills are included in `.agents/skills/`. Ordinary local checks and Linux CI do not require moo.

## Commit messages

Use one or two short summary sentences and a few concise bullets as needed. For example:

```text
Add authenticated Storage requests.

- Resolve the caller through local ADC.
- Verify allowed and denied uploads with recorded fixtures.
```

The setup script configures `.gitmessage` as the local commit template. There is no mandatory conventional-commit prefix.

## Fast-forward-only integration

Prepare work in a feature branch or PR. Once the work is ready and integration is authorized, use the command line to advance `main` without changing the feature commits:

```sh
git fetch origin
git switch main
git merge --ff-only origin/main
git merge --ff-only feature/my-change
# Run the applicable checks at this exact revision.
git push origin main
```

If `main` advanced, the merge or push must stop. Update your feature branch against the new base and repeat validation. Never force-push `main`. Do not use the GitHub merge buttons: [GitHub rebase merging creates new commit SHAs](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/about-merge-methods-on-github#rebasing-and-merging-your-commits).

Local `pull.ff=only` and `merge.ff=only`, the pre-push hook, and the CI history check implement the workflow safeguards. The hook rejects merge commits on branch pushes, deletion of `main`, and non-fast-forward updates to `main`. It reports a missing remote commit as a fetch-and-retry condition. Hooks can be bypassed and CI currently cannot block a direct push.

GitHub currently rejects branch protection and rulesets for this private repository with a plan-upgrade requirement. Merge-commit, squash, and automatic merging are disabled in repository settings; GitHub's rebase option remains enabled because a merge method must remain available, but project policy does not use it. If server protections become available, require linear history and passing `repository-checks`, disallow force pushes/deletion, and retain an integration path compatible with ordinary fast-forward pushes. Repository visibility is not changed for this purpose.

## Verification and publication

Run focused tests for behavior changes, including denial and state-isolation cases when relevant. Real-cloud recording is separate from normal tests and must use the explicitly authorized sandbox. Documentation changes need link/structure checks; do not manufacture runtime coverage.

Issue and PR templates capture scope, dependencies, and validation. Permission to work locally does not itself authorize a cloud probe or an external publication.
