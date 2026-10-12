# Contributing

Pick an issue from the [backlog](https://github.com/evanmarshall/gcp-local/issues) ([roadmap](https://github.com/evanmarshall/gcp-local/issues/1)), read [the design](docs/design.md), and change only what the acceptance criteria need. The runtime is Go ([runtime guide](docs/runtime.md)); tooling and conformance runners are Python. Agent rules are in [AGENTS.md](AGENTS.md).

## Local setup

Needs Python 3.11 or newer. No GCP credentials, Docker, or LocalStack checkout.

```sh
sh scripts/setup_git.sh
python3 -m venv .venv-tools
.venv-tools/bin/python -m pip install -r requirements-tools.txt -r requirements-sdk.txt
python3 scripts/install_go.py
python3 scripts/install_quality_tools.py
uv run --with 'PyYAML==6.0.3' python scripts/check_repo.py   # or .venv-tools/bin/python scripts/check_repo.py
python3 -m unittest discover -s tests/tooling -v
python3 scripts/check_history.py HEAD
sh scripts/check_go.sh
.venv-tools/bin/python scripts/check_quality.py
.venv-tools/bin/python -m pytest
```

`check_quality.py` fails on any finding not in the `quality/` baselines ([gcp-code-quality](.agents/skills/gcp-code-quality/SKILL.md)). Never add new findings to a baseline. `pytest` runs the offline harness ([validation](docs/validation.md)). Real-cloud inputs go in the ignored `.env` ([sandbox](docs/gcp-sandbox.md)); tests never load it.

## Checks

Run the checks for every area you change; CI runs all of them. "Linux" means CI or a [moo](docs/moo.md) task machine, as root or with network-namespace permission (`scripts/run_isolated.py`). Run Python scripts with `.venv-tools/bin/python`.

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
| `sh scripts/check_cloudkms.sh` | Cloud KMS asymmetric and MAC keys with the Python SDK | Linux | `requirements-services.txt` |
| `sh scripts/check_secretmanager.sh` | Secret Manager with the Python SDK over gRPC | Linux | `requirements-services.txt` |
| `sh scripts/check_cloudtasks.sh` | Cloud Tasks with the Python SDK over gRPC, including dispatch | Linux | `requirements-services.txt` |
| `sh scripts/check_terraform.sh` | Pinned upstream Terraform cases and an RPO negative control; never calls GCP | Linux | Java 17, every backend, `install_terraform.py` |
| `scripts/smoke_isolation.py` | `run_isolated.py` blocks egress and kills hung descendants | Linux, root | None |
| `scripts/smoke_environment.py` | Packaged environment: every service under IAM, reset, teardown | Docker | `requirements-pubsub.txt` |
| `scripts/smoke_workflow.py [--packaged [--cloud-run-docker]]` | Terraform Pub/Sub push to Cloud Run to Storage workflow; `--cloud-run-docker` runs a user-built image and checks per-environment container cleanup | Linux with Java 17, or Docker with `--packaged` | `requirements-pubsub.txt` |
| `scripts/smoke_package.py` | The built wheel's `gcp-local` CLI against an image | Docker | `requirements-pubsub.txt` |
| `scripts/smoke_cloudrun.py` | Cloud Run deploying a real container | Linux with Docker | None |

"Every backend" means `install_emulator_jar.py pubsub`, `install_emulator_jar.py firestore`, `install_spanner.py`, `install_redis.py`, and `install_postgres.py`. There is no single runner; CI splits the Linux checks across jobs.

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
| `plan_sandbox.py`, `plan_pubsub.py`, `record_iam_roles.py` | Live-cloud planning and role recording ([sandbox](docs/gcp-sandbox.md), [Pub/Sub](docs/pubsub.md), [IAM](docs/iam.md)) |
| `prepare_ci_profile.py`, `cleanup_ci.py` | Used only by the cloud-conformance workflow |
| `setup_git.sh` | Configure fast-forward defaults and the pre-push hook |
| `setup_moo.sh`, `install_moo.sh`, `provision_moo.sh`, `smoke_moo.py` | Moo baseline setup and its smoke ([moo guide](docs/moo.md)) |
| `smoke_moo_protocols.py`, `moo_protocol_guest.py`, `smoke_moo_docker.py`, `install_moo_compose.py` | Moo runtime diagnostics ([moo guide](docs/moo.md#smoke-checks)) |

Python dependencies are fully pinned, transitives included:

| File | Installs | Used by |
| --- | --- | --- |
| `requirements-tools.txt` | PyYAML, ruff, vulture | `check_repo.py`, `check_quality.py` |
| `requirements-cloud.txt` | pytest harness and google-auth | Live-cloud workflow; included by `requirements-sdk.txt` |
| `requirements-sdk.txt` | Storage SDK, plus `requirements-cloud.txt` | `pytest`, Storage checks, smoke scripts |
| `requirements-pubsub.txt` | Pub/Sub SDK, plus `requirements-sdk.txt` | Pub/Sub, workflow, packaged, and release smoke |
| `requirements-services.txt` | Firestore, Logging, KMS, Secret Manager, Cloud Tasks, and google-genai SDKs, plus `requirements-pubsub.txt` | `check_pubsub.sh`, `check_firestore.sh`, `check_logging.sh`, `check_vertexai.sh`, `check_cloudkms.sh`, `check_secretmanager.sh`, `check_cloudtasks.sh` |

- `requirements-sdk.txt` is a recording contract file. Changing it invalidates the Storage SDK recording.
- `requirements-pubsub.txt` stays separate because the release workflow and application docs install only the Pub/Sub SDK.

## Agent setup

[AGENTS.md](AGENTS.md) holds the agent rules; contributor skills are in `.agents/skills/`. Codex discovers both ([AGENTS.md](https://learn.chatgpt.com/docs/agent-configuration/agents-md), [skills](https://learn.chatgpt.com/docs/build-skills#where-codex-loads-local-skills)); restart the session to pick up changes. `skills/gcp-local-setup/` is the portable bundle for application repositories, not contributor guidance.

## Commit messages

One or two summary sentences, then short bullets if needed. No conventional-commit prefix. `setup_git.sh` sets `.gitmessage` as the template.

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
- Docs changes need `check_repo.py`. Don't claim coverage you didn't run.
- Permission to work locally doesn't authorize a cloud probe or publishing anything.
