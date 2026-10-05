# Contributing

Start with an issue in the [backlog](https://github.com/evanmarshall/gcp-local/issues) and the [design](docs/design.md). Keep changes focused on the stated acceptance criteria. The runtime uses Go; Python supplies repository tooling and conformance runners. [The runtime guide](docs/runtime.md) covers the pinned toolchain, build, tests, and process lifecycle.

## Local setup

After cloning, install the Git conventions for this clone:

```sh
sh scripts/setup_git.sh
```

Run repository checks with Python 3.11 or newer:

```sh
python3 -m venv .venv-tools
.venv-tools/bin/python -m pip install -r requirements-tools.txt
.venv-tools/bin/python scripts/check_repo.py
python3 -m unittest discover -s tests/tooling -v
python3 scripts/check_history.py HEAD
```

Alternatively, `uv run --with 'PyYAML==6.0.3' python scripts/check_repo.py` supplies the YAML parser without installing it globally. Normal checks do not require GCP credentials, Docker, or the sibling LocalStack checkout.

Install `requirements-sdk.txt` in the same environment (it includes the runner and keyless-auth dependencies) and run `.venv-tools/bin/python -m pytest` for the offline validation harness and existing tooling tests. [The validation guide](docs/validation.md) documents explicit local/live modes and reports. Prepare real-cloud project inputs in the ignored `.env` using [the sandbox guide](docs/gcp-sandbox.md); ordinary tests do not load it or discover ambient credentials.

## Agent setup

Root `AGENTS.md` supplies project instructions. Repository-local skills are in `.agents/skills/gcp-provider` and `.agents/skills/gcp-conformance`, each with `SKILL.md` and optional UI metadata. Open Codex in this repository; restart the session if newly added guidance is not discovered. These locations follow the official [AGENTS.md guidance](https://learn.chatgpt.com/docs/agent-configuration/agents-md) and [skill discovery documentation](https://learn.chatgpt.com/docs/build-skills#where-codex-loads-local-skills).

Keep task-specific workflow details in the skills and shared project rules in `AGENTS.md`. Add narrower instructions only when a directory acquires different requirements.

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
