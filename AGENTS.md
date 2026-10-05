# Agent instructions

gcp-local is a LocalStack-style GCP emulator. The repository contains a Go runtime with shared REST/gRPC dispatch, scoped state/lifecycle, local identity, basic IAM, and an authenticated Storage provider backed by pinned fake-gcs-server. Storage has seven recorded conformance scenarios; other service providers remain pending. Do not describe planned services as working features.

## Start here

- Read [the design](docs/design.md), [the LocalStack comparison](docs/localstack-comparison.md), and the issue being implemented. GitHub issues track scope and dependencies.
- Follow LocalStack's gateway, generated API, provider, scoped-store, and reusable-backend patterns. The reference checkout is optional at `../localstack`, pinned to `8b9a79f05846835cf4dff63ab7eefdde9df83783`; the comparison links to the same source online. Do not edit or vendor that checkout as part of ordinary gcp-local work.
- Implement the smallest useful behavior covered by the issue. A new framework, service rewrite, or runtime dependency needs a concrete benefit over the existing approach. Use Go for the runtime and Python/pytest for conformance; see [the runtime decision](docs/decisions/0001-runtime.md).

## Skills

- Use [gcp-provider](.agents/skills/gcp-provider/SKILL.md) for API providers, emulator integration, state ownership, or service execution.
- Use [gcp-conformance](.agents/skills/gcp-conformance/SKILL.md) for recorded GCP behavior, permission probes, Terraform compatibility, or fidelity claims.
- Use [moo-code](.agents/skills/moo-code/SKILL.md) for development in isolated machines and [moo-golden-image](.agents/skills/moo-golden-image/SKILL.md) for baseline setup. Give concurrent tasks separate worktrees and machines; follow [the moo guide](docs/moo.md).
- Skills live in `.agents/skills/`; keep descriptions specific and instructions concise. Do not copy this entire file into each skill.
- The portable application-onboarding bundle lives in `skills/gcp-local-setup/`. See [its installer guide](docs/agent-setup.md); do not copy emulator-contributor instructions into application repositories.

## Working agreements

- Start from the issue's acceptance criteria and identify the provider, backend, and state owner. Preserve the established architecture unless evidence supports a change.
- Authenticate and authorize before side effects. Unsupported authorization decisions must not silently become allowed requests. Functional support, IAM verification, and analytical reachability are separate claims.
- Keep generated API contracts separate from handwritten provider semantics. Fix the generator or input definition when generated output is wrong. See [the contract guide](api/README.md); `sh scripts/go.sh run ./tools/genapi -check` verifies committed outputs.
- Use [the baseline IAM evaluator](docs/iam.md) for provider permission plans. Resolve every required state feature, retain baseline checks, and complete authorization before mutation. Evaluator unit tests do not establish service conformance.
- Use [local ADC/metadata identity](docs/local-identity.md) for local SDK setup. Runtime identity configuration is explicit and private; never load the real-cloud `.env` or ambient ADC into an emulator.
- Use [scoped state and lifecycle hooks](docs/state-and-backends.md). Run provider/backend mutations inside `Manager.Use` after authorization; publish index changes after backend success and clear state only after backend reset succeeds.
- Use deterministic local tests for normal development. Real-GCP recording requires an authorized sandbox and explicit live-cloud scope; permission to edit a provider does not authorize cloud spending. Never fall back to real GCP when local emulation is missing.
- Preserve observed cloud fixtures; update them through the recorder rather than editing expected results to match the emulator. Keep credentials and sensitive payloads out of fixtures, logs, and Git.
- Run `sh scripts/check_storage.sh` inside moo/Linux when changing Storage. It tests production ADC and the real backend against committed cloud recordings without egress. Its synthetic credential adapter is test-only.
- Run `sh scripts/check_terraform.sh` inside moo/Linux for the pinned acceptance slice after installing `scripts/install_terraform.py`. It includes a negative control and never executes Terraform against GCP.
- Update the relevant coverage notes when adding behavior. Run the checks appropriate to the change and report what was actually verified.

## Commands

```sh
sh scripts/setup_git.sh
uv run --with 'PyYAML==6.0.3' python scripts/check_repo.py
python3 -m unittest discover -s tests/tooling -v
python3 scripts/check_history.py HEAD
python3 scripts/install_go.py
sh scripts/check_go.sh
```

The history check needs an existing commit. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup without `uv`. See [runtime.md](docs/runtime.md) for the server. Format with `sh scripts/go.sh fmt ./...`; run focused tests with `sh scripts/go.sh test -race ./internal/runtime ./cmd/gcp-local`. Use the pinned worktree toolchain instead of an ambient Go installation.

After installing `requirements-sdk.txt` and `requirements-tools.txt` in the tooling environment, run `.venv-tools/bin/python -m pytest`. It defaults to offline harness checks and writes separate harness/conformance results. Read [the validation guide](docs/validation.md) before writing a scenario. `.env` holds sandbox setup inputs described in [the sandbox guide](docs/gcp-sandbox.md); it is not automatically loaded by tests.

For moo setup, run `sh scripts/setup_moo.sh`. Run tooling tests inside the task machine with `moo run task-name -- 'cd /srv/app && python3 -m unittest discover -s tests/tooling -v'`. Keep the repository and history checks above on the host: moo syncs source files, not `.git`.

## Git and GitHub

- Keep commit messages short: one or two summary sentences followed by concise bullet points when there are details to capture. Use concrete changes and relevant validation; avoid long narratives. See `.gitmessage`.
- Fast-forward-only integration: `git merge --ff-only`, followed by a normal push. No merge commits, squash merges, or GitHub Rebase and merge. If branches diverge, update the feature work on the current base and rerun checks; do not force-push `main` or rewrite another contributor's history.
- `scripts/setup_git.sh` configures this clone's merge/pull defaults and pre-push hook. CI checks linear history too. These are safeguards, not server-enforced branch protection; the current private repository plan does not support that protection.
- Keep issue and PR descriptions short and reviewable: behavior, scope, acceptance criteria, dependencies, and validation. Link the issue. Do not claim an issue is complete until its acceptance criteria are met.
- These files do not grant permission to publish, merge, create issues, or contact others beyond the user's current authorization.

## Code Review Rules

Flag authorization bypasses, backend access that skips the provider, shared state that leaks across projects or locations, fabricated conformance evidence, and asynchronous actions running under the wrong principal. Check that a claimed LocalStack-inspired abstraction has an actual source analogue or a documented GCP requirement.
