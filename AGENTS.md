# Agent instructions

gcp-local is a LocalStack-style GCP emulator: one Go process with REST/gRPC dispatch, local identity, an IAM engine, scoped state, and service providers in front of reusable backends. [Service coverage](docs/coverage.md) is the only source of truth for what works. Don't describe planned services as working.

## Start here

- Read [the design](docs/design.md) and the issue. Issues track scope; [#1](https://github.com/evanmarshall/gcp-local/issues/1) is the roadmap.
- Implement the smallest behavior the issue's acceptance criteria need. A new framework, rewrite, or runtime dependency needs a concrete benefit. Runtime is Go, conformance is Python/pytest ([decision](docs/decisions/0001-runtime.md)).
- Follow LocalStack's patterns. A new abstraction needs a source analogue in [the LocalStack mapping](docs/localstack-comparison.md) or a documented GCP requirement. The optional `../localstack` checkout is pinned to `8b9a79f05846835cf4dff63ab7eefdde9df83783`; don't edit or vendor it.

## Skills

Skills live in `.agents/skills/`.

- [gcp-provider](.agents/skills/gcp-provider/SKILL.md): providers, backends, state ownership, background work.
- [gcp-conformance](.agents/skills/gcp-conformance/SKILL.md): recordings, permission probes, Terraform cases, fidelity claims.
- [gcp-code-quality](.agents/skills/gcp-code-quality/SKILL.md): the lint and dead-code gate, and new abstractions.
- [gcp-docs](.agents/skills/gcp-docs/SKILL.md): any change to what a service supports or how it's verified.
- [concise-and-blunt](.agents/skills/concise-and-blunt/SKILL.md): all documentation.
- [moo-code](.agents/skills/moo-code/SKILL.md) and [moo-golden-image](.agents/skills/moo-golden-image/SKILL.md): isolated machines ([moo guide](docs/moo.md)). One worktree and one machine per concurrent task.

`skills/gcp-local-setup/` is the portable bundle for application repositories ([agent setup](docs/agent-setup.md)). Don't copy contributor rules into it.

## Rules

- **Authorize before side effects.** Authenticate and authorize before any mutation or backend call. An unsupported authorization decision fails closed; it never becomes an allow. Unsupported calls return `UNIMPLEMENTED`, except Storage gaps, which return 400 because the Go client retries 5xx without limit ([Storage](docs/storage.md#gaps)). Workers act as their service agent, never as an implicit administrator.
- **Use the IAM engine** ([IAM](docs/iam.md)). Resolve every required state feature, keep baseline checks, and complete `Authorize` before mutation.
- **Use scoped state** ([state and backends](docs/state-and-backends.md)). Mutate inside `Manager.Use` after authorization. Publish index changes after backend success. Clear state only after backend reset succeeds. Never let state leak across projects or locations.
- **Generated contracts are outputs.** Fix wrong output in `tools/genapi` or its inputs, never by hand ([api/README.md](api/README.md)). `sh scripts/go.sh run ./tools/genapi -check` must pass.
- **Local identity only** ([local identity](docs/local-identity.md)). Runtime identity config is explicit and private. Never load the real-cloud `.env`, gcloud config, or ambient ADC into the emulator.
- **No anonymous shortcuts.** Never use an emulator's anonymous SDK mode (`PUBSUB_EMULATOR_HOST`, `FIRESTORE_EMULATOR_HOST`) or the test-only credential adapter to claim IAM support.
- **Recordings are evidence.** Update them only through the recorder; never edit expected values to match the emulator. Changing a scenario's `contract_files` invalidates its recording. Keep credentials and sensitive payloads out of fixtures, logs, and Git.
- **Cloud spending needs explicit scope.** Normal development is local and deterministic. Live recording needs an authorized sandbox and cost scope; permission to edit code isn't that. Never fall back to real GCP when local emulation is missing.
- **Separate claims.** Functional support, IAM verification, and analytical reachability are different claims. Report only the checks you ran.
- **Quality gate.** Never add new Go or Python findings to the `quality/` baselines; fix or delete the code.
- **Update [coverage](docs/coverage.md)** when behavior changes.

## Commands

[CONTRIBUTING.md](CONTRIBUTING.md#checks) lists every check, where it runs, and what to install. Host checks:

```sh
sh scripts/setup_git.sh
uv run --with 'PyYAML==6.0.3' python scripts/check_repo.py
python3 -m unittest discover -s tests/tooling -v
python3 scripts/check_history.py HEAD
python3 scripts/install_go.py
sh scripts/check_go.sh
python3 scripts/install_quality_tools.py
.venv-tools/bin/python scripts/check_quality.py
.venv-tools/bin/python -m pytest
```

Format with `sh scripts/go.sh fmt ./...`; focused tests with `sh scripts/go.sh test -race ./internal/runtime ./cmd/gcp-local`. Use the pinned toolchain, not an ambient Go. Run Linux checks in moo or CI; run `check_repo.py` and `check_history.py` on the host because moo doesn't sync `.git`. Read [validation](docs/validation.md) before writing a scenario.

## Git and GitHub

- Commit messages: one or two summary sentences, then concise bullets ([.gitmessage](.gitmessage)).
- Fast-forward only: `git merge --ff-only`, then a normal push. No merge commits, squash merges, GitHub "Rebase and merge", or force-pushing `main`. If branches diverge, update the feature branch and rerun checks.
- Issues and PRs: behavior, scope, acceptance criteria, dependencies, validation, and a link to the issue. Don't call an issue done until its criteria are met.
- These files don't authorize publishing, merging, creating issues, or contacting anyone beyond what the user authorized.

## Code review

Flag authorization bypasses, backend access that skips the provider, state leaking across projects or locations, fabricated conformance evidence, and asynchronous work running under the wrong principal. Check that each claimed LocalStack-inspired abstraction has a source analogue or a documented GCP requirement.
