---
name: gcp-code-quality
description: "Review gcp-local code for dead code, over-abstraction, and lost simplicity. Use before adding an abstraction, when a change feels heavier than its behavior, or when triaging the quality gate in quality/."
---

# Gcp Code Quality

This repository is judged on how little machinery it takes to match real GCP behavior. Every abstraction has to earn its place against the conformance tests it serves. Read [the design](../../../docs/design.md) and the issue before adding structure, and prefer the established provider, store, and backend patterns over a new mechanism.

## Run the gate

```sh
python3 scripts/install_go.py
python3 scripts/install_quality_tools.py
.venv-tools/bin/python -m pip install -r requirements-tools.txt
.venv-tools/bin/python scripts/check_quality.py
```

The gate runs `golangci-lint` (every defect-oriented Go linter, configured in `.golangci.yml`), `ruff` (every Python rule family except the formatter-owned ones, configured in `ruff.toml`), Staticcheck, and dead-code analysis (`deadcode` for Go, `vulture` for Python). It fails on any finding that is not baselined, on any baseline entry that has gone stale, and on any unreachable function or unused definition that is not written down with a reason.

## Triage rules

Never widen a baseline or add an allowlist entry to make a change pass. The three acceptable outcomes are:

1. **Delete it.** Unreachable functions, unused definitions, and single-use indirection are deleted, not parked.
2. **Fix it.** Refactor until the linter is satisfied without a suppression.
3. **Record why it is deliberate.** A disable in `.golangci.yml`, a `per-file-ignores` entry in `ruff.toml`, or a line in `quality/dead-code-go.txt` must name the contract that requires the code. A linter is disabled only when it cannot find a defect here, never because it is inconvenient.

`--update-baseline` rewrites `quality/baseline-*.txt` for findings that are still outstanding. Use it only after a change removes findings, and keep the baseline shrinking. The baselines are a debt list, not a permission slip; `git log -p quality/` should show them trending down.

## Rejecting over-abstraction

Ask these before merging new structure:

- **Who calls it?** A helper, option struct, interface, or generic wrapper with one caller is inlined. Two callers that only share a signature are not a shared abstraction.
- **What does it hide?** An abstraction must hide a decision, not rename a line. A wrapper that forwards every argument and returns every error unchanged is deleted.
- **Is it speculative?** Code written for a second provider, transport, or storage backend does not exist until that backend is implemented against it. Contract shapes come from the recorded scenario, not from what might be needed later.
- **Who owns the state?** Every field has one authoritative owner. A second copy of state, a cache, or a mirror index needs the reason it cannot be derived.
- **Is it the pattern or a new one?** New mechanisms (a workflow engine, a generic dispatcher, a universal model) are refused unless the issue asks for them. Extend the existing registry, provider, and backend hooks.
- **Could it be a function?** Prefer a function over a type with one method, a concrete type over an interface with one implementation, and a plain parameter over an options struct with one field.
- **Is the error path real?** Keep the error handling that the conformance or failure tests exercise. Never add an error, mode, flag, or fallback that no test reaches.

The linters enforce the measurable half of this: `unparam` and `revive`'s unused-parameter rule find parameters nobody passes, `ireturn` and `interfacebloat` bound interface surface, `dupl` finds copy-paste that should be one function, `cyclop`, `gocognit`, `nestif`, `funlen`, and `maintidx` cap how much a function hides, `PLR0913` and pylint's argument limits cap signatures, `C901` caps Python complexity, `ARG` finds Python arguments nobody reads, and `deadcode`/`vulture` find code nobody reaches at all. Treat a new finding as a question about the design, not as an obstacle.

## Fixing versus deleting

Delete code when it is unreachable, when a test is its only caller and the test asserts nothing about the production path, when a compatibility shim has no consumer, or when a comment describes behavior the code no longer performs. Do not delete a documented lifecycle hook or a validator that a unit test exercises directly: list it in `quality/dead-code-go.txt` with the contract that requires it. Never delete a conformance fixture or a divergence note to make a check green.
