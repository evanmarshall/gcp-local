---
name: gcp-code-quality
description: "Review gcp-local code for dead code, over-abstraction, and lost simplicity. Use before adding an abstraction, when a change feels heavier than its behavior, or when triaging the quality gate in quality/."
---

# Gcp Code Quality

Use the least machinery that matches the recorded GCP behavior. Every abstraction must earn its place against the tests it serves. Prefer the existing provider, store, and backend patterns ([design](../../../docs/design.md)).

## Run the gate

```sh
python3 scripts/install/go.py
python3 scripts/install/quality_tools.py
.venv-tools/bin/python -m pip install -r requirements/tools.txt
.venv-tools/bin/python scripts/check/quality.py
```

It runs `golangci-lint` (`quality/golangci.yml`), `ruff` (`[tool.ruff]` in `pyproject.toml`), Staticcheck, `deadcode`, and `vulture`. It fails on any finding not in `quality/baseline-*.txt`, on any stale baseline entry, and on any unreachable or unused code not listed with a reason.

## Triage

Never widen a baseline or add an allowlist entry to make a change pass. Pick one:

1. **Delete it.** Unreachable functions, unused definitions, and single-use indirection.
2. **Fix it.** Refactor until the linter passes without a suppression.
3. **Record why it's deliberate.** A disable in `quality/golangci.yml`, a `per-file-ignores` entry in `pyproject.toml`, or a line in `quality/dead-code-go.txt` must name the contract that requires the code. Disable a linter only when it can't find a defect here.

Use `--update-baseline` only after a change removes findings. Baselines only shrink.

## Questions before adding structure

- **Who calls it?** One caller: inline it. Sharing a signature isn't sharing an abstraction.
- **Does another provider already have it?** Providers are written one at a time, so every helper looks single-caller. Before declaring a provider helper, search `internal/providers/` recursively for an identical one; reuse or move it to `internal/providers/provider` once two providers need it. `scripts/check/repo.py` fails on an identical function, method (receiver name ignored), or type in two providers; `DISTINCT_PER_PROVIDER` lists the deliberate exceptions with reasons.
- **What does it hide?** It must hide a decision. A wrapper that forwards everything is deleted.
- **Is it speculative?** Code for a second backend or transport waits until that backend exists. Shapes come from recordings.
- **Who owns the state?** One owner per field. A copy, cache, or mirror needs a reason it can't be derived.
- **Is it a new mechanism?** No workflow engine, generic dispatcher, or universal model unless the issue asks.
- **Could it be a function?** Prefer a function to a one-method type, a concrete type to a one-implementation interface, a parameter to a one-field options struct.
- **Is the error path real?** Keep error handling that tests exercise. Don't add errors, modes, flags, or fallbacks no test reaches.

## Delete or keep

Delete unreachable code, code whose only caller is a test that asserts nothing about production, shims with no consumer, and comments that describe behavior the code no longer has. Keep a documented lifecycle hook or a directly tested validator, listed in `quality/dead-code-go.txt` with its contract. Never delete a conformance fixture or divergence note to make a check pass.
