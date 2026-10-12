---
name: repo-layout
description: Use when adding, moving, or naming files or directories in gcp-local, or when scripts/check_repo.py reports a layout error.
---

# Repo layout

Every file has an obvious home, and no directory is a dumping ground.

## Rules

- **Root:** only what a tool requires at the top level: `README.md`, `LICENSE`, `AGENTS.md`, `go.mod`, `go.sum`, `pyproject.toml`, `moo.toml`, `.gitignore`, `.editorconfig`. Everything else lives in a directory.
- **12 children per directory, everywhere except the root:** files plus subdirectories. Generated code, recordings, and role snapshots count too.
- **When a directory would pass 12, group by what a reader looks for first.** Usually that's the service (`recordings/storage/…`, `roles/storage/…`), then the kind of thing (`scripts/check/…`, `scripts/install/…`). Don't create `misc/`, `other/`, or `util/`.
- **Name directories by their contents, not by when they were added.**

## How to apply

1. Before adding a file, find its group. If the group is full, split it by the next obvious axis and move its siblings in the same change.
2. Update every reference to a moved path: code, CI, Dockerfile, docs, and skills.
3. Run `uv run --with 'PyYAML==6.0.3' python scripts/check_repo.py`. It fails on a new root entry or on any directory with more than 12 children.
