#!/bin/sh
set -eu
repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"
existing_hooks=$(git config --get core.hooksPath || true)
if [ -n "$existing_hooks" ] && [ "$existing_hooks" != .githooks ]; then
    printf 'Existing hooks path is %s; reconcile it with .githooks before setup.\n' "$existing_hooks" >&2
    exit 1
fi
git config --local pull.ff only
git config --local merge.ff only
git config --local fetch.prune true
git config --local core.hooksPath .githooks
git config --local commit.template .gitmessage
printf 'Configured fast-forward defaults, pre-push checks, and commit template.\n'
