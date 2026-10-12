#!/bin/sh
# gofmt, genapi -check, vet, race tests, a build, and the runtime smoke.
# GCP_LOCAL_LOW_DISK=1 clears the Go build cache after vet and after the race
# tests, because a moo guest's disk can't hold both caches (docs/contributing/moo.md).
set -eu
cd "$(dirname "$0")/../.."
low_disk() {
    if [ -n "${GCP_LOCAL_LOW_DISK:-}" ]; then
        sh scripts/go.sh clean -cache
    fi
}
unformatted=$(find cmd internal api/generated tools -name '*.go' -exec .tools/go/bin/gofmt -l {} +)
if [ -n "$unformatted" ]; then
    printf 'Run sh scripts/go.sh fmt ./... on these files:\n%s\n' "$unformatted" >&2
    exit 1
fi
sh scripts/go.sh run ./tools/genapi -check
sh scripts/go.sh vet ./...
low_disk
sh scripts/go.sh test -race -timeout 60s ./...
low_disk
mkdir -p bin
sh scripts/go.sh build -trimpath -o bin/gcp-local ./cmd/gcp-local
python3 scripts/smoke/runtime.py
