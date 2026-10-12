#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
unformatted=$(find cmd internal api/generated tools -name '*.go' -exec .tools/go/bin/gofmt -l {} +)
if [ -n "$unformatted" ]; then
    printf 'Run sh scripts/go.sh fmt ./... on these files:\n%s\n' "$unformatted" >&2
    exit 1
fi
sh scripts/go.sh run ./tools/genapi -check
sh scripts/go.sh vet ./...
sh scripts/go.sh test -race -timeout 60s ./...
mkdir -p bin
sh scripts/go.sh build -trimpath -o bin/gcp-local ./cmd/gcp-local
python3 scripts/smoke_runtime.py
