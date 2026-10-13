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
# BigQuery's tests drive one process-global GoogleSQL wasm module, which
# serializes them (t.Parallel gains nothing) and takes about 35 s under -race
# on an M-series Mac and up to twice that on GitHub runners. They run as
# separate processes in parallel shards, each under the same 60 s limit.
sharded=internal/providers/databases/bigquery
sh scripts/go.sh test -race -timeout 60s $(sh scripts/go.sh list ./... | grep -v "/$sharded\$")
mkdir -p bin
sh scripts/go.sh test -race -c -o "$PWD/bin/bigquery.test" "./$sharded"
python3 scripts/check/shards.py --binary "$PWD/bin/bigquery.test" --dir "$sharded" --shards 3 --timeout 60s
rm -f bin/bigquery.test
low_disk
mkdir -p bin
sh scripts/go.sh build -trimpath -o bin/gcp-local ./cmd/gcp-local
python3 scripts/smoke/runtime.py
