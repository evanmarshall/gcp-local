#!/bin/sh
# Replay every committed GCP recording against the local providers and fail
# unless each one is verified (scripts/run/recordings.py). CI runs this as the
# recordings job in .github/workflows/repository-checks.yml.
# Run inside Linux CI or a moo task machine with namespace permission, Java 17,
# every backend, and requirements/services.txt.
set -eu
cd "$(dirname "$0")/../.."
. scripts/run/backend_env.sh
GCP_LOCAL_TEST_PYTHON=${GCP_LOCAL_TEST_PYTHON:-"$PWD/.venv-tools/bin/python"}
export GCP_LOCAL_TEST_PYTHON
sh scripts/go.sh build -tags=conformance -o bin/storage-conformance ./tools/storage-conformance
# The transport matrix registers every provider, so it needs every backend
# and runs here; a skip is a failure (docs/architecture/transports.md).
matrix=$(sh scripts/go.sh test -count=1 -run '^TestTransportMatrix$' -v ./cmd/gcp-local) || {
    printf '%s\n' "$matrix" >&2
    exit 1
}
case $matrix in *"--- SKIP"*)
    printf '%s\n' "$matrix" >&2
    exit 1
    ;;
esac
sh scripts/go.sh test -c -tags=pubsubintegration -race -o bin/pubsub-integration ./internal/providers/messaging/pubsub
# GCP_LOCAL_LOW_DISK=1 frees the build cache before the servers start, because
# a moo guest disk can't hold it and Cloud SQL data (docs/contributing/moo.md).
if [ -n "${GCP_LOCAL_LOW_DISK:-}" ]; then
    sh scripts/go.sh clean -cache
fi
"$GCP_LOCAL_TEST_PYTHON" scripts/run/recordings.py
