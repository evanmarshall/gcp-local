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
sh scripts/go.sh test -c -tags=pubsubintegration -race -o bin/pubsub-integration ./internal/providers/messaging/pubsub
"$GCP_LOCAL_TEST_PYTHON" scripts/run/recordings.py
