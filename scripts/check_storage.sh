#!/bin/sh
# Run inside Linux CI or a moo task machine with namespace permission.
set -eu
cd "$(dirname "$0")/.."
GCP_LOCAL_JAVA=${GCP_LOCAL_JAVA:-$(command -v java)}
GCP_LOCAL_PUBSUB_JAR=${GCP_LOCAL_PUBSUB_JAR:-"$PWD/.tools/pubsub/emulator.jar"}
GCP_LOCAL_FIRESTORE_JAR=${GCP_LOCAL_FIRESTORE_JAR:-"$PWD/.tools/firestore/emulator.jar"}
GCP_LOCAL_SPANNER_EMULATOR=${GCP_LOCAL_SPANNER_EMULATOR:-"$PWD/.tools/spanner/emulator_main"}
GCP_LOCAL_SPANNER_LIBRARIES=${GCP_LOCAL_SPANNER_LIBRARIES:-"$PWD/.tools/spanner/lib"}
GCP_LOCAL_REDIS_SERVER=${GCP_LOCAL_REDIS_SERVER:-"$PWD/.tools/redis/bin/redis-server"}
GCP_LOCAL_POSTGRES_ROOT=${GCP_LOCAL_POSTGRES_ROOT:-"$PWD/.tools/postgres"}
# PostgreSQL refuses to run as root, so a root caller runs the servers as nobody.
if [ "$(id -u)" = 0 ] && [ -z "${GCP_LOCAL_POSTGRES_UID:-}" ]; then
    GCP_LOCAL_POSTGRES_UID=$(id -u nobody)
    GCP_LOCAL_POSTGRES_GID=$(id -g nobody)
    export GCP_LOCAL_POSTGRES_UID GCP_LOCAL_POSTGRES_GID
fi
export GCP_LOCAL_JAVA GCP_LOCAL_PUBSUB_JAR GCP_LOCAL_FIRESTORE_JAR GCP_LOCAL_SPANNER_EMULATOR GCP_LOCAL_SPANNER_LIBRARIES GCP_LOCAL_REDIS_SERVER GCP_LOCAL_POSTGRES_ROOT
storage_python=${GCP_LOCAL_TEST_PYTHON:-.venv-tools/bin/python}
sh scripts/go.sh build -trimpath -o bin/gcp-local ./cmd/gcp-local
sh scripts/go.sh build -tags=conformance -o bin/storage-conformance ./tools/storage-conformance
"$storage_python" scripts/run_isolated.py -- "$storage_python" scripts/smoke_storage.py
"$storage_python" scripts/run_isolated.py \
  --server-json '["bin/storage-conformance"]' --ready-url http://127.0.0.1:4566/_local/health -- \
  "$storage_python" -m pytest tests/conformance/test_storage*.py --gcp-mode local --gcp-project gcp-local-test \
  --gcp-endpoint http://127.0.0.1:4566 --gcp-artifacts .artifacts/storage-provider \
  --gcp-provider-version "gcp-local/${GITHUB_SHA:-working-tree}" \
  --gcp-backend-version fake-gcs-server/1.56.1 -q
