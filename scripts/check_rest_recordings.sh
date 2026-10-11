#!/bin/sh
# Compare the local providers against the shared REST recordings.
# Run inside Linux CI or a moo task machine with namespace permission.
set -eu
cd "$(dirname "$0")/.."
GCP_LOCAL_JAVA=${GCP_LOCAL_JAVA:-$(command -v java)}
GCP_LOCAL_PUBSUB_JAR=${GCP_LOCAL_PUBSUB_JAR:-"$PWD/.tools/pubsub/emulator.jar"}
export GCP_LOCAL_JAVA GCP_LOCAL_PUBSUB_JAR
python=${GCP_LOCAL_TEST_PYTHON:-.venv-tools/bin/python}
sh scripts/go.sh build -tags=conformance -o bin/storage-conformance ./tools/storage-conformance
"$python" scripts/run_isolated.py \
  --server-json '["bin/storage-conformance"]' --ready-url http://127.0.0.1:4566/_local/health -- \
  "$python" -m pytest tests/conformance/secretmanager tests/conformance/serviceaccounts tests/conformance/projectiam tests/conformance/artifactregistry tests/conformance/firestore tests/conformance/bigquery \
  --gcp-mode local --gcp-project gcp-local-test \
  --gcp-endpoint http://127.0.0.1:4566 --gcp-artifacts .artifacts/rest-provider \
  --gcp-provider-version "gcp-local/${GITHUB_SHA:-working-tree}" \
  --gcp-backend-version not-applicable -q
