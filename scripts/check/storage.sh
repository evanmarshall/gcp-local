#!/bin/sh
# Drive the production binary with production local ADC through the Storage SDK
# smoke. The Storage recordings replay in scripts/check/recordings.sh.
# Run inside Linux CI or a moo task machine with namespace permission.
set -eu
cd "$(dirname "$0")/../.."
. scripts/run/backend_env.sh
storage_python=${GCP_LOCAL_TEST_PYTHON:-.venv-tools/bin/python}
sh scripts/go.sh build -trimpath -o bin/gcp-local ./cmd/gcp-local
"$storage_python" scripts/run/isolated.py -- "$storage_python" scripts/smoke/storage.py
