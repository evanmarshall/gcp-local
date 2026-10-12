#!/bin/sh
# Drive the client matrix (tests/clients/matrix.json) against bin/gcp-local with
# local ADC and only loopback. Run inside Linux CI or a moo task machine after
# installing the backends, install_client_tools.py, the Node modules, and
# tests/clients/python/requirements.txt. Arguments go to scripts/run_clients.py.
set -eu
cd "$(dirname "$0")/.."
. scripts/backend_env.sh
python=${GCP_LOCAL_TEST_PYTHON:-"$PWD/.venv-tools/bin/python"}
export GCP_LOCAL_TEST_PYTHON="$python"
sh scripts/go.sh build -o bin/gcp-local ./cmd/gcp-local
sh scripts/go.sh -C tests/clients/go build -o "$PWD/bin/client-matrix-go" .
"$python" scripts/run_isolated.py --timeout 1500 -- "$python" scripts/run_clients.py "$@"
