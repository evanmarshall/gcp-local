#!/bin/sh
# Drive google-cloud-secret-manager over gRPC with local ADC and only loopback.
# Run inside Linux CI or a moo task machine after installing requirements/services.txt.
set -eu
cd "$(dirname "$0")/../../.."
GCP_LOCAL_TEST_PYTHON=${GCP_LOCAL_TEST_PYTHON:-"$PWD/.venv-tools/bin/python"}
export GCP_LOCAL_TEST_PYTHON
sh scripts/go.sh test -c -tags=sdkintegration -race -o bin/secretmanager-sdk ./internal/providers/security/secretmanager
cd internal/providers/security/secretmanager
python3 ../../../../scripts/run/isolated.py --timeout 180 -- ../../../../bin/secretmanager-sdk -test.count=1 -test.v -test.run TestSecretManagerPythonSDK
