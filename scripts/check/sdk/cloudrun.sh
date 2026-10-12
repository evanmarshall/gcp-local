#!/bin/sh
# Drive the google-cloud-run SDK over gRPC with local ADC and only loopback.
# Run inside Linux CI or a moo task machine after installing requirements/services.txt.
set -eu
cd "$(dirname "$0")/../../.."
GCP_LOCAL_TEST_PYTHON=${GCP_LOCAL_TEST_PYTHON:-"$PWD/.venv-tools/bin/python"}
export GCP_LOCAL_TEST_PYTHON
sh scripts/go.sh test -c -tags=sdkintegration -race -o bin/cloudrun-sdk ./internal/providers/run
cd internal/providers/run
python3 ../../../scripts/run/isolated.py --timeout 180 -- ../../../bin/cloudrun-sdk -test.count=1 -test.v -test.run TestCloudRunPythonSDK
