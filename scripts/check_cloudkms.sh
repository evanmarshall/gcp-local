#!/bin/sh
# Drive the google-cloud-kms SDK over REST with local ADC and only loopback.
# Run inside Linux CI or a moo task machine after installing requirements-services.txt.
set -eu
cd "$(dirname "$0")/.."
GCP_LOCAL_TEST_PYTHON=${GCP_LOCAL_TEST_PYTHON:-"$PWD/.venv-tools/bin/python"}
export GCP_LOCAL_TEST_PYTHON
sh scripts/go.sh test -c -tags=sdkintegration -race -o bin/cloudkms-sdk ./internal/providers/cloudkms
cd internal/providers/cloudkms
python3 ../../../scripts/run_isolated.py --timeout 180 -- ../../../bin/cloudkms-sdk -test.count=1 -test.v -test.run TestCloudKMSSDK
