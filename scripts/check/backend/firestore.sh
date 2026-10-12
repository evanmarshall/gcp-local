#!/bin/sh
# Run the Firestore provider against the pinned official emulator with loopback only.
# Run inside Linux CI or a moo task machine with namespace permission.
# The Python SDK test needs requirements/services.txt in GCP_LOCAL_TEST_PYTHON.
set -eu
cd "$(dirname "$0")/../../.."
GCP_LOCAL_JAVA=${GCP_LOCAL_JAVA:-$(command -v java)}
GCP_LOCAL_FIRESTORE_JAR=${GCP_LOCAL_FIRESTORE_JAR:-"$PWD/.tools/firestore/emulator.jar"}
GCP_LOCAL_TEST_PYTHON=${GCP_LOCAL_TEST_PYTHON:-"$PWD/.venv-tools/bin/python"}
GCP_LOCAL_FIRESTORE_SDK="$PWD/internal/providers/databases/firestore/testdata/sdk.py"
export GCP_LOCAL_JAVA GCP_LOCAL_FIRESTORE_JAR GCP_LOCAL_TEST_PYTHON GCP_LOCAL_FIRESTORE_SDK
sh scripts/go.sh test -c -tags=firestoreintegration -race -o bin/firestore-integration ./internal/providers/databases/firestore
python3 scripts/run/isolated.py --timeout 240 -- bin/firestore-integration -test.count=1 -test.v
