#!/bin/sh
# Run the Firestore provider against the pinned official emulator with loopback only.
# Run inside Linux CI or a moo task machine with namespace permission.
set -eu
cd "$(dirname "$0")/.."
GCP_LOCAL_JAVA=${GCP_LOCAL_JAVA:-$(command -v java)}
GCP_LOCAL_FIRESTORE_JAR=${GCP_LOCAL_FIRESTORE_JAR:-"$PWD/.tools/firestore/emulator.jar"}
export GCP_LOCAL_JAVA GCP_LOCAL_FIRESTORE_JAR
sh scripts/go.sh test -c -tags=firestoreintegration -race -o bin/firestore-integration ./internal/providers/firestore
python3 scripts/run_isolated.py --timeout 180 -- bin/firestore-integration -test.count=1 -test.v
