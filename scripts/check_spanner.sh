#!/bin/sh
# Drive the Spanner provider against the pinned official emulator with only loopback.
# Run inside Linux CI or a moo task machine after python3 scripts/install_spanner.py.
set -eu
cd "$(dirname "$0")/.."
GCP_LOCAL_SPANNER_EMULATOR=${GCP_LOCAL_SPANNER_EMULATOR:-"$PWD/.tools/spanner/emulator_main"}
GCP_LOCAL_SPANNER_LIBRARIES=${GCP_LOCAL_SPANNER_LIBRARIES:-"$PWD/.tools/spanner/lib"}
export GCP_LOCAL_SPANNER_EMULATOR GCP_LOCAL_SPANNER_LIBRARIES
sh scripts/go.sh test -c -tags=spannerintegration -race -o bin/spanner-integration ./internal/providers/spanner
python3 scripts/run_isolated.py --timeout 180 -- bin/spanner-integration -test.v -test.run TestOfficialEmulatorLifecycle
