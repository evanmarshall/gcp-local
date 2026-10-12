#!/bin/sh
# Drive the Cloud SQL provider against real pinned PostgreSQL with only loopback.
# Run inside Linux CI or a moo task machine after python3 scripts/install_postgres.py.
# PostgreSQL refuses to run as root, so a root caller runs the servers as nobody.
set -eu
cd "$(dirname "$0")/.."
GCP_LOCAL_POSTGRES_ROOT=${GCP_LOCAL_POSTGRES_ROOT:-"$PWD/.tools/postgres"}
export GCP_LOCAL_POSTGRES_ROOT
if [ "$(id -u)" = 0 ] && [ -z "${GCP_LOCAL_POSTGRES_UID:-}" ]; then
    GCP_LOCAL_POSTGRES_UID=$(id -u nobody)
    GCP_LOCAL_POSTGRES_GID=$(id -g nobody)
    export GCP_LOCAL_POSTGRES_UID GCP_LOCAL_POSTGRES_GID
fi
if [ -n "${GCP_LOCAL_POSTGRES_UID:-}" ] && command -v setpriv >/dev/null &&
    ! setpriv --reuid "$GCP_LOCAL_POSTGRES_UID" --regid "${GCP_LOCAL_POSTGRES_GID:-$GCP_LOCAL_POSTGRES_UID}" --clear-groups \
        test -x "$GCP_LOCAL_POSTGRES_ROOT"; then
    echo "uid $GCP_LOCAL_POSTGRES_UID cannot reach $GCP_LOCAL_POSTGRES_ROOT; make its parent directories traversable (chmod o+x)" >&2
    exit 1
fi
sh scripts/go.sh test -c -tags=cloudsqlintegration -race -o bin/cloudsql-integration ./internal/providers/cloudsql
python3 scripts/run_isolated.py --timeout 300 -- bin/cloudsql-integration -test.v -test.run TestPostgresThroughConnector
