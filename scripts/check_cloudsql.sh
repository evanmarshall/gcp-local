#!/bin/sh
# Drive the Cloud SQL provider against real pinned PostgreSQL and MySQL with only loopback.
# Run inside Linux CI or a moo task machine after python3 scripts/install_postgres.py
# and python3 scripts/install_mysql.py (mysqld also needs libaio.so.1 and libnuma.so.1).
# backend_env.sh runs the servers as nobody for a root caller.
set -eu
cd "$(dirname "$0")/.."
. scripts/backend_env.sh
for root in "$GCP_LOCAL_POSTGRES_ROOT" "$GCP_LOCAL_MYSQL_ROOT"; do
    if [ -n "${GCP_LOCAL_POSTGRES_UID:-}" ] && command -v setpriv >/dev/null &&
        ! setpriv --reuid "$GCP_LOCAL_POSTGRES_UID" --regid "${GCP_LOCAL_POSTGRES_GID:-$GCP_LOCAL_POSTGRES_UID}" --clear-groups \
            test -x "$root"; then
        echo "uid $GCP_LOCAL_POSTGRES_UID cannot reach $root; make its parent directories traversable (chmod o+x)" >&2
        exit 1
    fi
done
sh scripts/go.sh test -c -tags=cloudsqlintegration -race -o bin/cloudsql-integration ./internal/providers/cloudsql
python3 scripts/run_isolated.py --timeout 300 -- bin/cloudsql-integration -test.v -test.run 'TestPostgresThroughConnector|TestMySQLThroughConnector'
