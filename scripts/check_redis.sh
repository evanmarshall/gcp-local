#!/bin/sh
# Drive the Memorystore provider against the pinned redis-server with only loopback.
# Run inside Linux CI or a moo task machine after python3 scripts/install_redis.py.
set -eu
cd "$(dirname "$0")/.."
GCP_LOCAL_REDIS_SERVER=${GCP_LOCAL_REDIS_SERVER:-"$PWD/.tools/redis/bin/redis-server"}
export GCP_LOCAL_REDIS_SERVER
sh scripts/go.sh test -c -tags=redisintegration -race -o bin/redis-integration ./internal/providers/memorystore
python3 scripts/run_isolated.py --timeout 180 -- bin/redis-integration -test.count=1 -test.v -test.run TestRealRedisLifecycle
