#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
GCP_LOCAL_JAVA=${GCP_LOCAL_JAVA:-$(command -v java)}
GCP_LOCAL_PUBSUB_JAR=${GCP_LOCAL_PUBSUB_JAR:-"$PWD/.tools/pubsub/emulator.jar"}
GCP_LOCAL_FIRESTORE_JAR=${GCP_LOCAL_FIRESTORE_JAR:-"$PWD/.tools/firestore/emulator.jar"}
GCP_LOCAL_SPANNER_EMULATOR=${GCP_LOCAL_SPANNER_EMULATOR:-"$PWD/.tools/spanner/emulator_main"}
GCP_LOCAL_SPANNER_LIBRARIES=${GCP_LOCAL_SPANNER_LIBRARIES:-"$PWD/.tools/spanner/lib"}
GCP_LOCAL_REDIS_SERVER=${GCP_LOCAL_REDIS_SERVER:-"$PWD/.tools/redis/bin/redis-server"}
export GCP_LOCAL_JAVA GCP_LOCAL_PUBSUB_JAR GCP_LOCAL_FIRESTORE_JAR GCP_LOCAL_SPANNER_EMULATOR GCP_LOCAL_SPANNER_LIBRARIES GCP_LOCAL_REDIS_SERVER
python3 scripts/generate_terraform_tests.py --check
sh scripts/go.sh -C tests/terraform/runner test -c -o "$PWD/bin/terraform-tests" .
sh scripts/go.sh build -tags=conformance -o bin/storage-conformance ./tools/storage-conformance
python3 scripts/run_isolated.py --timeout 420 \
 --server-json '["bin/storage-conformance"]' --ready-url http://127.0.0.1:4566/_local/health -- \
 python3 scripts/run_terraform.py
python3 scripts/run_isolated.py --timeout 420 \
 --server-json '["bin/storage-conformance","-fault-rpo"]' --ready-url http://127.0.0.1:4566/_local/health -- \
 python3 scripts/check_terraform_fault.py
