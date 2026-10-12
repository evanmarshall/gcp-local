#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
. scripts/backend_env.sh
python3 scripts/generate_terraform_tests.py --check
sh scripts/go.sh -C tests/terraform/runner test -c -o "$PWD/bin/terraform-tests" .
sh scripts/go.sh build -tags=conformance -o bin/storage-conformance ./tools/storage-conformance
python3 scripts/run_isolated.py --timeout 420 \
 --server-json '["bin/storage-conformance","-trace"]' --ready-url http://127.0.0.1:4566/_local/health -- \
 python3 scripts/run_terraform.py
python3 scripts/run_isolated.py --timeout 420 \
 --server-json '["bin/storage-conformance","-fault-rpo"]' --ready-url http://127.0.0.1:4566/_local/health -- \
 python3 scripts/run_terraform.py --negative-control
