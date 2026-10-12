#!/bin/sh
set -eu
cd "$(dirname "$0")/../../.."
GCP_LOCAL_JAVA=${GCP_LOCAL_JAVA:-$(command -v java)}
GCP_LOCAL_PUBSUB_JAR="$PWD/.tools/pubsub/emulator.jar"
GCP_LOCAL_TEST_PYTHON=${GCP_LOCAL_TEST_PYTHON:-"$PWD/.venv-tools/bin/python"}
export GCP_LOCAL_JAVA GCP_LOCAL_PUBSUB_JAR GCP_LOCAL_TEST_PYTHON
sh scripts/go.sh test -c -tags=pubsubintegration -race -o bin/pubsub-integration ./internal/providers/messaging/pubsub
python3 scripts/run/isolated.py --timeout 180 -- python3 scripts/run/pubsub.py
