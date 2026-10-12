#!/bin/sh
set -eu
cd "$(dirname "$0")/../../.."
sh scripts/go.sh build -tags=backendprobe -o bin/pubsub-probe ./tools/pubsub-probe
python3 scripts/run/isolated.py --timeout 90 -- python3 scripts/run/probe_pubsub_backend.py
