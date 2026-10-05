#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ ! -x .tools/go/bin/go ]; then
    echo 'Install the pinned toolchain with python3 scripts/install_go.py' >&2
    exit 1
fi
export GOTOOLCHAIN=local
exec .tools/go/bin/go "$@"
