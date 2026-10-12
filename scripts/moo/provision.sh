#!/bin/sh
# Runs inside a fresh moo guest. Do not provision the host with this script.
set -eu

if [ "$(uname -s)" != Linux ] || [ ! -x /usr/local/bin/moo-agent ]; then
    echo "Run this through scripts/moo/setup.sh inside a moo machine." >&2
    exit 1
fi
cd /srv/app
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
# Everything the Linux checks need, so a fork of this baseline can run any of
# them (see .github/CONTRIBUTING.md's checks table). Keep this list current when a
# check gains a dependency.
apt-get install -y --no-install-recommends ca-certificates curl git python3 python3-venv \
    build-essential pkg-config iproute2 procps openjdk-17-jre-headless libaio1 libnuma1
python3 -m venv .venv-tools
.venv-tools/bin/python -m pip install --disable-pip-version-check \
    -r requirements/tools.txt -r requirements/sdk.txt -r requirements/cloud.txt \
    -r requirements/services.txt -r tests/clients/python/requirements.txt
python3 scripts/install/go.py
python3 scripts/install/emulator_jar.py pubsub
python3 scripts/install/emulator_jar.py firestore
python3 scripts/install/spanner.py
python3 scripts/install/redis.py
python3 scripts/install/postgres.py
python3 scripts/install/mysql.py
python3 scripts/install/terraform.py
python3 scripts/install/client_tools.py
PATH="$PWD/.tools/clients/node/bin:$PATH" npm ci --prefix tests/clients/node --no-audit --no-fund
sh scripts/go.sh mod download
sh scripts/go.sh -C tests/clients/go mod download
sh scripts/go.sh -C tests/terraform/runner mod download
python3 -m unittest discover -s tests/tooling -v
# The core build (the BigQuery GoogleSQL engine) leaves 2.4 GB of the 7.8 GB
# guest disk and peaks near 6 GiB of the 8 GiB memory (docs/contributing/moo.md):
# keep the go command's work directory on an exec tmpfs (/dev/shm is noexec),
# compile one package at a time with GOGC=25, clear the cache between check
# steps, and leave no cache in the baseline for forks to inherit.
sh scripts/go.sh clean -cache
build=/mnt/go-build
mkdir -p "$build"
mount -t tmpfs -o size=3g,mode=0700 tmpfs "$build"
GOTMPDIR="$build" TMPDIR="$build" GOGC=25 GOFLAGS=-p=1 GCP_LOCAL_LOW_DISK=1 sh scripts/check/go.sh
umount "$build"
sh scripts/go.sh clean -cache
apt-get clean
