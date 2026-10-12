#!/bin/sh
# Runs inside a fresh moo guest. Do not provision the host with this script.
set -eu

if [ "$(uname -s)" != Linux ] || [ ! -x /usr/local/bin/moo-agent ]; then
    echo "Run this through scripts/setup_moo.sh inside a moo machine." >&2
    exit 1
fi
cd /srv/app
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
# Everything the Linux checks need, so a fork of this baseline can run any of
# them (see CONTRIBUTING.md's checks table). Keep this list current when a
# check gains a dependency.
apt-get install -y --no-install-recommends ca-certificates curl git python3 python3-venv \
    build-essential pkg-config iproute2 procps openjdk-17-jre-headless
python3 -m venv .venv-tools
.venv-tools/bin/python -m pip install --disable-pip-version-check \
    -r requirements-tools.txt -r requirements-sdk.txt -r requirements-cloud.txt \
    -r requirements-services.txt -r tests/clients/python/requirements.txt
python3 scripts/install_go.py
python3 scripts/install_emulator_jar.py pubsub
python3 scripts/install_emulator_jar.py firestore
python3 scripts/install_spanner.py
python3 scripts/install_redis.py
python3 scripts/install_postgres.py
python3 scripts/install_terraform.py
python3 scripts/install_client_tools.py
PATH="$PWD/.tools/clients/node/bin:$PATH" npm ci --prefix tests/clients/node --no-audit --no-fund
sh scripts/go.sh mod download
sh scripts/go.sh -C tests/clients/go mod download
sh scripts/go.sh -C tests/terraform/runner mod download
python3 -m unittest discover -s tests/tooling -v
sh scripts/check_go.sh
apt-get clean
