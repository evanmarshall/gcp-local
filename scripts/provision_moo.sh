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
apt-get install -y --no-install-recommends ca-certificates git python3 python3-venv build-essential iproute2
python3 -m venv .venv-tools
.venv-tools/bin/python -m pip install --disable-pip-version-check -r requirements-tools.txt -r requirements-sdk.txt
python3 scripts/install_go.py
python3 -m unittest discover -s tests/tooling -v
sh scripts/check_go.sh
apt-get clean
