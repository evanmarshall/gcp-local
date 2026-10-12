#!/bin/sh
# Provision a repository-scoped baseline, or a separately named replacement.
set -eu

if [ "$#" -gt 1 ]; then
    echo "Usage: sh scripts/moo/setup.sh [baseline-name]" >&2
    exit 1
fi
moo_baseline=${1:-base}
case "$moo_baseline" in
    ''|*[!a-zA-Z0-9._-]*)
        echo "Use a baseline name containing letters, digits, dots, underscores, or hyphens." >&2
        exit 1
        ;;
esac
cd "$(dirname "$0")/../.."
sh scripts/moo/install.sh
moo new "$moo_baseline"
moo run "$moo_baseline" -- 'sh /srv/app/scripts/moo/provision.sh'
moo save "$moo_baseline"
