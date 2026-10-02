#!/bin/sh
# Keep an existing installation; otherwise install the reviewed upstream release.
set -eu

if command -v moo >/dev/null 2>&1; then
    moo doctor
    exit 0
fi

if [ "$(uname -s)" != Darwin ] || [ "$(uname -m)" != arm64 ]; then
    echo "moo requires macOS on Apple Silicon; use the ordinary checks on other hosts." >&2
    exit 1
fi
command -v brew >/dev/null 2>&1 || {
    echo "Install Homebrew before running the moo installer." >&2
    exit 1
}

moo_install_dir=$(mktemp -d)
trap 'rm -rf "$moo_install_dir"' EXIT HUP INT TERM
curl --fail --show-error --silent --location \
    https://github.com/heyito/moo/releases/download/v0.3.1/install.sh \
    --output "$moo_install_dir/install.sh"
moo_installer_sha=$(shasum -a 256 "$moo_install_dir/install.sh" | cut -d ' ' -f 1)
if [ "$moo_installer_sha" != d4d499726ee3c8931d6b253ff5ef086c6ea27dde379bfc1c66d9c7b4881b050f ]; then
    echo "moo installer checksum mismatch." >&2
    exit 1
fi
MOO_VERSION=v0.3.1 sh "$moo_install_dir/install.sh"
