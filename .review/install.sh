#!/usr/bin/env bash
# Installs the scanners that .review/reports.sh runs: gitleaks (secrets), osv-scanner and
# cargo-deny (the Rust dependencies), shellcheck, actionlint, typos, and zlint (Zig).
#
# With Homebrew it installs the first six from there. Without it, it lists the ones that are
# missing for you to install with your own package manager. zlint is not packaged (Homebrew's
# "zlint" is a different tool, an X.509 certificate linter), so its release binary is
# downloaded from GitHub, checked against the SHA-256 pinned below, and put in
# ~/.local/share/zig-zlint/, where reports.sh looks for it (or set ZLINT to another path).
#
# To move zlint to a new release: change zlint_version, download each asset, and replace the
# four checksums with `shasum -a 256` of what you downloaded.
#
# Usage: .review/install.sh
set -euo pipefail

if [ $# -ne 0 ]; then
  echo "usage: $0" >&2
  exit 2
fi

# Command name, then the Homebrew formula that provides it.
tools="gitleaks:gitleaks osv-scanner:osv-scanner cargo-deny:cargo-deny shellcheck:shellcheck actionlint:actionlint typos:typos-cli"
missing=""
for pair in $tools; do
  command -v "${pair%%:*}" >/dev/null 2>&1 || missing="$missing ${pair##*:}"
done
if [ -z "$missing" ]; then
  echo "gitleaks, osv-scanner, cargo-deny, shellcheck, actionlint, and typos are installed"
elif command -v brew >/dev/null 2>&1; then
  # shellcheck disable=SC2086
  brew install $missing
else
  echo "install.sh: Homebrew not found. Install these with your package manager:$missing" >&2
  echo "install.sh: reports.sh reports each one that is missing; the rest still run" >&2
fi

zlint_version=v0.10.0
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) asset=zlint-macos-aarch64 sha=9dcc4b27f450c7ba6707403283b9a44092a41b777356ec7326a611bd9ec67200 ;;
  Darwin-x86_64) asset=zlint-macos-x86_64 sha=59f6ad25c625dabdfb4bed6cb640e400c4235fdd5475608966060e548c208b3b ;;
  Linux-aarch64) asset=zlint-linux-aarch64 sha=ec5e31eacc889540dbfa0a32f0e694199d06c16f640b832bf2a3bd8ecc07c3e3 ;;
  Linux-x86_64) asset=zlint-linux-x86_64 sha=0b331646d5e40bec3cfcb0694c5da0b13b9de8b10af12937a777ab3144b70b65 ;;
  *)
    echo "install.sh: no pinned zlint build for $(uname -s) $(uname -m); reports.sh will report zlint as missing" >&2
    exit 0
    ;;
esac
dest="$HOME/.local/share/zig-zlint"
if [ -x "$dest/zlint" ] && [ "$("$dest/zlint" --version 2>/dev/null)" = "$zlint_version" ]; then
  echo "zlint $zlint_version is installed in $dest"
  exit 0
fi
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
url="https://github.com/DonIsaac/zlint/releases/download/$zlint_version/$asset"
curl -fsSL -o "$tmp/zlint" "$url"
got=$(shasum -a 256 "$tmp/zlint" | cut -d' ' -f1)
if [ "$got" != "$sha" ]; then
  echo "install.sh: $url has SHA-256 $got, not the pinned $sha; not installing it" >&2
  exit 1
fi
mkdir -p "$dest"
install -m 755 "$tmp/zlint" "$dest/zlint"
echo "zlint $zlint_version installed in $dest"
