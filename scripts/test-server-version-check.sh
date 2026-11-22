#!/bin/sh
# Checks that the docs name the test server release build.zig pins as the default:
# README.md's "(<tag> in this release)", and the same phrase wherever it appears in
# src/testing.zig, must equal build.zig's `verified_test_server_version`. README.md must
# name it once at least, so a reworded sentence can't drop the check unnoticed.
# Exits 1 when a release named that way differs or README.md names none, 0 otherwise.
#
# Usage: scripts/test-server-version-check.sh [<repository root>]
# scripts/test-server-version-check-test.sh tests this script.
set -eu
root="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
for f in build.zig README.md src/testing.zig; do
  [ -f "$root/$f" ] || { echo "test-server-version-check: $root/$f not found" >&2; exit 1; }
done

pin="$(sed -n 's/^const verified_test_server_version = "\([^"]*\)";$/\1/p' "$root/build.zig")"
if [ -z "$pin" ] || [ "$(printf '%s\n' "$pin" | wc -l | tr -d ' ')" -ne 1 ]; then
  echo "test-server-version-check: build.zig has no single 'const verified_test_server_version = \"<tag>\";' line" >&2
  exit 1
fi

failed=0
# named <file>: prints "<line>:<tag>" for each "(<tag> in this release)" in the file.
named() {
  grep -no '(v[0-9][0-9.]*[0-9A-Za-z.-]* in this release)' "$root/$1" |
    sed 's/^\([0-9]*\):(\(.*\) in this release)$/\1:\2/' || true
}
for f in README.md src/testing.zig; do
  found="$(named "$f")"
  if [ "$f" = README.md ] && [ -z "$found" ]; then
    echo "test-server-version-check: README.md doesn't name the default test server release as \"($pin in this release)\"" >&2
    failed=1
  fi
  for entry in $found; do
    line="${entry%%:*}" tag="${entry#*:}"
    if [ "$tag" != "$pin" ]; then
      echo "test-server-version-check: $f:$line names $tag as the default test server release, but build.zig's verified_test_server_version is $pin" >&2
      failed=1
    fi
  done
done
[ "$failed" -eq 0 ] || exit 1
echo "test-server-version-check: README.md names $pin, build.zig's verified_test_server_version"
