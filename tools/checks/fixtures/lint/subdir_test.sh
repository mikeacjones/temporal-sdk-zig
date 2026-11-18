#!/bin/sh
# Tests that `zig build lint` gives the same result from a subdirectory of the checkout as from
# its root, as every other `zig build` step does: zig finds build.zig in a parent directory, so
# contributors and editors run steps from where they are. On a clean tree the lint exits 0.
# Prints one line per failed case and exits 1 if any failed.
#
# Usage: tools/checks/fixtures/lint/subdir_test.sh
set -eu
root="$(cd "$(dirname "$0")/../../../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
failures=0

# lint_from_subdirectory <dir>: runs `zig build lint` with <dir> (relative to the root) as the
# working directory and expects exit 0.
lint_from_subdirectory() {
  if (cd "$root/$1" && zig build lint >"$tmp/out" 2>&1); then
    return 0
  fi
  echo "FAIL: lint_from_subdirectory $1: zig build lint exited non-zero from $1; its output:"
  sed 's/^/  /' "$tmp/out"
  failures=$((failures + 1))
}

# The root itself, as a control: a failure here is not about the working directory.
lint_from_subdirectory .
lint_from_subdirectory src
lint_from_subdirectory samples

if [ "$failures" -ne 0 ]; then
  exit 1
fi
