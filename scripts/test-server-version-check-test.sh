#!/bin/sh
# Tests scripts/test-server-version-check.sh on throwaway copies of build.zig, README.md and
# src/testing.zig, so it changes nothing in this checkout. Prints one line per failed case and
# exits 1 if any failed.
#
# Usage: scripts/test-server-version-check-test.sh
set -eu
src="$(cd "$(dirname "$0")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
failures=0
cases=0

# expect <pass|fail> <name> <text the output must contain> <pin> <README line> <testing.zig line>
expect() {
  outcome="$1" name="$2" want="$3" pin="$4" readme="$5" testing="$6"
  cases=$((cases + 1))
  repo="$tmp/repo$cases"
  mkdir -p "$repo/src"
  printf 'const std = @import("std");\nconst verified_test_server_version = "%s";\n' "$pin" > "$repo/build.zig"
  printf '# Title\n\n%s\n' "$readme" > "$repo/README.md"
  printf '/// %s\nconst x = 1;\n' "$testing" > "$repo/src/testing.zig"
  status=0
  out="$(sh "$src/scripts/test-server-version-check.sh" "$repo" 2>&1)" || status=$?
  if [ "$outcome" = pass ] && [ "$status" -ne 0 ]; then
    echo "FAIL $name: expected success, got exit $status: $out"
    failures=$((failures + 1))
  elif [ "$outcome" = fail ] && [ "$status" -eq 0 ]; then
    echo "FAIL $name: expected failure, got success: $out"
    failures=$((failures + 1))
  elif [ -n "$want" ] && ! printf '%s\n' "$out" | grep -qF -- "$want"; then
    echo "FAIL $name: output lacks \"$want\": $out"
    failures=$((failures + 1))
  fi
}

expect pass "README names the pin" "README.md names v1.41.1" v1.41.1 \
  "By default it's the newest release (v1.41.1 in this release), not sdk-core's." \
  'A release tag such as "v1.40.0".'
expect fail "README names another release" "README.md:3 names v1.41.0 as the default test server release, but build.zig's verified_test_server_version is v1.41.1" v1.41.1 \
  "By default it's the newest release (v1.41.0 in this release), not sdk-core's." \
  'A release tag such as "v1.40.0".'
expect fail "the pin moved and README didn't" "build.zig's verified_test_server_version is v1.42.0" v1.42.0 \
  "By default it's the newest release (v1.41.1 in this release)." ''
expect fail "README names no release" "README.md doesn't name the default test server release" v1.41.1 \
  "By default it's the newest release the tests pass with." ''
expect pass "testing.zig names the pin" "" v1.41.1 \
  "Default (v1.41.1 in this release)." 'Defaults to the newest (v1.41.1 in this release).'
expect fail "testing.zig names another release" "src/testing.zig:1 names v1.40.0" v1.41.1 \
  "Default (v1.41.1 in this release)." 'Defaults to the newest (v1.40.0 in this release).'
expect fail "a pre-release tag that differs" "names v1.41.1-rc.1" v1.41.1 \
  "Default (v1.41.1-rc.1 in this release)." ''
expect fail "build.zig without the constant" "no single" "" \
  "Default (v1.41.1 in this release)." ''

if [ "$failures" -ne 0 ]; then
  echo "test-server-version-check-test: $failures of $cases cases failed"
  exit 1
fi
echo "test-server-version-check-test: $cases cases passed"
