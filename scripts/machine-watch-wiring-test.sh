#!/usr/bin/env bash
# Tests build.zig's machine-watch wiring: only `zig build test` and `zig build core` start a
# sampler. A top-level step that shares a step with `test` (readme-check and test-allocators share
# their check runs with it) must not start the test watch, because no report runs for it then:
# its sampler ends with an `ended=without-report` line for a test run that never happened, in
# the step summary of a job that ran no tests (CI's check job runs readme-check).
#
# Usage: bash scripts/machine-watch-wiring-test.sh
set -uo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
name=machine-watch-wiring-test

skip() { echo "$name: skipped: $1"; exit 0; }
command -v zig >/dev/null || skip "zig is not installed"

cd "$root" || exit 1
host=$(uname -n)
failures=0
for step in readme-check test-allocators; do
  summary="$(mktemp)"
  GITHUB_STEP_SUMMARY="$summary" zig build "$step" >/dev/null 2>&1 &
  pid=$!
  wait "$pid"
  status=$?
  [ "$status" -eq 0 ] || { echo "$name: zig build $step exited $status"; failures=$((failures + 1)); }
  dir="$root/.zig-cache/machine-watch/$host-$pid-test"
  if [ -d "$dir" ]; then
    echo "$name: zig build $step started the test watch in $dir"
    failures=$((failures + 1))
    # Stop the sampler it left the way report would, so this test leaves nothing running.
    : >"$dir/stop"
  fi
  rm -f "$summary"
done

if [ "$failures" -ne 0 ]; then
  echo "$name: $failures checks failed"
  exit 1
fi
echo "$name: passed"
