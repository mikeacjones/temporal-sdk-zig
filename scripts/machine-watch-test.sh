#!/usr/bin/env bash
# Tests scripts/machine-watch.sh: start then report; a watched process killed before report;
# two runs at once; failing probes; and report without start. It checks the shape of the lines
# (field names and their order, missing=, ended=without-report) and that every sampler is gone
# afterwards, never the measured values. Everything runs under a fresh temporary directory with
# one sample a second, so two runs at once don't meet. zig build test runs it.
#
# Usage: bash scripts/machine-watch-test.sh
set -uo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
name=machine-watch-test
watch="$root/scripts/machine-watch.sh"

skip() { echo "$name: skipped: $1"; exit 0; }
command -v ps >/dev/null || skip "ps is not installed"
probe=$(bash -c 'sleep 5 </dev/null & p=$!; l=$(LC_ALL=C ps -o lstart= -p "$p" 2>/dev/null); kill "$p"; [ -n "$l" ] && echo ok' 2>/dev/null || true)
[ "$probe" = ok ] || skip "ps -o lstart= or background jobs do not work here"

work="$(mktemp -d)"
sleeper=""
cleanup() {
  if [ -n "$sleeper" ]; then kill -KILL "$sleeper" 2>/dev/null || true; fi
  # A sampler a failed case left behind stops through its stop file, as report stops it.
  for d in "$work"/base*/*/; do [ -d "$d" ] && : >"$d/stop"; done 2>/dev/null
  sleep 1
  rm -rf "$work"
}
trap cleanup EXIT
export MACHINE_WATCH_INTERVAL=1
export GITHUB_STEP_SUMMARY="$work/step-summary.md"
: >"$GITHUB_STEP_SUMMARY"
host=$(uname -n)
cases=0
failures=0

fail() { # <case> <what failed>
  echo "$name: $1: $2"
  failures=$((failures + 1))
}

# The line's shape, fields in order. Values are words without spaces.
v='[^ ]+'
fields="total=$v used_peak=$v used_peak_at=$v swap_peak=$v compressor_peak=$v memorystatus_min=$v load1_peak=$v samples=[0-9]+ span=-?[0-9]+ started_after=$v top=$v missing=$v"
shape="^machine-watch [^:]+: $fields"

# Fails the case unless the sampler of <dir> has exited and left <dir>/done.
check_gone() { # <case> <dir>
  local pid
  pid=$(cat "$2/pid" 2>/dev/null || true)
  [ -n "$pid" ] || { fail "$1" "start wrote no $2/pid"; return; }
  for _ in $(seq 1 30); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
  done
  kill -0 "$pid" 2>/dev/null && fail "$1" "the sampler $pid is still running"
  [ -e "$2/done" ] || fail "$1" "no $2/done"
}

# (a) start, then report.
cases=$((cases + 1))
base="$work/base-a"
status=0
MACHINE_WATCH_LABEL="test a" bash "$watch" start "$base" test $$ 2>"$work/a.err" || status=$?
[ "$status" -eq 0 ] || fail start-report "start exited $status: $(cat "$work/a.err")"
sleep 2
status=0
line=$(bash "$watch" report "$base" test "test x86_64-macos Debug" $$ 2>>"$work/a.err") || status=$?
[ "$status" -eq 0 ] || fail start-report "report exited $status: $(cat "$work/a.err")"
[ "$(printf '%s\n' "$line" | wc -l | tr -d ' ')" -eq 1 ] || fail start-report "not one line: $line"
printf '%s\n' "$line" | grep -qE "^machine-watch test x86_64-macos Debug: $fields\$" ||
  fail start-report "line has the wrong shape: $line"
case "$line" in *" missing=none") ;; *) fail start-report "a probe is missing on this host: $line" ;; esac
case "$line" in *" samples=0 "* | *" samples=1 "*) fail start-report "fewer than 2 samples in 2 s: $line" ;; esac
case "$(uname)" in
  Linux) case "$line" in *" compressor_peak=n/a memorystatus_min=n/a "*) ;; *) fail start-report "compressor and memorystatus are not n/a on Linux: $line" ;; esac ;;
  Darwin) case "$line" in *"n/a"*) fail start-report "a field reads n/a on macOS: $line" ;; esac ;;
esac
grep -qxF -- "- \`$line\`" "$GITHUB_STEP_SUMMARY" || fail start-report "not in GITHUB_STEP_SUMMARY: $(cat "$GITHUB_STEP_SUMMARY")"
check_gone start-report "$base/$host-$$-test"
[ -s "$work/a.err" ] && fail start-report "wrote to stderr: $(cat "$work/a.err")"

# (b) The watched process is killed before report runs: the sampler writes its own line and exits.
cases=$((cases + 1))
base="$work/base-b"
sleep 300 &
sleeper=$!
MACHINE_WATCH_LABEL="test b" bash "$watch" start "$base" test "$sleeper"
dir="$base/$host-$sleeper-test"
sleep 1.5
kill -KILL "$sleeper" 2>/dev/null
wait "$sleeper" 2>/dev/null
for _ in $(seq 1 30); do
  [ -e "$dir/done" ] && break
  sleep 0.1
done
line=$(cat "$dir/summary" 2>/dev/null || true)
printf '%s\n' "$line" | grep -qE "^machine-watch test b: $fields ended=without-report\$" ||
  fail killed-parent "no ended=without-report line within 3 s: '$line' $(cat "$dir/sampler.log" 2>/dev/null)"
grep -qF -- "- \`$line\`" "$GITHUB_STEP_SUMMARY" || fail killed-parent "not in GITHUB_STEP_SUMMARY"
check_gone killed-parent "$dir"
sleeper=""

# (c) Two runs of one step at once in one base directory, as Debug and ReleaseSafe builds share a
# .zig-cache: two directories, two lines.
cases=$((cases + 1))
base="$work/base-c"
sleep 300 &
sleeper=$!
bash "$watch" start "$base" test $$
bash "$watch" start "$base" test "$sleeper"
sleep 1.5
line1=$(bash "$watch" report "$base" test "test one" $$)
line2=$(bash "$watch" report "$base" test "test two" "$sleeper")
kill -KILL "$sleeper" 2>/dev/null
wait "$sleeper" 2>/dev/null
[ -d "$base/$host-$$-test" ] && [ -d "$base/$host-$sleeper-test" ] ||
  fail concurrent "not two directories: $(ls "$base")"
printf '%s\n' "$line1" | grep -qE "^machine-watch test one: total=" || fail concurrent "first line: $line1"
printf '%s\n' "$line2" | grep -qE "^machine-watch test two: total=" || fail concurrent "second line: $line2"
case "$line1$line2" in *"no samples"*) fail concurrent "a run has no samples: $line1 / $line2" ;; esac
check_gone concurrent "$base/$host-$$-test"
check_gone concurrent "$base/$host-$sleeper-test"
sleeper=""

# (d) Failing probes: vm_stat and ps that fail, first on PATH. Exit 0, `unavailable` fields, and
# missing= names them.
cases=$((cases + 1))
base="$work/base-d"
mkdir -p "$work/stub"
printf '#!/bin/sh\nexit 1\n' >"$work/stub/vm_stat"
printf '#!/bin/sh\nexit 1\n' >"$work/stub/ps"
chmod +x "$work/stub/vm_stat" "$work/stub/ps"
status=0
PATH="$work/stub:$PATH" bash "$watch" start "$base" test $$ || status=$?
sleep 1.5
line=$(PATH="$work/stub:$PATH" bash "$watch" report "$base" test "test d" $$) || status=$?
[ "$status" -eq 0 ] || fail missing-probes "exited $status"
printf '%s\n' "$line" | grep -qE "$shape\$" || fail missing-probes "line has the wrong shape: $line"
case "$line" in *" top=unavailable "*) ;; *) fail missing-probes "top is not unavailable: $line" ;; esac
case "$line" in *" missing="*ps*) ;; *) fail missing-probes "missing= does not name ps: $line" ;; esac
if [ "$(uname)" = Darwin ]; then
  case "$line" in *" used_peak=unavailable "*" compressor_peak=unavailable "*) ;; *) fail missing-probes "vm_stat fields are not unavailable: $line" ;; esac
  case "$line" in *" missing="*vm_stat*) ;; *) fail missing-probes "missing= does not name vm_stat: $line" ;; esac
fi
check_gone missing-probes "$base/$host-$$-test"

# (e) report without start.
cases=$((cases + 1))
status=0
line=$(bash "$watch" report "$work/base-e" core "core x86_64-linux Debug" $$) || status=$?
[ "$status" -eq 0 ] || fail no-start "exited $status"
[ "$line" = "machine-watch core x86_64-linux Debug: no samples (start did not run)" ] || fail no-start "line: $line"
grep -qxF -- "- \`$line\`" "$GITHUB_STEP_SUMMARY" || fail no-start "not in GITHUB_STEP_SUMMARY"

# (f) An unwritable GITHUB_STEP_SUMMARY and bad usage.
cases=$((cases + 1))
status=0
GITHUB_STEP_SUMMARY="$work/no-such-dir/summary.md" bash "$watch" report "$work/base-f" test "test f" $$ >/dev/null 2>&1 ||
  status=$?
[ "$status" -eq 0 ] || fail unwritable-summary "exited $status"
status=0
bash "$watch" bogus >/dev/null 2>&1 || status=$?
[ "$status" -eq 2 ] || fail usage "an unknown subcommand exited $status, not 2"

# (g) An interval with a leading zero is a decimal number, not an octal error that kills the
# sampler.
cases=$((cases + 1))
base="$work/base-g"
MACHINE_WATCH_INTERVAL=08 bash "$watch" start "$base" test $$
dir="$base/$host-$$-test"
sleep 1.5
pid=$(cat "$dir/pid" 2>/dev/null || true)
{ [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; } ||
  fail leading-zero-interval "the sampler died: $(cat "$dir/sampler.log" 2>/dev/null)"
line=$(bash "$watch" report "$base" test "test g" $$)
printf '%s\n' "$line" | grep -qE "^machine-watch test g: $fields\$" || fail leading-zero-interval "line: $line"
[ -s "$dir/sampler.log" ] && fail leading-zero-interval "the sampler wrote errors: $(cat "$dir/sampler.log")"
check_gone leading-zero-interval "$dir"

# (h) ps fails for a while (as it may under memory pressure) while the watched process lives:
# the sampler goes on, and writes no ended=without-report line.
cases=$((cases + 1))
base="$work/base-h"
mkdir -p "$work/flaky"
real_ps=$(command -v ps)
printf '#!/bin/sh\n[ -e "%s" ] && exit 1\nexec "%s" "$@"\n' "$work/ps-fails" "$real_ps" >"$work/flaky/ps"
chmod +x "$work/flaky/ps"
PATH="$work/flaky:$PATH" bash "$watch" start "$base" test $$
dir="$base/$host-$$-test"
: >"$work/ps-fails"
sleep 2.5
rm -f "$work/ps-fails"
[ -e "$dir/summary" ] && fail ps-failure "the sampler took a ps failure for the end of the build: $(cat "$dir/summary")"
pid=$(cat "$dir/pid" 2>/dev/null || true)
{ [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; } || fail ps-failure "the sampler stopped when ps failed"
line=$(bash "$watch" report "$base" test "test h" $$)
case "$line" in *ended=without-report*) fail ps-failure "line: $line" ;; esac
check_gone ps-failure "$dir"

# (i) The watched process's whole process group is killed, as a harness or Ctrl-C does: the
# sampler is not in that group, so it still writes the ended=without-report line and exits.
cases=$((cases + 1))
base="$work/base-i"
set -m
# shellcheck disable=SC2016 # expanded by the inner shell
bash -c 'bash "$1" start "$2" test $$; exec sleep 300' sh "$watch" "$base" </dev/null &
leader=$!
set +m
sleeper=$leader
dir="$base/$host-$leader-test"
for _ in $(seq 1 30); do [ -s "$dir/pid" ] && break; sleep 0.1; done
sleep 0.5
kill -KILL -- -"$leader" 2>/dev/null
wait "$leader" 2>/dev/null
for _ in $(seq 1 30); do [ -e "$dir/done" ] && break; sleep 0.1; done
grep -q ' ended=without-report$' "$dir/summary" 2>/dev/null ||
  fail group-kill "no ended=without-report line within 3 s of the group kill: $(cat "$dir/sampler.log" 2>/dev/null)"
check_gone group-kill "$dir"
sleeper=""

# (j) With MACHINE_WATCH_IF_REQUESTED, start starts a sampler only for a watched process whose
# command line names the step, as `zig build test` names test and `zig build readme-check` does
# not.
cases=$((cases + 1))
base="$work/base-j"
# Shells whose command lines end in readme-check and in test; each records its sleep's PID so
# that both can be killed afterwards.
# shellcheck disable=SC2016 # a script for the inner shell
bash -c 'sleep 300 & echo $! >"$0"; wait' "$work/j-other.pid" readme-check </dev/null &
other=$!
# shellcheck disable=SC2016 # a script for the inner shell
bash -c 'sleep 300 & echo $! >"$0"; wait' "$work/j-asked.pid" test </dev/null &
asked=$!
for _ in $(seq 1 30); do [ -s "$work/j-other.pid" ] && [ -s "$work/j-asked.pid" ] && break; sleep 0.1; done
status=0
MACHINE_WATCH_IF_REQUESTED=1 bash "$watch" start "$base" test "$other" || status=$?
MACHINE_WATCH_IF_REQUESTED=1 bash "$watch" start "$base" test "$asked" || status=$?
[ "$status" -eq 0 ] || fail if-requested "start exited $status"
[ -e "$base/$host-$other-test" ] && fail if-requested "started for a command line without the step"
[ -s "$base/$host-$asked-test/pid" ] || fail if-requested "did not start for a command line naming the step"
line=$(bash "$watch" report "$base" test "test j" "$asked")
case "$line" in "machine-watch test j: total="*) ;; *) fail if-requested "line: $line" ;; esac
[ -d "$base/$host-$asked-test" ] && check_gone if-requested "$base/$host-$asked-test"
kill -KILL "$other" "$asked" "$(cat "$work/j-other.pid")" "$(cat "$work/j-asked.pid")" 2>/dev/null
wait "$other" "$asked" 2>/dev/null

if [ "$failures" -ne 0 ]; then
  echo "$name: $failures checks failed in $cases cases"
  exit 1
fi
