#!/usr/bin/env bash
# Tests scripts/run-with-hang-dump.sh: a command that exits within its limit, and one that
# times out while an unrelated process runs from a .zig-cache/o/ path. The timeout must dump and
# kill the command's tree, including a grandchild in a process group of its own, and leave the
# unrelated process alone. Stubs for sample, gdb and sudo log the PIDs they are given instead of
# attaching a debugger. Everything runs under a fresh temporary directory, so two runs at once
# don't meet. zig build test runs it.
#
# Usage: bash scripts/run-with-hang-dump-test.sh
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
name=run-with-hang-dump-test

skip() { echo "$name: skipped: $1"; exit 0; }
command -v ps >/dev/null || skip "ps is not installed"
probe_pgid=$(bash -c 'set -m; sleep 5 </dev/null & p=$!; set +m; g=$(ps -o pgid= -p "$p" 2>/dev/null | tr -d " "); kill "$p"; [ "$g" = "$p" ] && echo ok' 2>/dev/null || true)
[ "$probe_pgid" = ok ] || skip "ps or job control (set -m) does not work here"

work="$(mktemp -d)"
unrelated=""
cleanup() {
  if [ -n "$unrelated" ]; then kill -KILL "$unrelated" 2>/dev/null || true; fi
  rm -rf "$work"
}
trap cleanup EXIT
cases=0
failures=0

fail() { # <case> <what failed>
  echo "$name: $1: $2"
  failures=$((failures + 1))
}

# Stubs first on PATH: each logs the PID it would attach to.
mkdir -p "$work/bin"
cat >"$work/bin/sample" <<EOF
#!/bin/sh
echo "sample \$1" >>"$work/dump.log"
EOF
cat >"$work/bin/gdb" <<EOF
#!/bin/sh
while [ \$# -gt 0 ]; do
  if [ "\$1" = -p ]; then echo "gdb \$2" >>"$work/dump.log"; fi
  shift
done
EOF
cat >"$work/bin/sudo" <<'EOF'
#!/bin/sh
[ "$1" = -n ] && shift
exec "$@"
EOF
chmod +x "$work/bin/sample" "$work/bin/gdb" "$work/bin/sudo"
export PATH="$work/bin:$PATH"
: >"$work/dump.log"

# Sleeps whose args contain a .zig-cache/o/ path, as the script's dump looks for.
sleep_bin=$(command -v sleep)
mkdir -p "$work/.zig-cache/o/wrapped" "$work/.zig-cache/o/unrelated"
ln -s "$sleep_bin" "$work/.zig-cache/o/wrapped/sleep"
ln -s "$sleep_bin" "$work/.zig-cache/o/unrelated/sleep"

# (a) A command that exits within its limit: its status, and no dump.
cases=$((cases + 1))
status=0
"$root/scripts/run-with-hang-dump.sh" 30 sh -c 'exit 3' >"$work/a.out" 2>&1 || status=$?
[ "$status" -eq 3 ] || fail exit-status "expected exit 3, got $status: $(cat "$work/a.out")"
[ -s "$work/dump.log" ] && fail exit-status "dumped a command that exited: $(cat "$work/dump.log")"

# (b) A timeout, with an unrelated process running from a .zig-cache/o/ path.
cases=$((cases + 1))
"$work/.zig-cache/o/unrelated/sleep" 300 </dev/null &
unrelated=$!
disown "$unrelated"
status=0
# shellcheck disable=SC2016 # expanded by the wrapped shells
"$root/scripts/run-with-hang-dump.sh" 3 sh -c '
  "$1" 300 & echo $! >"$2"
  bash -c "set -m; \"\$0\" 300 </dev/null & echo \$! >\"\$1\"; wait" "$1" "$3" &
  wait' sh "$work/.zig-cache/o/wrapped/sleep" "$work/child.pid" "$work/grandchild.pid" \
  >"$work/b.out" 2>&1 || status=$?
[ "$status" -eq 124 ] || fail timeout "expected exit 124, got $status: $(cat "$work/b.out")"
child=$(cat "$work/child.pid" 2>/dev/null || true)
grandchild=$(cat "$work/grandchild.pid" 2>/dev/null || true)
if [ -z "$child" ] || [ -z "$grandchild" ]; then
  fail timeout "the wrapped command did not record its PIDs: $(cat "$work/b.out")"
else
  grep -qE "^(sample|gdb) $child\$" "$work/dump.log" ||
    fail timeout "the wrapped sleep $child was not dumped: $(cat "$work/dump.log")"
  grep -qE "^(sample|gdb) $grandchild\$" "$work/dump.log" ||
    fail timeout "the grandchild sleep $grandchild in its own group was not dumped: $(cat "$work/dump.log")"
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    kill -0 "$child" 2>/dev/null || kill -0 "$grandchild" 2>/dev/null || break
    sleep 0.5
  done
  kill -0 "$child" 2>/dev/null && fail timeout "the wrapped sleep $child is still running"
  kill -0 "$grandchild" 2>/dev/null && fail timeout "the grandchild sleep $grandchild is still running"
fi
grep -qE "^(sample|gdb) $unrelated\$" "$work/dump.log" &&
  fail timeout "the unrelated sleep $unrelated was dumped"
kill -0 "$unrelated" 2>/dev/null || fail timeout "the unrelated sleep $unrelated was killed"

# (c) A hangup sent to the script's process group, as a shell does to its jobs when its terminal
# closes. The command runs in a group of its own, so it no longer gets that SIGHUP itself: the
# script must kill the command's tree rather than die and leave it running with no time limit.
cases=$((cases + 1))
set -m
# shellcheck disable=SC2016 # expanded by the wrapped shell
"$root/scripts/run-with-hang-dump.sh" 60 sh -c '"$1" 300 & echo $! >"$2"; wait' \
  sh "$work/.zig-cache/o/wrapped/sleep" "$work/hup.pid" >"$work/c.out" 2>&1 &
script_pid=$!
set +m
for _ in $(seq 1 100); do [ -s "$work/hup.pid" ] && break; sleep 0.1; done
hup_child=$(cat "$work/hup.pid" 2>/dev/null || true)
kill -HUP -- -"$script_pid" 2>/dev/null || true
wait "$script_pid" 2>/dev/null || true
if [ -z "$hup_child" ]; then
  fail hangup "the wrapped command did not record its PID: $(cat "$work/c.out")"
else
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    kill -0 "$hup_child" 2>/dev/null || break
    sleep 0.5
  done
  if kill -0 "$hup_child" 2>/dev/null; then
    fail hangup "the wrapped sleep $hup_child still runs after SIGHUP to the script's group"
    kill -KILL "$hup_child" 2>/dev/null || true
  fi
fi

if [ "$failures" -ne 0 ]; then
  echo "$name: $failures checks failed in $cases cases"
  exit 1
fi
