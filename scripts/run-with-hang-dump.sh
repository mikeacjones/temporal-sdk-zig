#!/usr/bin/env bash
# Runs a command with a time limit. If the limit passes, prints the process tree and the thread
# stacks of every process built in .zig-cache (test binaries, sample workers, the samples
# runner), then kills the command and fails. CI uses it so a hang shows where it hangs.
#
# Usage: scripts/run-with-hang-dump.sh <seconds> <command> [args...]
set -uo pipefail

limit=$1
shift
"$@" &
cmd=$!

waited=0
while kill -0 "$cmd" 2>/dev/null; do
  if [ "$waited" -ge "$limit" ]; then
    echo "::error::'$*' still running after ${limit}s; dumping stacks"
    ps -eo pid,ppid,etime,args | grep -E '\.zig-cache|zig build|temporal' | grep -v grep
    for pid in $(pgrep -f '\.zig-cache/o/'); do
      echo "===== stacks of $pid: $(ps -o args= -p "$pid" | cut -c1-200)"
      case "$(uname)" in
        Linux) sudo -n gdb -p "$pid" -batch -ex 'thread apply all bt' 2>&1 | grep -v '^\[New LWP' ;;
        Darwin) sample "$pid" 1 2>&1 ;;
      esac
    done
    pkill -KILL -P "$cmd" 2>/dev/null
    kill -KILL "$cmd" 2>/dev/null
    pkill -KILL -f '\.zig-cache/o/' 2>/dev/null
    exit 124
  fi
  sleep 5
  waited=$((waited + 5))
done
wait "$cmd"
