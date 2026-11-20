#!/usr/bin/env bash
# Runs a command with a time limit, so a hang shows where it hangs (samples.yml runs
# `zig build samples-test` under it). If the limit passes, it prints the command's process tree
# and the thread stacks of every process in it that was built in .zig-cache (test binaries,
# sample workers, the samples runner), then kills the tree and fails.
#
# It is safe to run on a shared machine: it starts the command in a process group of its own,
# and dumps and signals only that group and the descendants of the command, never another
# process that happens to run from a .zig-cache.
#
# Usage: scripts/run-with-hang-dump.sh <seconds> <command> [args...]
# Exits with the command's status, 124 after a timeout, and 129 on SIGHUP, 130 on SIGINT or
# 143 on SIGTERM, after killing the command's tree. The command no longer shares the script's
# process group, so the script passes a terminal's hangup on to it rather than leave it running.
set -uo pipefail

limit=$1
shift
# With job control on, the background command leads a new process group whose ID is its PID.
# Its stdin is /dev/null, so a group in the background of a terminal never stops on SIGTTIN.
set -m
"$@" </dev/null &
cmd=$!
set +m

# Prints the PIDs of the tree: the command, every process in its group, and every descendant
# of those, from one snapshot of the process table taken before any signal is sent.
tree() {
  ps -A -o pid=,ppid=,pgid= 2>/dev/null | awk -v root="$cmd" '
    { pid[NR] = $1; ppid[NR] = $2; if ($1 == root || $3 == root) inside[$1] = 1 }
    END {
      inside[root] = 1
      do {
        grew = 0
        for (i = 1; i <= NR; i++)
          if ((ppid[i] in inside) && !(pid[i] in inside)) { inside[pid[i]] = 1; grew = 1 }
      } while (grew)
      for (p in inside) print p
    }'
}

# Prints "<pid> <start time>" for each PID given that is still running.
started() { # <PIDs>
  [ "$#" -gt 0 ] || return 0
  ps -o pid=,lstart= -p "$(echo "$@" | tr ' ' ',')" 2>/dev/null | awk '{ $1 = $1; print }' | sort
}

kill_tree() { # <PIDs of the tree>
  kill -KILL -- -"$cmd" 2>/dev/null
  local pid
  for pid in "$@"; do kill -KILL "$pid" 2>/dev/null; done
}

on_signal() { # <exit status>
  # shellcheck disable=SC2046 # one word per PID
  kill_tree $(tree)
  exit "$1"
}
trap 'on_signal 129' HUP
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

waited=0
while kill -0 "$cmd" 2>/dev/null; do
  if [ "$waited" -ge "$limit" ]; then
    pids=$(tree)
    # shellcheck disable=SC2086 # one word per PID
    pids_started=$(started $pids)
    echo "::error::'$*' still running after ${limit}s; dumping stacks"
    # shellcheck disable=SC2086 # one word per PID
    ps -o pid,ppid,etime,args -p "$(echo $pids | tr ' ' ',')"
    if [ "$(uname)" = Linux ] && ! command -v gdb >/dev/null; then
      sudo -n apt-get install -y -qq gdb >/dev/null 2>&1 || echo "gdb is not installed and could not be installed"
    fi
    for pid in $pids; do
      args=$(ps -o args= -p "$pid" 2>/dev/null) || continue
      case "$args" in *.zig-cache/o/*) ;; *) continue ;; esac
      echo "===== stacks of $pid: $(echo "$args" | cut -c1-200)"
      case "$(uname)" in
        Linux) sudo -n gdb -p "$pid" -batch -ex 'thread apply all bt' 2>&1 | grep -v '^\[New LWP' ;;
        Darwin) sample "$pid" 1 2>&1 ;;
      esac
    done
    # The dump can take a while, so the tree is taken again before the kill: a process of the
    # first snapshot is killed only if it is still in the tree or still has its start time, so
    # a PID the system reused for another process in the meantime is left alone.
    # shellcheck disable=SC2046,SC2086 # one word per PID
    kill_tree $({ tree; comm -12 <(started $pids) <(echo "$pids_started") | awk '{ print $1 }'; } | sort -u)
    exit 124
  fi
  sleep 1
  waited=$((waited + 1))
done
wait "$cmd"
