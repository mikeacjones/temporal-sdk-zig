#!/usr/bin/env bash
# Samples the machine's memory, swap and load while a `zig build` runs, and prints one summary
# line at the end. build.zig runs `start` before every other step of `test` and `core`, and
# `report` after them (addMachineWatch), so CI's job logs and step summaries show the peaks of
# every run. It only measures: every path of start and report exits 0, and nothing it does
# changes, slows or fails the build.
#
# Usage:
#   scripts/machine-watch.sh start <base-dir> <step-name> [<watch-pid>]
#     Starts a sampler in <base-dir>/<host>-<watch-pid>-<step-name>. <watch-pid> (default: the
#     parent process, which is the `zig build` process for a Run step) is the process the
#     sampler watches; the sampler stops by itself once that process is gone. The label of a
#     line the sampler writes itself is $MACHINE_WATCH_LABEL (default: <step-name>). With
#     MACHINE_WATCH_IF_REQUESTED set, start does nothing unless <step-name> is one of the
#     watched process's arguments, so a command that runs start without asking for the step
#     (build.zig's shared steps make readme-check run test's start) gets no sampler.
#   scripts/machine-watch.sh report <base-dir> <step-name> <label> [<watch-pid>]
#     Stops that sampler (waiting at most 2 s), prints the summary line and appends it to
#     $GITHUB_STEP_SUMMARY when that is set.
#   scripts/machine-watch.sh sampler <dir> <watch-pid> <watch-lstart> <label>
#     Internal: the sampler loop that start runs in the background.
#
# The summary line:
#   machine-watch <label>: total=<G> used_peak=<G> used_peak_at=<s> swap_peak=<G>
#   compressor_peak=<G> memorystatus_min=<pct> load1_peak=<x.xx> samples=<n> span=<s>
#   started_after=<s> top=<name>:<G>,... missing=<probe,...|none>
# G is GiB with one decimal; used_peak_at and span are seconds from start; started_after is the
# seconds from the watched process's start to the first sample; top is the five processes with
# the most RSS at the used_peak sample. compressor_peak and memorystatus_min are macOS only (n/a
# on Linux). A probe that fails gives `unavailable` and is named in missing=. A line the sampler
# writes because the watched process ended before report ran ends with ` ended=without-report`.
#
# MACHINE_WATCH_INTERVAL (integer seconds, default 5) sets the time between samples.
set -u

usage() {
  cat >&2 <<'EOF'
usage: machine-watch.sh start <base-dir> <step-name> [<watch-pid>]
       machine-watch.sh report <base-dir> <step-name> <label> [<watch-pid>]
       machine-watch.sh sampler <dir> <watch-pid> <watch-lstart> <label>
EOF
  exit 2
}

script="${BASH_SOURCE[0]}"
case "$script" in /*) ;; *) script="$(pwd)/$script" ;; esac

# The run directory of a step of one zig build process on this host.
run_dir() { # <base-dir> <step-name> <watch-pid>
  printf '%s/%s-%s-%s' "$1" "$(uname -n 2>/dev/null || echo host)" "$3" "$2"
}

# Prints "<state> <lstart>" of a process, or nothing when it is gone or ps fails.
proc_info() { # <pid>
  LC_ALL=C ps -o stat=,lstart= -p "$1" 2>/dev/null | awk 'NF { $1 = $1; print; exit }'
}

# Epoch seconds of a `ps -o lstart=` time, or nothing.
lstart_epoch() { # <lstart>
  [ -n "$1" ] || return 0
  case "$(uname)" in
    Darwin) LC_ALL=C date -j -f '%a %b %e %T %Y' "$1" +%s 2>/dev/null ;;
    *) LC_ALL=C date -d "$1" +%s 2>/dev/null ;;
  esac
}

# Appends a line to $GITHUB_STEP_SUMMARY when it names a writable file.
to_step_summary() { # <line>
  [ -n "${GITHUB_STEP_SUMMARY:-}" ] || return 0
  # shellcheck disable=SC2016 # the backticks are Markdown
  { printf -- '- `%s`\n' "$1" >>"$GITHUB_STEP_SUMMARY"; } 2>/dev/null ||
    echo "machine-watch: cannot append to GITHUB_STEP_SUMMARY ($GITHUB_STEP_SUMMARY)" >&2
}

# Appends one sample line to <dir>/samples:
#   <epoch> <used> <swap> <compressor> <memorystatus> <load1> <total> <missing> <top>
# Sizes are bytes; top is up to five <name>:<rss KiB> pairs, comma-separated.
sample() { # <dir>
  local now used=unavailable swap=unavailable comp=n/a mslevel=n/a load1=unavailable
  local total=unavailable missing="" top=unavailable out
  now=$(date +%s)
  case "$(uname)" in
    Darwin)
      comp=unavailable
      mslevel=unavailable
      if out=$(vm_stat 2>/dev/null) && [ -n "$out" ]; then
        out=$(printf '%s\n' "$out" | awk '
          /page size of/ { for (i = 1; i <= NF; i++) if ($i == "of") ps = $(i + 1) }
          /^Pages active:/ { gsub(/\./, "", $NF); a = $NF }
          /^Pages wired down:/ { gsub(/\./, "", $NF); w = $NF }
          /^Pages occupied by compressor:/ { gsub(/\./, "", $NF); c = $NF }
          END { if (ps == "" || a == "" || w == "" || c == "") exit 1; printf "%.0f %.0f\n", (a + w + c) * ps, c * ps }')
      else
        false
      fi && [ -n "$out" ] && { used=${out% *}; comp=${out#* }; } || missing="$missing,vm_stat"
      if out=$(sysctl -n vm.swapusage 2>/dev/null) && [ -n "$out" ]; then
        out=$(printf '%s\n' "$out" | awk '{
          for (i = 1; i <= NF; i++) if ($i == "used" && $(i + 1) == "=") v = $(i + 2)
          if (v == "") exit 1
          u = substr(v, length(v)); n = v + 0
          m = (u == "K") ? 1024 : (u == "M") ? 1048576 : (u == "G") ? 1073741824 : (u == "T") ? 1099511627776 : 1
          printf "%.0f\n", n * m }')
      else
        false
      fi && [ -n "$out" ] && swap=$out || missing="$missing,swapusage"
      out=$(sysctl -n hw.memsize 2>/dev/null) && [ -n "$out" ] && total=$out || missing="$missing,memsize"
      out=$(sysctl -n kern.memorystatus_level 2>/dev/null) && [ -n "$out" ] && mslevel=$out ||
        missing="$missing,memorystatus"
      out=$(sysctl -n vm.loadavg 2>/dev/null | awk '{ for (i = 1; i <= NF; i++) if ($i ~ /^[0-9.]+$/) { print $i; exit } }') &&
        [ -n "$out" ] && load1=$out || missing="$missing,loadavg"
      out=$(ps -axo rss=,comm= 2>/dev/null) && [ -n "$out" ] || out=""
      ;;
    *)
      if [ -r /proc/meminfo ] && out=$(awk '
          $1 == "MemTotal:" { t = $2 } $1 == "MemAvailable:" { a = $2 }
          $1 == "SwapTotal:" { st = $2 } $1 == "SwapFree:" { sf = $2 }
          END { if (t == "" || a == "" || st == "" || sf == "") exit 1
                printf "%.0f %.0f %.0f\n", t * 1024, (t - a) * 1024, (st - sf) * 1024 }' /proc/meminfo 2>/dev/null) &&
        [ -n "$out" ]; then
        read -r total used swap <<<"$out"
      else
        missing="$missing,meminfo"
      fi
      out=$(awk '{ print $1; exit }' /proc/loadavg 2>/dev/null) && [ -n "$out" ] && load1=$out ||
        missing="$missing,loadavg"
      out=$(ps -eo rss=,comm= 2>/dev/null) && [ -n "$out" ] || out=""
      ;;
  esac
  if [ -n "$out" ]; then
    top=$(printf '%s\n' "$out" | awk '
      $1 ~ /^[0-9]+$/ {
        rss = $1; $1 = ""; name = $0; sub(/^ +/, "", name); sub(/.*\//, "", name)
        gsub(/[^A-Za-z0-9._+-]/, "_", name); if (name == "") name = "?"
        print rss, name
      }' | sort -rn | head -5 | awk '{ printf "%s%s:%s", (NR > 1 ? "," : ""), $2, $1 }')
    [ -n "$top" ] || top=unavailable
  fi
  [ "$top" = unavailable ] && missing="$missing,ps"
  missing=${missing#,}
  printf '%s %s %s %s %s %s %s %s %s\n' "$now" "$used" "$swap" "$comp" "$mslevel" "$load1" "$total" \
    "${missing:-none}" "$top" >>"$1/samples"
}

# Prints the summary line of a run directory, or nothing when it has no samples.
summary() { # <dir> <label>
  local dir=$1 label=$2 started=0 watch_epoch=""
  [ -s "$dir/samples" ] || return 0
  [ -r "$dir/started" ] && started=$(cat "$dir/started" 2>/dev/null)
  [ -r "$dir/watch" ] && watch_epoch=$(awk 'NR == 3 { print; exit }' "$dir/watch" 2>/dev/null)
  awk -v label="$label" -v started="${started:-0}" -v watch_epoch="$watch_epoch" '
    function g(bytes) { return sprintf("%.1f", bytes / 1073741824) }
    function num(v) { return v ~ /^[0-9]+(\.[0-9]+)?$/ }
    NF >= 9 {
      n++
      if (n == 1) first = $1
      last = $1
      if (num($2) && (used_peak == "" || $2 + 0 > used_peak + 0)) { used_peak = $2; used_at = $1; peak_top = $9 }
      if (num($3) && (swap_peak == "" || $3 + 0 > swap_peak + 0)) swap_peak = $3
      if ($4 == "n/a") comp_na = 1; else if (num($4) && (comp_peak == "" || $4 + 0 > comp_peak + 0)) comp_peak = $4
      if ($5 == "n/a") ms_na = 1; else if (num($5) && (ms_min == "" || $5 + 0 < ms_min + 0)) ms_min = $5
      if (num($6) && (load_peak == "" || $6 + 0 > load_peak + 0)) load_peak = $6
      if (num($7) && $7 + 0 > total + 0) total = $7
      last_top = $9
      if ($8 != "none") { k = split($8, m, ","); for (i = 1; i <= k; i++) if (!(m[i] in seen)) { seen[m[i]] = 1; missing = missing (missing == "" ? "" : ",") m[i] } }
    }
    END {
      if (n == 0) exit
      top = (peak_top != "") ? peak_top : last_top
      if (top != "unavailable") {
        k = split(top, t, ","); top = ""
        for (i = 1; i <= k; i++) { j = split(t[i], p, ":"); top = top (i > 1 ? "," : "") p[1] ":" g(p[j] * 1024) }
      }
      printf "machine-watch %s: total=%s used_peak=%s used_peak_at=%s swap_peak=%s compressor_peak=%s memorystatus_min=%s load1_peak=%s samples=%d span=%d started_after=%s top=%s missing=%s\n",
        label,
        (total != "" ? g(total) : "unavailable"),
        (used_peak != "" ? g(used_peak) : "unavailable"),
        (used_peak != "" ? used_at - started : "unavailable"),
        (swap_peak != "" ? g(swap_peak) : "unavailable"),
        (comp_na ? "n/a" : comp_peak != "" ? g(comp_peak) : "unavailable"),
        (ms_na ? "n/a" : ms_min != "" ? ms_min : "unavailable"),
        (load_peak != "" ? sprintf("%.2f", load_peak) : "unavailable"),
        n, last - started,
        (watch_epoch ~ /^[0-9]+$/ ? first - watch_epoch : "unavailable"),
        top, (missing == "" ? "none" : missing)
    }' "$dir/samples"
}

# Succeeds when the command line of <pid> has <word> as one of its arguments, or cannot be read.
# For a `zig build` process that is whether the step was asked for: `zig build test` names
# test, `zig build readme-check` does not. A value of an option that happens to equal the
# step's name (`--prefix test`) also counts; no caller does that.
requested() { # <pid> <word>
  local args
  if [ -r "/proc/$1/cmdline" ]; then
    args=$(tr '\0' '\n' <"/proc/$1/cmdline" 2>/dev/null)
  else
    args=$(ps -ww -o args= -p "$1" 2>/dev/null | tr ' ' '\n')
  fi
  [ -n "$args" ] || return 0
  printf '%s\n' "$args" | grep -qxF -- "$2"
}

cmd_start() { # <base-dir> <step-name> [<watch-pid>]
  [ $# -ge 2 ] && [ $# -le 3 ] || usage
  local pid=${3:-$PPID} dir info lstart="" epoch
  if [ -n "${MACHINE_WATCH_IF_REQUESTED:-}" ] && ! requested "$pid" "$2"; then
    return 0
  fi
  dir=$(run_dir "$1" "$2" "$pid")
  if ! mkdir -p "$dir" 2>/dev/null; then
    echo "machine-watch: cannot create $dir" >&2
    return 0
  fi
  rm -f "$dir/stop" "$dir/done" "$dir/samples" "$dir/summary" "$dir/pid" 2>/dev/null
  info=$(proc_info "$pid")
  [ -n "$info" ] && lstart=${info#* }
  epoch=$(lstart_epoch "$lstart")
  printf '%s\n%s\n%s\n' "$pid" "${lstart:-unknown}" "$epoch" >"$dir/watch" 2>/dev/null
  date +%s >"$dir/started" 2>/dev/null
  # Detached, with no stdio of the Run step, so nothing waiting for that pipe's EOF waits on it.
  # Job control puts it in a process group of its own, so a signal to the build's group (a
  # terminal's Ctrl-C, a harness that kills the group) leaves it to see the build gone and
  # write its ended=without-report line.
  # bash sets the group from both sides of the fork, and under load the forked child can lose
  # that race and print "child setpgid (N to N): Operation not permitted" before the command's
  # own redirections apply. The group's 2>/dev/null is inherited at the fork, so that message
  # never reaches the Run step's log.
  set -m
  {
    bash "$script" sampler "$dir" "$pid" "${lstart:-unknown}" "${MACHINE_WATCH_LABEL:-$2}" \
      </dev/null >>"$dir/sampler.log" 2>&1 &
  } 2>/dev/null
  echo $! >"$dir/pid" 2>/dev/null
  set +m
  disown 2>/dev/null
  return 0
}

cmd_report() { # <base-dir> <step-name> <label> [<watch-pid>]
  [ $# -ge 3 ] && [ $# -le 4 ] || usage
  local pid=${4:-$PPID} dir line
  dir=$(run_dir "$1" "$2" "$pid")
  if [ -d "$dir" ]; then
    touch "$dir/stop" 2>/dev/null || echo "machine-watch: cannot write $dir/stop" >&2
    for _ in $(seq 1 20); do
      [ -e "$dir/done" ] && break
      sleep 0.1
    done
    line=$(summary "$dir" "$3")
  fi
  [ -n "${line:-}" ] || line="machine-watch $3: no samples (start did not run)"
  printf '%s\n' "$line"
  to_step_summary "$line"
  return 0
}

cmd_sampler() { # <dir> <watch-pid> <watch-lstart> <label>
  [ $# -eq 4 ] || usage
  local dir=$1 pid=$2 lstart=$3 label=$4 interval=${MACHINE_WATCH_INTERVAL:-5} tick=0 info line
  case "$interval" in '' | *[!0-9]*) interval=5 ;; esac
  interval=$((10#$interval)) # 08 is eight, not a bad octal number
  [ "$interval" -ge 1 ] || interval=5
  sample "$dir"
  while :; do
    # Half-second ticks: the stop file is checked each tick, the watched process each second,
    # and a sample is taken every <interval> seconds.
    sleep 0.5
    tick=$((tick + 1))
    if [ -e "$dir/stop" ]; then
      [ -s "$dir/samples" ] || sample "$dir"
      break
    fi
    if [ $((tick % 2)) -eq 0 ]; then
      info=$(proc_info "$pid")
      if [ -n "$info" ]; then
        case "$info" in Z*) info="" ;; esac
        [ "$lstart" != unknown ] && [ "${info#* }" != "$lstart" ] && info=""
      elif kill -0 "$pid" 2>/dev/null; then
        # ps printed nothing for a process that still exists: ps failed (it may, under the
        # memory pressure this measures) or is missing. That is no sign the build is gone; the
        # next tick's ps checks the start time again.
        info=alive
      fi
      if [ -z "$info" ]; then
        line=$(summary "$dir" "$label")
        [ -n "$line" ] || line="machine-watch $label: no samples"
        line="$line ended=without-report"
        printf '%s\n' "$line" >"$dir/summary"
        to_step_summary "$line"
        break
      fi
    fi
    [ $((tick % (interval * 2))) -eq 0 ] && sample "$dir"
  done
  : >"$dir/done"
}

[ $# -ge 1 ] || usage
sub=$1
shift
case "$sub" in
  start) cmd_start "$@" ;;
  report) cmd_report "$@" ;;
  sampler) cmd_sampler "$@" ;;
  *) usage ;;
esac
