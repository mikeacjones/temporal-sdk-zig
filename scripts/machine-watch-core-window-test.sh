#!/usr/bin/env bash
# Tests that `zig build core` samples the machine while cargo builds the core, which is the step
# CI lost its runner in. The machine watch's start step must have run, and so have written
# <dir>/started, before cargo finishes; a sampler that starts only after cargo (or never, when
# cargo stalls) measures nothing of the build.
#
# A stand-in cargo, first in PATH, waits up to 10 s for the start step of its own `zig build`
# (the run directory <cache>/machine-watch/<host>-<its parent PID>-core) to write `started`,
# records what it saw, and fails the build. Which of the build's initial steps the build runner
# gets to first varies from run to run (and with --seed), so the case runs 40 times with -j3, the
# job count of CI's 4-CPU x86_64-macos runner, and fails on the first run in which the watch had
# not started while cargo ran.
#
# Usage: bash scripts/machine-watch-core-window-test.sh
set -uo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
name=machine-watch-core-window-test

skip() { echo "$name: skipped: $1"; exit 0; }
command -v zig >/dev/null || skip "zig is not installed"
[ -e "$root/deps/sdk-core/Cargo.toml" ] || skip "no deps/sdk-core submodule, so zig build core runs no cargo"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/bin"
cat >"$work/bin/cargo" <<'EOF'
#!/usr/bin/env bash
cache=""
for a in "$@"; do
  case "$a" in --target-dir=*) cache=${a#--target-dir=}; cache=${cache%/cargo-target} ;; esac
done
dir="$cache/machine-watch/$(uname -n)-$PPID-core"
echo "$dir" >"$MACHINE_WATCH_WINDOW_DIR"
for _ in $(seq 1 100); do
  if [ -e "$dir/started" ]; then
    echo started >"$MACHINE_WATCH_WINDOW_RESULT"
    exit 1
  fi
  sleep 0.1
done
echo "not-started" >"$MACHINE_WATCH_WINDOW_RESULT"
exit 1
EOF
chmod +x "$work/bin/cargo"

cd "$root" || exit 1
failures=0
for seed in $(seq 1 40); do
  rm -f "$work/result" "$work/dir"
  MACHINE_WATCH_WINDOW_RESULT="$work/result" MACHINE_WATCH_WINDOW_DIR="$work/dir" PATH="$work/bin:$PATH" \
    zig build core -j3 --seed="$seed" >"$work/out" 2>&1
  result=$(cat "$work/result" 2>/dev/null || echo "cargo-not-run")
  dir=$(cat "$work/dir" 2>/dev/null || true)
  # The build failed at cargo, so its sampler ends by itself; wait for it so nothing is left.
  if [ -n "$dir" ]; then
    for _ in $(seq 1 50); do
      { [ ! -e "$dir/pid" ] || [ -e "$dir/done" ]; } && break
      sleep 0.1
    done
  fi
  case "$result" in
    started) ;;
    not-started)
      echo "$name: zig build core -j3 --seed=$seed: cargo ran for 10 s and the machine watch had not started"
      failures=$((failures + 1))
      break
      ;;
    *)
      echo "$name: zig build core -j3 --seed=$seed did not run the stand-in cargo:"
      cat "$work/out"
      failures=$((failures + 1))
      break
      ;;
  esac
done

if [ "$failures" -ne 0 ]; then
  echo "$name: $failures checks failed"
  exit 1
fi
echo "$name: passed"
