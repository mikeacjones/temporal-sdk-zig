#!/usr/bin/env bash
# Rewrites src/bridge_abi.txt from the current C bridge header, after you have reviewed the
# header changes (scripts/core-header-diff.sh) and updated the bridge wrappers.
# Usage: scripts/update-abi-snapshot.sh
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
: > src/bridge_abi.txt
# With an empty snapshot the ABI test fails and prints the full current layout table.
out="$(zig build test 2>&1 || true)"
printf '%s\n' "$out" | sed -n 's/^ *\(TemporalCore[A-Za-z0-9_]* size=.*\)$/\1/p' > src/bridge_abi.txt
if [ ! -s src/bridge_abi.txt ]; then
  echo "could not read the layout table from the test output:" >&2
  printf '%s\n' "$out" | tail -20 >&2
  exit 1
fi
zig build test
echo "wrote src/bridge_abi.txt ($(wc -l < src/bridge_abi.txt | tr -d ' ') types)"
