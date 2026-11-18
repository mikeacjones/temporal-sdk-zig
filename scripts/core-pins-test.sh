#!/usr/bin/env bash
# Tests scripts/core-pins.sh on manifests written here: a valid one, and pins that are
# commented out, under the wrong key, or missing their URL or hash. The broken pin is for
# x86_64-linux-musl, a target the release smoke test doesn't build.
#
# Usage: scripts/core-pins-test.sh
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
v=v9.9.9
targets=(aarch64-macos x86_64-linux-musl)
cases=0
failures=0

pin() { # <target> [url override] [hash override]
  local url="${2-https://example.com/releases/download/core-$v-r1/temporal-core-$v-$1.tar.xz}"
  local hash="${3-N-V-__8AAexample$1}"
  printf '        .@"core-%s-%s" = .{\n' "$v" "$1"
  [ -n "$url" ] && printf '            .url = "%s",\n' "$url"
  [ -n "$hash" ] && printf '            .hash = "%s",\n' "$hash"
  printf '            .lazy = true,\n        },\n'
}

# A command substitution drops the trailing newline, so pins are joined with one.
pins() { local t; for t in "$@"; do pin "$t"; echo; done; }

manifest() { # <name> <dependencies body> [text after .dependencies]
  printf '.{\n    .name = .temporal,\n    .version = "0.0.0",\n    // A comment with "quotes" and .{ braces }.\n    .dependencies = .{\n%s\n    },\n%s    .paths = .{ "build.zig", "src" },\n}\n' "$2" "${3-}" > "$work/$1.zon"
}

expect() { # pass|fail <name> [text the output must contain]
  cases=$((cases + 1))
  local out status=0
  out="$("$root/scripts/core-pins.sh" "$work/$2.zon" "$v" "${targets[@]}" 2>&1)" || status=$?
  if { [ "$1" = pass ] && [ "$status" -ne 0 ]; } || { [ "$1" = fail ] && [ "$status" -ne 1 ]; } ||
    { [ -n "${3-}" ] && ! grep -qF -- "$3" <<<"$out"; }; then
    local wanted=""
    [ -n "${3-}" ] && wanted=" with: $3"
    echo "FAIL $2: expected $1$wanted, got exit $status:"
    echo "$out"
    failures=$((failures + 1))
  fi
}

manifest valid "$(pins aarch64-macos x86_64-linux-musl)"
expect pass valid

manifest commented "$(pins aarch64-macos)
$(pin x86_64-linux-musl | sed 's|^|// |')"
expect fail commented "core-$v-x86_64-linux-musl: not an entry under .dependencies"

manifest wrong-key "$(pins aarch64-macos)" "    .bundles = .{
$(pins x86_64-linux-musl)
    },
"
expect fail wrong-key "core-$v-x86_64-linux-musl: not an entry under .dependencies"

manifest no-hash "$(pins aarch64-macos)
$(pin x86_64-linux-musl "" "")"
expect fail no-hash "core-$v-x86_64-linux-musl: it has no .hash"

manifest wrong-url "$(pins aarch64-macos)
$(pin x86_64-linux-musl https://example.com/temporal-core-$v-aarch64-macos.tar.xz)"
expect fail wrong-url "core-$v-x86_64-linux-musl: its .url doesn't point at temporal-core-$v-x86_64-linux-musl.tar.xz"

manifest only-comments "// .@\"core-$v-aarch64-macos\" = .{ .url = \"x\", .hash = \"y\" },
"
expect fail only-comments "core-$v-aarch64-macos: not an entry under .dependencies"

# A manifest cut short fails with a reason, not a crash.
printf '.{\n    .dependencies = .{\n        .a = .{ .url = "x",\n' > "$work/cut.zon"
cases=$((cases + 1))
status=0
"$root/scripts/core-pins.sh" "$work/cut.zon" "$v" aarch64-macos > "$work/cut.txt" 2>&1 || status=$?
if [ "$status" -eq 0 ] || ! grep -q "never closed" "$work/cut.txt"; then
  echo "FAIL a cut manifest: exit $status"
  cat "$work/cut.txt"
  failures=$((failures + 1))
fi

# The repository's own manifest can be read.
cases=$((cases + 1))
status=0
"$root/scripts/core-pins.sh" "$root/build.zig.zon" "$v" aarch64-macos > "$work/own.txt" 2>&1 || status=$?
if [ "$status" -gt 1 ] || ! grep -q "not an entry under .dependencies" "$work/own.txt"; then
  echo "FAIL reading build.zig.zon: exit $status"
  cat "$work/own.txt"
  failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
  echo "core-pins-test: $failures of $cases cases failed"
  exit 1
fi
echo "core-pins-test: $cases cases passed"
