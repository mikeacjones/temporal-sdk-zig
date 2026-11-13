#!/usr/bin/env bash
# Builds and runs tools/release-smoke, a project that depends on the SDK through a release
# tarball from package-release.sh, the way a user's project does: `zig fetch --save` adds the
# tarball, and the build runs with every directory holding cargo removed from PATH. The
# program connects through the core (see tools/release-smoke/main.zig). On Linux gnu it also
# prints the newest glibc symbol version the program needs.
#
# Usage: scripts/smoke-release.sh <release tarball> [<core bundle dir>]
#   <core bundle dir>: an extracted core bundle, passed to the SDK as -Dcore-bundle; without
#                      it the SDK links the core bundle its build.zig.zon pins
# Extra zig build arguments (such as -Doptimize=ReleaseSafe) go in SMOKE_BUILD_ARGS.
set -euo pipefail
if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  echo "usage: $0 <release tarball> [<core bundle dir>]" >&2
  exit 2
fi
root="$(cd "$(dirname "$0")/.." && pwd)"
tarball="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
[ -f "$tarball" ] || { echo "no release tarball at $1; make one with scripts/package-release.sh" >&2; exit 2; }
bundle_args=()
if [ $# -eq 2 ]; then
  [ -d "$2" ] || { echo "core bundle directory $2 doesn't exist; extract a bundle from scripts/package-core-bundle.sh" >&2; exit 2; }
  bundle_args=("-Dcore-bundle=$(cd "$2" && pwd)")
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cp -R "$root/tools/release-smoke" "$work/consumer"
cd "$work/consumer"

no_cargo_path=""
IFS=: read -ra dirs <<< "$PATH"
for d in "${dirs[@]}"; do
  [ -n "$d" ] && [ -x "$d/cargo" ] && continue
  no_cargo_path="${no_cargo_path:+$no_cargo_path:}$d"
done
if PATH="$no_cargo_path" command -v cargo >/dev/null; then
  echo "cargo is still on PATH at $(PATH="$no_cargo_path" command -v cargo); the smoke test must not be able to run it" >&2
  exit 1
fi

zig fetch --save=temporal "file://$tarball"
# shellcheck disable=SC2086 # SMOKE_BUILD_ARGS is a list of arguments.
PATH="$no_cargo_path" zig build install run ${bundle_args[@]+"${bundle_args[@]}"} ${SMOKE_BUILD_ARGS:-} --summary line

# A static (musl) program has no GLIBC_ versions to print.
bin=zig-out/bin/release-smoke
if [ "$(uname -s)" = Linux ] && objdump -p "$bin" | grep -q 'NEEDED.*libc\.so'; then
  echo "glibc: $(objdump -T "$bin" | grep -o 'GLIBC_[0-9.]*' | sort -V | tail -1)"
fi
