#!/usr/bin/env bash
# Fails if the deps/sdk-core submodule is not checked out at the tag in CORE_VERSION.
# Usage: scripts/check-core-version.sh
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
want="$(tr -d '[:space:]' < "$root/CORE_VERSION")"

if [ ! -e "$root/deps/sdk-core/.git" ]; then
  echo "deps/sdk-core is missing. Run: git submodule update --init --depth 1" >&2
  exit 1
fi

want_sha="$(git -C "$root/deps/sdk-core" rev-parse --verify --quiet "$want^{commit}" || true)"
if [ -z "$want_sha" ]; then
  git -C "$root/deps/sdk-core" fetch --quiet --depth 1 origin tag "$want"
  want_sha="$(git -C "$root/deps/sdk-core" rev-parse "$want^{commit}")"
fi
have_sha="$(git -C "$root/deps/sdk-core" rev-parse HEAD)"

if [ "$want_sha" != "$have_sha" ]; then
  echo "deps/sdk-core is at $have_sha, but CORE_VERSION says $want ($want_sha)." >&2
  exit 1
fi
echo "deps/sdk-core matches CORE_VERSION ($want, ${have_sha:0:7})"

# deps/Cargo.toml mirrors sdk-core's [workspace.*] tables because member crates inherit
# them; deps/rust-toolchain.toml mirrors sdk-core's toolchain. Both must stay in sync.
tables() { sed -n '/^\[workspace.dependencies\]/,$p' "$1"; }
if ! diff <(tables "$root/deps/sdk-core/Cargo.toml") <(tables "$root/deps/Cargo.toml") >/dev/null; then
  echo "deps/Cargo.toml [workspace.*] tables differ from deps/sdk-core/Cargo.toml:" >&2
  diff <(tables "$root/deps/sdk-core/Cargo.toml") <(tables "$root/deps/Cargo.toml") >&2 || true
  exit 1
fi
channel() { sed -n 's/^channel *= *"\(.*\)"/\1/p' "$1"; }
if [ "$(channel "$root/deps/sdk-core/rust-toolchain.toml")" != "$(channel "$root/deps/rust-toolchain.toml")" ]; then
  echo "deps/rust-toolchain.toml channel differs from deps/sdk-core/rust-toolchain.toml" >&2
  exit 1
fi
echo "deps/Cargo.toml and deps/rust-toolchain.toml match sdk-core"

"$root/scripts/list-protos.sh" --check
