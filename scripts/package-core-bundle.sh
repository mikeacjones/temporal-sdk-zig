#!/usr/bin/env bash
# Builds the sdk-core C bridge for a Zig target and packs it as a core bundle that
# `-Dcore-bundle=<dir>` and build.zig.zon's core-<CORE_VERSION>-<target> pins link instead of
# running cargo. The archive's one top-level directory holds lib/, include/ and manifest.json;
# `zig fetch` strips that directory, so an extracted bundle and a fetched one look the same.
# The tarball is deterministic (sorted entries, fixed times, numeric owner 0), so repacking the
# same build gives the same SHA-256 and zig hash.
#
# Usage: scripts/package-core-bundle.sh <zig target> <out dir>
#   <zig target>: aarch64-macos, x86_64-macos, x86_64-linux-gnu, aarch64-linux-gnu,
#                 x86_64-linux-musl or aarch64-linux-musl
# A musl target needs a musl C compiler for cc-rs in CC_<rust target>, as in ci.yml.
# Prints the archive path, its SHA-256 and its zig hash (key: value lines).
set -euo pipefail
if [ $# -ne 2 ]; then
  echo "usage: $0 <zig target> <out dir>" >&2
  exit 2
fi
target="$1"
out="$2"
root="$(cd "$(dirname "$0")/.." && pwd)"
version="$(tr -d '[:space:]' < "$root/CORE_VERSION")"

case "$target" in
  aarch64-macos) rust_target=aarch64-apple-darwin ;;
  x86_64-macos) rust_target=x86_64-apple-darwin ;;
  x86_64-linux-gnu) rust_target=x86_64-unknown-linux-gnu ;;
  aarch64-linux-gnu) rust_target=aarch64-unknown-linux-gnu ;;
  x86_64-linux-musl) rust_target=x86_64-unknown-linux-musl ;;
  aarch64-linux-musl) rust_target=aarch64-unknown-linux-musl ;;
  *)
    echo "no core bundle for target $target; use one of aarch64-macos, x86_64-macos, x86_64-linux-gnu, aarch64-linux-gnu, x86_64-linux-musl, aarch64-linux-musl" >&2
    exit 2
    ;;
esac
if [ ! -e "$root/deps/sdk-core/.git" ]; then
  echo "deps/sdk-core is missing. Run: git submodule update --init --depth 1" >&2
  exit 1
fi

name="temporal-core-$version-$target"
mkdir -p "$out"
out="$(cd "$out" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
stage="$work/$name"

(cd "$root" && zig build core -Dtarget="$target" --prefix "$stage")

core_commit="$(git -C "$root/deps/sdk-core" rev-parse HEAD)"
rustc_version="$(cd "$root/deps" && rustc --version)"
zig_version="$(zig version)"
cat > "$stage/manifest.json" <<EOF
{
  "core_version": "$version",
  "core_commit": "$core_commit",
  "toolchain": {
    "rustc": "$rustc_version",
    "zig": "$zig_version"
  },
  "targets": {
    "zig": "$target",
    "rust": "$rust_target"
  }
}
EOF

archive="$out/$name.tar.xz"
python3 -I - "$work" "$name" "$archive" <<'EOF'
import lzma, os, sys, tarfile

work, name, archive = sys.argv[1:]

def reset(info):
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    info.mtime = 0
    info.mode = 0o755 if info.isdir() else 0o644
    return info

paths = [name]
for dirpath, dirnames, filenames in os.walk(os.path.join(work, name)):
    rel = os.path.relpath(dirpath, work)
    paths += [os.path.join(rel, n) for n in dirnames + filenames]
with lzma.open(archive, "wb", preset=6) as xz, tarfile.open(fileobj=xz, mode="w", format=tarfile.PAX_FORMAT) as tar:
    for p in sorted(paths):
        tar.add(os.path.join(work, p), arcname=p, recursive=False, filter=reset)
EOF

sha256="$(python3 -I -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$archive")"
# A throwaway global cache: zig 0.17.1's `zig fetch` caches the package's tarball with the
# archive's top-level directory still in it, and a later build that pins the bundle then
# unpacks that copy and reports a hash mismatch.
zig_hash="$(cd "$work" && ZIG_GLOBAL_CACHE_DIR="$work/zig-global-cache" zig fetch "$archive")"
echo "archive: $archive"
echo "sha256: $sha256"
echo "zig-hash: $zig_hash"
