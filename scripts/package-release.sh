#!/usr/bin/env bash
# Packs the SDK's release tarball, temporal-sdk-zig-<version>.tar.gz: a `git archive` of HEAD
# holding build.zig.zon's .paths without the deps/sdk-core submodule, under one top-level
# directory that `zig fetch` strips. A build of the fetched package has no submodule, so it
# links the core bundle build.zig.zon pins for its target instead of running cargo.
# The archive depends only on HEAD's tree and commit time, so packing a commit again gives
# the same bytes.
#
# Usage: scripts/package-release.sh <version> <out dir>
#   <version>: the release version without a leading v (release-preflight.sh checks it)
# Prints the archive path, its SHA-256 and its zig hash (key: value lines).
set -euo pipefail
if [ $# -ne 2 ]; then
  echo "usage: $0 <version> <out dir>" >&2
  exit 2
fi
version="$1"
out="$2"
if [ -z "$version" ] || [[ "$version" == v* ]] || [[ "$version" == */* ]]; then
  echo "version '$version' must be a release version without a leading v or a slash, such as 0.1.0-alpha.1" >&2
  exit 2
fi
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

# build.zig.zon's .paths, one per line, minus the submodule.
paths=()
while IFS= read -r p; do paths+=("$p"); done < <(python3 -I - build.zig.zon <<'PY'
import re, sys
text = open(sys.argv[1]).read()
block = re.search(r"\.paths\s*=\s*\.\{(.*?)\}", text, re.S)
if not block:
    sys.exit("build.zig.zon has no .paths list")
for p in re.findall(r'"([^"]*)"', block.group(1)):
    if p != "deps/sdk-core":
        print(p)
PY
)
for p in "${paths[@]}"; do
  if ! git cat-file -e "HEAD:$p" 2>/dev/null; then
    echo "build.zig.zon lists $p in .paths, but HEAD has no such path; commit it or remove it from .paths" >&2
    exit 1
  fi
done
if [ -n "$(git status --porcelain -- "${paths[@]}")" ]; then
  echo "warning: uncommitted changes under .paths are not in the archive, which packs HEAD ($(git rev-parse --short HEAD))" >&2
fi

name="temporal-sdk-zig-$version"
mkdir -p "$out"
out="$(cd "$out" && pwd)"
archive="$out/$name.tar.gz"
git archive --format=tar.gz --prefix="$name/" -o "$archive" HEAD -- "${paths[@]}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
sha256="$(python3 -I -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$archive")"
# A throwaway global cache, as in package-core-bundle.sh: zig 0.17.1's `zig fetch` caches the
# tarball with its top-level directory still in it, which a later pinned build rejects.
zig_hash="$(cd "$work" && ZIG_GLOBAL_CACHE_DIR="$work/zig-global-cache" zig fetch "$archive")"
echo "archive: $archive"
echo "sha256: $sha256"
echo "zig-hash: $zig_hash"
