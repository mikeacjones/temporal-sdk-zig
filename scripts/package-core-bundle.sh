#!/usr/bin/env bash
# Builds the sdk-core C bridge for a Zig target and packs it as a core bundle that
# `-Dcore-bundle=<dir>` and build.zig.zon's core-<CORE_VERSION>-<target> pins link instead of
# running cargo. The archive's one top-level directory holds lib/, include/, manifest.json,
# sdk-core's LICENSE.txt and THIRD_PARTY_NOTICES.txt, which carries the license of every Rust
# crate linked into the library for that target; `zig fetch` strips that directory, so an
# extracted bundle and a fetched one look the same. The script fails if either notice is
# missing from the packed archive, and lists the archive's contents on stderr.
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

# sdk-core's MIT license asks that its notice go with every copy, and the library also holds
# the crates sdk-core depends on. Only crates linked into the library count: build scripts and
# proc-macros run on the build machine and are not part of it.
sdk_license="$root/deps/sdk-core/LICENSE.txt"
if [ ! -f "$sdk_license" ]; then
  echo "$sdk_license is missing: the bundle can't ship without sdk-core's license" >&2
  exit 1
fi
cp "$sdk_license" "$stage/LICENSE.txt"
(cd "$root/deps" && cargo tree --locked -p temporalio-sdk-core-c-bridge -e normal,no-proc-macro \
  --target "$rust_target" --prefix none --format '{p}') > "$work/crates.txt"
(cd "$root/deps" && cargo metadata --locked --format-version 1 --filter-platform "$rust_target") \
  > "$work/metadata.json"
python3 -I - "$work/crates.txt" "$work/metadata.json" "$root/deps/sdk-core" "$stage/THIRD_PARTY_NOTICES.txt" "$version" "$rust_target" <<'EOF'
import json, os, re, sys

crates_txt, metadata_json, sdk_core, notices, version, rust_target = sys.argv[1:]
linked = set()
for line in open(crates_txt):
    parts = line.split()
    if len(parts) >= 2 and parts[1].startswith("v"):
        linked.add((parts[0], parts[1][1:]))
packages = {(p["name"], p["version"]): p for p in json.load(open(metadata_json))["packages"]}
sdk_core = os.path.realpath(sdk_core)
license_name = re.compile(r"^(licen[cs]e|copying|notice|copyright|unlicense)", re.IGNORECASE)

out = [
    f"Third-party notices for the sdk-core {version} C bridge library built for {rust_target}.",
    "sdk-core itself is under LICENSE.txt beside this file. These are the licenses of the Rust",
    "crates linked into the library, each with the license files its package ships.",
    "",
]
unknown = []
for key in sorted(linked):
    package = packages.get(key)
    if package is None:
        unknown.append(f"{key[0]} {key[1]}: not in cargo metadata")
        continue
    directory = os.path.dirname(package["manifest_path"])
    if os.path.realpath(directory).startswith(sdk_core + os.sep):
        continue
    files = set()
    if package.get("license_file"):
        files.add(os.path.normpath(os.path.join(directory, package["license_file"])))
    for entry in os.listdir(directory):
        path = os.path.join(directory, entry)
        if license_name.match(entry) and os.path.isfile(path):
            files.add(path)
    expression = package.get("license") or ""
    if not expression and not files:
        unknown.append(f"{key[0]} {key[1]}: no license expression and no license file")
        continue
    out.append("=" * 78)
    out.append(f"{key[0]} {key[1]}")
    out.append(f"License: {expression or 'see the files below'}")
    if not files:
        out.append("Its package ships no license file; the license above is what it declares.")
    for path in sorted(files):
        out.append("-" * 78)
        out.append(os.path.basename(path))
        out.append("")
        out.append(open(path, encoding="utf-8", errors="replace").read().rstrip())
    out.append("")
# Apache-2.0 asks that recipients get a copy of the license, and some crates that declare it
# ship no license file. One copy, taken from a linked crate that ships it, covers them all.
apache = [line for line in out if line.startswith("License: ") and "Apache-2.0" in line]
if apache:
    text = None
    for key in sorted(linked):
        package = packages.get(key)
        if package is None:
            continue
        directory = os.path.dirname(package["manifest_path"])
        for entry in sorted(os.listdir(directory)):
            path = os.path.join(directory, entry)
            if re.match(r"^licen[cs]e[-_.]?apache", entry, re.IGNORECASE) and os.path.isfile(path):
                body = open(path, encoding="utf-8", errors="replace").read()
                if "Apache License" in body and "Version 2.0" in body:
                    text = body.rstrip()
                    break
        if text:
            break
    if text is None:
        unknown.append("crates declare Apache-2.0, and no linked crate ships the license's text")
    else:
        out += ["=" * 78, "The Apache License, Version 2.0, for every crate above that declares it", "", text, ""]
if unknown:
    sys.exit("can't write the third-party notices:\n  " + "\n  ".join(unknown))
open(notices, "w").write("\n".join(out) + "\n")
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

# The archive must hold the library, its header and both notices. Its contents go to stderr:
# stdout is the key: value lines core-bundles.yml reads.
python3 -I - "$archive" "$name" <<'EOF'
import sys, tarfile

archive, name = sys.argv[1:]
with tarfile.open(archive) as tar:
    members = tar.getnames()
for member in members:
    print(f"  {member}", file=sys.stderr)
needed = ["lib/libtemporalio_sdk_core_c_bridge.a", "include/temporal-sdk-core-c-bridge.h",
          "manifest.json", "LICENSE.txt", "THIRD_PARTY_NOTICES.txt"]
missing = [n for n in needed if f"{name}/{n}" not in members]
if missing:
    sys.exit(f"{archive} lacks {', '.join(missing)}")
EOF

sha256="$(python3 -I -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$archive")"
# A throwaway global cache: zig 0.17.1's `zig fetch` caches the package's tarball with the
# archive's top-level directory still in it, and a later build that pins the bundle then
# unpacks that copy and reports a hash mismatch.
zig_hash="$(cd "$work" && ZIG_GLOBAL_CACHE_DIR="$work/zig-global-cache" zig fetch "$archive")"
echo "archive: $archive"
echo "sha256: $sha256"
echo "zig-hash: $zig_hash"
