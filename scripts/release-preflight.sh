#!/usr/bin/env bash
# Checks that this checkout can be released as <version>, the checks release.yml's gate job
# runs before anything that needs GitHub (the tag, main's HEAD, CI results):
#   semver     <version> is a SemVer 2.0 version, without a leading v
#   zon        build.zig.zon's .version is <version>
#   changelog  CHANGELOG.md has a "## [<version>]" section with at least one entry
#   fragments  no changelog fragments remain under changelog/<category>/
#   core-pins  build.zig.zon pins a core bundle, core-<CORE_VERSION>-<target>, per tier-1 target
# Every failing check is reported, each named, and the script exits 1 if any failed.
#
# Usage: scripts/release-preflight.sh <version>
set -euo pipefail
if [ $# -ne 1 ]; then
  echo "usage: $0 <version>   (SemVer without a leading v, such as 0.1.0-alpha.1)" >&2
  exit 2
fi
version="$1"
root="$(cd "$(dirname "$0")/.." && pwd)"
# The core bundles core-bundles.yml builds; README.md and docs/releasing.md list the same tier 1.
tier1=(aarch64-macos x86_64-macos x86_64-linux-gnu aarch64-linux-gnu x86_64-linux-musl aarch64-linux-musl)

failed=0
fail() {
  failed=1
  if [ -n "${GITHUB_ACTIONS:-}" ]; then echo "::error::release check $1 failed: $2"; else echo "release check $1 failed: $2" >&2; fi
}

# The regular expression semver.org gives for SemVer 2.0, without its named groups.
num='(0|[1-9][0-9]*)'
ident='(0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*)'
semver="^$num\\.$num\\.$num(-$ident(\\.$ident)*)?(\\+[0-9a-zA-Z-]+(\\.[0-9a-zA-Z-]+)*)?\$"
if [[ "$version" == v* ]]; then
  fail semver "version '$version' starts with v; pass it without the v (the tag adds it): ${version#v}"
elif ! [[ "$version" =~ $semver ]]; then
  fail semver "version '$version' is not a SemVer 2.0 version (MAJOR.MINOR.PATCH, optionally -prerelease and +build), such as 0.1.0-alpha.1"
fi

zon="$root/build.zig.zon"
zon_version="$(sed -nE 's/^    \.version = "([^"]*)",$/\1/p' "$zon")"
if [ "$zon_version" != "$version" ]; then
  fail zon "build.zig.zon's .version is '$zon_version', not '$version'; set .version = \"$version\" in the release preparation commit"
fi

# The version's section, from its heading to the next release heading or the link list.
section="$(awk -v h="## [$version]" '
    substr($0, 1, length(h)) == h && (length($0) == length(h) || substr($0, length(h) + 1, 1) == " ") { on = 1; found = 1; next }
    on && (/^## \[/ || /^\[[^]]*\]: /) { exit }
    on { print }
    END { if (!found) print "missing" }' "$root/CHANGELOG.md")"
if [ "$section" = missing ]; then
  fail changelog "CHANGELOG.md has no '## [$version]' section; write it from the fragments with scripts/changelog-assemble.sh $version"
elif ! grep -q '^- ' <<<"$section"; then
  fail changelog "CHANGELOG.md's '## [$version]' section has no entries; a release lists at least one change"
fi

remaining="$(cd "$root" && { find changelog -type f ! -path changelog/README.md 2>/dev/null || true; } | sort)"
if [ -n "$remaining" ]; then
  fail fragments "changelog fragments remain: $(echo "$remaining" | tr '\n' ' ')- assemble them with scripts/changelog-assemble.sh, or, for a section that is already written, move their entries into it and delete them"
fi

core_version="$(tr -d '[:space:]' < "$root/CORE_VERSION")"
missing=()
for t in "${tier1[@]}"; do
  grep -qF ".@\"core-$core_version-$t\" = .{" "$zon" || missing+=("core-$core_version-$t")
done
if [ ${#missing[@]} -gt 0 ]; then
  fail core-pins "build.zig.zon doesn't pin ${missing[*]}; publish bundles for sdk-core $core_version with core-bundles.yml and add the entries its summary lists"
fi

if [ "$failed" -ne 0 ]; then exit 1; fi
echo "release checks passed for $version (sdk-core $core_version, ${#tier1[@]} core pins)"
