#!/usr/bin/env bash
# Writes the CHANGELOG.md section for <version> from the fragments under changelog/ and
# deletes the fragments it used. The section is "## [<version>] - <date>", a line naming the
# Zig version (build.zig.zon's minimum_zig_version) and the sdk-core release (CORE_VERSION),
# and one heading per category that has fragments, in this order:
#   breaking-changes  ### :boom: Breaking Changes
#   added             ### Added
#   stabilized        ### Stabilized
#   changed           ### Changed
#   deprecated        ### Deprecated
#   fixed             ### Fixed
#   security          ### Security
# Within a category, entries keep their order in each fragment, fragments sorted by name.
# The section goes above the newest release, and its link (a compare link from the previous
# release, or the release itself for the first) goes above the link list at the bottom.
# The script refuses to run when there are no fragments, when a fragment is malformed
# (scripts/changelog-check.sh), or when CHANGELOG.md already has the version, so running it
# twice changes nothing. It edits only the working tree; commit the result.
#
# Usage: scripts/changelog-assemble.sh <version> [<date>]
#   <version>: the release version without a leading v, such as 0.1.0-alpha.1
#   <date>:    the release date, YYYY-MM-DD (default: today, UTC)
set -euo pipefail
if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  echo "usage: $0 <version> [<date>]   (version without a leading v, such as 0.1.0-alpha.1; date as YYYY-MM-DD)" >&2
  exit 2
fi
version="$1"
date="${2:-$(date -u +%F)}"
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
repo_url="https://github.com/mikeacjones/temporal-sdk-zig"

if [ -z "$version" ] || [[ "$version" == v* ]] || ! [[ "$version" =~ ^[0-9A-Za-z.+-]+$ ]]; then
  echo "version '$version' must be a release version without a leading v, such as 0.1.0-alpha.1" >&2
  exit 2
fi
if ! [[ "$date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "date '$date' must be YYYY-MM-DD" >&2
  exit 2
fi
if grep -qF "## [$version]" CHANGELOG.md; then
  echo "CHANGELOG.md already has a '## [$version]' section; nothing to assemble (a released version's section isn't rewritten)" >&2
  exit 1
fi
"$root/scripts/changelog-check.sh" >/dev/null

order=(breaking-changes added stabilized changed deprecated fixed security)
declare -A heading=(
  [breaking-changes]=':boom: Breaking Changes' [added]=Added [stabilized]=Stabilized
  [changed]=Changed [deprecated]=Deprecated [fixed]=Fixed [security]=Security
)

zig="$(sed -nE 's/^    \.minimum_zig_version = "([^"]*)",$/\1/p' build.zig.zon)"
core="$(tr -d '[:space:]' < CORE_VERSION)"
if [ -z "$zig" ] || [ -z "$core" ]; then
  echo "couldn't read minimum_zig_version from build.zig.zon or the version in CORE_VERSION" >&2
  exit 1
fi

section="$(mktemp)"
trap 'rm -f "$section" "$section.new"' EXIT
used=()
{
  echo "## [$version] - $date"
  echo
  echo "Requires Zig $zig. Built against sdk-core $core."
  for c in "${order[@]}"; do
    [ -d "changelog/$c" ] || continue
    files=()
    while IFS= read -r -d '' f; do files+=("$f"); done < <(find "changelog/$c" -maxdepth 1 -type f -name '*.md' -print0 | sort -z)
    [ ${#files[@]} -gt 0 ] || continue
    echo
    echo "### ${heading[$c]}"
    echo
    for f in "${files[@]}"; do
      sed 's/^/- /' "$f"
      used+=("$f")
    done
  done
  echo
} > "$section"

if [ ${#used[@]} -eq 0 ]; then
  echo "no changelog fragments under changelog/<category>/; a release needs at least one entry (see changelog/README.md)" >&2
  exit 1
fi

previous="$(sed -nE 's/^## \[([^]]+)\].*/\1/p' CHANGELOG.md | head -1)"
if [ -n "$previous" ]; then
  link="[$version]: $repo_url/compare/v$previous...v$version"
else
  link="[$version]: $repo_url/releases/tag/v$version"
fi

# Section above the first release heading (or the link list, or the end); link above the
# first link.
awk -v section="$section" -v link="$link" '
  function put_section() { while ((getline l < section) > 0) print l; close(section); placed = 1 }
  !placed && /^## \[/ { put_section() }
  !placed && /^\[[^]]*\]: / { put_section() }
  !linked && /^\[[^]]*\]: / { print link; linked = 1 }
  { print; last = $0 }
  END {
    if (!placed) { if (last != "") print ""; put_section() }
    if (!linked) { print link }
  }' CHANGELOG.md > "$section.new"
mv "$section.new" CHANGELOG.md

rm -- "${used[@]}"
for c in "${order[@]}"; do rmdir "changelog/$c" 2>/dev/null || true; done
echo "wrote CHANGELOG.md's [$version] section from ${#used[@]} fragments and deleted them"
