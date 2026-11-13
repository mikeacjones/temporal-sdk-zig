#!/usr/bin/env bash
# Tests the changelog scripts (changelog-check.sh, changelog-assemble.sh, changelog-draft.sh
# and release-preflight.sh's changelog checks) in throwaway git repositories, so it changes
# nothing in this checkout. Prints one line per failed case and exits 1 if any failed.
#
# Usage: scripts/changelog-test.sh
set -euo pipefail
src="$(cd "$(dirname "$0")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
boom=$'\xf0\x9f\x92\xa5'
failures=0
cases=0
repos=0

# A repository with the scripts, CHANGELOG.md's header, and changelog/README.md committed.
new_repo() {
  repos=$((repos + 1))
  repo="$tmp/repo$repos"
  mkdir -p "$repo/scripts" "$repo/changelog"
  cp "$src"/scripts/changelog-{check,assemble,draft}.sh "$src/scripts/release-preflight.sh" "$repo/scripts/"
  printf '.{\n    .version = "0.1.0",\n    .minimum_zig_version = "0.17.1",\n}\n' > "$repo/build.zig.zon"
  echo v1.2.0 > "$repo/CORE_VERSION"
  printf '# Changelog\n\nCompleted releases only.\n\n' > "$repo/CHANGELOG.md"
  echo "# Changelog fragments" > "$repo/changelog/README.md"
  git -C "$repo" init -q -b main
  git -C "$repo" -c user.name=t -c user.email=t@example.com add -A
  git -C "$repo" -c user.name=t -c user.email=t@example.com commit -q -m "Start"
  base="$(git -C "$repo" rev-parse HEAD)"
}
commit() { git -C "$repo" add -A && git -C "$repo" -c user.name=t -c user.email=t@example.com commit -q --allow-empty -m "$1"; }
fragment() { mkdir -p "$repo/changelog/$(dirname "$1")" && printf -- "$2" > "$repo/changelog/$1"; }

# expect <pass|fail> <name> <command...>: runs the command in the repository; for fail, the
# output must also contain $want.
want=""
expect() {
  local outcome="$1" name="$2" out status=0
  shift 2
  cases=$((cases + 1))
  out="$(cd "$repo" && env -u GITHUB_ACTIONS "$@" 2>&1)" || status=$?
  if [ "$outcome" = pass ] && [ "$status" -ne 0 ]; then
    echo "FAIL $name: expected success, got exit $status: $out"
    failures=$((failures + 1))
  elif [ "$outcome" = fail ] && [ "$status" -eq 0 ]; then
    echo "FAIL $name: expected failure, got success: $out"
    failures=$((failures + 1))
  elif [ -n "$want" ] && [[ "$out" != *"$want"* ]]; then
    echo "FAIL $name: output lacks '$want': $out"
    failures=$((failures + 1))
  fi
  want=""
}

# Fragment format.
new_repo
fragment added/new-thing.md 'Added `Client.thing`.\nAdded `Client.other`.\n'
expect pass "well-formed fragments" scripts/changelog-check.sh
fragment added/listed.md '- Added `x`.\n'; want="list marker"
expect fail "list marker" scripts/changelog-check.sh
rm "$repo/changelog/added/listed.md"
fragment fixed/blank.md 'Fixed `x`.\n\nFixed `y`.\n'; want="is blank"
expect fail "blank line" scripts/changelog-check.sh
rm "$repo/changelog/fixed/blank.md"
fragment removed/old.md 'Removed `x`.\n'; want="category"
expect fail "unknown category" scripts/changelog-check.sh
rm -r "$repo/changelog/removed"
fragment loose.md 'Added `x`.\n'; want="category"
expect fail "fragment outside a category" scripts/changelog-check.sh
rm "$repo/changelog/loose.md"
fragment added/Bad_Name.md 'Added `x`.\n'; want="kebab"
expect fail "name not kebab-case" scripts/changelog-check.sh
rm "$repo/changelog/added/Bad_Name.md"
fragment added/no-newline.md 'Added `x`.'; want="newline"
expect fail "no final newline" scripts/changelog-check.sh
rm "$repo/changelog/added/no-newline.md"
fragment added/heading.md '### Added\n'; want="heading"
expect fail "heading in a fragment" scripts/changelog-check.sh
rm "$repo/changelog/added/heading.md"
fragment added/indented.md '  Added `x`.\n'; want="whitespace"
expect fail "indented entry" scripts/changelog-check.sh
rm "$repo/changelog/added/indented.md"

# A pull request's commits.
new_repo
commit "Change something"
want="adds no changelog fragment"
expect fail "neither fragment nor trailer" scripts/changelog-check.sh "$base"
new_repo
fragment added/thing.md 'Added `thing`.\n'
commit "Add thing"
expect pass "fragment" scripts/changelog-check.sh "$base"
new_repo
commit $'Tune CI\n\nChangelog: none (CI configuration only)\nBead: x-1'
expect pass "Changelog: none with a reason" scripts/changelog-check.sh "$base" HEAD
new_repo
commit $'Tune CI\n\nChangelog: none'
want="give the reason"
expect fail "Changelog: none without a reason" scripts/changelog-check.sh "$base"
new_repo
commit $'Tune CI\n\nChangelog: skip'
want="only value"
expect fail "Changelog trailer other than none" scripts/changelog-check.sh "$base"
new_repo
fragment added/thing.md 'Added `thing`.\n'
commit "Add thing"
commit "Fix a typo in thing's test"
expect pass "fragment in an earlier commit of the range" scripts/changelog-check.sh "$base"
new_repo
git -C "$repo" rm -q changelog/README.md
printf '# Changelog\n\n## [0.1.0] - 2026-01-01\n\n- x\n' > "$repo/CHANGELOG.md"
commit "Prepare release 0.1.0"
expect pass "release preparation" scripts/changelog-check.sh "$base"

# Breaking changes.
new_repo
fragment breaking-changes/run.md '`Worker.run` returns poll failures. It logged them before. Handle the error.\n'
commit "$boom Return poll failures from Worker.run"
want="no commit carries a 'Breaking-Change"
expect fail "breaking fragment without trailer" scripts/changelog-check.sh "$base"
new_repo
fragment added/run.md 'Added `x`.\n'
commit $'\xf0\x9f\x92\xa5 Return poll failures from Worker.run\n\nBreaking-Change: Worker.run returns poll failures'
want="adds no changelog/breaking-changes/ fragment"
expect fail "trailer without breaking fragment" scripts/changelog-check.sh "$base"
new_repo
fragment breaking-changes/run.md '`Worker.run` returns poll failures.\n'
commit $'Return poll failures from Worker.run\n\nBreaking-Change: Worker.run returns poll failures'
want="subject starts with"
expect fail "trailer without the emoji subject" scripts/changelog-check.sh "$base"
new_repo
fragment breaking-changes/run.md '`Worker.run` returns poll failures.\n'
commit $'\xf0\x9f\x92\xa5 Return poll failures from Worker.run\n\nBreaking-Change: Worker.run returns poll failures'
expect pass "breaking change with fragment, trailer and subject" scripts/changelog-check.sh "$base"
new_repo
fragment breaking-changes/run.md '`Worker.run` returns poll failures.\n'
commit $'Merge bead/x-1: \xf0\x9f\x92\xa5 return poll failures from Worker.run\n\nBreaking-Change: Worker.run returns poll failures'
expect pass "breaking merge subject" scripts/changelog-check.sh "$base"

# Assembling a release.
new_repo
before="$(cat "$repo/CHANGELOG.md")"
want="no changelog fragments"
expect fail "assemble without fragments" scripts/changelog-assemble.sh 0.1.0 2026-01-02
cases=$((cases + 1))
if [ "$(cat "$repo/CHANGELOG.md")" != "$before" ]; then
  echo "FAIL assemble without fragments changed CHANGELOG.md"
  failures=$((failures + 1))
fi
fragment fixed/a.md 'Fixed `a`.\n'
fragment added/b.md 'Added `b`.\nAdded `c`.\n'
fragment added/a.md 'Added `a`.\n'
fragment breaking-changes/z.md '`z` is gone. It was there. Use `y`.\n'
fragment security/s.md 'Fixed `s`.\n'
expect pass "assemble" scripts/changelog-assemble.sh 0.1.0 2026-01-02
expected='# Changelog

Completed releases only.

## [0.1.0] - 2026-01-02

Requires Zig 0.17.1. Built against sdk-core v1.2.0.

### :boom: Breaking Changes

- `z` is gone. It was there. Use `y`.

### Added

- Added `a`.
- Added `b`.
- Added `c`.

### Fixed

- Fixed `a`.

### Security

- Fixed `s`.

[0.1.0]: https://github.com/mikeacjones/temporal-sdk-zig/releases/tag/v0.1.0'
cases=$((cases + 1))
if [ "$(cat "$repo/CHANGELOG.md")" != "$expected" ]; then
  echo "FAIL assemble wrote:"
  cat "$repo/CHANGELOG.md"
  failures=$((failures + 1))
fi
cases=$((cases + 1))
if [ -n "$(find "$repo/changelog" -type f ! -name README.md)" ]; then
  echo "FAIL assemble left fragments: $(find "$repo/changelog" -type f)"
  failures=$((failures + 1))
fi
expect pass "preflight changelog checks after assembling" bash -c 'scripts/release-preflight.sh 0.1.0 2>&1 | grep -v core-pins | grep -q "check \(changelog\|fragments\)" && exit 1 || exit 0'
fragment changed/d.md 'Changed `d`.\n'
want="already has"
expect fail "assemble a version twice" scripts/changelog-assemble.sh 0.1.0
want="fragments remain"
expect fail "preflight with fragments left" scripts/release-preflight.sh 0.1.0
expect pass "assemble the next release" scripts/changelog-assemble.sh 0.2.0 2026-02-03
cases=$((cases + 1))
if ! grep -q '^\[0.2.0\]: https://github.com/mikeacjones/temporal-sdk-zig/compare/v0.1.0...v0.2.0$' "$repo/CHANGELOG.md" ||
  [ "$(grep -n '^## \[' "$repo/CHANGELOG.md" | cut -d: -f2-)" != $'## [0.2.0] - 2026-02-03\n## [0.1.0] - 2026-01-02' ] ||
  [ "$(grep -n '^\[' "$repo/CHANGELOG.md" | cut -d: -f2- | cut -d']' -f1)" != $'[0.2.0\n[0.1.0' ]; then
  echo "FAIL the second release's section or link is misplaced:"
  cat "$repo/CHANGELOG.md"
  failures=$((failures + 1))
fi
want="no '## [0.3.0]' section"
expect fail "preflight without the section" scripts/release-preflight.sh 0.3.0
printf '\n## [0.3.0] - 2026-03-01\n\nRequires Zig 0.17.1.\n' > "$repo/CHANGELOG.new"
cat "$repo/CHANGELOG.md" >> "$repo/CHANGELOG.new"
mv "$repo/CHANGELOG.new" "$repo/CHANGELOG.md"
want="has no entries"
expect fail "preflight with an empty section" scripts/release-preflight.sh 0.3.0

# The cross-check of merged commits.
new_repo
fragment added/a.md 'Added `a`.\n'
commit "Add a"
commit "Change b without saying"
commit $'Tune CI\n\nChangelog: none (CI only)'
want="Change b without saying"
expect pass "draft lists the commit without a fragment" scripts/changelog-draft.sh "$base"
out="$(cd "$repo" && scripts/changelog-draft.sh "$base" 2>&1)"
cases=$((cases + 1))
if [[ "$out" == *"Add a"* ]] || [[ "$out" == *"Tune CI"* ]] || [[ "$out" != *"1 of the 3 commits"* ]]; then
  echo "FAIL draft listed covered commits: $out"
  failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
  echo "$failures of $cases changelog script cases failed" >&2
  exit 1
fi
echo "all $cases changelog script cases passed"
