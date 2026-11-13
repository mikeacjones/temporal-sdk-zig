#!/usr/bin/env bash
# Checks the changelog fragments under changelog/ (changelog/README.md has the rules).
#
# With no arguments it checks the fragments in the working tree: each file is
# changelog/<category>/<kebab-name>.md with a known category, and holds one entry per line,
# with no list marker, no blank or indented lines, and a final newline.
#
# With <base> [<head>] (default HEAD) it also checks the commits in <base>..<head>, as for a
# pull request:
#   - they add a fragment, or one of them carries a "Changelog: none (<reason>)" trailer,
#     or one of them is a "Prepare release <version>" commit;
#   - a commit with a "Breaking-Change:" trailer starts its subject with the collision
#     emoji (after "Merge bead/<id>: " for a merge), and the range adds a breaking-changes/
#     fragment exactly when one of its commits carries that trailer.
# Every failure is reported, and the script exits 1 if any check failed.
#
# Usage: scripts/changelog-check.sh [<base> [<head>]]
# scripts/changelog-test.sh tests this script.
set -euo pipefail
if [ $# -gt 2 ]; then
  echo "usage: $0 [<base> [<head>]]" >&2
  exit 2
fi
root="$(git rev-parse --show-toplevel)"
cd "$root"
categories="breaking-changes added stabilized changed deprecated fixed security"
boom=$'\xf0\x9f\x92\xa5'

failed=0
fail() {
  failed=1
  if [ -n "${GITHUB_ACTIONS:-}" ]; then echo "::error::changelog check failed: $1"; else echo "changelog check failed: $1" >&2; fi
}

is_category() {
  local c
  for c in $categories; do [ "$c" = "$1" ] && return 0; done
  return 1
}

# The fragment files in the working tree, tracked or not.
if [ -d changelog ]; then
  while IFS= read -r -d '' f; do
    f="${f#./}"
    [ "$f" = changelog/README.md ] && continue
    rel="${f#changelog/}"
    category="${rel%%/*}"
    name="${rel#*/}"
    if [ "$category" = "$rel" ] || ! is_category "$category"; then
      fail "$f is not under a category directory; move it to changelog/<category>/, where <category> is one of: $categories"
      continue
    fi
    if ! [[ "$name" =~ ^[a-z0-9]+(-[a-z0-9]+)*\.md$ ]]; then
      fail "$f: name the fragment changelog/$category/<short-kebab-name>.md (lower-case letters, digits and single hyphens), in the category directory itself"
      continue
    fi
    if [ ! -s "$f" ]; then
      fail "$f is empty; write one entry per line, or delete the file"
      continue
    fi
    if [ "$(tail -c1 "$f" | od -An -c | tr -d ' ')" != '\n' ]; then
      fail "$f doesn't end with a newline; end each entry's line with one"
    fi
    n=0
    while IFS= read -r line || [ -n "$line" ]; do
      n=$((n + 1))
      if [ -z "$line" ] || [[ "$line" =~ ^[[:space:]]*$ ]]; then
        fail "$f:$n is blank; an entry is one line, and entries follow each other without blank lines"
      elif [[ "$line" =~ ^[[:space:]] ]] || [[ "$line" =~ [[:space:]]$ ]]; then
        fail "$f:$n starts or ends with whitespace; an entry is one unindented line"
      elif [[ "$line" =~ ^([-*+]|[0-9]+[.\)])[[:space:]] ]]; then
        fail "$f:$n starts with a list marker; write the entry without it (the release notes add it)"
      elif [[ "$line" =~ ^# ]]; then
        fail "$f:$n is a heading; the release notes add the category headings, so write only the entry"
      fi
    done < "$f"
  done < <(find changelog -type f -print0 | sort -z)
fi

if [ $# -ge 1 ]; then
  base="$1"
  head="${2:-HEAD}"
  for ref in "$base" "$head"; do
    if ! git rev-parse --verify --quiet "$ref^{commit}" >/dev/null; then
      echo "$ref is not a commit in this repository; fetch the full history (fetch-depth: 0) or pass another ref" >&2
      exit 2
    fi
  done
  range="$base..$head"
  # Fragments the range adds, against the merge base, as a pull request's diff shows them.
  added="$(git diff --no-renames --diff-filter=A --name-only "$base...$head" -- changelog | grep -v '^changelog/README\.md$' || true)"
  added_breaking="$(printf '%s\n' "$added" | grep '^changelog/breaking-changes/' || true)"

  none=0
  prepare=0
  breaking=0
  while IFS= read -r commit; do
    [ -n "$commit" ] || continue
    subject="$(git log -1 --format=%s "$commit")"
    short="$(git log -1 --format=%h "$commit")"
    if [[ "$subject" =~ ^Prepare\ release\ [0-9] ]]; then prepare=1; fi
    while IFS= read -r value; do
      [ -n "$value" ] || continue
      if [[ "$value" =~ ^none([^[:alnum:]].*)?$ ]]; then
        reason="$(printf '%s' "${value#none}" | tr -d '[:punct:][:space:]')"
        if [ -z "$reason" ]; then
          fail "$short ($subject): its Changelog trailer is '$value'; give the reason after it, such as 'Changelog: none (CI configuration only)'"
        else
          none=1
        fi
      else
        fail "$short ($subject): its Changelog trailer is '$value'; the only value is 'none (<reason>)', and a user-visible change adds a fragment under changelog/ instead"
      fi
    done < <(git log -1 --format='%(trailers:key=Changelog,valueonly,unfold)' "$commit")
    if [ -n "$(git log -1 --format='%(trailers:key=Breaking-Change,valueonly)' "$commit")" ]; then
      breaking=1
      rest="${subject#Merge bead/*: }"
      if [[ "$rest" != "$boom "* ]]; then
        fail "$short ($subject): it carries a Breaking-Change trailer, so its subject starts with '$boom ' (after 'Merge bead/<id>: ' for a merge)"
      fi
    fi
  done < <(git rev-list "$range")

  if [ -z "$(git rev-list "$range")" ]; then
    echo "no commits in $range"
  elif [ -z "$added" ] && [ "$none" -eq 0 ] && [ "$prepare" -eq 0 ]; then
    fail "$range adds no changelog fragment and no commit carries a 'Changelog: none (<reason>)' trailer; add changelog/<category>/<name>.md for a user-visible change, or the trailer to the commit message when users can't observe it"
  fi
  if [ "$breaking" -eq 1 ] && [ -z "$added_breaking" ]; then
    fail "a commit in $range carries a Breaking-Change trailer, but the range adds no changelog/breaking-changes/ fragment; add one that says what changed, what it did before, and what to do"
  fi
  if [ "$breaking" -eq 0 ] && [ -n "$added_breaking" ]; then
    fail "$range adds $(echo "$added_breaking" | tr '\n' ' ')but no commit carries a 'Breaking-Change: <what breaks>' trailer; add it to the commit that makes the change"
  fi
fi

if [ "$failed" -ne 0 ]; then exit 1; fi
if [ $# -ge 1 ]; then echo "changelog checks passed for $base..${2:-HEAD}"; else echo "changelog fragments are well formed"; fi
