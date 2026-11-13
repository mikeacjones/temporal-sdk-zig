#!/usr/bin/env bash
# Cross-checks the changelog fragments against the history: lists each commit on main's
# first-parent history since <since-ref> that added no fragment under changelog/<category>/
# and carries no "Changelog: none" trailer. For a merge, the fragment may come from any of
# its branch's commits and the trailer may be on the merge or on any branch commit. Commits
# from before changelog/ existed are left out: the fragments that introduced it cover them.
# A "Prepare release <version>" commit is left out too.
#
# Read each listed commit and add the fragment it is missing before running
# scripts/changelog-assemble.sh. The script changes nothing, and it exits 0 whatever it
# lists.
#
# Usage: scripts/changelog-draft.sh <since-ref>
#   <since-ref>: the previous release tag (v0.1.0-alpha.1), or any commit
set -euo pipefail
if [ $# -ne 1 ]; then
  echo "usage: $0 <since-ref>" >&2
  exit 2
fi
since="$1"
if ! git rev-parse --verify --quiet "$since^{commit}" >/dev/null; then
  echo "$since is not a commit or tag in this repository; pass the previous release tag, such as v0.1.0-alpha.1" >&2
  exit 2
fi

# The first commit that added changelog/README.md; earlier ones predate fragments.
start="$(git log --first-parent --diff-filter=A --format=%H -- changelog/README.md | tail -1)"

missing=0
total=0
while IFS= read -r commit; do
  if [ -n "$start" ] && [ "$commit" != "$start" ] && ! git merge-base --is-ancestor "$start" "$commit"; then
    continue
  fi
  subject="$(git log -1 --format=%s "$commit")"
  [[ "$subject" =~ ^Prepare\ release\  ]] && continue
  total=$((total + 1))
  if git rev-parse --verify --quiet "$commit^1" >/dev/null; then
    parent="$commit^1"
    fragments="$(git diff --no-renames --diff-filter=A --name-only "$parent" "$commit" -- changelog | grep -v '^changelog/README\.md$' || true)"
    trailers="$(git log --format='%(trailers:key=Changelog,valueonly)' "$parent..$commit" | grep -E '^none' || true)"
  else
    fragments="$(git show --no-renames --diff-filter=A --name-only --format= "$commit" -- changelog | grep -v '^changelog/README\.md$' || true)"
    trailers="$(git log -1 --format='%(trailers:key=Changelog,valueonly)' "$commit" | grep -E '^none' || true)"
  fi
  if [ -z "$fragments" ] && [ -z "$trailers" ]; then
    echo "- $(git log -1 --format=%h "$commit") $subject"
    missing=$((missing + 1))
  fi
done < <(git rev-list --first-parent --reverse "$since..HEAD")

if [ "$missing" -eq 0 ]; then
  echo "every one of the $total commits since $since has a changelog fragment or a 'Changelog: none' trailer"
else
  echo "$missing of the $total commits since $since have neither a changelog fragment nor a 'Changelog: none' trailer" >&2
fi
