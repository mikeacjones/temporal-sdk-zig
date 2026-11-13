#!/usr/bin/env bash
# Drafts a CHANGELOG.md section from the work merged since <since-ref>: one line per
# first-parent "Merge bead/<id>: <summary>" commit, oldest first, as "- <summary> (<id>)".
# Other first-parent commits (direct fixes, index regenerations) are left out; check them in
# `git log --first-parent <since-ref>..HEAD`. The draft goes to stdout for the maintainer to
# sort into Added, Changed, Fixed and so on; the script never edits CHANGELOG.md.
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

entries="$(git log --first-parent --reverse --format=%s "$since..HEAD" |
  sed -nE 's/^Merge bead\/([^:]+): (.+)$/- \2 (\1)/p')"
echo "## [Unreleased]"
echo
if [ -n "$entries" ]; then
  echo "$entries"
else
  echo "(no bead merges since $since)"
fi
