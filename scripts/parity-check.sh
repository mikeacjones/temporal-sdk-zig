#!/bin/sh
# Checks PARITY.md's feature table, the rows between "## Feature table" and the next "## "
# heading. Each row is `| Feature | SDKs | Stage | Zig | Where |`, and the check fails on:
#
# - a stage other than `GA`, `GA, deferred`, or `pre-release`, or a Zig status other than
#   `supported`, `partial`, `missing`, or `not applicable`;
# - `GA, deferred` (a GA feature the owner deferred past the first release) on a row that
#   isn't partial or missing, or that names no bead ID (`unfiled` is not enough);
# - an SDKs cell that isn't "all" or a list of two or more official SDKs;
# - a supported row with no link to README.md (`](README.md#anchor)`) or to a sample
#   (`](samples/<name>)` or a file in one), or with a README.md anchor no heading has
#   (anchors are GitHub's: scripts/github-anchors.awk), an anchor of a stub section, or a
#   sample path that isn't in a sample directory (one with a sample.zon);
# - a README.md heading whose anchor it can't be sure of;
# - a partial or missing row with neither a bead ID (`temporal-sdk-zig-...`) nor `unfiled`,
#   or a partial row that doesn't say what is missing (`Missing:`);
# - a not-applicable row that gives no reason.
#
# It then prints the GA rows that are partial or missing: what the first release still needs,
# and, under their own heading, the deferred ones.
# Exits 1 when any row fails, 0 otherwise.
#
# Usage: scripts/parity-check.sh [<repository root>]
set -eu
root="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
parity="$root/PARITY.md"
readme="$root/README.md"
for f in "$parity" "$readme"; do
  [ -f "$f" ] || { echo "parity-check: $f not found" >&2; exit 1; }
done

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# README.md's heading anchors as GitHub makes them, with whether each section is a stub
# (scripts/github-anchors.awk has the rules). A construct it doesn't read fails the check,
# after the rows are checked against the anchors it is sure of.
LC_ALL=C awk -f "$(dirname "$0")/github-anchors.awk" "$readme" > "$tmp/headings"
unsure="$(grep '^!' "$tmp/headings" || true)"
if [ -n "$unsure" ]; then
  printf '%s\n' "$unsure" | while IFS="$(printf '\t')" read -r _ n reason; do
    echo "README.md:$n: parity-check can't be sure which headings and anchors GitHub makes here ($reason); write README.md more simply (plain top-level # headings, fences and comments at column 0)" >&2
  done
fi
grep -v '^!' "$tmp/headings" | cut -f1 > "$tmp/anchors" || true
awk -F '\t' '$1 != "!" && $2 == "stub" { print $1 }' "$tmp/headings" > "$tmp/stubs"

# The rows of the tables under "## Feature table", fields separated by the unit separator (a
# tab would merge empty cells): line, column count, feature, SDKs, stage, status, where. As on
# GitHub, a table starts at a header row followed by a delimiter row, a row may be indented up
# to three spaces and may leave out its outer pipes, `\|` is a pipe inside a cell, and the
# table runs to the next blank line or heading: every other line in it is a row. A line that
# would start another block inside a table, and any other line in the section holding a `|`
# (a row with no table above it, or a table in a block quote or list item), fails the check,
# so no row GitHub renders escapes it.
sep="$(printf '\037')"
awk -v sep="$sep" '
  function delimiter(l) {
    return l ~ /^ ? ? ?\|?[ \t]*:?-+:?[ \t]*(\|[ \t]*:?-+:?[ \t]*)*\|?[ \t]*$/ && l ~ /-/
  }
  { sub(/\r$/, ""); text[NR] = $0 }
  END {
    esc = "\036"
    for (i = 1; i <= NR; i++) {
      l = text[i]
      if (l ~ /^ ? ? ?## /) { in_section = (l ~ /^ ? ? ?## Feature table[ \t]*$/); in_table = 0; continue }
      if (!in_section) continue
      if (l ~ /^[ \t]*$/ || l ~ /^ ? ? ?#+([ \t]|$)/) { in_table = 0; continue }
      if (!in_table) {
        if (l ~ /\|/ && i < NR && delimiter(text[i + 1])) { in_table = 1; i++ }
        else if (l ~ /\|/) print i sep "orphan"
        continue
      }
      if (l ~ /^ ? ? ?(>|[-+*][ \t]|[0-9]+[.)][ \t]|```|~~~|<)/ || l ~ /^ ? ? ?([-*_][ \t]*)([-*_][ \t]*)([-*_][ \t]*)+$/) { print i sep "block"; in_table = 0; continue }
      row = l
      gsub(/\\\|/, esc, row)
      sub(/^[ \t]+/, "", row); sub(/[ \t]+$/, "", row)
      sub(/^\|/, "", row); sub(/\|$/, "", row)
      n = split(row, cell, "|")
      out = i sep n
      for (j = 1; j <= n; j++) {
        c = cell[j]
        gsub(esc, "|", c)
        gsub(/^[ \t]+|[ \t]+$/, "", c)
        out = out sep c
      }
      print out
    }
  }
' "$parity" > "$tmp/rows"

if [ ! -s "$tmp/rows" ]; then
  echo "PARITY.md: no rows under \"## Feature table\"" >&2
  exit 1
fi

errors=0
fail() { echo "PARITY.md:$1: $2" >&2; errors=$((errors + 1)); }

# The destinations of the Markdown inline links in a cell, one per line, as GitHub reads
# them: not inside code spans, and only with a destination GitHub accepts (no spaces unless
# in <...>, and at most a quoted title after it).
link_targets() {
  awk '
    {
      s = $0
      # Code spans hold no links.
      while (match(s, /`+/)) {
        run = substr(s, RSTART, RLENGTH); before = substr(s, 1, RSTART - 1); rest = substr(s, RSTART + RLENGTH)
        k = index(rest, run)
        if (k == 0) { s = before rest; continue }
        s = before substr(rest, k + RLENGTH)
      }
      gsub(/\\[][()]/, "", s)
      # Every `](` must belong to a link read below; any other is reported as "!".
      t = s; total = gsub(/\]\(/, "", t); seen = 0
      while (match(s, /\[[^][]*\]\(/)) {
        rest = substr(s, RSTART + RLENGTH); s = rest; seen++
        if (match(rest, /^ *<[^<>]*> *("[^"]*"|\047[^\047]*\047)? *\)/)) { d = rest; sub(/^ *</, "", d); sub(/>.*/, "", d) }
        else if (match(rest, /^ *[^ ()<>]+ *("[^"]*"|\047[^\047]*\047)? *\)/)) { d = rest; sub(/^ */, "", d); sub(/[ )].*/, "", d) }
        else { print "!"; continue }
        print d
      }
      if (seen < total) print "!"
    }'
}

# The official SDKs, as PARITY.md's "How parity is measured" section links them (their
# repositories are github.com/temporalio/sdk-*), one name per line. This script names none.
awk '
  /^## / { in_section = ($0 ~ /^## How parity is measured/); next }
  in_section {
    s = $0
    while (match(s, /\[[^]]+\]\(https:\/\/github\.com\/temporalio\/sdk-[a-z]+\)/)) {
      name = substr(s, RSTART + 1); sub(/\].*/, "", name); print name
      s = substr(s, RSTART + RLENGTH)
    }
  }' "$parity" | sort -u > "$tmp/sdks"
if [ ! -s "$tmp/sdks" ]; then
  echo "PARITY.md: the \"How parity is measured\" section links no SDK repository (https://github.com/temporalio/sdk-...)" >&2
  exit 1
fi
known="$(paste -sd, "$tmp/sdks" | sed 's/,/, /g')"

# How many official SDKs an SDKs cell names (all of them for "all"), or "bad".
sdk_count() {
  [ "$1" = all ] && { wc -l < "$tmp/sdks" | tr -d ' '; return; }
  printf '%s\n' "$1" | awk -F ', *' -v list="$tmp/sdks" '
    BEGIN { while ((getline n < list) > 0) ok[n] = 1 }
    { for (i = 1; i <= NF; i++) { if (!($i in ok) || seen[$i]++) { print "bad"; exit } } print NF }'
}
ga_gaps=""
deferred=""
bead_re='temporal-sdk-zig-[a-z0-9]+(\.[0-9]+)*'

while IFS="$sep" read -r line columns feature sdks stage status where; do
  case "$columns" in
    block) fail "$line" "this line starts another block inside a table, which ends the table on GitHub; put a blank line before it"; continue ;;
    orphan) fail "$line" "a | outside a top-level table: a table row with no header and delimiter row above it, or a table in a block quote or list item, which this check doesn't read; put feature rows in a table at column 0 and keep | out of the section's prose"; continue ;;
  esac
  # A comment hides what it holds, and an image is not a link to a section or sample.
  where="$(printf '%s\n' "$where" | sed -e 's/<!--.*-->//g' -e 's/<!--.*//')"
  if printf '%s\n' "$where" | grep -q '!\['; then
    fail "$line" "\"$feature\": an image in a row; link the README.md section or sample instead"
    continue
  fi
  if [ "$columns" != 5 ]; then
    fail "$line" "a row needs 5 columns (Feature, SDKs, Stage, Zig, Where), not $columns"
    continue
  fi
  [ -n "$feature" ] || fail "$line" "a row needs a feature name"
  case "$(sdk_count "$sdks")" in
    bad) fail "$line" "\"$feature\": SDKs are \"all\" or a comma-separated list of $known, not \"$sdks\"" ;;
    0 | 1) fail "$line" "\"$feature\": a row is for a feature more than one SDK offers; list one-SDK features under \"Features only one SDK offers\"" ;;
  esac
  case "$stage" in
    GA | pre-release) ;;
    "GA, deferred")
      case "$status" in
        partial | missing)
          printf '%s\n' "$where" | grep -qE "$bead_re" ||
            fail "$line" "\"$feature\": a deferred row names the bead that still tracks it (\"unfiled\" is not enough)"
          ;;
        *) fail "$line" "\"$feature\": only a partial or missing GA row can be deferred, not a $status one; its stage is \"GA\"" ;;
      esac
      ;;
    *) fail "$line" "\"$feature\": unknown stage \"$stage\" (GA, GA, deferred, or pre-release)" ;;
  esac
  case "$status" in
    supported)
      targets="$(printf '%s\n' "$where" | link_targets)"
      if printf '%s\n' "$targets" | grep -qx '!'; then
        fail "$line" "\"$feature\": a link this check can't read; write each as [text](README.md#anchor) or [text](samples/name)"
      fi
      links="$(printf '%s\n' "$targets" | grep -E '^(README\.md#|samples/)' || true)"
      if [ -z "$links" ]; then
        fail "$line" "\"$feature\": a supported row links a README.md section or a sample"
      fi
      # One target per line, read whole: a destination may hold spaces, and is never a pattern.
      while IFS= read -r target; do
        [ -n "$target" ] || continue
        case "$target" in
          README.md#*)
            anchor="${target#README.md#}"
            if [ -z "$anchor" ] || ! grep -qxF -- "$anchor" "$tmp/anchors"; then
              fail "$line" "\"$feature\": README.md has no heading for #$anchor"
            elif grep -qxF -- "$anchor" "$tmp/stubs"; then
              fail "$line" "\"$feature\": README.md#$anchor is a stub, not a section that shows the feature"
            fi
            ;;
          samples/*)
            path="${target%/}"
            name="${path#samples/}"
            name="${name%%/*}"
            case "/$path/" in
              */../* | */./* | //* | /samples/) fail "$line" "\"$feature\": $target is not a sample" ;;
              *)
                if [ ! -f "$root/samples/$name/sample.zon" ]; then
                  fail "$line" "\"$feature\": no sample directory samples/$name (a directory with a sample.zon)"
                elif [ ! -e "$root/$path" ]; then
                  fail "$line" "\"$feature\": no sample directory or file $path"
                fi
                ;;
            esac
            ;;
        esac
      done <<LINKS
$links
LINKS
      ;;
    partial | missing)
      if ! printf '%s\n' "$where" | grep -qE "$bead_re"'|(^|[^a-z])unfiled([^a-z]|$)'; then
        fail "$line" "\"$feature\": a $status row names its bead ID or \"unfiled\""
      fi
      if [ "$status" = partial ] && ! printf '%s\n' "$where" | grep -q 'Missing:'; then
        fail "$line" "\"$feature\": a partial row says what is missing (\"Missing: ...\")"
      fi
      gap="$(printf '  %-8s %s (%s)' "$status" "$feature" "$(printf '%s\n' "$where" | grep -oE "$bead_re"'|unfiled' | tr '\n' ' ' | sed 's/ $//; s/ /, /g')")"
      case "$stage" in
        GA) ga_gaps="$ga_gaps$gap
" ;;
        "GA, deferred") deferred="$deferred$gap
" ;;
      esac
      ;;
    "not applicable")
      [ -n "$where" ] || fail "$line" "\"$feature\": a not-applicable row gives the reason"
      ;;
    *) fail "$line" "\"$feature\": unknown Zig status \"$status\" (supported, partial, missing, or not applicable)" ;;
  esac
done < "$tmp/rows"

if [ -n "$unsure" ]; then
  errors=$((errors + $(printf '%s\n' "$unsure" | wc -l)))
fi
if [ "$errors" -gt 0 ]; then
  echo "parity-check: $errors problem(s) in PARITY.md's feature table and the README.md headings it links" >&2
  exit 1
fi

rows="$(wc -l < "$tmp/rows" | tr -d ' ')"
if [ -n "$ga_gaps" ]; then
  echo "parity-check: $rows rows checked. GA features the first release still needs:"
  printf '%s' "$ga_gaps"
elif [ -n "$deferred" ]; then
  echo "parity-check: $rows rows checked. Every GA feature the first release needs is supported or not applicable."
else
  echo "parity-check: $rows rows checked. Every GA feature is supported or not applicable."
fi
if [ -n "$deferred" ]; then
  echo "GA features deferred past the first release:"
  printf '%s' "$deferred"
fi
