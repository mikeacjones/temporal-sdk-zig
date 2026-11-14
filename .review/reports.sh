#!/usr/bin/env bash
# Writes deterministic reports about a change, for the people and agents who review it.
# Every file it writes comes from a tool, not from a model: a reviewer reads them before
# reading the code. A scan hit is a line to look at, not a finding.
#
# The change is everything in the working tree that differs from the merge base with
# <base>: committed, uncommitted, and untracked files alike. Nothing is staged or committed.
#
# Reports, under <out> (default .review/out/<branch>/, which git ignores):
#   INDEX.txt            one line per check, and the tool versions; read this first
#   diff.patch           the change; changed-files.txt and added-lines.txt index it
#   fmt, ast-check       zig build fmt, and zig ast-check on each changed .zig file
#   zlint                the Zig linter on the changed .zig files
#   scan-*.txt           added lines that match a hazard pattern (panics, casts, clocks and
#                        threads, fixed sleeps, leftover debugging, other SDKs' names, log calls)
#   public-api-diff.txt  pub declarations added or removed under src/
#   dependencies.txt     build, cargo, submodule, and generated files that changed
#   gitleaks             secrets in the change (with --all: in the whole git history)
#   osv-scanner          known vulnerabilities in deps/Cargo.lock
#   cargo-deny           advisories and crate sources (with --all, or when deps/ changed)
#   actionlint, shellcheck, typos
#                        the workflows, the changed shell scripts, and spelling in changed files
#   docs.txt, readme-check, changelog-check
#                        README stubs, changelog fragments, and their checks
#   test-debug, test-releasesafe
#                        zig build test in both modes (skipped with --quick)
#
# The scanners are installed by .review/install.sh, and their settings are the .toml files
# beside this script. A scanner that isn't installed is reported as missing, not skipped
# silently. osv-scanner and cargo-deny need the network.
#
# Exits 0 when every check that can fail passed, 1 when one failed (INDEX.txt says which),
# and 2 on a usage error.
#
# Usage: .review/reports.sh [--base <ref>] [--out <dir>] [--quick] [--all]
#   --base <ref>  what the change is compared with (default origin/main)
#   --out <dir>   where the reports go
#   --quick       skip the test suites
#   --all         scan every tracked file and the whole history, not only the change
set -u

usage() { sed -n '2,/^set -u/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; }

base=origin/main
out=
quick=0
all=0
while [ $# -gt 0 ]; do
  case "$1" in
    --base) [ $# -ge 2 ] || { echo "reports.sh: --base needs a ref" >&2; exit 2; }; base=$2; shift 2 ;;
    --out) [ $# -ge 2 ] || { echo "reports.sh: --out needs a directory" >&2; exit 2; }; out=$2; shift 2 ;;
    --quick) quick=1; shift ;;
    --all) all=1; shift ;;
    -h | --help) usage; exit 0 ;;
    *) echo "reports.sh: unknown argument '$1' (see --help)" >&2; exit 2 ;;
  esac
done

root=$(git rev-parse --show-toplevel) || exit 2
cd "$root" || exit 2
here=.review
merge_base=$(git merge-base "$base" HEAD 2>/dev/null) || {
  echo "reports.sh: --base '$base' has no merge base with HEAD; fetch it or pass another ref" >&2
  exit 2
}
[ -n "$out" ] || out="$here/out/$(git rev-parse --abbrev-ref HEAD | tr '/' '-')"
mkdir -p "$out" || exit 2
rm -f "$out"/*.txt "$out"/*.log "$out"/*.json "$out"/*.patch
out=$(cd "$out" && pwd)

failed=""
summary="$out/.summary"
: >"$summary"
note() { printf '%-18s %s\n' "$1" "$2" >>"$summary"; }
fail() { failed="$failed $1"; }

# Runs a command into <name>.log. As a "gate" its exit status is recorded and a nonzero one
# fails the run; as a "report" the caller records what the log holds.
run() {
  local name=$1 kind=$2
  shift 2
  { echo "\$ $*"; "$@"; } >"$out/$name.log" 2>&1
  local status=$?
  [ "$kind" = gate ] || return 0
  note "$name" "exit $status"
  [ "$status" -eq 0 ] || fail "$name"
}

have() {
  command -v "$1" >/dev/null 2>&1 && return 0
  note "$2" "NOT INSTALLED: run $here/install.sh"
  return 1
}

# --- the change ---------------------------------------------------------------------------
untracked=$(git ls-files --others --exclude-standard)
{
  git diff "$merge_base"
  printf '%s\n' "$untracked" | while IFS= read -r f; do
    [ -n "$f" ] && git diff --no-index -- /dev/null "$f"
  done
} >"$out/diff.patch"
{
  git diff --name-only "$merge_base"
  printf '%s\n' "$untracked"
} | sed '/^$/d' | sort -u >"$out/changed-files.txt"
git diff --stat "$merge_base" >"$out/diff-stat.txt"
# Changed files that still exist, one list per kind.
existing() { while IFS= read -r f; do [ -f "$f" ] && printf '%s\n' "$f"; done <"$out/changed-files.txt"; }
existing >"$out/.existing"
if [ "$all" -eq 1 ]; then git ls-files >"$out/.scope"; else cp "$out/.existing" "$out/.scope"; fi
zig_files() { grep '\.zig$' "$1" | grep -v -e '^src/proto/' -e '^src/proto\.zig$' -e '^deps/sdk-core/'; }

added="$out/added-lines.txt"
awk '
  /^\+\+\+ /{ file = ($2 == "/dev/null") ? "" : substr($2, 3); next }
  /^@@/{ split($3, a, ","); line = substr(a[1], 2) + 0; next }
  /^\+/{ if (file != "") print file ":" line ": " substr($0, 2); line++ }
' "$out/diff.patch" >"$added"
note change "$(wc -l <"$out/changed-files.txt" | tr -d ' ') files, $(wc -l <"$added" | tr -d ' ') added lines"

# --- Zig ----------------------------------------------------------------------------------
run fmt gate zig build fmt
ast_failed=0
: >"$out/ast-check.log"
while IFS= read -r f; do
  [ -n "$f" ] || continue
  { echo "== $f"; zig ast-check "$f"; } >>"$out/ast-check.log" 2>&1 || ast_failed=$((ast_failed + 1))
done <<EOF
$(zig_files "$out/.existing")
EOF
note ast-check "$ast_failed file(s) failed"
[ "$ast_failed" -eq 0 ] || fail ast-check

zlint=${ZLINT:-$HOME/.local/share/zig-zlint/zlint}
if [ -x "$zlint" ]; then
  zig_files "$out/.scope" | "$zlint" -S -f json >"$out/zlint.json" 2>"$out/zlint.log"
  python3 - "$out/zlint.json" >"$out/zlint.txt" <<'EOF'
import collections, json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.lstrip().startswith("{")]
by_rule = collections.Counter((r.get("level"), r.get("code")) for r in rows)
print("# zlint diagnostics by rule")
for (level, code), n in by_rule.most_common():
    print(f"{n:5d}  {level:5s} {code}")
print()
for r in rows:
    at = r["labels"][0]["start"] if r.get("labels") else {"line": 0, "column": 0}
    print(f'{r["source_name"]}:{at["line"]}:{at["column"]}: {r.get("level")}[{r.get("code")}] {r.get("message")}')
EOF
  note zlint "$(grep -c ': error\[' "$out/zlint.txt") error(s), $(grep -c ': warn\[' "$out/zlint.txt") warning(s)"
else
  note zlint "NOT INSTALLED: run $here/install.sh"
fi

# --- hazard patterns in added lines -------------------------------------------------------
scan() { # name, what it is, regex, path regex
  local name=$1 what=$2 re=$3 paths=${4:-.}
  {
    echo "# $what"
    echo "# regex: $re   paths: $paths"
    grep -E "^[^:]*($paths)[^:]*:[0-9]+: " "$added" | grep -E -e "$re" || echo "(no matches)"
  } >"$out/scan-$name.txt"
  note "scan-$name" "$(grep -vc -e '^#' -e '^(no matches)$' "$out/scan-$name.txt") line(s)"
}
scan panics "Added lines that can panic or assume an invariant about outside input" '@panic|unreachable|std\.debug\.assert|\.\?[^a-zA-Z]' 'src/'
scan casts "Added casts and undefined: check each for values from the server or the user" '@intCast|@ptrCast|@alignCast|@truncate|@bitCast|@fromBackingInt|@enumFromInt|= undefined' 'src/'
scan determinism "Added threads, wall clock, randomness, or direct I/O (wrong if workflow code reaches it)" 'std\.Thread|std\.time\.|Timestamp\.now|std\.crypto\.random|std\.Random|std\.fs\.|std\.posix\.' 'src/'
scan sleeps "Sleeps in added test, example, or sample code: each must wait on a condition" 'sleep\(' 'src/testing|_test\.zig|examples/|samples/'
scan debug "Leftover debugging and markers" 'std\.debug\.print|TODO|FIXME|XXX|HACK' '.'
scan other-sdks "Another SDK named outside PARITY.md" '(Go|Java|Python|TypeScript|\.NET|Ruby|PHP) SDK|samples-go|Golang' 'README|src/|examples/|samples/|docs/|changelog/'
scan logging "Added log calls: actionable, and no payload contents" 'log\.(err|warn|info|debug)\(' 'src/'

# --- public API, dependencies, docs -------------------------------------------------------
{
  echo "# pub declarations added (+) or removed (-) under src/ (generated code excluded)"
  awk '
    /^(\+\+\+|---) /{ if ($2 != "/dev/null") file = substr($2, 3); next }
    /^[+-] *pub (fn|const|var) /{ if (file ~ /^src\// && file !~ /^src\/proto/) print substr($0, 1, 1) " " file ": " substr($0, 2) }
  ' "$out/diff.patch" | sed 's/  */ /g' | grep . || echo "(none)"
} >"$out/public-api-diff.txt"
note public-api-diff "$(grep -c '^[+-] ' "$out/public-api-diff.txt") declaration(s)"

deps_changed=0
{
  echo "# Build, dependency, and generated files (a change here needs its own justification)"
  for p in build.zig build.zig.zon CORE_VERSION deps/Cargo.toml deps/Cargo.lock deps/rust-toolchain.toml deps/sdk-core deps/protos.zig src/proto src/proto.zig src/bridge_abi.txt samples/dispatch.zig .github .review; do
    if grep -q -e "^$p\$" -e "^$p/" "$out/changed-files.txt"; then
      echo "CHANGED    $p"
      case "$p" in deps/*) deps_changed=1 ;; esac
    else
      echo "unchanged  $p"
    fi
  done
  echo
  echo "# sdk-core pin"
  git submodule status deps/sdk-core
  echo
  echo "# @import, @cImport, extern, and callconv in added lines"
  grep -E '@import\(|@cImport|\bextern\b|callconv' "$added" || echo "(none)"
} >"$out/dependencies.txt"
note dependencies "$(grep -c '^CHANGED' "$out/dependencies.txt") path(s) changed"

{
  echo "# README.md stubs (a landed capability removes its stub)"
  grep -n 'stub:' README.md || echo "(none)"
  echo
  echo "# changelog fragments in the change"
  grep '^changelog/' "$out/changed-files.txt" || echo "(none)"
} >"$out/docs.txt"
run readme-check gate zig build readme-check
run changelog-check gate scripts/changelog-check.sh "$base"

# --- scanners -----------------------------------------------------------------------------
if have gitleaks gitleaks; then
  if [ "$all" -eq 1 ]; then
    run gitleaks gate gitleaks git --no-banner --redact --config "$here/gitleaks.toml" --report-format json --report-path "$out/gitleaks.json" .
  else
    run gitleaks gate gitleaks stdin --no-banner --redact --config "$here/gitleaks.toml" --report-format json --report-path "$out/gitleaks.json" <"$out/diff.patch"
    # A finding read from stdin names a line of diff.patch: say which file that line is in.
    python3 - "$out/gitleaks.json" "$out/diff.patch" >"$out/gitleaks.txt" <<'EOF'
import json, sys
patch = open(sys.argv[2], errors="replace").read().split("\n")
for f in json.load(open(sys.argv[1])):
    line = f["StartLine"]
    file = next((l[6:] for l in reversed(patch[:line]) if l.startswith("+++ b/")), "?")
    print(f'{file}: {f["RuleID"]} (diff.patch line {line}): {f["Description"]}')
EOF
  fi
fi

if have osv-scanner osv-scanner; then
  run osv-scanner gate osv-scanner scan source --lockfile deps/Cargo.lock
fi

if [ "$all" -eq 1 ] || [ "$deps_changed" -eq 1 ]; then
  if have cargo-deny cargo-deny; then
    run cargo-deny gate cargo deny --manifest-path deps/Cargo.toml --config "$here/deny.toml" check advisories sources
  fi
else
  note cargo-deny "skipped: deps/ unchanged (--all runs it)"
fi

if have shellcheck shellcheck; then
  scripts=$(grep '\.sh$' "$out/.scope")
  if [ -n "$scripts" ]; then
    # SC2016 flags a $ inside single quotes, which is how these scripts pass awk and sed programs.
    # shellcheck disable=SC2086
    run shellcheck report shellcheck -f gcc -e SC2016 $scripts
    note shellcheck "$(grep -c ': \(note\|warning\|error\): ' "$out/shellcheck.log") note(s)"
  else
    note shellcheck "no shell scripts in the change"
  fi
fi

if have actionlint actionlint; then
  run actionlint report actionlint
  note actionlint "$(grep -c '^\.github/' "$out/actionlint.log") note(s)"
fi

if have typos typos; then
  if [ -s "$out/.scope" ]; then
    tr '\n' '\0' <"$out/.scope" | xargs -0 typos --config "$here/typos.toml" --force-exclude --format brief >"$out/typos.log" 2>&1
    note typos "$(grep -c 'error: ' "$out/typos.log") word(s)"
  else
    note typos "no files in the change"
  fi
fi

# --- test suites --------------------------------------------------------------------------
if [ "$quick" -eq 1 ]; then
  note tests "skipped (--quick)"
else
  run test-debug gate zig build test --summary all
  run test-releasesafe gate zig build test -Doptimize=ReleaseSafe --summary all
  for mode in debug releasesafe; do
    note "test-$mode" "$(grep 'Build Summary' "$out/test-$mode.log" | tail -1)"
  done
fi

# --- index --------------------------------------------------------------------------------
version() { command -v "$1" >/dev/null 2>&1 && "$@" 2>&1 | head -1 || echo "not installed"; }
{
  echo "Review reports for $(git rev-parse --abbrev-ref HEAD) at $(git rev-parse --short HEAD)$(git diff --quiet HEAD || echo ' plus uncommitted changes')"
  echo "against $base (merge base $(git rev-parse --short "$merge_base")), written $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  [ "$all" -eq 1 ] && echo "Scope: every tracked file and the whole history (--all)"
  echo "Every file here was written by a tool. A scan hit is a line to look at, not a finding."
  echo
  cat "$summary"
  echo
  if [ -n "$failed" ]; then echo "FAILED:$failed"; else echo "No check failed."; fi
  echo
  echo "Tools:"
  printf '  %-12s %s\n' zig "$(version zig version)" zlint "$([ -x "$zlint" ] && "$zlint" --version 2>&1 | head -1 || echo 'not installed')" \
    gitleaks "$(version gitleaks version)" osv-scanner "$(version osv-scanner --version)" cargo-deny "$(version cargo-deny --version)" \
    shellcheck "$(command -v shellcheck >/dev/null 2>&1 && shellcheck --version | sed -n 2p || echo 'not installed')" \
    actionlint "$(version actionlint -version)" typos "$(version typos --version)"
} >"$out/INDEX.txt"
rm -f "$summary" "$out/.existing" "$out/.scope"
cat "$out/INDEX.txt"
echo
echo "Reports are in $out"
[ -z "$failed" ]
